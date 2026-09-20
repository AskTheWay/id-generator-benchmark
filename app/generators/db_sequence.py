"""PostgreSQL Sequence 方案(数据库类)。

每次发号执行一次 SELECT nextval('idbench_seq') 的数据库往返:
- 数据库保证强一致与全局单调递增, 但吞吐受网络往返(RTT)与连接数约束
- 使用 LifoQueue 自实现的轻量连接池(见 _LifoQueuePool 类注释)复用连接,
  惰性初始化(首次使用时才建池建序列)
- ID 格式: "DB" + 10 位零填充序号, 如 DB0000000123
"""

import queue
import socket
import threading
from concurrent.futures import ThreadPoolExecutor

try:
    import psycopg2
    import psycopg2.pool
except ImportError:  # 未安装驱动时也允许导入本模块(注册表可加载, check_ready 返回 False)
    psycopg2 = None

from app.config import PG_DATABASE, PG_HOST, PG_PASSWORD, PG_PORT, PG_USER
from app.generators.base import BaseIDGenerator, GeneratorMeta


class _LifoQueuePool:
    """基于 LifoQueue 的极简线程安全连接池。

    为什么不用 psycopg2.pool.ThreadedConnectionPool: 它的归还逻辑是
    "仅当池内空闲连接数 < minconn 时才收回, 否则立即 conn.close()"
    (见 psycopg2/pool.py 的 _putconn)——超出 minconn 的空闲连接从不缓存。
    压测场景需要的是"预热建满工作集、全程复用、空闲不回收", 这正是
    队列语义: 借出 pop / 归还 push。LIFO(而非 FIFO)让最近用过的连接
    优先被复用, 服务端进程缓存保持热度。
    """

    def __init__(self, connect_kwargs: dict):
        self._connect_kwargs = connect_kwargs
        self._conns: queue.LifoQueue = queue.LifoQueue()

    def getconn(self):
        """取一条空闲连接; 池空时新建(并发风暴期各线程自行建连, 无需加锁)"""
        try:
            return self._conns.get_nowait()
        except queue.Empty:
            return psycopg2.connect(**self._connect_kwargs)

    def putconn(self, conn, close: bool = False) -> None:
        """归还连接; close=True 或连接已死时关闭丢弃, 绝不放回坏连接"""
        if close or conn is None:
            try:
                conn.close()
            except Exception:
                pass
            return
        if conn.closed:
            return
        try:
            self._conns.put_nowait(conn)
        except Exception:  # 队列异常兜底: 宁可关连接也不让归还失败炸调用方
            try:
                conn.close()
            except Exception:
                pass

    def closeall(self) -> None:
        """排空并关闭全部空闲连接"""
        while True:
            try:
                conn = self._conns.get_nowait()
            except queue.Empty:
                return
            try:
                conn.close()
            except Exception:
                pass


class DBSequenceGenerator(BaseIDGenerator):
    """基于 PostgreSQL 序列的发号器(线程安全, 依赖 PostgreSQL)"""

    SEQUENCE_NAME = "idbench_seq"  # 序列名(模块内常量, 不接受外部输入, 无注入风险)
    ID_PAD_WIDTH = 10              # 序号零填充宽度, 如 DB0000000001
    # 池的最大连接数: 必须 >= main.py 允许的压测并发上限(512)并留余量,
    # 否则压测中途动态建连的排队会污染延迟分位数(池空时各线程自行建连)。
    # 注意: 服务端也要配合调大(docker-compose 已设 max_connections=600),
    # 否则会撞上 PostgreSQL 默认 100 连接的 "too many clients" 上限
    MAX_POOL_SIZE = 600

    def __init__(self) -> None:
        meta = GeneratorMeta(
            name="db_sequence",
            display_name="PostgreSQL Sequence",
            category="database",
            description="PostgreSQL 序列 nextval: 强一致全局自增, 每次发号一次数据库往返",
            bit_layout="数据库序列, 64bit 自增",
            monotonic=True,
            dependency="PostgreSQL",
            theoretical_limit="受连接池与数据库往返限制",
        )
        super().__init__(meta)
        self._pool = None                 # 惰性创建的线程安全连接池
        self._pool_lock = threading.Lock()

    # ---------------------------------------------------------------- 连接池管理
    def _connection_kwargs(self) -> dict:
        """组装 psycopg2 连接参数(来自 app/config.py, 均可被环境变量覆盖)"""
        return dict(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DATABASE,
            connect_timeout=2,  # 建连超时 2s: 数据库不可用时避免调用方长时间挂起
        )

    def _get_pool(self):
        """惰性初始化连接池(双重检查锁, 全进程只建一次), 并幂等建好序列"""
        if self._pool is None:
            with self._pool_lock:
                if self._pool is None:
                    if psycopg2 is None:
                        raise RuntimeError("psycopg2 未安装, 无法使用 db_sequence 方案")
                    pool = _LifoQueuePool(self._connection_kwargs())
                    try:
                        self._ensure_sequence(pool)
                    except Exception:
                        pool.closeall()  # 初始化失败要回收连接, 下次调用重新建池
                        raise
                    self._pool = pool
        return self._pool

    def _ensure_sequence(self, pool) -> None:
        """幂等创建序列(首次建池时执行一次; 已存在则自动跳过)"""
        conn = pool.getconn()
        try:
            with conn.cursor() as cur:
                try:
                    cur.execute(f"CREATE SEQUENCE IF NOT EXISTS {self.SEQUENCE_NAME} START 1")
                except psycopg2.errors.DuplicateObject:
                    # 多进程同时冷启动(uvicorn --workers)的残余竞态:
                    # PG 的 "IF NOT EXISTS 检查" 与 "创建" 不是原子的, 两个进程
                    # 可同时通过检查, 后到者以 duplicate_object 报错 ——
                    # 序列已被对方建好, 回滚后直接复用即可
                    conn.rollback()
            conn.commit()
        finally:
            pool.putconn(conn)

    # ---------------------------------------------------------------- 发号
    @staticmethod
    def _fetch_nextval(conn) -> int:
        """在给定连接上取序列下一个值(nextval 非事务, 提交即可)"""
        with conn.cursor() as cur:
            cur.execute("SELECT nextval(%s)", (DBSequenceGenerator.SEQUENCE_NAME,))
            (value,) = cur.fetchone()
        conn.commit()
        return int(value)

    def generate(self) -> str:
        pool = self._get_pool()
        last_exc: Exception | None = None
        # 丢弃式重试(最多 3 条连接): 服务端重启/网络抖动后, 池里可能积压了
        # 多条已死连接, 客户端不主动探测是感知不到的 —— 每次失败把这条
        # 连接丢弃(close), 换下一条再试, 而不是只重试一次就向上抛 500
        for _ in range(3):
            conn = None
            try:
                conn = pool.getconn()
                value = self._fetch_nextval(conn)
                return f"DB{value:0{self.ID_PAD_WIDTH}d}"
            except psycopg2.Error as exc:
                last_exc = exc
                if conn is not None:
                    # 坏连接(含建连后即失败)直接关闭丢弃, 不再放回池里
                    pool.putconn(conn, close=True)
                    conn = None
                # 建连本身失败(conn 为 None)或本条连接用坏: 继续下一轮换连接
            finally:
                if conn is not None:
                    pool.putconn(conn)
        raise last_exc

    # ---------------------------------------------------------------- 生命周期
    def check_ready(self) -> bool:
        """依赖可用性检测: 原始 socket 探测 TCP(0.5s 超时) + SELECT 1, 任何异常返回 False"""
        if psycopg2 is None:
            return False
        try:
            # 第一层: 原始 TCP 探测, 把建连阶段硬性限制在 0.5s 内
            with socket.create_connection((PG_HOST, PG_PORT), timeout=0.5):
                pass
        except OSError:
            return False
        try:
            # 第二层: 完整建连并执行 SELECT 1; statement_timeout=500ms 兜底查询阶段
            conn = psycopg2.connect(
                **self._connection_kwargs(),
                options="-c statement_timeout=500",
            )
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            finally:
                conn.close()
            return True
        except Exception:
            return False

    def warmup(self, n: int = 64) -> None:
        """压测前预热: 先建池, 再用屏障(threading.Barrier)强制 n 线程"同时"
        getconn 发号, 把连接池真正扩张到工作集大小, 排除压测首请求建连
        延迟对分位数的污染(裸 ex.map 下任务完成快于线程启动, 连接会被
        归还复用而建不满 —— 与 Redis 生成器同源的预热缺陷)。"""
        try:
            self._get_pool()
        except Exception:
            return  # 池都建不起来说明依赖不可用, 交给 check_ready / 压测错误数去表达
        workers = max(1, min(n, self.MAX_POOL_SIZE))  # 不超过连接池 maxconn, 避免 PoolError
        barrier = threading.Barrier(workers)

        def _one(_: int) -> None:
            try:
                # 屏障对齐: n 个线程同时发号 -> 同时占用并建立 n 条连接
                barrier.wait(timeout=15)
                self.generate()
            except Exception:
                pass  # 预热失败不抛出, 不影响后续压测流程

        with ThreadPoolExecutor(max_workers=workers) as executor:
            list(executor.map(_one, range(workers)))

    def close(self) -> None:
        """关闭整个连接池; 之后的下一次使用会重新惰性建池"""
        with self._pool_lock:
            if self._pool is not None:
                try:
                    self._pool.closeall()
                except Exception:
                    pass
                self._pool = None
