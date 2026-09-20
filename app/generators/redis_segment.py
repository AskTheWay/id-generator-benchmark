"""Redis 号段模式发号器(美团 Leaf-segment, 双 buffer)

核心思想: 向 Redis 用 INCRBY 一次性批量取一段序号(步长 SEGMENT_STEP, 如 1000 个),
之后取号在本地内存完成, 取号路径零网络 IO; 通过双 buffer(当前段 _current + 预取段
_next)消除"段耗尽 → 同步取段"的延迟毛刺。

并发语义(重要, 面试常问):
- 一把 threading.Lock 保护 _current / _next / _prefetching 三份共享状态;
- 正常取号路径在锁内只做纯内存操作(段内指针自增), 无网络 IO, 临界区极短;
- 预取线程在锁外做网络 IO(INCRBY), 只在写入 _next / 清除预取占位时短暂持锁,
  因此预取完全不阻塞取号;
- 号段耗尽时的切换(_next 整体顶替 _current)在锁内一步完成, 不存在"半个段"的中间态;
- 唯一的锁内网络 IO 是冷启动兜底(双 buffer 均空时同步取段), 属罕见降级路径,
  它短暂阻塞其他取号线程, 换取实现简单与正确性。

唯一性保证: 所有号段由同一个 Redis 计数器 INCRBY 分配, 段区间 [hi-step+1, hi]
互不相交, 无论线程如何交错都不可能发出重复号。
"""

import threading
from concurrent.futures import ThreadPoolExecutor

import redis

from app.config import REDIS_HOST, REDIS_PORT, REDIS_DB, SEGMENT_STEP
from app.generators.base import BaseIDGenerator, GeneratorMeta

# 号段计数器的 Redis key: 全局单调自增, INCRBY 一次即分配一个号段
_COUNTER_KEY = "idbench:segment:counter"


class _Segment:
    """一个号段: 闭区间 [lo, hi], next_ptr 指向下一个待发放的序号。

    非线程安全: next_ptr 只允许"已持有 RedisSegmentGenerator._lock 的线程"修改。
    """

    __slots__ = ("lo", "hi", "next_ptr")

    def __init__(self, lo: int, hi: int):
        self.lo = lo
        self.hi = hi
        self.next_ptr = lo  # 下一个待发号

    @property
    def remaining(self) -> int:
        """段内剩余可发号数"""
        return self.hi - self.next_ptr + 1

    def take(self) -> int:
        """取出一个序号(调用方必须已持锁且已确认 remaining > 0)"""
        if self.next_ptr > self.hi:
            # 防御性兜底: 防止越段发号与下一段重叠造成重复 ID, 宁可报错不可重号
            raise RuntimeError("号段已耗尽, 禁止继续取号")
        value = self.next_ptr
        self.next_ptr += 1
        return value


class RedisSegmentGenerator(BaseIDGenerator):
    """方案: Redis 号段(Leaf-segment) —— 批量取段本地发号 + 双 buffer 后台预取"""

    def __init__(self):
        super().__init__(GeneratorMeta(
            name="redis_segment",
            display_name="Redis Segment",
            category="redis",
            description="美团 Leaf-segment 号段模式: 批量取段本地发号, 双 buffer 后台预取, 取号路径零网络 IO",
            bit_layout="业务格式: SG + 10 位全局十进制序号(由 Redis 号段批量分配)",
            monotonic=True,
            dependency="Redis",
            theoretical_limit="受取号段频率限制, 单机本地发号可达百万 QPS",
        ))
        self._step = SEGMENT_STEP
        # 预取触发阈值: 当前段剩余量低于步长的一半时启动后台预取。
        # 20%(剩 200)在本工具 512 线程极限档下不够 —— 纯内存取号可在 1ms 内
        # 耗尽 200 个号, 而预取线程还要与其他线程竞争 GIL 才能完成 INCRBY,
        # 来不及在段耗尽前写入 _next, 导致频繁落入"锁内同步取段"降级路径;
        # 提高到 50% 让预取拥有约半个步长的时间窗口
        self._prefetch_threshold = max(1, self._step // 2)

        # ---- 双 buffer 共享状态(以下四个字段全部由 self._lock 保护) ----
        self._lock = threading.Lock()
        self._current: _Segment | None = None  # 当前正在发号的段
        self._next: _Segment | None = None     # 已预取好的下一段, 耗尽时原子切换
        self._prefetching = False              # 是否存在在途的后台预取线程

        # 预取失败记录(静默降级用, 仅供诊断观察, 不影响发号)
        self._prefetch_error_count = 0
        self._last_prefetch_error = ""

        # 连接池: 取段只发生在预取线程/冷启动兜底, 但容量与超时仍要与压测上限对齐:
        # 池满载时 redis-py 直接抛 ConnectionError 而不等待; socket 超时防止
        # 远端不可达时 TCP SYN 重试挂起压测(兑现 base.py "check_ready 带短超时"契约)
        self._pool = redis.ConnectionPool(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True,
            socket_connect_timeout=2,  # 建连超时
            socket_timeout=2,          # 命令读写超时
            max_connections=600,
        )
        self._client = redis.Redis(connection_pool=self._pool)

    # ---- 契约实现 ----

    def generate(self) -> str:
        with self._lock:
            # ---- 1) 取号: 锁内纯内存操作, 无网络 IO ----
            if self._current is None or self._current.remaining <= 0:
                if self._next is not None:
                    # 原子切换: 当前段耗尽, 已预取好的下一段整体顶上(锁内一步, 无中间态)
                    self._current = self._next
                    self._next = None
                else:
                    # 冷启动兜底: 双 buffer 均空(首次取号 / 预取未能及时完成),
                    # 锁内同步网络 IO 取一段; 会短暂阻塞其他取号线程, 属罕见降级路径
                    self._current = self._fetch_segment()
            seq = self._current.take()

            # ---- 2) 预取触发判定(锁内原子判定, 保证同一时刻最多一个在途预取) ----
            # 三个条件须同时满足: 当前段剩余 < 预取阈值, 且 _next 为空, 且无在途预取
            if (
                self._next is None
                and not self._prefetching
                and self._current.remaining < self._prefetch_threshold
            ):
                # 只在锁内置位 _prefetching 占位: 占位动作本身是原子的,
                # 后续任何线程重入此判定都会看到 True, 从而不会重复起预取线程
                self._prefetching = True
                start_prefetch = True
            else:
                start_prefetch = False
        # Thread.start() 放锁外执行: start() 要等新线程真正开始运行才返回,
        # 线程创建/调度开销不应落在取号临界区内; 若启动失败(线程资源耗尽等)
        # 必须复位占位, 否则 _prefetching 永久卡 True, 预取机制就此失效
        if start_prefetch:
            try:
                threading.Thread(
                    target=self._prefetch_worker,
                    daemon=True,
                    name="redis-segment-prefetch",
                ).start()
            except Exception:
                with self._lock:
                    self._prefetching = False
        # 格式化在锁外完成, 进一步缩短临界区
        # "SG" + 10 位零填充全局序号, 如 SG0000000042
        return f"SG{seq:010d}"

    def check_ready(self) -> bool:
        """依赖服务可用性检测: ping; 任何异常(连接拒绝/超时等)一律返回 False, 绝不向上抛"""
        try:
            return bool(self._client.ping())
        except Exception:
            return False

    def warmup(self, n: int = 64) -> None:
        """压测前预热, 两步缺一不可:
        (1) 同步预取 2 个号段填满双 buffer, 保证压测期间取号全程无网络 IO;
        (2) 用屏障(threading.Barrier)强制 n 线程"同时"执行 ping, 把连接池
            真正建满 n 条连接 —— 只做 (1) 的话池里只有 1~2 条连接, 压测中
            触发的后台预取/冷启动兜底需要新建连接, 会撞上 Redis 服务端的
            建连排队, 把尾延迟拉到秒级(实测 2058ms)。
        第 (2) 步用 ping 而非真实取段来建连: 不消耗号段。
        """
        # ---- (1) 填充双 buffer ----
        try:
            # 第 1 段: 保证 _current 可用(已有未耗尽的段则直接复用, 不浪费号段)
            with self._lock:
                need_current = self._current is None or self._current.remaining <= 0
            if need_current:
                seg = self._fetch_segment()  # 网络 IO 放锁外, 不阻塞取号
                with self._lock:
                    # 双重校验: 仅在仍缺当前段时写入, 避免覆盖并发切换/兜底的结果
                    if self._current is None or self._current.remaining <= 0:
                        self._current = seg
            # 第 2 段: 填充 _next, 至此双 buffer 就绪
            with self._lock:
                need_next = self._next is None
            if need_next:
                seg = self._fetch_segment()  # 网络 IO 放锁外
                with self._lock:
                    if self._next is None:
                        self._next = seg
        except Exception:
            # 预热失败不抛出: Redis 真不可用时, 让压测阶段把错误计入统计
            pass
        # ---- (2) 屏障并发建满连接池 ----
        if n <= 0:
            return
        workers = min(n, 600)  # 并发度不超过连接池容量
        barrier = threading.Barrier(workers)

        def _one(_: int) -> None:
            try:
                # 屏障对齐: n 个线程同时发命令 -> 同时占用并建立 n 条连接
                barrier.wait(timeout=10)
                self._client.ping()
            except Exception:
                pass

        try:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                list(ex.map(_one, range(workers)))
        except Exception:
            pass

    def close(self) -> None:
        """清理: 断开连接池中的全部连接"""
        try:
            self._pool.disconnect()
        except Exception:
            pass

    # ---- 内部实现 ----

    def _fetch_segment(self) -> _Segment:
        """从 Redis 取一个号段: INCRBY 原子自增步长并返回自增后的计数, 即段上界 hi,
        段区间为 [hi - step + 1, hi]。所有取段请求在 Redis 端串行分配,
        区间互不相交, 天然保证跨段无重复号。"""
        hi = int(self._client.incrby(_COUNTER_KEY, self._step))
        return _Segment(lo=hi - self._step + 1, hi=hi)

    def _prefetch_worker(self) -> None:
        """后台预取线程(daemon): 锁外网络 IO 取下一段, 完成后短暂持锁写入 _next。

        任何情况下 finally 都会清除 _prefetching 占位, 预取失败不会卡死预取机制。
        """
        try:
            seg = self._fetch_segment()  # 网络 IO 在锁外, 不阻塞取号路径
            with self._lock:
                if self._next is None and (
                    self._current is None or seg.lo > self._current.hi
                ):
                    # 正常路径: 写入 _next, 供当前段耗尽时原子切换
                    # (seg.lo > _current.hi 确保本段是"未来"的段, 递增趋势成立)
                    self._next = seg
                else:
                    # 竞态兜底(极罕见): 预取在途期间发生了锁内同步兜底取段,
                    # 本段区间已被越过(或 _next 已被并发填充), 丢弃本段。
                    # 代价只是浪费一段号, 唯一性与递增趋势均不受影响
                    pass
        except Exception as exc:
            # 预取失败: 静默记录, 绝不影响当前段继续发号;
            # 若段最终耗尽仍无 _next, generate() 会走同步兜底, 且预取条件允许再次触发
            self._prefetch_error_count += 1
            self._last_prefetch_error = f"{type(exc).__name__}: {exc}"
        finally:
            with self._lock:
                # 释放预取占位: 无论成败都允许下一次预取触发
                # (_prefetching 的读写始终在锁内, 与 generate() 的判定天然互斥)
                self._prefetching = False
