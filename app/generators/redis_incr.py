"""Redis INCR 原子自增发号器

每次取号向 Redis 提交一条 Lua 脚本(INCR + 首见设置 EXPIRE), 全局严格连续无重复,
代价是每次取号一次网络往返, 吞吐受 Redis 单线程处理能力与网络 RTT 制约。
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import redis

from app.config import REDIS_HOST, REDIS_PORT, REDIS_DB
from app.generators.base import BaseIDGenerator, GeneratorMeta

# 按日分 key 的前缀: 每天一个新计数器, 日期进入 ID 本身, 保证跨天仍全局唯一
_KEY_PREFIX = "idbench:incr:"
# key 过期时间: 2 天(秒), 历史日期的计数器 key 自动清理, 不留垃圾
_EXPIRE_SECONDS = 172800

# Lua 脚本: 原子执行 INCR + 首次到达时设置 EXPIRE
# 说明: EXPIRE 必须与 INCR 在同一个脚本内原子完成。若拆成两条命令
# ("先 INCR, 客户端看到返回 1 再补 EXPIRE"), 进程在两条命令之间崩溃,
# 就会留下一个永不过期、且之后永远不会再触发 EXPIRE 的 key —— 这就是
# 被禁止的竞态写法; Lua 在 Redis 内单线程原子执行, 不存在该窗口。
_LUA_INCR_WITH_EXPIRE = """
local v = redis.call("INCR", KEYS[1])
if v == 1 then
    redis.call("EXPIRE", KEYS[1], ARGV[1])
end
return v
"""


class RedisIncrGenerator(BaseIDGenerator):
    """方案: Redis INCR —— 单命令原子自增, 实现最简单, 性能受网络 RTT 限制"""

    def __init__(self):
        super().__init__(GeneratorMeta(
            name="redis_incr",
            display_name="Redis INCR",
            category="redis",
            description="每次取号一条 Redis 命令原子自增, 序号全局连续, 代价是每次取号一次网络往返",
            bit_layout="业务格式: 日期 + Redis 原子自增序号",
            monotonic=True,
            dependency="Redis",
            theoretical_limit="受限于 Redis 单线程与网络 RT, 单实例约 10万 QPS",
        ))
        # 连接池容量必须 >= main.py 允许的压测并发上限(512)并留余量:
        # redis-py 的 ConnectionPool 在满载时直接抛 ConnectionError("Too many
        # connections")而不等待, 池小于并发数会让压测大量报错、数据失真;
        # socket 超时兑现 base.py 契约"check_ready 带短超时", 并防止 Redis
        # 远端不可达(丢包型)时 TCP SYN 重试把压测/采样挂起 20s 以上
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
        # 注册 Lua 脚本: 客户端优先走 EVALSHA, 脚本缓存失效时自动回退 EVAL
        self._incr_script = self._client.register_script(_LUA_INCR_WITH_EXPIRE)

    # ---- 契约实现 ----

    def generate(self) -> str:
        # 按日分 key: 新的一天从 1 重新计数, 日期编入 ID 保证跨天仍不重复
        date_str = time.strftime("%Y%m%d")
        key = _KEY_PREFIX + date_str
        # keys/args 分别映射脚本内的 KEYS[1] / ARGV[1](过期秒数)
        seq = self._incr_script(keys=[key], args=[_EXPIRE_SECONDS])
        # "RI" + 日期 + 8 位零填充序号, 如 RI2026092100000042
        return f"RI{date_str}{int(seq):08d}"

    def check_ready(self) -> bool:
        """依赖服务可用性检测: ping; 任何异常(连接拒绝/超时等)一律返回 False, 绝不向上抛"""
        try:
            return bool(self._client.ping())
        except Exception:
            return False

    def warmup(self, n: int = 64) -> None:
        """压测前预热: 用屏障(threading.Barrier)强制 n 个线程"同时"发号,
        保证连接池真正建满 n 条连接, 排除冷启动 TCP 建连 + Lua 脚本首次加载
        对压测首请求延迟的污染。

        为什么必须用 Barrier 而不是裸的 ex.map: ex.map 的任务完成快于线程启动,
        先完成的任务会把连接归还池中供后续任务复用, 实测 50 并发只建出 28 条
        连接 —— 压测中途再补建的连接撞上 Redis 服务端的建连排队, 会把 P99
        拉到秒级(本项目实测: 未建满 2050ms vs 建满后 21ms)。
        """
        if n <= 0:
            return
        workers = min(n, 600)  # 并发度不超过连接池容量
        barrier = threading.Barrier(workers)

        def _one(_: int) -> None:
            try:
                # 屏障对齐: n 个线程同时发起命令 -> 同时占用并建立 n 条连接
                barrier.wait(timeout=10)
                self.generate()
            except Exception:
                pass  # 预热失败不在此抛出: 服务真正不可用时, 让压测阶段把错误计入统计

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
