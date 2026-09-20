"""UUIDv7 生成器: 按 RFC 9562 手写实现(Python 3.13 标准库尚无 uuid7)。

位布局(共 128bit, 从高位到低位):
    unix_ts_ms(48) | ver=0b0111(4) | rand_a(12) | var=0b10(2) | rand_b(62)

时间戳占据最高 48bit, 因此同一毫秒内生成的 UUIDv7 前缀相同、整体随时间趋势递增,
作为数据库主键时新数据总落在 B+ 树右侧, 避免 UUIDv4 那种随机写导致的页分裂。
"""

import os
import time

from app.generators.base import BaseIDGenerator, GeneratorMeta

# rand_b 的掩码: 62 个二进制 1, 用于从随机数中截取低 62bit
_RAND_B_MASK = (1 << 62) - 1


def _uuid7() -> str:
    """生成一个 UUIDv7 字符串(标准 8-4-4-4-12 hex 形式)"""
    # 48bit Unix 毫秒时间戳: time_ns() 纳秒精度整除 1e6 得毫秒, 避免浮点误差
    ts_ms = time.time_ns() // 1_000_000

    # 一次取 80bit(10 字节)密码学随机数, 拆出 rand_a 与 rand_b 两段互不重叠的随机位:
    #   rnd 共 80bit: [79..68] 共 12bit 作 rand_a, [61..0] 共 62bit 作 rand_b,
    #   中间 [67..62] 共 6bit 弃用, 保证两段来自相互独立的随机比特
    rnd = int.from_bytes(os.urandom(10), "big")
    rand_a = rnd >> 68                       # 高 12bit
    rand_b = rnd & _RAND_B_MASK              # 低 62bit

    # 位拼接: 按各自位宽左移到目标位置后按位或
    #   ts_ms   在 [127:80]
    #   ver     在 [79:76]
    #   rand_a  在 [75:64]
    #   var     在 [63:62]
    #   rand_b  在 [61:0]
    value = (ts_ms << 80) | (0b0111 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b

    # 格式化为 32 个 hex 字符(不足左侧补 0), 再切出 8-4-4-4-12 的标准 UUID 形式
    h = f"{value:032x}"
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


class UUIDv7Generator(BaseIDGenerator):
    def __init__(self):
        super().__init__(GeneratorMeta(
            name="uuid_v7",
            display_name="UUID v7",
            category="local",
            description="毫秒时间戳在高位的时间有序 UUID(RFC 9562), 数据库索引友好",
            bit_layout="48bit 毫秒时间戳 | ver(4) | rand_a(12) | var(2) | rand_b(62)",
            monotonic=True,
            dependency="无",
            theoretical_limit="毫秒级趋势递增, 毫秒内 74bit 随机空间防碰撞",
        ))

    def generate(self) -> str:
        return _uuid7()
