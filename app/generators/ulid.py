"""ULID 生成器: 手写 ULID 规范实现。

结构: 48bit 毫秒时间戳 + 80bit 随机数, 共 128bit, 编码为 26 个字符
(时间戳部分 10 字符 + 随机部分 16 字符), 采用 Crockford Base32 字母表
"0123456789ABCDEFGHJKMNPQRSTVWXYZ"(排除 I/L/O/U 四个易混淆字符), 统一大写。

时间戳在编码后仍位于字符串最高位, 因此 ULID 的字符串字典序 == 生成时间序,
可直接按字符串排序。
"""

import os
import time

from app.generators.base import BaseIDGenerator, GeneratorMeta

# Crockford Base32 字母表: 32 个字符, 排除 I / L / O / U(形似 1 / 1 / 0 / V)
_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _to_base32(value: int, length: int) -> str:
    """大整数 → Crockford Base32 定长字符串。

    原理: 反复对 32 做 divmod, 余数(0~31)查表得最低位字符, 商继续分解,
    得到的字符序列是"低位在前", 最后逆序拼接即为高位在前的正常表示。
    定长 length 位要求 value < 32**length, 不足时左侧自动补 '0'
    (循环固定执行 length 次, 高位余数自然为 0)。
    """
    chars = []
    for _ in range(length):
        value, rem = divmod(value, 32)
        chars.append(_ALPHABET[rem])
    return "".join(reversed(chars))


def _ulid() -> str:
    """生成一个 26 字符的 ULID"""
    # 48bit 毫秒时间戳(纳秒精度整除得毫秒, 避免浮点误差)
    ts_ms = time.time_ns() // 1_000_000
    # 80bit 密码学随机数, 恰好 10 字节
    rnd = int.from_bytes(os.urandom(10), "big")
    # 时间戳 10 字符(32^10 = 2^50 >= 2^48, 足够编码) + 随机 16 字符(32^16 = 2^80, 恰好编满)
    return _to_base32(ts_ms, 10) + _to_base32(rnd, 16)


class ULIDGenerator(BaseIDGenerator):
    def __init__(self):
        super().__init__(GeneratorMeta(
            name="ulid",
            display_name="ULID",
            category="local",
            description="48bit 时间戳 + 80bit 随机, 26 字符可排序且 URL 友好",
            bit_layout="48bit 毫秒时间戳 | 80bit 随机数(共 26 字符 Crockford Base32)",
            monotonic=True,
            dependency="无",
            theoretical_limit="单机受限于随机数生成, 毫秒内随机部分防碰撞",
        ))

    def generate(self) -> str:
        return _ulid()
