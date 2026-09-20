"""NanoID 生成器: 手写 NanoID 实现, 默认 21 字符, 字母表 64 字符。

熵: 21 字符 × log2(64) = 21 × 6 = 126bit, 与 UUIDv4 的 122bit 同量级,
但编码更紧凑(21 字符 vs 36 字符), 且字母表含 "_-" 两种 URL 安全符号。
"""

import os

from app.generators.base import BaseIDGenerator, GeneratorMeta

# 64 字符字母表: 大小写字母 + 数字 + URL 安全符号 "_" "-"
_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
_BASE = len(_ALPHABET)  # 64
_SIZE = 21              # 默认长度

# 拒绝采样(rejection sampling)的可用字节上限:
# 一个随机字节取值 0~255, 若直接对 _BASE 取模, 当 256 不能被 _BASE 整除时,
# 低位字符会出现概率偏高(模偏差)。只有落在 [0, 256 // _BASE * _BASE) 区间内的
# 字节才被接受, 其余丢弃重取, 这样每个字符严格等概率。
# 本字母表 _BASE=64 时 256 % 64 == 0, 上限恰为 256, 拒绝率为 0,
# 但保留通用的拒绝逻辑, 换任意长度字母表依然均匀。
_MAX_BYTE = (256 // _BASE) * _BASE


def _nanoid(size: int = _SIZE) -> str:
    """生成指定长度的 NanoID(默认 21 字符)"""
    chars: list[str] = []
    while len(chars) < size:
        # 按需批量取随机字节, 避免逐字节调用 os.urandom 的开销
        buf = os.urandom(size)
        for b in buf:
            if b < _MAX_BYTE:  # 拒绝采样: 超出均匀区间的字节直接丢弃
                chars.append(_ALPHABET[b % _BASE])
                if len(chars) == size:
                    break
    return "".join(chars)


class NanoIDGenerator(BaseIDGenerator):
    def __init__(self):
        super().__init__(GeneratorMeta(
            name="nanoid",
            display_name="NanoID",
            category="local",
            description="URL 友好的紧凑随机 ID, 默认 21 字符约 126bit 熵",
            bit_layout="约 126bit 熵(21 字符 × 6bit)",
            monotonic=False,
            dependency="无",
            theoretical_limit="受限于随机数生成速度(126bit 熵碰撞概率可忽略)",
        ))

    def generate(self) -> str:
        return _nanoid()
