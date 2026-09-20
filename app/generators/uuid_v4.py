"""UUIDv4 生成器: 122bit 完全随机, 标准库 uuid 模块实现, 无任何时间/顺序信息。"""

import uuid

from app.generators.base import BaseIDGenerator, GeneratorMeta


class UUIDv4Generator(BaseIDGenerator):
    def __init__(self):
        super().__init__(GeneratorMeta(
            name="uuid_v4",
            display_name="UUID v4",
            category="local",
            description="122bit 完全随机的通用唯一标识符, 实现最简单但完全无序",
            bit_layout="128bit: 122bit 随机 + 6bit 版本/变体固定",
            monotonic=False,
            dependency="无",
            theoretical_limit="受限于随机数生成速度",
        ))

    def generate(self) -> str:
        # uuid4 底层调用操作系统安全随机源(os.urandom), 返回 UUID 对象后转标准 36 字符字符串
        return str(uuid.uuid4())
