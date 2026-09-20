"""生成器注册表: 按固定顺序聚合全部 8 种 ID 生成方案。

REGISTRY / ALL_GENERATORS 存放"类"(非实例), 由调用方(main.py 的模块级单例字典)
惰性实例化; create_all() 则一次性实例化全部, 供脚本化批量压测使用。

注: 多人并行开发阶段, 兄弟模块(依赖 redis / psycopg2 的方案)可能尚未落盘,
此处对 ImportError 做容忍——导入失败的类自动跳过, 不影响本地方案可用;
待全部模块就位后 REGISTRY 即恢复完整的 8 项契约顺序。
"""

from app.generators.base import BaseIDGenerator

# 按契约顺序逐一导入; 单个模块缺失/依赖未安装时跳过该项并继续
from app.generators.uuid_v4 import UUIDv4Generator
from app.generators.uuid_v7 import UUIDv7Generator
from app.generators.ulid import ULIDGenerator
from app.generators.nanoid import NanoIDGenerator

try:
    from app.generators.snowflake import SnowflakeGenerator
except ImportError:
    SnowflakeGenerator = None

try:
    from app.generators.redis_incr import RedisIncrGenerator
except ImportError:
    RedisIncrGenerator = None

try:
    from app.generators.redis_segment import RedisSegmentGenerator
except ImportError:
    RedisSegmentGenerator = None

try:
    from app.generators.db_sequence import DBSequenceGenerator
except ImportError:
    DBSequenceGenerator = None

# 注册顺序(即仪表盘展示顺序), None 表示该模块暂不可用, 已被过滤
_CANDIDATES = [
    UUIDv4Generator,
    UUIDv7Generator,
    ULIDGenerator,
    NanoIDGenerator,
    SnowflakeGenerator,
    RedisIncrGenerator,
    RedisSegmentGenerator,
    DBSequenceGenerator,
]

REGISTRY = [cls for cls in _CANDIDATES if cls is not None]

# 别名导出, 方便外部引用同一份列表
ALL_GENERATORS = REGISTRY


def create_all() -> list[BaseIDGenerator]:
    """惰性实例化全部生成器(调用本函数时才真正构造各实例)"""
    return [cls() for cls in REGISTRY]
