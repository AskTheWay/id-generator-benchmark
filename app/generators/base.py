from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class GeneratorMeta:
    name: str              # 唯一标识(小写下划线), 如 "snowflake"
    display_name: str      # 展示名, 如 "Snowflake"
    category: str          # "local" | "redis" | "database"
    description: str       # 一句话中文描述
    bit_layout: str        # 位组成说明, 如 "0 | 41bit 时间戳 | 10bit 机器ID | 12bit 序列号"; 无位概念写 ""
    monotonic: bool        # 是否趋势递增(数据库索引友好)
    dependency: str        # "无" | "Redis" | "PostgreSQL"
    theoretical_limit: str # 理论容量说明, 如 "单机 4096/ms"


class BaseIDGenerator(ABC):
    def __init__(self, meta: GeneratorMeta):
        self.meta = meta

    @abstractmethod
    def generate(self) -> str: ...

    def check_ready(self) -> bool:
        """依赖服务可用性检测(带短超时); 本地方案恒返回 True"""
        return True

    def warmup(self, n: int = 64) -> None:
        """压测前预热: 建满连接池, 排除冷启动建连对首请求延迟的污染。默认空实现"""

    def close(self) -> None:
        """清理连接; 默认空实现"""
