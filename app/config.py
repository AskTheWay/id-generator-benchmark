"""集中配置: 所有连接参数与生成器可调参数统一在此定义, 均支持环境变量覆盖。"""

import os


def _env_str(name: str, default: str) -> str:
    """读取字符串环境变量, 未设置时返回默认值"""
    raw = os.getenv(name)
    return default if raw is None or raw == "" else raw


def _env_int(name: str, default: int) -> int:
    """读取整数环境变量, 未设置或非法时返回默认值(避免拼错环境变量导致启动失败)"""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


# ---- Redis 连接配置(供 redis_incr / redis_segment 使用) ----
REDIS_HOST = _env_str("REDIS_HOST", "localhost")
REDIS_PORT = _env_int("REDIS_PORT", 6379)
REDIS_DB = _env_int("REDIS_DB", 0)

# ---- PostgreSQL 连接配置(供 db_sequence 使用) ----
PG_HOST = _env_str("PG_HOST", "localhost")
PG_PORT = _env_int("PG_PORT", 15432)
PG_USER = _env_str("PG_USER", "postgres")
PG_PASSWORD = _env_str("PG_PASSWORD", "postgres")
PG_DATABASE = _env_str("PG_DATABASE", "idbench")

# ---- 生成器参数 ----
# 号段模式步长: 每次从 Redis 批量取一段 ID, 减少网络往返
SEGMENT_STEP = _env_int("SEGMENT_STEP", 1000)
# Snowflake 机器ID(10bit, 0~1023)
SNOWFLAKE_MACHINE_ID = _env_int("SNOWFLAKE_MACHINE_ID", 1)
# Snowflake 自定义纪元: 2024-01-01 00:00:00 UTC 的毫秒时间戳,
# 让 41bit 时间戳从较近的时间起点计数, 延长可用年限
SNOWFLAKE_EPOCH_MS = _env_int("SNOWFLAKE_EPOCH_MS", 1704067200000)
