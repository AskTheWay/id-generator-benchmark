"""基准测试统计模块: 结果数据结构 + 延迟分位数计算。

BenchmarkResult 是 runner 与 FastAPI 层(/api/benchmark 响应体)共用的
结果契约, 字段顺序与类型以本文件为准。
"""

from dataclasses import dataclass, field


@dataclass
class BenchmarkResult:
    """单个 ID 生成方案一次压测的完整结果。

    计数口径:
      - success: 无异常且首次出现(非重复)的 ID 数量
      - duplicates: 重复 ID 的出现次数(单独计数, 不计入 success)
      - errors: 抛异常的调用次数
      - 三者满足 success + duplicates + errors == total
    """

    name: str               # 方案唯一标识(小写下划线), 如 "snowflake"
    display_name: str       # 展示名, 如 "Snowflake"
    total: int              # 计划执行的请求总数
    success: int            # 成功且首次出现的数量
    duplicates: int         # 重复 ID 数量
    errors: int             # 异常数量
    qps: float              # 吞吐: success / 压测耗时(秒)
    mean_ms: float          # 平均延迟(毫秒)
    p50_ms: float           # 中位数延迟(毫秒)
    p90_ms: float           # P90 延迟(毫秒)
    p99_ms: float           # P99 延迟(毫秒)
    max_ms: float           # 最大延迟(毫秒)
    samples: list = field(default_factory=list)  # 前 5 个成功生成的 ID 样例
    error_detail: str = ""  # 首个错误信息(为空表示无异常)


def percentile(latencies_ms: list, p: float) -> float:
    """线性插值分位数(手写清晰版, 不用 statistics.quantiles)。

    算法: 升序排序后, 把目标分位 p% 映射到下标区间 [0, n-1] 上的浮点
    位置 rank, 取相邻两个样本 xs[lo] 与 xs[hi] 按 rank 的小数部分加权:

        rank = p/100 * (n-1)
        lo   = floor(rank), hi = min(lo+1, n-1)
        结果  = xs[lo] * (1-frac) + xs[hi] * frac      其中 frac = rank - lo

    这样 P50 在偶数样本下恰为中间两值的平均, P100 为最大值,
    与 NumPy 的默认 'linear'/'inclusive' 语义一致。

    空列表返回 0.0; p 越界时夹紧到 [0, 100]。
    """
    if not latencies_ms:
        return 0.0
    xs = sorted(latencies_ms)  # sorted 返回新列表, 不改动调用方数据
    if len(xs) == 1:
        return float(xs[0])
    # 夹紧分位数到合法区间, 防御性处理
    p = max(0.0, min(100.0, float(p)))
    # 目标分位映射到 [0, n-1] 的浮点下标
    rank = (p / 100.0) * (len(xs) - 1)
    lo = int(rank)  # rank 非负, int() 即 floor
    hi = min(lo + 1, len(xs) - 1)
    frac = rank - lo
    return xs[lo] * (1.0 - frac) + xs[hi] * frac


def summarize(latencies_ms: list) -> dict:
    """汇总一份延迟样本(毫秒), 返回 mean/p50/p90/p99/max。

    键名与 BenchmarkResult 的延迟字段一一对应(mean_ms/p50_ms/...),
    便于 runner 直接展开填充; 空样本时各项为 0.0。
    """
    if not latencies_ms:
        return {"mean_ms": 0.0, "p50_ms": 0.0, "p90_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0}
    return {
        "mean_ms": sum(latencies_ms) / len(latencies_ms),
        "p50_ms": percentile(latencies_ms, 50),
        "p90_ms": percentile(latencies_ms, 90),
        "p99_ms": percentile(latencies_ms, 99),
        "max_ms": float(max(latencies_ms)),
    }
