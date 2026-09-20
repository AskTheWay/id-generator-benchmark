"""基准测试执行器: 用线程池并发压测单个 ID 生成方案。

设计约束: 只依赖 app.generators.base 的抽象契约 BaseIDGenerator,
不 import 任何具体生成器 —— 保证本地 / Redis / PostgreSQL 三类方案
走完全相同的压测流程, 结果才有可比性。
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from app.benchmark.stats import BenchmarkResult, summarize
from app.generators.base import BaseIDGenerator

_SAMPLE_COUNT = 5  # 结果中保留的 ID 样例数量


def run_benchmark(gen: BaseIDGenerator, total: int, concurrency: int) -> BenchmarkResult:
    """压测一个生成器: 并发执行 total 次 generate(), 汇总延迟/吞吐/重复率。

    流程(顺序不可变):
      1. check_ready() 依赖可用性检测, 不可用直接返回全错误结果, 不跑压测;
      2. warmup() 预热连接池, 排除冷启动建连对尾部延迟的污染;
      3. ThreadPoolExecutor(max_workers=concurrency) 并发执行, 逐次计延迟并查重。
    """
    # ---------- 步骤 1: 依赖服务可用性检测 ----------
    # Redis/PostgreSQL 未启动属"环境缺失"而非"方案缺陷":
    # 返回 errors=total + error_detail="依赖服务不可用" 的空结果,
    # 由上层(API)决定跳过或在页面上如实展示, 绝不能把它混进性能数据。
    if not gen.check_ready():
        return BenchmarkResult(
            name=gen.meta.name,
            display_name=gen.meta.display_name,
            total=total,
            success=0,
            duplicates=0,
            errors=total,
            qps=0.0,
            mean_ms=0.0,
            p50_ms=0.0,
            p90_ms=0.0,
            p99_ms=0.0,
            max_ms=0.0,
            samples=[],
            error_detail="依赖服务不可用",
        )

    # ---------- 步骤 2: 预热(本项目方法论的核心卖点) ----------
    # 为什么必须预热: Redis/PostgreSQL 方案的首次请求要经历 TCP 三次握手、
    # 认证、连接池初始化等一次性开销(可达几十毫秒)。若不做预热, 这些冷启动
    # 成本全部落在最开始的几个请求上, 会把 P99/max 等尾部分位数拉高一个
    # 数量级, 让"稳态服务能力"的对比彻底失真。先用 concurrency 次并发调用
    # 把连接池填满, 计时阶段测到的才是稳态延迟 —— 这也是本基准与
    # "随手写个 for 循环测一下"的本质区别。
    gen.warmup(concurrency)

    # ---------- 步骤 3: 并发压测 ----------
    seen: set = set()                 # 全局查重集合(所有无异常返回的 ID)
    lock = threading.Lock()           # 保护 seen / samples / 计数器 / 首错信息
    latencies_ms: list = []           # 无异常调用的延迟(毫秒)
    samples: list = []                # 前 5 个"首次出现且无异常"的 ID 样例
    duplicates = 0
    errors = 0
    first_error = ""

    def _task() -> None:
        """单个压测任务: 纳秒级计时调用一次 generate(), 记录延迟/查重/异常。"""
        nonlocal duplicates, errors, first_error
        start_ns = time.perf_counter_ns()
        try:
            rid = gen.generate()
        except Exception as exc:  # 任何异常只计数不中断, 保证整体压测完整跑完
            with lock:
                errors += 1
                if not first_error:
                    first_error = f"{type(exc).__name__}: {exc}"
            return
        # 无异常: 记录本次调用延迟(重复 ID 的调用同样计入 —— 它测的是真实的
        # 生成耗时, 与该 ID 是否重复这一正确性问题相互独立)。
        # 注: list.append 在 CPython 的 GIL 保护下是线程安全操作。
        latencies_ms.append((time.perf_counter_ns() - start_ns) / 1e6)
        with lock:
            if rid in seen:
                # 重复 ID: 单独计入 duplicates, 不计入 success
                duplicates += 1
            else:
                seen.add(rid)
                if len(samples) < _SAMPLE_COUNT:
                    samples.append(rid)

    # 墙钟计时覆盖"提交全部任务 → 全部完成"的完整区间, qps 由此得出。
    # 分批提交: total 上限 20 万, 一次性物化 20 万个 Future 会有数百 MB 的
    # 内存峰值; 按块提交并等待, 计时区间与统计口径完全不变
    _CHUNK = 4096
    wall_start_ns = time.perf_counter_ns()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        done = 0
        while done < total:
            futures = [pool.submit(_task) for _ in range(min(_CHUNK, total - done))]
            for fut in futures:
                fut.result()  # _task 内部已吞掉异常, 这里仅做等待
            done += len(futures)

    elapsed_s = (time.perf_counter_ns() - wall_start_ns) / 1e9
    success = len(seen)  # 首次出现且无异常的数量, 与 seen 集合天然等价
    stats = summarize(latencies_ms)

    return BenchmarkResult(
        name=gen.meta.name,
        display_name=gen.meta.display_name,
        total=total,
        success=success,
        duplicates=duplicates,
        errors=errors,
        qps=(success / elapsed_s) if elapsed_s > 0 else 0.0,
        mean_ms=stats["mean_ms"],
        p50_ms=stats["p50_ms"],
        p90_ms=stats["p90_ms"],
        p99_ms=stats["p99_ms"],
        max_ms=stats["max_ms"],
        samples=samples,
        error_detail=first_error,
    )
