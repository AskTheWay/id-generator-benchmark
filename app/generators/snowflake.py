"""Twitter Snowflake ID 生成器(本地方案)。

64bit 布局: 1bit 符号(恒 0) | 41bit 毫秒时间戳 | 10bit 机器ID | 12bit 序列号
- 时间戳 = 当前时间 - 自定义纪元(2024-01-01 UTC), 41bit 可用约 69 年
- 单机每毫秒 4096 个序号, 趋势递增, 对数据库索引友好
- 必须正确处理三种时钟情况(见 generate() 内分段注释):
  1) 同毫秒序列耗尽: 自旋等待下一毫秒
  2) 时钟小幅回拨(<= 5ms): 自旋等待时钟追平
  3) 时钟大幅回拨(> 5ms): 拒绝发号(抛 RuntimeError)
"""

import threading
import time

from app.config import SNOWFLAKE_EPOCH_MS, SNOWFLAKE_MACHINE_ID
from app.generators.base import BaseIDGenerator, GeneratorMeta


class SnowflakeGenerator(BaseIDGenerator):
    """线程安全的 Snowflake 发号器(发号状态由全局互斥锁保护)"""

    # ---- 位宽分配 ----
    MACHINE_ID_BITS = 10                                # 机器ID 10bit, 最多 1024 台机器
    SEQUENCE_BITS = 12                                  # 序列号 12bit, 单毫秒 4096 个
    TIMESTAMP_SHIFT = MACHINE_ID_BITS + SEQUENCE_BITS   # 时间戳整体左移 22bit
    MAX_MACHINE_ID = (1 << MACHINE_ID_BITS) - 1         # 1023
    MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1             # 4095

    # ---- 时钟回拨策略 ----
    ROLLBACK_TOLERANCE_MS = 5    # 可容忍的最大回拨幅度(毫秒), 超过即拒绝发号
    SPIN_TIMEOUT_SECONDS = 1.0   # 自旋等待的兜底超时(基于单调时钟), 防系统时钟冻结导致死循环

    def __init__(self) -> None:
        meta = GeneratorMeta(
            name="snowflake",
            display_name="Snowflake",
            category="local",
            description="Twitter Snowflake: 本地内存拼接 时间戳+机器ID+序列号, 无外部依赖, 趋势递增",
            bit_layout="1bit 符号(0) | 41bit 毫秒时间戳 | 10bit 机器ID | 12bit 序列号",
            monotonic=True,
            dependency="无",
            theoretical_limit="单机 4096/ms ≈ 400万 QPS, 可用 69 年",
        )
        super().__init__(meta)
        # 机器ID 越界直接抛错而非静默截断: 截断会让"配置 0 与配置 1024"的两台
        # 机器得到相同 machine_id, 时间戳/序列号同源必然发出重复 ID —— 这正是
        # Snowflake 的经典事故形态, 必须在启动期暴露配置错误
        if not 0 <= SNOWFLAKE_MACHINE_ID <= self.MAX_MACHINE_ID:
            raise ValueError(
                f"SNOWFLAKE_MACHINE_ID={SNOWFLAKE_MACHINE_ID} 超出 10bit 范围 "
                f"[0, {self.MAX_MACHINE_ID}], 请修正配置"
            )
        self._machine_id = SNOWFLAKE_MACHINE_ID
        self._epoch_ms = SNOWFLAKE_EPOCH_MS
        # ---- 发号状态(以下成员全部由 _lock 保护) ----
        self._lock = threading.Lock()
        self._last_ts = -1            # 上一次发号所用的毫秒时间戳
        self._sequence = 0            # 当前毫秒内已用到的序列号
        self.rollback_wait_count = 0  # 因时钟小幅回拨而自旋等待的累计次数(观测指标)

    # ---------------------------------------------------------------- 内部工具
    @staticmethod
    def _now_ms() -> int:
        """读取当前系统时间戳(毫秒; wall-clock, 可能发生回拨)"""
        return int(time.time() * 1000)

    def _spin_until(self, target_ms: int, *, count_wait: bool = False) -> int:
        """自旋等待系统时钟到达 target_ms, 返回追平后的时间戳。

        - count_wait=True 时统计自旋次数(用于观测小幅回拨的等待开销)
        - 兜底超时基于 time.perf_counter()(单调时钟, 不受系统时钟回拨影响),
          防止系统时钟被冻结时陷入死循环
        """
        deadline = time.perf_counter() + self.SPIN_TIMEOUT_SECONDS
        while True:
            now = self._now_ms()
            if now >= target_ms:
                return now
            if count_wait:
                self.rollback_wait_count += 1
            if time.perf_counter() >= deadline:
                raise RuntimeError("等待系统时钟追平超时(系统时钟疑似被冻结), 拒绝发号")

    # ---------------------------------------------------------------- 发号
    def generate(self) -> str:
        with self._lock:
            ts = self._now_ms()

            # ================================================================
            # 情况三: 时钟回拨 —— 当前时间戳小于上一次发号的时间戳
            # ================================================================
            if ts < self._last_ts:
                backward_ms = self._last_ts - ts
                if backward_ms > self.ROLLBACK_TOLERANCE_MS:
                    # (b) 大幅回拨(> 5ms): 等待代价过高且不可靠, 直接抛错拒绝发号。
                    #     绝不允许"重置序列号后继续发号"——时间戳倒退 + 序列号
                    #     复用会必然产生重复 ID, 这是 Snowflake 实现的经典事故点。
                    raise RuntimeError("时钟回拨超过阈值, 拒绝发号")
                # (a) 小幅回拨(<= 5ms): 自旋等待时钟追平, 等待期间不生成任何 ID。
                #     追平后时间戳 >= 上次时间戳, 落入下方"同毫秒/新毫秒"分支:
                #     - 恰好追平到同一毫秒 → 在原序列号基础上继续 +1(不重置),
                #       保证不与回拨前发出的 ID 冲突;
                #     - 已超过到更新的毫秒 → 按新毫秒正常清零序列号。
                ts = self._spin_until(self._last_ts, count_wait=True)

            if ts == self._last_ts:
                # ============================================================
                # 情况一: 同一毫秒内继续发号 —— 序列号 +1
                # ============================================================
                self._sequence = (self._sequence + 1) & self.MAX_SEQUENCE
                if self._sequence == 0:
                    # 序列号回绕到 0, 说明本毫秒 4096 个序号已全部耗尽:
                    # 自旋等待进入下一毫秒, 用全新时间戳从序列号 0 重新计数
                    # (sequence 保持 0, 作为新毫秒的第一个 ID)
                    ts = self._spin_until(self._last_ts + 1)
            else:
                # ============================================================
                # 情况二: 进入新的毫秒 —— 序列号清零, 从 0 重新计数
                # ============================================================
                self._sequence = 0

            self._last_ts = ts
            # 位拼接: ((时间戳 - 纪元) << 22) | (机器ID << 12) | 序列号
            value = (
                ((ts - self._epoch_ms) << self.TIMESTAMP_SHIFT)
                | (self._machine_id << self.SEQUENCE_BITS)
                | self._sequence
            )
            return str(value)
