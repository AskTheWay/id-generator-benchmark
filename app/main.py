"""FastAPI 应用入口: 交互式唯一 ID 生成方案对比基准。

职责:
- 托管前端单页仪表盘(web/index.html)
- 提供 4 组 API: 依赖健康检查 / 方案元信息列表 / 单方案采样 / 批量压测
- 方案注册表惰性实例化(模块级单例字典), check_ready 结果缓存 10 秒,
  避免每次列表请求都真实 ping Redis/PostgreSQL

启动方式(项目根目录): python -m uvicorn app.main:app
"""

import logging
import socket
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

from app.benchmark.runner import run_benchmark
from app.benchmark.stats import BenchmarkResult
from app.config import PG_HOST, PG_PORT, REDIS_DB, REDIS_HOST, REDIS_PORT
from app.generators.base import BaseIDGenerator

logger = logging.getLogger(__name__)

# 项目根目录(app/ 的上一级), 用于定位 web/ 下的静态资源
ROOT = Path(__file__).resolve().parent.parent


# --------------------------------------------------------------------------
# 生成器注册表: 惰性实例化的模块级单例字典
# --------------------------------------------------------------------------

def _resolve_registry_entries() -> list:
    """从 app.generators 解析注册表, 返回"类或实例"的有序列表。

    兼容三种导出形式(按优先级, 项目实际导出为第 1 种):
      1. REGISTRY       —— 契约约定的类注册列表(由本模块惰性实例化)
      2. ALL_GENERATORS —— 别名, 与 REGISTRY 同一份列表
      3. create_all()   —— 工厂函数, 直接返回实例列表
    """
    import app.generators as generators_pkg

    for attr in ("REGISTRY", "ALL_GENERATORS"):
        obj = getattr(generators_pkg, attr, None)
        if isinstance(obj, (list, tuple)) and len(obj) > 0:
            return list(obj)
    create_all = getattr(generators_pkg, "create_all", None)
    if callable(create_all):
        return list(create_all())
    raise RuntimeError(
        "app.generators 未导出可用的注册表(需要 REGISTRY / ALL_GENERATORS / create_all 之一)"
    )


# 注册表条目: 可能是"生成器类"(主流形式, 惰性实例化)或"现成实例"(create_all 形式)
_registry_entries: list = _resolve_registry_entries()

# 模块级单例字典: 生成器 meta.name -> 实例(首次 API 访问时才真正构造,
# 避免导入 app.main 就去连 Redis/PostgreSQL, 拖慢启动或直接炸掉进程)
_instances: dict[str, BaseIDGenerator] = {}
_instances_lock = threading.Lock()

# check_ready 结果缓存: name -> (检查时刻 time.monotonic(), 结果)
# TTL 内的重复请求直接复用, 避免每次列表请求都真实 ping 依赖服务
_ready_cache: dict[str, tuple[float, bool]] = {}
_READY_CACHE_TTL = 10.0  # 秒


def _ordered_instances() -> list[BaseIDGenerator]:
    """按注册表顺序返回全部生成器实例(首次调用时完成惰性实例化)。

    单个方案实例化失败只跳过该方案(记录告警), 不影响其余方案的可用性。
    """
    with _instances_lock:
        ordered: list[BaseIDGenerator] = []
        for entry in _registry_entries:
            inst: BaseIDGenerator | None = None
            if isinstance(entry, BaseIDGenerator):
                # 注册表本身就是实例(create_all 形式): 登记为单例
                inst = _instances.setdefault(entry.meta.name, entry)
            elif isinstance(entry, type):
                # 注册表是类: 先找该类已有的单例, 没有才实例化(保证模块级单例语义)
                inst = next((i for i in _instances.values() if type(i) is entry), None)
                if inst is None:
                    try:
                        inst = entry()
                    except Exception as exc:
                        logger.warning("生成器 %s 实例化失败, 已跳过: %s", entry.__name__, exc)
                        continue
                    _instances[inst.meta.name] = inst
            # 按 meta.name 去重(防止注册表同时含类与其实例导致重复展示)
            if inst is not None and all(i.meta.name != inst.meta.name for i in ordered):
                ordered.append(inst)
        return ordered


def _find_instance(name: str) -> BaseIDGenerator | None:
    """按 name 查找生成器实例; 未注册返回 None。"""
    for inst in _ordered_instances():
        if inst.meta.name == name:
            return inst
    return None


def _is_available(gen: BaseIDGenerator) -> bool:
    """生成器可用性检测(check_ready), 结果缓存 10 秒。

    本地方案 check_ready 恒为 True, 缓存对其几乎零成本;
    Redis/PostgreSQL 方案的真实 ping 一轮列表请求最多发生一次。
    """
    now = time.monotonic()
    cached = _ready_cache.get(gen.meta.name)
    if cached is not None and now - cached[0] < _READY_CACHE_TTL:
        return cached[1]
    try:
        ok = bool(gen.check_ready())
    except Exception as exc:
        logger.warning("生成器 %s check_ready 异常, 视为不可用: %s", gen.meta.name, exc)
        ok = False
    _ready_cache[gen.meta.name] = (now, ok)
    return ok


# --------------------------------------------------------------------------
# 依赖服务健康检查
# --------------------------------------------------------------------------

def _check_redis() -> bool:
    """Redis 连通性: 直连 ping, 0.5 秒超时。"""
    try:
        import redis  # 延迟导入: redis 库缺失时仅影响本探测, 不影响应用启动

        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            socket_connect_timeout=0.5,
            socket_timeout=0.5,
        )
        try:
            return bool(client.ping())
        finally:
            try:
                client.close()  # 及时释放探测连接
            except Exception:
                pass
    except Exception:
        return False


def _check_postgres() -> bool:
    """PostgreSQL 连通性: TCP 端口可达即可(不做认证/建连), 0.5 秒超时。"""
    try:
        with socket.create_connection((PG_HOST, PG_PORT), timeout=0.5):
            return True
    except OSError:
        return False


# --------------------------------------------------------------------------
# FastAPI 应用
# --------------------------------------------------------------------------

@asynccontextmanager
async def _lifespan(_app: FastAPI):
    """应用生命周期: 退出时清理所有生成器持有的连接(Redis 连接池 / DB 连接等)。"""
    yield
    with _instances_lock:
        for inst in list(_instances.values()):
            try:
                inst.close()
            except Exception as exc:
                logger.warning("生成器 %s close 异常: %s", inst.meta.name, exc)
        _instances.clear()


app = FastAPI(
    title="id-generator-benchmark",
    description="8 种主流唯一 ID 生成方案的交互式性能对比(延迟分位数 / 吞吐 / 重复率)",
    version="1.0.0",
    lifespan=_lifespan,
)

# CORS: 演示项目, 允许所有源(allow_credentials=True 与 "*" 互斥, 故关闭凭证)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --------------------------------------------------------------------------
# 路由 1: 托管前端单页仪表盘
# --------------------------------------------------------------------------

@app.get("/")
def index() -> FileResponse:
    """返回 web/index.html 单页仪表盘。"""
    return FileResponse(ROOT / "web" / "index.html")


# --------------------------------------------------------------------------
# 路由 2: 依赖服务健康检查
# --------------------------------------------------------------------------

@app.get("/api/health")
def health() -> dict:
    """Redis ping + PostgreSQL 端口连通性; 均带 0.5 秒超时, 不可用返回 False 而非报错。"""
    return {"redis": _check_redis(), "postgres": _check_postgres()}


# --------------------------------------------------------------------------
# 路由 3: 方案元信息列表
# --------------------------------------------------------------------------

@app.get("/api/generators")
def list_generators() -> dict:
    """全部方案的 meta 全字段 + 可用性 + 现场生成的 1 个样例 ID(不可用或生成失败为 null)。"""
    payload = []
    for gen in _ordered_instances():
        available = _is_available(gen)
        sample: str | None = None
        if available:
            try:
                sample = gen.generate()
            except Exception as exc:
                logger.warning("生成器 %s 采样失败: %s", gen.meta.name, exc)
                sample = None
        meta = gen.meta
        payload.append(
            {
                "name": meta.name,
                "display_name": meta.display_name,
                "category": meta.category,
                "description": meta.description,
                "bit_layout": meta.bit_layout,
                "monotonic": meta.monotonic,
                "dependency": meta.dependency,
                "theoretical_limit": meta.theoretical_limit,
                "available": available,
                "sample": sample,
            }
        )
    return {"generators": payload}


# --------------------------------------------------------------------------
# 路由 4: 单方案采样
# --------------------------------------------------------------------------

@app.post("/api/sample/{name}")
def sample_one(name: str, count: int = 1) -> dict:
    """用指定方案现场生成 ID; 未知方案 404, 依赖不可用 503。

    count 缺省 1(返回单个 id 字段); 传 count>1(夹紧到 ≤100)为批量采样,
    返回 ids 数组 —— 教学用途: 连续生成可直观看到 INCR 连续发号 /
    号段模式的跨段跳变 / 时间有序 ID 的前缀推进。
    """
    gen = _find_instance(name)
    if gen is None:
        raise HTTPException(status_code=404, detail=f"unknown generator: {name}")
    if not _is_available(gen):
        raise HTTPException(status_code=503, detail="依赖服务不可用")
    n = max(1, min(100, count))
    ids: list[str] = []
    try:
        for _ in range(n):
            ids.append(gen.generate())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"生成失败: {exc}") from exc
    # id 字段保留(单个采样向后兼容), 批量时取第一个
    return {"name": name, "count": n, "id": ids[0] if ids else None, "ids": ids}


# --------------------------------------------------------------------------
# 路由 5: 批量压测
# --------------------------------------------------------------------------

class BenchmarkRequest(BaseModel):
    """压测请求体; generators 为 null/缺省表示跑全部可用方案。"""

    generators: list[str] | None = None
    total: int = 5000
    concurrency: int = 50


@app.post("/api/benchmark")
def benchmark(req: BenchmarkRequest) -> dict:
    """批量压测选定的可用方案。

    注意: 本路由是普通 def —— FastAPI 会自动丢进线程池执行,
    压测要跑数秒, 绝不能写成 async def 阻塞事件循环。
    """
    # 参数夹紧: total ∈ [100, 200000], concurrency ∈ [1, 512]
    total = max(100, min(200_000, req.total))
    concurrency = max(1, min(512, req.concurrency))

    instances = _ordered_instances()
    by_name = {g.meta.name: g for g in instances}

    # 确定待测列表: None → 全部; 指定列表 → 先整体校验未知名(去重保序)
    if req.generators is None:
        selected = instances
    else:
        selected = []
        for name in req.generators:
            if name not in by_name:
                raise HTTPException(status_code=400, detail=f"unknown generator: {name}")
            if all(g.meta.name != name for g in selected):
                selected.append(by_name[name])

    skipped: list[str] = []   # 因依赖不可用而被跳过的方案名
    results: list[dict] = []  # 各方案的 BenchmarkResult(dict 形式)
    for gen in selected:
        if not _is_available(gen):
            skipped.append(gen.meta.name)
            continue
        try:
            result = run_benchmark(gen, total=total, concurrency=concurrency)
            results.append(asdict(result))
        except Exception as exc:
            # 兜底: 单个生成器跑挂不能炸掉整个请求 —— 构造 errors=total 的
            # 占位结果计入返回, 其余方案继续跑
            logger.exception("生成器 %s 压测过程崩溃", gen.meta.name)
            results.append(
                asdict(
                    BenchmarkResult(
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
                        error_detail=f"压测过程异常终止: {type(exc).__name__}: {exc}",
                    )
                )
            )
    return {
        "config": {"total": total, "concurrency": concurrency},
        "skipped": skipped,
        "results": results,
    }


# ==========================================================================
# 路由组 2: 分库分表模拟(sharding)
# ==========================================================================

def _require_postgres() -> None:
    """sharding/offline 演示强依赖 PostgreSQL, 不可用时统一 503。"""
    if not _check_postgres():
        raise HTTPException(status_code=503, detail="PostgreSQL 不可用, 请先启动依赖服务")


@app.post("/api/sharding/init")
def sharding_init() -> dict:
    """建 4 个分片 schema 与演示表(幂等; 可重复调用)。"""
    _require_postgres()
    import app.sharding as sh

    sh.init()
    return {"ok": True, "shards": sh.SHARD_COUNT}


@app.post("/api/sharding/reset")
def sharding_reset() -> dict:
    """清空分片演示数据并重置序列(可反复重放)。"""
    _require_postgres()
    import app.sharding as sh

    sh.reset()
    return {"ok": True}


@app.get("/api/sharding/stats")
def sharding_stats() -> dict:
    """各场景各分片行数总览。"""
    _require_postgres()
    import app.sharding as sh

    return {"stats": sh.stats()}


@app.post("/api/sharding/scenario/{name}")
def sharding_scenario(name: str, shards: int = 4, rows: int = 1000,
                       users: int = 200, orders: int = 10,
                       total: int = 4000, pct: int = 80) -> dict:
    """运行一个分片演示场景(参数可自定义, 越界自动夹紧):

    - independent / step: shards(分片数 1-16), rows(每分片行数 10-20000)
    - gene:               shards, users(用户数), orders(每用户订单数)
    - hotspot:            shards, total(总行数), pct(最新分片写入占比 50-99)
    """
    _require_postgres()
    import app.sharding as sh

    handlers = {
        "independent": lambda: sh.scenario_independent(shards=shards, rows_per_shard=rows),
        "step": lambda: sh.scenario_step(shards=shards, rows_per_shard=rows),
        "gene": lambda: sh.scenario_gene(shards=shards, users=users, orders_per_user=orders),
        "hotspot": lambda: sh.scenario_hotspot(shards=shards, total=total, latest_pct=pct),
    }
    if name not in handlers:
        raise HTTPException(status_code=404, detail=f"unknown scenario: {name}")
    try:
        return handlers[name]()
    except HTTPException:
        raise
    except Exception as exc:
        # PG 建表/插入等数据库层失败统一 500(连接类错误在 _require_postgres 已挡)
        raise HTTPException(status_code=500, detail=f"场景执行失败: {exc}") from exc


# ==========================================================================
# 路由组 3: 离线摆渡模拟(air-gapped / sneakernet)
# ==========================================================================

_offline_ready = False


def _ensure_offline() -> None:
    """首次访问时建中心表并注册演示站点(幂等)。"""
    global _offline_ready
    _require_postgres()
    import app.offline as off

    if not _offline_ready:
        off.init(reset=False)
        if not off.sites():  # 无站点文件(首次): 注册 S01/S02 演示站点
            off.init(reset=True)
        _offline_ready = True


@app.get("/api/offline/sites")
def offline_sites() -> dict:
    """站点总览: 配额区间/水位/余量/待摆渡数。"""
    _ensure_offline()
    import app.offline as off

    return {"sites": off.sites(), "central": off.central_stats()}


class OfflineIssueRequest(BaseModel):
    site_id: str
    count: int = 10
    fake_date: str | None = None  # 模拟终端时钟错误, 如 "2020-01-01"


@app.post("/api/offline/issue")
def offline_issue(req: OfflineIssueRequest) -> dict:
    """离线发号(纯终端本地行为: 只读写本地水位文件, 不碰数据库)。"""
    _ensure_offline()
    import app.offline as off

    try:
        return off.issue(req.site_id, req.count, req.fake_date)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class OfflineAllocateRequest(BaseModel):
    site_id: str
    size: int = 10_000


class OfflineRegisterRequest(BaseModel):
    site_id: str   # 形如 S03(1-3 位字母数字)
    name: str = ""


@app.post("/api/offline/register")
def offline_register(req: OfflineRegisterRequest) -> dict:
    """注册新的离线站点(分配首个配额区间; 模拟装机时烧录站点号 + 下发配额)。"""
    _ensure_offline()
    import app.offline as off

    site_id = req.site_id.strip().upper()
    if not (1 < len(site_id) <= 4 and site_id.isalnum()):
        raise HTTPException(status_code=400, detail="site_id 需为 2-4 位字母数字, 如 S03")
    try:
        site = off._register(site_id, req.name.strip() or f"{site_id} 打印站")
        return {"site_id": site["site_id"], "quota": [site["quota_start"], site["quota_end"]]}
    except FileExistsError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/offline/allocate")
def offline_allocate(req: OfflineAllocateRequest) -> dict:
    """中心为站点追加新配额(模拟: 登记中心库 + 随摆渡下发到站点)。"""
    _ensure_offline()
    import app.offline as off

    try:
        return off.allocate(req.site_id, req.size)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/api/offline/import/{site_id}")
def offline_import(site_id: str) -> dict:
    """摆渡导入: 待摆渡队列灌入中心库, 唯一索引做冲突检测。"""
    _ensure_offline()
    import app.offline as off

    try:
        return off.ferry_import(site_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/api/offline/demo-collision")
def offline_demo_collision(count: int = 50) -> dict:
    """反面教材: 两个没嵌站点号的"裸奔终端"互相撞号, 被中心唯一索引拦下。"""
    _ensure_offline()
    import app.offline as off

    return off.demo_collision(count)


@app.post("/api/offline/reset")
def offline_reset() -> dict:
    """清空中心表与站点本地状态, 重建 S01/S02。"""
    global _offline_ready
    _require_postgres()
    import app.offline as off

    off.init(reset=True)
    _offline_ready = True
    return {"ok": True}
