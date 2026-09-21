"""离线摆渡场景模拟(气隙网络 / sneakernet)

模拟"打印设备在监狱等无法联网环境, 数据定期经光盘/U 盘摆渡回中心"的离线发号:

三层防线(核心设计):
  1. 站点号嵌入 ID(空间防撞):   ID = LB{日期}S{站点号}{序号}, 站点号中心登记、全局唯一
  2. 本地持久水位(时间防重):    序号 = 本地单调递增水位, 落本地 JSON 文件,
                                断电/重启不丢; 即使终端时钟错了, 唯一性也不受影响
                                (日期字段只作展示, 不承担唯一性 —— 水位才是唯一真相)
  3. 中心预分配配额(重装防丢):  每站点一个配额区间, 摆渡回来上报用量、
                                随下次摆渡下发新配额; 重装机器时从配额区间尾部续号,
                                宁可跳一段, 绝不回退

最终兜底: 摆渡导入中心库时 label_id 唯一索引冲突检测(正常应为零冲突;
demo_collision 演示"裸奔站点"——没嵌站点号的两台终端互相撞号的全过程)。

状态存放(刻意模拟离线现实):
  - 站点侧(发号/待摆渡队列): data/offline/*.json  ← 模拟终端本地盘, 完全不碰数据库
  - 中心侧(导入结果/配额登记): PostgreSQL 表       ← 只有摆渡导入时才接触
"""

import json
import threading
import time
from pathlib import Path

import psycopg2

from app.config import PG_DATABASE, PG_HOST, PG_PASSWORD, PG_PORT, PG_USER

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "offline"
_LOCK = threading.Lock()  # 保护站点状态文件的读改写(同进程模拟多终端并发操作)

DEFAULT_QUOTA_SIZE = 10_000  # 每次分配的配额大小(演示用小值; 真实=摆渡周期预估用量)


# ---------------------------------------------------------------- 基础设施

def _connect():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER,
        password=PG_PASSWORD, dbname=PG_DATABASE, connect_timeout=2,
    )


def _site_path(site_id: str) -> Path:
    return DATA_DIR / f"site_{site_id}.json"


def _load_site(site_id: str) -> dict:
    p = _site_path(site_id)
    if not p.exists():
        raise KeyError(f"站点 {site_id} 未注册")
    return json.loads(p.read_text(encoding="utf-8"))


def _save_site(site: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    _site_path(site["site_id"]).write_text(
        json.dumps(site, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------- 初始化 / 清理

def init(reset: bool = False) -> None:
    """中心建表 + 初始化两个演示站点(S01/S02, 各分配首个配额区间)"""
    conn = _connect()
    try:
        with conn.cursor() as cur:
            # 中心标签总表: 唯一索引就是最终兜底
            cur.execute("""
                CREATE TABLE IF NOT EXISTS central_labels (
                    label_id    TEXT PRIMARY KEY,
                    site_id     TEXT NOT NULL,
                    batch_no    INT  NOT NULL,
                    local_ts    TEXT NOT NULL,
                    imported_at TIMESTAMP DEFAULT NOW()
                )""")
            # 配额登记表: 全局单调推进, 新配额永远接在所有已分配区间之后
            cur.execute("""
                CREATE TABLE IF NOT EXISTS site_quota_log (
                    site_id     TEXT NOT NULL,
                    seg_start   BIGINT NOT NULL,
                    seg_end     BIGINT NOT NULL,
                    allocated_at TIMESTAMP DEFAULT NOW()
                )""")
            # 不规范站点(反面教材)的导入记录单独存放, 不污染正常数据
            cur.execute("""
                CREATE TABLE IF NOT EXISTS central_labels_bad (
                    label_id    TEXT PRIMARY KEY,
                    site_id     TEXT NOT NULL,
                    imported_at TIMESTAMP DEFAULT NOW()
                )""")
        conn.commit()
    finally:
        conn.close()

    if reset:
        conn = _connect()
        try:
            with conn.cursor() as cur:
                for tbl in ("central_labels", "site_quota_log", "central_labels_bad"):
                    cur.execute(f"TRUNCATE {tbl}")
            conn.commit()
        finally:
            conn.close()
        # 清空站点状态文件后重建演示站点
        if DATA_DIR.exists():
            for p in DATA_DIR.glob("site_*.json"):
                p.unlink()
        _register("S01", "1号打印站")
        _register("S02", "2号打印站")


def _max_quota_end() -> int:
    """全局已分配配额的最大终点(空表返回 0)"""
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(max(seg_end), 0) FROM site_quota_log")
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _register(site_id: str, name: str) -> dict:
    """注册新站点并分配首个配额区间(模拟: 真实流程是装机时烧录+随摆渡下发)"""
    with _LOCK:
        if _site_path(site_id).exists():
            raise FileExistsError(f"站点 {site_id} 已注册")
        site = {
            "site_id": site_id, "name": name,
            "quota_start": _max_quota_end() + 1,
            "quota_end": _max_quota_end() + DEFAULT_QUOTA_SIZE,
            "watermark": _max_quota_end(),   # 水位 = 已发到的最大序号, 单调不回退
            "issued_count": 0,
            "ferry_queue": [],               # 待摆渡队列: [{label_id, local_ts}]
            "batch_no": 0,
        }
        conn = _connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO site_quota_log (site_id, seg_start, seg_end) VALUES (%s,%s,%s)",
                    (site_id, site["quota_start"], site["quota_end"]))
            conn.commit()
        finally:
            conn.close()
        _save_site(site)
        return site


# ---------------------------------------------------------------- 站点侧(离线终端行为, 纯本地)

def issue(site_id: str, count: int, fake_date: str | None = None) -> dict:
    """离线发号: 完全在"终端本地"完成(读写本地 JSON, 不碰数据库)

    :param fake_date: 模拟终端时钟错误(如 '2020-01-01'), 演示
                      "日期字段错乱不影响唯一性 —— 水位才是唯一真相"
    """
    count = max(1, min(count, 5000))
    with _LOCK:
        site = _load_site(site_id)
        # 配额检查: 水位 + count 不得越过配额区间(耗尽需摆渡下发新配额)
        if site["watermark"] + count > site["quota_end"]:
            raise RuntimeError(
                f"站点 {site_id} 配额不足: 水位 {site['watermark']}, "
                f"配额区间 [{site['quota_start']}, {site['quota_end']}], "
                f"剩余 {site['quota_end'] - site['watermark']} 个 — "
                "需要中心随下次摆渡下发新配额(allocate)")
        # 日期只作展示; 时钟错了(时钟回拨/漂移/被拨错)只影响这 6 位,
        # 序号来自持久化水位, 永不回退 → 唯一性免疫时钟问题
        date_str = fake_date.replace("-", "")[2:] if fake_date else time.strftime("%y%m%d")
        ids = []
        for _ in range(count):
            site["watermark"] += 1
            # ID = LB + 日期 + 站点号(S01) + 8位序号, 如 LB260921S0100000042
            label_id = f"LB{date_str}{site_id}{site['watermark']:08d}"
            site["ferry_queue"].append({"label_id": label_id, "local_ts": time.strftime("%Y-%m-%d %H:%M:%S")})
            ids.append(label_id)
        site["issued_count"] += count
        _save_site(site)
    return {"site_id": site_id, "issued": ids[:100], "total": count,
            "watermark": site["watermark"], "quota_remaining": site["quota_end"] - site["watermark"]}


def allocate(site_id: str, size: int = DEFAULT_QUOTA_SIZE) -> dict:
    """中心为站点追加新配额(模拟: 登记在中心库, 并"随摆渡下发"写入站点本地文件)"""
    size = max(100, min(size, 1_000_000))
    with _LOCK:
        site = _load_site(site_id)
        new_start = _max_quota_end() + 1
        conn = _connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO site_quota_log (site_id, seg_start, seg_end) VALUES (%s,%s,%s)",
                    (site_id, new_start, new_start + size - 1))
            conn.commit()
        finally:
            conn.close()
        # 站点直接扩展到新区间(真实流程: 光盘下发 → 站点导入脚本写入本地配置)
        site["quota_end"] = new_start + size - 1
        _save_site(site)
        return {"site_id": site_id, "new_quota": [new_start, new_start + size - 1],
                "quota_end": site["quota_end"]}


# ---------------------------------------------------------------- 摆渡导入(中心侧)

def ferry_import(site_id: str) -> dict:
    """摆渡导入: 把站点待摆渡队列灌入中心库, 唯一索引做冲突检测。

    使用 ON CONFLICT DO NOTHING + 逐批比对, 统计实际冲突清单
    (正常应为零冲突 —— 前三层防线生效时, 这里一次都不该触发)。"""
    with _LOCK:
        site = _load_site(site_id)
        queue = site["ferry_queue"]
        if not queue:
            return {"site_id": site_id, "imported": 0, "conflicts": [],
                    "verdict": "待摆渡队列为空(先 issue 发号)"}
        conn = _connect()
        try:
            with conn.cursor() as cur:
                # 先查: 队列中已存在于中心的(冲突候选)
                labels = [r["label_id"] for r in queue]
                cur.execute(
                    "SELECT label_id FROM central_labels WHERE label_id = ANY(%s)", (labels,))
                existing = {r[0] for r in cur.fetchall()}
                conflicts = [l for l in labels if l in existing]
                fresh = [l for l in labels if l not in existing]
                # 新记录批量插入(batch_no 递增, 模拟每次摆渡一个批次)
                batch_no = site["batch_no"] + 1
                cur.executemany(
                    "INSERT INTO central_labels (label_id, site_id, batch_no, local_ts) "
                    "VALUES (%s, %s, %s, %s)",
                    [(r["label_id"], site_id, batch_no, r["local_ts"]) for r in queue
                     if r["label_id"] not in existing])
            conn.commit()
        finally:
            conn.close()
        # 导入完成: 清空待摆渡队列
        site["ferry_queue"] = []
        site["batch_no"] = batch_no
        _save_site(site)
        return {
            "site_id": site_id, "batch_no": batch_no,
            "imported": len(fresh), "conflicts": conflicts[:10], "conflict_count": len(conflicts),
            "verdict": (
                f"摆渡批次 {batch_no}: {len(fresh)} 条导入成功, {len(conflicts)} 条冲突。"
                + ("三层防线生效, 零冲突 —— 冲突清单永远不该出现, 出现即说明防线破了。"
                   if not conflicts else
                   "出现冲突! 有防线失效, 需要排查(演示中通常是反面教材场景)。")
            ),
        }


# ---------------------------------------------------------------- 反面教材: 裸奔站点撞号

def demo_collision(count: int = 50) -> dict:
    """两个"不规范实现"的终端: ID 没嵌站点号, 各自从 1 自增 → 摆渡导入互相撞号。

    与正常站点(LB...S01/S02...)格式不同, 不会污染正常数据。"""
    count = max(1, min(count, 500))
    # 两个裸奔终端各自本地发号(内存模拟即可, 撞号发生在导入时)
    date_str = time.strftime("%y%m%d")
    site_x = [f"LB{date_str}{i:08d}" for i in range(1, count + 1)]
    site_y = [f"LB{date_str}{i:08d}" for i in range(1, count + 1)]  # 完全相同的号

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE central_labels_bad")
            # X 终端先导入(全部成功)
            cur.executemany(
                "INSERT INTO central_labels_bad (label_id, site_id) VALUES (%s, 'X') "
                "ON CONFLICT DO NOTHING",
                [(l,) for l in site_x])
            # Y 终端再导: 已被 X 占用的号被唯一索引拦下, 其余进入中心库
            inserted_y = set()
            for label in site_y:
                cur.execute(
                    "INSERT INTO central_labels_bad (label_id, site_id) VALUES (%s, 'Y') "
                    "ON CONFLICT DO NOTHING RETURNING label_id", (label,))
                row = cur.fetchone()
                if row:
                    inserted_y.add(row[0])
            conflicts = [l for l in site_y if l not in inserted_y]
        conn.commit()
    finally:
        conn.close()
    return {
        "site_x_issued": count, "site_y_issued": count,
        "imported": count + len(inserted_y), "conflict_count": len(conflicts),
        "conflict_samples": conflicts[:5],
        "verdict": (
            f"两台终端都没嵌站点号、各自从 1 发号: 共 {count * 2} 条数据, "
            f"只有 {count + len(inserted_y)} 条能进中心库, {len(conflicts)} 条撞号"
            "被唯一索引在导入时拦下。裸奔终端靠'事后检测'已经太晚 —— 摆渡周期是"
            "天/周级, 发现时标签早已打印流通, 只能作废重打; 这就是为什么唯一性"
            "必须在'事前'由站点号+水位保证, 而不是指望中心事后发现。"
        ),
    }


# ---------------------------------------------------------------- 状态查询

def sites() -> list[dict]:
    """站点总览: 配额区间/水位/余量/待摆渡数"""
    out = []
    if DATA_DIR.exists():
        for p in sorted(DATA_DIR.glob("site_*.json")):
            s = json.loads(p.read_text(encoding="utf-8"))
            out.append({
                "site_id": s["site_id"], "name": s["name"],
                "quota": [s["quota_start"], s["quota_end"]],
                "watermark": s["watermark"],
                "quota_remaining": s["quota_end"] - s["watermark"],
                "issued_count": s["issued_count"],
                "ferry_pending": len(s["ferry_queue"]),
                "batch_no": s["batch_no"],
                "recent_ids": [r["label_id"] for r in s["ferry_queue"][-3:]],
            })
    return out


def central_stats() -> dict:
    """中心库统计: 导入总量/按站点/按批次"""
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM central_labels")
            total = cur.fetchone()[0]
            cur.execute(
                "SELECT site_id, count(*), max(batch_no) FROM central_labels GROUP BY site_id")
            by_site = [{"site_id": r[0], "rows": r[1], "batches": r[2]} for r in cur.fetchall()]
            cur.execute("SELECT count(*) FROM site_quota_log")
            quotas = cur.fetchone()[0]
            cur.execute("SELECT max(seg_end) FROM site_quota_log")
            max_end = cur.fetchone()[0] or 0
        return {"total_imported": total, "by_site": by_site,
                "quota_segments": quotas, "global_watermark_ceiling": max_end}
    finally:
        conn.close()
