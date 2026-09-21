"""分库分表与 ID 生成关系模拟(教学演示, 参数全部可自定义, 数据量刻意小)

用 PostgreSQL 的多个 schema(shard0..shardN)模拟分片, 四个经典场景各自独立、
可反复重放(每次运行自动清空重置)。每个场景只使用与该场景匹配的 ID 方法,
不强行套用全部 8 种方案 —— 教学目标是讲清"ID 生成 × 分片路由"的耦合点。

场景 1  独立自增(反面教材): 各分片各自从 1 自增 → 跨分片必然撞号
场景 2  步长自增:   分片 i 用 START=i+1, INCREMENT=N → 零协调实现全局唯一, 但扩容困难
场景 3  基因法:     订单 ID 低位嵌入用户基因(Snowflake 作基底) → 仅凭 ID 就能路由
场景 4  range 热点: 按月 range 分片 + 单调 ID(对比 hash + 随机 ID)→ 写热点可视化

所有参数(分片数/行数/用户数/热点占比)均可由调用方自定义, 便于教学时
现场改参数观察效果(例如: 把分片数从 4 改成 8, 亲眼看步长法为什么扩容困难)。
"""

import zlib

import psycopg2

from app.config import PG_DATABASE, PG_HOST, PG_PASSWORD, PG_PORT, PG_USER

DEFAULT_SHARDS = 4        # 默认分片数
DEFAULT_ROWS = 1000       # 场景1/2 默认每分片行数
MAX_SHARDS = 16           # 分片数上限(防误操作建太多 schema)
MAX_ROWS = 20_000         # 每分片行数上限(演示数据量控制)


def _connect():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER,
        password=PG_PASSWORD, dbname=PG_DATABASE, connect_timeout=2,
    )


def _shards(n: int) -> list[str]:
    return [f"shard{i}" for i in range(n)]


def _clamp(v, lo, hi, default):
    """参数夹紧: 非法值回退默认值(教学 demo 的健壮性, 不让奇怪参数报 500)"""
    try:
        v = int(v)
    except (TypeError, ValueError):
        return default
    return max(lo, min(hi, v))


# ---------------------------------------------------------------- 初始化 / 清理

def init(shard_count: int = DEFAULT_SHARDS) -> int:
    """建分片 schema 与各场景演示表(幂等, 只增不减; 返回当前 schema 总数)"""
    conn = _connect()
    try:
        # 先查已存在的分片 schema 数, 取 max(已有, 本次请求)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM information_schema.schemata "
                "WHERE schema_name LIKE 'shard%'")
            existing = int(cur.fetchone()[0])
        n = max(existing, _clamp(shard_count, 1, MAX_SHARDS, DEFAULT_SHARDS))
        with conn.cursor() as cur:
            for s in _shards(n):
                i = int(s[-1])
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
                # 场景1: 独立自增(各分片自己的序列, 都从 1 开始)
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {s}.labels_indep (
                        label_id TEXT PRIMARY KEY, seq BIGINT NOT NULL)""")
                cur.execute(f"CREATE SEQUENCE IF NOT EXISTS {s}.indep_seq START 1 INCREMENT 1")
                # 场景2: 步长自增(分片 i 从 i+1 开始, 步长 = 分片总数)
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {s}.labels_step (
                        label_id TEXT PRIMARY KEY, seq BIGINT NOT NULL)""")
                cur.execute(
                    f"CREATE SEQUENCE IF NOT EXISTS {s}.step_seq START %s INCREMENT %s",
                    (i + 1, n))
                # 场景3: 基因法订单表
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {s}.orders_gene (
                        order_id TEXT PRIMARY KEY, user_id BIGINT NOT NULL)""")
                # 场景4: range 按月分片
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {s}.labels_range (
                        label_id TEXT PRIMARY KEY, month INT NOT NULL)""")
        conn.commit()
        return n
    finally:
        conn.close()


def _reset_scenario(cur, n: int, table: str, seq: str | None = None,
                    seq_starts: list[int] | None = None) -> None:
    """场景自包含: 运行前清空自己的表并重置序列, 重复运行得到相同演示结果。

    seq_starts[i] = 分片 i 的序列重启值(步长场景每片起点不同)。"""
    for s in _shards(n):
        cur.execute(f"TRUNCATE {s}.{table}")
        if seq:
            start = seq_starts[int(s[-1])] if seq_starts else 1
            cur.execute(f"ALTER SEQUENCE {s}.{seq} RESTART WITH {start}")


def reset() -> None:
    """清空全部已建分片的数据并重置序列"""
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM information_schema.schemata "
                "WHERE schema_name LIKE 'shard%'")
            n = int(cur.fetchone()[0])
            for s in _shards(n):
                i = int(s[-1])
                for tbl in ("labels_indep", "labels_step", "orders_gene", "labels_range"):
                    cur.execute(f"TRUNCATE {s}.{tbl}")
                cur.execute(f"ALTER SEQUENCE {s}.indep_seq RESTART WITH 1")
                cur.execute(f"ALTER SEQUENCE {s}.step_seq RESTART WITH %s", (i + 1,))
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------- 场景 1: 独立自增 → 撞号

def scenario_independent(shards: int = DEFAULT_SHARDS,
                         rows_per_shard: int = DEFAULT_ROWS) -> dict:
    """各分片独立 AUTO INCREMENT 从 1 发号: 单分片内唯一, 跨分片大面积撞号。

    这是分库分表后"为什么不能继续用数据库自增"的最直接证明。"""
    n = _clamp(shards, 1, MAX_SHARDS, DEFAULT_SHARDS)
    init(n)  # 只保证 schema 建够, 场景按请求的分片数运行
    rows = _clamp(rows_per_shard, 10, MAX_ROWS, DEFAULT_ROWS)
    conn = _connect()
    try:
        with conn.cursor() as cur:
            _reset_scenario(cur, n, "labels_indep", "indep_seq")
            for s in _shards(n):
                # 子查询先取序列再拼 label_id, 避免 nextval 双取
                cur.execute(f"""
                    INSERT INTO {s}.labels_indep (label_id, seq)
                    SELECT 'L' || lpad(v::text, 8, '0'), v
                    FROM (
                        SELECT nextval('{s}.indep_seq') AS v
                        FROM generate_series(1, {rows})
                    ) t
                """)
        conn.commit()

        # 跨分片统计撞号: 同一 label_id 出现在几个分片
        with conn.cursor() as cur:
            union_sql = " UNION ALL ".join(
                f"SELECT label_id, '{s}' AS shard FROM {s}.labels_indep" for s in _shards(n))
            cur.execute(f"""
                SELECT label_id, count(DISTINCT shard) AS shards,
                       string_agg(shard, ',' ORDER BY shard) AS shard_list
                FROM ({union_sql}) u
                GROUP BY label_id
                HAVING count(DISTINCT shard) > 1
                ORDER BY label_id
            """)
            dup_rows = cur.fetchall()
        collisions = [{"label_id": r[0], "shards": r[2].split(",")} for r in dup_rows[:10]]
        return {
            "scenario": "independent",
            "params": {"shards": n, "rows_per_shard": rows},
            "total_rows": rows * n,
            "distinct_label_ids": rows,
            "collision_count": len(dup_rows),
            "collision_samples": collisions,
            "verdict": (
                f"{n} 个分片各自从 1 自增, 每个 label_id 在全部分片重复出现 → "
                f"{len(dup_rows)} 个 label_id 跨分片撞号(去重后只剩 {rows} 个号)。"
                "分库分表后数据库自增的唯一性只在分片内成立 —— 这就是需要分布式 ID 的原点。"
            ),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------- 场景 2: 步长自增

def scenario_step(shards: int = DEFAULT_SHARDS,
                  rows_per_shard: int = DEFAULT_ROWS) -> dict:
    """分片 i 用 START=i+1, INCREMENT=N 的序列: ID 集合互不相交, 零协调全局唯一。

    代价: 扩容(N→2N)必须迁移一半数据且原 ID 不能变; 跨分片 ID 交错, 全局无序。
    教学玩法: 先用 shards=4 跑一次, 再用 shards=8 跑一次, 观察号段区间变化。"""
    n = _clamp(shards, 1, MAX_SHARDS, DEFAULT_SHARDS)
    init(n)  # 只保证 schema 建够, 场景按请求的分片数运行
    rows = _clamp(rows_per_shard, 10, MAX_ROWS, DEFAULT_ROWS)
    conn = _connect()
    try:
        with conn.cursor() as cur:
            # 步长场景序列起点随分片数变化: 分片 i 起点 = i+1, 步长 = n
            _reset_scenario(cur, n, "labels_step", "step_seq",
                            seq_starts=[i + 1 for i in range(n)])
            for s in _shards(n):
                i = int(s[-1])
                # 序列步长在建 schema 时固定, 分片数变化时同步更新步长
                cur.execute(f"ALTER SEQUENCE {s}.step_seq INCREMENT BY {n}")
                cur.execute(f"""
                    INSERT INTO {s}.labels_step (label_id, seq)
                    SELECT 'L' || lpad(v::text, 8, '0'), v
                    FROM (
                        SELECT nextval('{s}.step_seq') AS v
                        FROM generate_series(1, {rows})
                    ) t
                """)
        conn.commit()

        shard_info = []
        with conn.cursor() as cur:
            union_sql = " UNION ALL ".join(
                f"SELECT label_id, seq, '{s}' AS shard FROM {s}.labels_step" for s in _shards(n))
            for s in _shards(n):
                cur.execute(f"SELECT min(seq), max(seq) FROM {s}.labels_step")
                mn, mx = cur.fetchone()
                shard_info.append({"shard": s, "min_seq": mn, "max_seq": mx})
            cur.execute(f"""
                SELECT count(*) FROM (
                    SELECT label_id FROM ({union_sql}) u GROUP BY label_id HAVING count(*) > 1
                ) d
            """)
            dup_groups = cur.fetchone()[0]
            cur.execute(f"SELECT count(*) FROM ({union_sql}) u")
            total = cur.fetchone()[0]
        return {
            "scenario": "step",
            "params": {"shards": n, "rows_per_shard": rows},
            "total_rows": total,
            "collision_count": dup_groups,
            "shards": shard_info,
            "verdict": (
                f"shard0 发 1,{n + 1},…; shard1 发 2,{n + 2},…; 交错而不相交, "
                f"{total} 个号全局唯一, 全程零协调、零外部依赖。"
                f"但扩容到 {n * 2} 分片需迁移一半数据(且原 ID 不能变), "
                "跨分片 ID 交错导致全局无序 —— 用'静态规划'换'运行时协调'的典型取舍。"
            ),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------- 场景 3: 基因法

def scenario_gene(shards: int = DEFAULT_SHARDS, users: int = 200,
                  orders_per_user: int = 10) -> dict:
    """订单 ID 低位嵌入用户基因(user_id % 64 占 6 bit), Snowflake 作基底:

        order_id = (snowflake_id << 6) | (user_id % 64)
        路由分片 = (order_id & 63) % 分片数   ← 仅凭 ID 就能定位数据所在分片

    效果: 同一用户的所有订单天然落同一分片, 查"用户的订单列表"无需广播/映射表。"""
    n = _clamp(shards, 1, MAX_SHARDS, DEFAULT_SHARDS)
    init(n)  # 只保证 schema 建够, 场景按请求的分片数运行
    users = _clamp(users, 4, 2000, 200)
    per_user = _clamp(orders_per_user, 1, 50, 10)
    conn = _connect()
    try:
        from app.generators.snowflake import SnowflakeGenerator
        gen = SnowflakeGenerator()  # machine_id 走 config 默认值
        with conn.cursor() as cur:
            _reset_scenario(cur, n, "orders_gene")
            for uid in range(1, users + 1):
                gene = uid % 64
                shard = f"shard{gene % n}"
                vals = []
                for _ in range(per_user):
                    oid = (int(gen.generate()) << 6) | gene
                    vals.append((str(oid), uid))
                cur.executemany(
                    f"INSERT INTO {shard}.orders_gene (order_id, user_id) VALUES (%s, %s)",
                    vals,
                )
        conn.commit()

        # 验证: 每个用户的订单是否 100% 落在同一分片
        with conn.cursor() as cur:
            union_sql = " UNION ALL ".join(
                f"SELECT order_id, user_id, '{s}' AS shard FROM {s}.orders_gene"
                for s in _shards(n))
            cur.execute(f"""
                SELECT count(*) FROM (
                    SELECT user_id FROM ({union_sql}) u
                    GROUP BY user_id HAVING count(DISTINCT shard) > 1
                ) split_users
            """)
            split_users = cur.fetchone()[0]
            cur.execute(f"SELECT count(*) FROM ({union_sql}) u")
            total = cur.fetchone()[0]
            dist = []
            for s in _shards(n):
                cur.execute(f"SELECT count(*) FROM {s}.orders_gene")
                dist.append({"shard": s, "rows": cur.fetchone()[0]})
            # 抽样演示"凭 ID 直接路由"(前 5 个用户各 1 单)
            cur.execute(f"""
                SELECT order_id, user_id, shard FROM ({union_sql}) u
                WHERE user_id <= 5 ORDER BY user_id, order_id
            """)
            route_demo = []
            for oid, uid, actual_shard in cur.fetchall():
                gene = int(oid) & 63
                routed = f"shard{gene % n}"
                route_demo.append({
                    "order_id_tail": oid[-12:], "user_id": uid,
                    "gene": gene, "routed_shard": routed,
                    "actual_shard": actual_shard, "hit": routed == actual_shard,
                })
        return {
            "scenario": "gene",
            "params": {"shards": n, "users": users, "orders_per_user": per_user},
            "total_rows": total,
            "users_split_across_shards": split_users,
            "shards": dist,
            "route_demo": route_demo,
            "verdict": (
                f"{users} 个用户 × {per_user} 单, 全部 100% 同用户同分片"
                f"(跨分片用户数 = {split_users}); 凭 ID 提取低 6 位基因即可直路由, 免广播。"
                "代价: 每类业务要设计自己的基因位, 且基因位数限制了分片数上限"
                "(6bit 基因 → 分片数只能是 64 的约数: 2/4/8/16/32/64)。"
            ),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------- 场景 4: range 热点

def scenario_hotspot(shards: int = DEFAULT_SHARDS, total: int = 4000,
                     latest_pct: int = 80) -> dict:
    """按月 range 分片(分片 i = 第 i+1 个月), 对比两种写入分布:

    a) 单调 ID + 业务活跃期集中: 最新分片承载 latest_pct% 的写入 → 热点
    b) hash 分片(uuid_v4 做 crc32 取模): 均匀散开, 但跨月范围查询要广播全部分片
    """
    n = _clamp(shards, 1, MAX_SHARDS, DEFAULT_SHARDS)
    init(n)  # 只保证 schema 建够, 场景按请求的分片数运行
    total = _clamp(total, n * 40, MAX_ROWS * n, 4000)
    pct = _clamp(latest_pct, 50, 99, 80)
    # 前面的分片平摊 (100-pct)%, 最新分片独占 pct%
    rest = max(1, round(total * (100 - pct) / 100))
    old_each = max(1, rest // (n - 1)) if n > 1 else 0
    month_rows = [old_each] * (n - 1) + [total - old_each * (n - 1)]
    monotonic_dist = [{"shard": f"shard{i}", "month": i + 1, "rows": month_rows[i]}
                      for i in range(n)]

    conn = _connect()
    try:
        from app.generators.snowflake import SnowflakeGenerator
        from app.generators.uuid_v4 import UUIDv4Generator
        gen = SnowflakeGenerator()
        with conn.cursor() as cur:
            _reset_scenario(cur, n, "labels_range")
            # a) 单调 ID 按 range 写入: 老月份少量历史, 最新分片热点
            for month, cnt in enumerate(month_rows, start=1):
                shard = f"shard{month - 1}"   # 月份 1..n → shard0..n-1
                vals = []
                for _ in range(cnt):
                    vals.append((gen.generate(), month))
                cur.executemany(
                    f"INSERT INTO {shard}.labels_range (label_id, month) VALUES (%s, %s)", vals)
        conn.commit()

        # b) hash 分片(uuid_v4 做 crc32 取模)同量数据的分布
        # 注: 不能用 Python 内置 hash() —— 字符串哈希带 PYTHONHASHSEED 随机化,
        # 进程重启后同一 ID 会路由到不同分片, 教学演示必须用稳定哈希
        ug = UUIDv4Generator()
        hash_counts = [0] * n
        seen = set()
        made = 0
        while made < total:
            u = ug.generate()
            if u in seen:
                continue
            seen.add(u)
            hash_counts[zlib.crc32(u.encode()) % n] += 1
            made += 1
        hash_dist = [{"shard": f"shard{i}", "rows": hash_counts[i]} for i in range(n)]

        return {
            "scenario": "hotspot",
            "params": {"shards": n, "total": total, "latest_pct": pct},
            "total_rows": total,
            "range_monotonic": monotonic_dist,
            "hash_random": hash_dist,
            "hotspot_pct": pct,
            "verdict": (
                f"range 分片 + 单调 ID: 最新分片承载 {pct}% 写入(热点); "
                f"hash 分片: {n} 片均匀(各约 {round(100 / n)}%), 但时间范围查询要广播全部分片。"
                "这正是 TiDB 用 auto_random 打散自增主键防热点的原因 —— "
                "'有序'在分布式存储里会从优点变成缺点。"
            ),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------- 总览统计

def stats() -> dict:
    """各场景各分片行数总览(基于实际已建的 schema 数)"""
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM information_schema.schemata "
                "WHERE schema_name LIKE 'shard%'")
            n = int(cur.fetchone()[0])
            out: dict[str, dict] = {}
            for tbl, key in [("labels_indep", "independent"),
                             ("labels_step", "step"),
                             ("orders_gene", "gene"),
                             ("labels_range", "hotspot")]:
                out[key] = {}
                for s in _shards(n):
                    cur.execute(f"SELECT count(*) FROM {s}.{tbl}")
                    out[key][s] = cur.fetchone()[0]
            out["shard_count"] = n
        return out
    finally:
        conn.close()
