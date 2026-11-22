# bcache — 按实际读取行为验证缓存的离线加工运行器

确定性作业在不可变资料快照上运行。缓存复用**不**比较“原返回 row_id 是否仍存在”，
而是在新快照上**按原始调用顺序重放全部读取**，确认每个可观察结果与登记的读取证据
逐字节相同，才允许复用原输出；第一个不一致立即判失效并真实重跑，不拼接旧路径。

## 设计要点

### 五种可区分的观察结果

| 观察 | 表现 |
| --- | --- |
| 表不存在 | `exists -> False`；`read`/`lookup` 为 `TableNotFound` |
| 表存在但为空 | `exists -> True`；`lookup -> empty_table`；`read -> []` |
| 表存在但缺键 | `lookup -> key_not_found` |
| 字段不存在 | `UnknownColumn`（与 NULL 不同） |
| 字段值为 NULL | 类型化 `None`（与缺字段 `MISSING` 不同） |

### 防碰撞的类型化编码（`bcache/encoding.py`）

所有值带域标签 + 长度前缀编码（BLAKE2b-256）：

- `true` ≠ `1`（bool 与 int64 不同域）
- `null` ≠ 缺字段（`TAG_NULL` ≠ `TAG_MISSING`）
- `"1"` ≠ `1`，拼接字符串 `("ab","c")` ≠ `("a","bc")`（uleb128 长度帧）

### 谓词三值逻辑（`bcache/predicate.py`）

等值/不等值、开闭范围（`gt ge lt le`）、`and/or/not`、`is_null/not_null`，
遵循 SQL 三值逻辑（真/假/未知）。bool 不与 int 比较相等；跨类型排序得“未知”。

### 读取证据与流式重放

- 真实执行时 `RecordingReader` 按序记录每个调用的参数、结局、结果摘要；
  `read` 的返回行**流式**写入证据 SQLite，不收齐大结果。
- 作业若提前放弃 `read` 迭代器（`break`），证据标记 `truncated=True` 且只登记
  实际消费的前缀；重放只比较该前缀，作业从未读到的后续行不参与失效判断。
- `ReplayValidator`（`bcache/replayer.py`）不运行作业代码，只按序重放：
  调用序列、负读取、schema（投影列类型漂移）、row_id 次序、逐行投影内容、
  多出/缺失行全部参与相等。命中行删除/修改、新增命中行、补上缺键、
  目录新增匹配分区都会失效；未投影列变更、谓词外行变更不影响。
- 重放按行从 SQLite 游标读取，验证峰值内存与结果规模无关。

### 快照代际固定

一次验证或执行开始时 `registry.pin()` 固定同一个不可变快照；期间其他进程发布
新快照只改 `current` 指针，不影响已 pin 的只读句柄，杜绝前后读取混代。

### 所有权、失败与确定性合同

- 跨进程 `flock` 串行化同一缓存库的执行权；竞争同一逻辑请求时恰好一次真实
  启动，其余进程复用先发布的条目，包文件经 `os.replace` 原子发布，无半包。
  进程被 SIGKILL 时 flock 随进程自动释放，残留 run 记录保持 `running` 而非
  被误标 `ok`，且不产生任何缓存条目或包文件。
- 作业抛错或被杀：工作目录清理、`runs` 记 `failed`/保持非 `ok`，**不产生缓存**。
- 同一作业版本 + 同一观察摘要得到不同输出：登记 `determinism_violations`，
  抛出 `DeterminismViolation`，冲突输出不提升、不任选一个当正确缓存。
- 作业版本 = 模块源码摘要 + 声明依赖摘要 + 规范化参数 + 运行环境标识。
- 声明了键列的关系在写入层强制键唯一（text/int64/bool 均校验，NULL 遵循
  SQL 语义互不相等）；派生快照继承该约束。

## 目录结构

```
bcache/            库代码
  encoding.py      类型化编码/摘要/流式 hasher
  predicate.py     三值逻辑谓词
  snapshot.py      不可变 SQLite 快照、builder、registry、pin
  errors.py        可观察异常
  reader.py        Reader 与 RecordingReader
  evidence.py      读取证据（SQLite 暂存，流式行）
  replayer.py      重放验证器（不运行作业）
  output.py        类型化输出关系包（.bpkg，自校验摘要）
  jobs.py          作业加载、版本固定、合作式合同执行
  store.py         缓存库、锁、所有权、确定性合同、run/inspect/why/verify
  materialize.py   时点快照（基准 + 无序更正日志 → 新快照）
  cli.py           命令行
jobs/              两个手工合同作业
tests/             单元测试、独立验收、并发测试、大快照生成器（无大数据入库）
bcache_run.py      python3 -I 下的顶层 CLI 入口
run_tests.py       隔离模式测试入口
```

## 命令行

```bash
python3 -I bcache_run.py run <job.py> --registry <reg> --cache <cache> \
    [--params '<json>'] [--snapshot-id <id>] [--runtime-id <id>]
python3 -I bcache_run.py why-recompute <job.py> --registry <reg> --cache <cache>
python3 -I bcache_run.py inspect <entry_id> --cache <cache>
python3 -I bcache_run.py verify-result <entry_id> --cache <cache>
python3 -I bcache_run.py materialize-as-of --registry <reg> --base <id> \
    --changes <log.jsonl> --effective-at <T> --known-through <S> --output-id <id>
```

## 时点快照：`materialize-as-of`

操作员从一份显式指定的基准快照和一份**无序**更正日志生成某个业务时点的
新快照。产物是普通不可变快照，现有作业用 `--snapshot-id <id>`（或
`CacheStore.run(..., snapshot_id=<id>)`）直接读取；**不**改动 `current`，
也不运行或改造缓存作业。

```bash
python3 -I bcache_run.py materialize-as-of \
    --registry /srv/bcache/registry \
    --base base-2026-11-15 \
    --changes corrections.jsonl \
    --effective-at 1763000000 \
    --known-through 42 \
    --output-id 2026-11-20-known-42
```

成功时打印 `output_id`、内容 `digest`、`outcome`（`published` 或
`unchanged`）与计数。退出码：`0` 成功；`2` 日志非法/参数非法；`3` 最终视图
键冲突；`4` 同一 ID 已有不同内容（拒绝且不覆盖）。

### 日志格式（UTF-8 JSONL）

每行一个对象：

```json
{"seq": 8, "table": "sales", "row_id": 1, "from": 10, "to": 40, "op": "put", "values": {"k": "B", "n": 11}}
{"seq": 14, "table": "sales", "row_id": 1, "from": 20, "to": 30, "op": "delete"}
```

- `seq`（全局唯一正整数）、`row_id`、`from`、`to` 均为有符号 64 位整数；
  布尔值不被当作整数。区间 `[from, to)`，`to:null` 表示无结束时间，有限区间
  需 `from < to`。
- `op` 为 `put`（必须带 `values`，且给出该表**全部列**的完整新值，允许显式
  `null`，沿用基准列类型）或 `delete`（不带 `values`）。
- 不支持增删表或改 schema；`table` 必须是基准中存在的表。

### 选择规则

仅 `seq <= S` 且区间包含 `T` 的记录参与选行；对每个 `(table, row_id)`，登记
序号最大者获胜；无候选则保留基准状态。获胜 `delete` 不输出该行，获胜 `put`
替换该行全部单元格。区间过期后，该行自动回落到仍适用的更早记录或基准值。

### 校验与安全

- 所有日志行都做输入/类型校验，包括本次时点不可见的更正；同 `seq` 的**相同**
  事件（按已解析且通过类型校验的字段比较，忽略 JSON 键顺序）允许重送，同号
  不同内容、重复 JSON 成员、未知字段或坏类型拒绝整次请求。
- lookup 键唯一性只针对**实际交付的最终视图**做集合式检查：键交换或删除一
  行由另一行接手其键，只要最终合法即成功，绝不按文件顺序逐条套用基准约束。
- 完整构建并校验成功后才把目录原子移入 registry，该 ID 才对读者可见；失败
  不留半成品，旧快照继续可读。同一 ID 相同内容返回成功（`unchanged`），不同
  内容拒绝且不覆盖。
- 日志逐行解析并落入临时 SQLite 暂存；只保留 T 的获胜者，基准逐表逐行流式
  复制，最终摘要也以流式游标计算——峰值内存不随日志条数、基准行数或获胜行
  数线性增长。逻辑内容相同的快照内容摘要与日志顺序、重送及输出 ID 无关。

## 作业合同

作业是 Python 模块，提供 `run(reader, sink, params)`，可选 `DEPENDENCIES`
（`(name, digest)` 列表）与 `RUNTIME_ID`。数据只能经 `reader` 访问：

```python
def run(reader, sink, params):
    rate = 10
    if reader.exists("rates"):
        r = reader.lookup("rates", "A", key_col="code")
        if r.status == "found":
            rate = r.value["value"]
    total = sum(row.values["amount"]
                for row in reader.read("sales", eq("region", "N"), ["amount"]))
    sink.declare([("total", "int64")])
    sink.write_row({"total": total * rate})
```

合作式合同：框架不提供文件系统/网络/时钟/随机数入口，但**不**声称能安全执行
恶意代码，作业是受信任的合作模块。

## 手工合同场景（`jobs/rate_sales.py`、`jobs/daily_parts.py`）

1. 起初 A 缺失，N 区 amount=[2,3] → 50；只改 S 区行或 N 区未投影备注 → 复用 50。
2. 补入 A=4 → 重跑得 20（`exists/lookup` 观察变化）。
3. 再补 N 区 amount=5 → 重跑得 40（检测到**新增命中行**，不只看旧两行）。
4. 分区作业：新增一个匹配的**空**分区，目录观察变化 → 必须重跑，即便数值不变。
5. 作业真实启动次数作为可观察证据（`launches` 与 `runs` 表）。

## 测试

```bash
# 默认套件（大快照测试用 5 万行，约 30 秒）
python3 -I run_tests.py

# 完整百万行（先生成快照，再跑流式测试）
python3 -I tests/generate_large.py --root /tmp/m --rows 1000000 --publish
BCACHE_LARGE_ROWS=1000000 python3 -I run_tests.py tests.test_large_streaming
```

独立验收（`tests/direct.py`）以**禁用缓存的直跑**作业务对照，并用**手写观察清单**
逐条推导复用/重跑决定，不调用产品失效判定器。覆盖：空查询后来有行、null 与缺表、
谓词边界、bool/int 区分、列类型变化、动态分支、相同结果但资料路径变化、验证中并发
发布、作业退出、SIGKILL 被杀、并发竞争、截断读取、键唯一性。大快照测试记录验证
峰值内存、实际重放行数与执行次数；仓库只含小夹具和生成器，不含巨量快照。

## 实测指标（百万行快照）

```
rows=1000000  predicate 命中=500000  实际重放比较行=500000
replay 峰值内存(tracemalloc)=0.04 MiB   真实作业启动=1（复用阶段 0）
```

验证内存与结果规模无关；百万行 SQLite 快照仅在运行时由
`tests/generate_large.py` 生成，不进入 Git。
