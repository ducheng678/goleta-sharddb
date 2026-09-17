# sharddb

`sharddb` 是一个只使用 Python 标准库的、可观察的小型事务 KV 数据库演示项目。它模拟三个固定逻辑分片 A/B/C、真实隔离的 worker 进程、异步 host action、崩溃重启、消息延迟/重复/乱序，以及单分片迁移与 A/B 原子重平衡。

它不是生产数据库；重点是让一致性与恢复过程可重复地被观察和测试。

## 快速开始

Linux / CPython 3.13：

```bash
cd /workspace/project
python3.13 -m pip install -e .
python3.13 -m sharddb.devhost --help
python3.13 -m sharddb.devhost run scenarios/healthy_migration.json
python3.13 -m unittest discover -s tests -v
```

运行器不需要安装第三方运行库。每次未指定 `--output` 的运行都会创建一个新的 `repro-runs/<场景>-<时间>-<随机值>/` 目录。若希望指定位置，`--output` 可以放在 `run` 前或后：

```bash
python3.13 -m sharddb.devhost --output /tmp/demo run scenarios/rebalance_exchange.json
python3.13 -m sharddb.devhost run scenarios/rebalance_exchange.json --output /tmp/demo2
```

退出码为：`0` 场景完成；`1` 业务断言或逻辑轮次失败；`2` JSON、场景或 host 输入错误。

Windows 用户请使用 WSL：

```powershell
wsl --install -d Ubuntu
wsl
sudo apt update && sudo apt install -y build-essential curl libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev libffi-dev liblzma-dev
cd /tmp
curl -O https://www.python.org/ftp/python/3.13.0/Python-3.13.0.tgz
tar xf Python-3.13.0.tgz && cd Python-3.13.0
./configure --prefix=/opt/cpython-3.13 --with-ensurepip=install
sudo make altinstall
cd /mnt/c/path/to/project
/opt/cpython-3.13/bin/python3.13 -m sharddb.devhost run scenarios/healthy_migration.json
```

也可以使用 Docker（Docker Desktop 已启用 Linux containers）：

```powershell
docker build -t sharddb .
docker run --rm -v "${PWD}:/work" sharddb run scenarios/healthy_migration.json
```

## 已附带的可执行场景

| 场景 | 覆盖内容 |
|---|---|
| `healthy_migration.json` | 跨 A/B 转账、同 ID 重试、C 写、A 单迁移、旧源下线后的目标服务、最终快照 |
| `rebalance_exchange.json` | A/B 原子交换，然后进行新的跨分片写和快照 |
| `rebalance_reuse.json` | 同驻、再次分离、回到旧 owner，以及早期计划的历史结果重试 |
| `offline_coord_recovery.json` | 已接纳跨分片事务时 coordinator 离线，A 仍可迁移，C 写/读继续服务，恢复后结清 |
| `faulty_rebalance.json` | 目的 worker 崩溃重启、真实 `PLAN_INSTALL` 迟到重复、CAS 已持久但 callback 丢失、后续计划不被旧消息覆盖 |
| `lost_persist_completion.json` | 本地 `persist` 已落盘但 completion 丢失时，maintenance 重新读取镜像并完成原请求 |
| `read_release_recovery.json` | 丢弃一条真实 A/B snapshot 的 `READ_RELEASE`、重启 coord，再重试 A 写；这是释放责任恢复回归场景 |
| `read_release_recovery_final.json` | 上述故障路径后同 ID 重试与全量实际 snapshot，核查 `a=101,b=100,c=0` |
| `read_release_late_duplicate.json` | coord 重启后投递真实晚到重复 `READ_RELEASE`，确认不重新阻塞写入或重复增量 |
| `read_release_migration_epoch.json` | 释放丢失时让 A 实际迁移、再重启 coord；以新 owner epoch 重传释放并核查目标端写入 |
| `baseline_v1_fresh.json` | 由 host 以公开 v1 初始布局创建的新库加载、转换、崩溃恢复 |
| `conditional_transactions.json` | 单/跨分片条件提交与中止、条件键非修改键、重试、A/B 同驻与重启恢复 |
| `checkpoint_seed.json` + `checkpoint_continue.json` | 真实导出已结清检查点、恢复后旧事务/计划重试、新条件事务 |

例如一次完整故障运行：

```bash
python3.13 -m sharddb.devhost run scenarios/faulty_rebalance.json
python3.13 -m sharddb.devhost run scenarios/read_release_recovery.json --output /tmp/sharddb-read-release
python3.13 -m sharddb.devhost check /tmp/sharddb-read-release
```

输出目录包含：

- `result.json`：成功/失败、轮次、权威 owner 和实际响应；
- `history.json`：实际请求—响应历史；
- `trace.jsonl`：逐轮 event、真实 host action、消息、timer、故障与 response 轨迹；
- `stores/<worker>/image.json`：每个独立 worker 的实际持久化镜像；
- `metadata.json`：线性化 owner 元数据服务的状态。
- `evidence.json`：保留调用轮次、响应、topology、初始值和最终 owner 的离线核查证据；旧的 `history.json` 响应列表仍保留。

## 条件事务

新增请求严格为：

```json
{"op":"conditional_txn","txn_id":"ct1","expected":{"a":100,"b":100},"delta_map":{"a":-7,"b":7}}
```

响应与 `txn`/`status` 相同：`{"txn_id":"ct1","status":"COMMITTED"|"ABORTED"|"UNKNOWN"}`。`expected` 和 `delta_map` 都必须是非空的 `key -> exact integer` 映射；两组键可以不同。参与逻辑分片是两组键的并集，所以仅比较 B、修改 A 仍是 A/B 跨分片事务，即使二者物理同驻也仍由 `coord` 协调。

每个参与分片先取得同一个持久写锁，再在锁保护下比较自己的条件键；任何比较失败都会让协调者持久写入 `ABORT`，不改变任何业务值。全部成功才会持久 `COMMIT` 并应用完整增量。重试按完整请求绑定 ID，绝不会以当前值重新判断。

摘要规则是：原 `txn` 继续使用 `SHA256(C({"op":"txn","txn_id":t,"delta_map":d}))`；条件事务使用 `SHA256(C({"op":"conditional_txn","txn_id":t,"expected":e,"delta_map":d}))`。`C` 是项目原有的严格 canonical JSON 编码，因此新字段、浮点数、布尔伪整数和重复键均会被拒绝。

## 已结清检查点与恢复

导出来自一次真实运行，不复制内存或伪造磁盘：

```bash
python3.13 -m sharddb.devhost run scenarios/checkpoint_seed.json \
  --output /tmp/sharddb-seed --checkpoint-out /tmp/sharddb-checkpoint

python3.13 -m sharddb.devhost run scenarios/checkpoint_continue.json \
  --restore /tmp/sharddb-checkpoint --output /tmp/sharddb-continued
```

恢复用的 continuation 文件顶层只能有 `steps`，因为 initial topology、初始值、业务 ID、owner 向量及序号高水位均从检查点取得。示例会重试已提交 `seed-txn` 与已完成 `seed-together`，随后提交新的条件事务，最终得到 `a=9,b=11,c=0`，没有重复旧增量。

导出前 host 最多实际推进 10,000 轮以排空 worker FIFO、消息、completion 和已接纳 action，并检查所有事务终态、读锁/写锁、打开的读记录和搬迁回复。仍有责任时导出明确失败；仅未到期的 maintenance timer 可被取消。检查点含 `checkpoint.json`（版本、来源、topology、身份与高水位）和每个镜像/元数据的 SHA256 清单。恢复先校验版本、完整文件与哈希，然后复制到新的运行输出目录；检查点本身不被改写，新的 worker 都用更高 incarnation 启动，旧 request token 不会复活。

这证明的是本项目自身的已结清检查点恢复，不是外部 baseline v1 历史 fixture 的证明。

## 离线一致性检查

```bash
python3.13 -m sharddb.devhost check /tmp/sharddb-continued
```

该命令不启动 worker，也不写入目标目录。它输出机器可读 JSON，退出码为 `0` 通过、`1` 一致性失败、`2` 输入或证据不足。检查内容包括：请求/响应形状和 ID、同 ID 终态一致性、公开 Decision journal 的摘要及结果、每个权威分片的 exactly-once Applied/Aborted 记录、最终权威值，以及最多六个写和八个成功快照的共同严格串行历史搜索。条件事务只有在同一搜索位置满足其全部 `expected` 才能解释为提交；UNKNOWN 不被当作 ABORTED。

检查器必须读取 `evidence.json`；没有调用时间等必要证据的旧运行目录会明确以退出码 `2` 报告，绝不会静默通过。测试还会在名称明确为 `test-copy-*` 的人工副本上验证错误响应、冲突 Decision 与半事务快照都被拒绝；这些副本不是数据库实际产生的故障。

## 批量场景

```bash
python3.13 -m sharddb.devhost suite scenarios/suite.json --output /tmp/sharddb-suite
python3.13 -m sharddb.devhost suite scenarios/suite_mixed.json --output /tmp/sharddb-suite-mixed
```

清单格式为：

```json
{"scenarios":[{"name":"healthy_migration","file":"healthy_migration.json"}]}
```

路径相对清单目录解析，每项都在 `<output>/<name>/` 中启动独立 worker、镜像和历史，随后运行离线 checker。`suite.json` 覆盖健康迁移、重平衡和条件事务；`suite_mixed.json` 证明一个有意的业务断言失败不会阻止后续条件事务场景执行。汇总写入 `suite.json`，列出每项 run/check 退出码、轮次、输出和错误。整体码是全成功 `0`、业务/一致性失败 `1`、输入/host/证据错误 `2`（优先于 `1`）；重名或已有输出目录会被拒绝。

## 场景格式与故障注入

新库场景是严格 JSON，顶层只能有 `initial` 和 `steps`。`initial` 提供 `keys` 与逐键 `key_shards`，可选 `roles` 和初始 `epochs`；`--restore` 的 continuation 则只能有 `steps`。业务请求严格对应公开接口，例如：

```json
{"action":"request","label":"t1","request":{"op":"txn","txn_id":"t1","delta_map":{"a":-7,"b":7}}}
```

常用 step 如下：

- `{"action":"await","label":"t1","max_rounds":1000}`：等待某次实际 reply。
- `{"action":"rounds","count":20}`：推进确定性的逻辑轮次。
- `{"action":"crash","worker":"coord"}` / `restart`：销毁并创建真实子进程；内存、队列、timer 和旧 token 消失，文件镜像保留。
- `{"action":"network_rule","rule":{"type":"PLAN_INSTALL","duplicate":1,"late_duplicate":180,"count":1}}`：对下一条匹配的真实发送复制一个晚到副本。规则也支持 `delay`、`drop`、`channel:"response"`。
- `{"action":"drop_completion","op":"cas_owners","count":1}`：执行 host action，但不向该 incarnation 投递 completion，用于验证持久结果的恢复读取。
- `assert_response`、`assert_values` 与 `assert_owners`：从实际 reply 或元数据断言，不读取业务磁盘伪造结果。

每轮每个健康 worker 最多处理一个 FIFO event，按 `coord,a_old,a_target,b,c` 顺序执行。worker callback 产生的 action 在轮末执行，消息和 completion 最早下一轮进入队列。`maintenance` timer 在 boot 和每次维护 timer 中严格以 30 轮重新设置。

## 设计概要

`sharddb.worker` 是被 host 启动的独立 Python 进程；它只经 stdin/stdout 接收 event 和发出 host action。业务引擎在 `sharddb/engine/node.py`，没有文件、进程、线程或网络调用。

- 单分片事务把值、终态和公开 `Decision` journal 一次持久化。
- 跨分片事务先持久预备写锁；协调者持久化 `COMMIT`/`ABORT` Decision 后才广播。分片把一次应用和本地 Decision 原子持久化，因此重复消息和重试不会二次生效。
- 条件事务将条件键和修改键共同锁定；条件比较与提交增量由同一 prepare/decision 状态机保护。
- 跨分片 snapshot 先取得持久读锁。它遇到未完成写时不返回混合值；成功值来自同一组锁保护的状态。关闭读的 grant/release 责任也持久化：`maintenance` 每 30 轮会向尚未确认的 owner epoch 重送幂等 `READ_RELEASE`，因此丢失释放消息后 coord 重启不会永久遗留读锁；重复或晚到释放只确认已清除的锁，绝不会重建它。确认带 shard epoch，旧 epoch 的迟到释放不会清除迁移后新 owner 的锁。
- 迁移先冻结源端并持久安装整个逻辑分片状态（值、事务、锁、读锁、终态与历史计划）到每一个目的端。仅全部目的端 ready 后才执行一项 `cas_owners`，故 A/B 不会观察到半个 placement。
- 已完成计划随以后分片 handoff 携带，所以较早 `plan_id` 的重试返回原 replacement vector，而不会撤销新归属。
- 检查点只在已结清边界复制实际 host 镜像，并保存身份高水位和哈希；恢复不会重放旧 token 或覆盖新 owner 的状态。

镜像的普通私有状态放在 `data["sharddb/v2"]`；终态始终通过 ABI `public_decisions` 追加。host 初始库采用公开的 `baseline/v1` 布局，engine 在第一次需要写入时转换为 v2。项目没有捏造“历史 baseline 引擎执行前缀”：附带 v1 场景只验证真实新库的公开 v1 genesis 布局与后续恢复。没有随附件提供的旧引擎或已证明可达的 v1 迁移 checkpoint，因此没有把手写历史镜像宣称为兼容性证明。

## 验证边界

`unittest` 覆盖上述全部场景、严格 JSON 重复键拒绝、完成迁移后的旧源下线、跨 coordinator 故障恢复、worker 重启、丢 completion 和迟到重复真实消息。另有 `read_release_recovery.json`（报告的释放丢失后 coord 重启路径）及其值核查、迟到重复消息同伴场景；二者均以真实 worker、消息和持久镜像执行。

项目实现的是题目给定的小规模边界（最多八键、有限事务和三项顺序重平衡），没有实现 SQL、复制仲裁、任意分片拆分、磁盘损坏或永久丢失必要节点。v1 loader 已保守读取公开的 values、Applied/Aborted/Intent/ReadGrant/TxnBegin/Migration 记录；这里没有官方 baseline 生成器，因此未验证由未知旧实现生成的所有复杂未决 v1 fixture 的字节级可达性。
