# HF V3 数据与独立后处理完整交接

核对日期：2026-11-14。此文用于在另一台机器下载**同一份历史快照**，
转换为精确 Parquet 与 Nautilus 可读事件。入口代码及其依赖均在本仓库，
不需要访问采集服务器，也不需要克隆私有 `market-data-collector` 仓库。

## 1. 数据在哪里、这次到底用了多少

| 项目 | 固定值 |
| --- | --- |
| HF 类型 | Dataset |
| HF 仓库 | `chchen13/forecastagent-data` |
| 路径前缀 | `production/markets1000-20261110-r2` |
| revision | `e0a7982b34e4b66c3254e58024e2f9efdd7497cf` |
| 发布布局 | `segments`，包装 `file-spool-publication/3` |
| 数据 schema / encoding | `market-backtest/2` / `market-backtest/2-delta-jsonl-zstd6` |
| 分块 | 1,174，源 sequence 从 1 到 67,326,978，连续 |
| 核心压缩文件总量 | 2,027,619,354 字节，约 2.03 GB / 1.89 GiB |
| 全量回放时间范围 | UTC 2026-11-10 13:48:46.727353113 至 2026-11-11 16:11:49.039553233 |
| 对应北京时间 | 2026-11-10 21:48:46.727353113 至 2026-11-12 00:11:49.039553233 |
| 回放时间跨度 | 26小时23分02.312200120秒 |
| 历史配对集合 | 221 对物理市场定义，442 个买入方向 |

[直接打开固定 HF 目录](https://huggingface.co/datasets/chchen13/forecastagent-data/tree/e0a7982b34e4b66c3254e58024e2f9efdd7497cf/production/markets1000-20261110-r2)。
若数据集要求鉴权，请在新机器使用自己的只读 HF 凭据及数据集访问权限。
策略仓库改为 public 不会改变 HF 数据集的权限。

完整机器清单见 [hf-v3-e0a7982.snapshot.json](data/hf-v3-e0a7982.snapshot.json)。
它列出每个分块的范围、manifest 路径、Git blob、SHA256、ZIP路径、大小和SHA256。
此次重新读取了 HF 固定 revision 的全部 manifest 列表，逐项核对了本地清单。

这个目录混合配对市场与独立市场。必须先保留完整分块与连续检查点，再按 header 的
pair 映射筛选。221是整段历史的去重集合，不是每时刻同时可交易的221对，也不代表
每对都覆盖26小时。此快照是该前缀在该版本的全部核心分块，不代表 HF 所有前缀或
后续新增数据，更不证明交易所所有成交均已收集。

源 sequence 是采集源记录编号，不是转换后的行数。全量研究回放记录了
112,516,997批 `OrderBookDeltas`、116,415条 `TradeTick`，另加121,402条研究用
`BookRefresh`。最后一种来自标准层的未变化盘口确认，普通 `BacktestDataset.events()`
只输出前两种原生事件。上述数量不是订单数或真实成交策略的收益证明。

## 2. 数据用途与边界

适合盘口重建、盘口可用性与价差统计、跨平台同题pair机会研究、时序回放及
在明确撮合假设下的离线策略实验。金融数值保留精确十进制，盘口同时有快照、增量、
基线、重置和刷新语义；成交保留可知时间、执行时间及已报告费用证据。

真实首块元数据含 Poly 的 `question`、`description`、`resolutionSource`、
`outcomes`，以及 Predict 的 `question`、`title`、`description`、`outcomes` 等。
字段出现情况以各市场历史版本为准，不能假定所有行都完整。pair映射本身不证明
两平台的异常结算、作废、延期和规则解释完全等价。

本层不会恢复原始HTTP分页或完整WS报文，不保证完整maker/taker地址、订单身份、
全交易所成交覆盖或实际到账费用。某些原生成交ID已换成恢复身份。
Nautilus `XPROB` 数据工具是回放表示，不是经过认证的真实下单规格。
跨平台两笔订单没有因本数据或转换而获得原子成交保证。

上传前质量证据按源数据保留。没有嵌入质量证书时应读作 `unknown`，不能自行补成通过。
下载时的字节校验、记录一致性和冷读回属于传输/转换验证，不是在 HF 上重新审核
交易所数据质量。原生API证据若上游已经裁剪，本工具不会补拉。

## 3. 文件与字段

```text
production/markets1000-20261110-r2/
  segments/<first:020d>-<last:020d>.json
  v3/events/<first:020d>-<last:020d>.zip
  v3/quality/...             # 仅按manifest实际引用，可能不存在
```

ZIP内是 `records.jsonl.zst`，外层为ZIP_STORED、内容为Zstd压缩。不能仅解ZIP
就当普通JSONL使用。codec会恢复差分时钟和局部身份，先产出header，再产出解码记录。
负时钟差分合法；null不更新累加器。使用随仓库提供的 `iter_archive`，不要手写猜测列序。

### Header

| 字段 | 含义 |
| --- | --- |
| `markets` | `[局部ID, venue, 原生market_id]` |
| `instruments` | `[局部ID, 局部market ID, 原生asset ID, indexSet, origin]` |
| `metadata_versions` | `[版本ID, 局部market ID, available_at_ns, 原生字段对象]` |
| `metadata_asof_start` | 分块起点已知元数据版本关系 |
| `pairs` | `[pair局部ID, market局部ID列表, outcomes, start_ns, end_ns]`；outcomes项为 `[标签, instrument局部ID列表]` |
| `book_checkpoints` | 起始已知盘口，不当作新到达的快照 |
| `fee_trade_definitions` | 费用记录关联所需的成交定义 |
| `columns` / `fee_columns` | 各类型位置数组的精确列定义 |
| `clocks` / `record_encoding` | 时钟与行编码约定 |
| `research_contract` / `trade_identity` | 源端研究语义与成交身份边界 |

### 解码记录

| Tag | 含义 | 处理要点 |
| --- | --- | --- |
| `m` | 元数据版本生效 | 按可知时间关联，禁止用最新版本回填历史 |
| `s` | 盘口快照 | CLEAR + ADD重建 |
| `d` | 原生绝对量增量 | 同价格数量是替换值，零表示删除，检查valid |
| `u` | 快照派生的稀疏变化 | 不能当作完整盘口 |
| `h` | 盘口未变化确认 | 更新观察新鲜度，不增加挂单量、不制造成交 |
| `q` | BBO报价 | 缺少数量时不能推断可执行深度 |
| `l` | WS最新成交价见证 | 不再累计为一笔API成交 |
| `k` | tick变化证据 | 保留版本，执行规则不能自动猜测 |
| `r` | 指定instrument重置 | 只清理该作用域，等待有效恢复 |
| `g` | readiness状态 | 不把所有状态变化解释为断线 |
| `t` | 上游身份规则下的成交 | 精确price/size、单位、执行/可知时间、fees |
| `f` | 已知成交的费用更新 | 更新费用证据，不重复成交量 |
| `a` | 历史价格锚点 | 仅上下文 |
| `n` / `e` | 结算通知 / 生命周期 | 保留证据，不凭通知伪造策略到账收益 |

全部列、Arrow类型、nullable标志与首块真实元数据字段名见
[机器可读字段字典](data/v3-field-dictionary.json)，由发布代码直接生成。

ID只能在其collection及对应header映射内解释，不能把两平台相同数字ID当同一市场。
pair窗口按 `[start_ns, end_ns)` 解释，结束未知不表示无限保证可交易。
Predict的 `derived_complement` 是原生结果盘口的互补视图，其价格由 `1-p` 与对侧映射而来，
共享同一池深度，不能和原生盘口相加当作额外流动性。

### 时间、精度与费用

- `sequence` 保留源顺序；`received_ns` / `available_at_ns` 表示采集可知时间。
- `source_ns` / `execution_ns` 表示上游源时间或成交执行时间，可能为空；
  `source_precision_ns` / `precision_ns` 表示原始精度，纳秒容器不等于纳秒级测量。
- Nautilus `ts_init` 是回放可用时钟，`ts_event` 是表示层事件时间。源时钟逆序时，
  导出器保持instrument内因果顺序，并保留原时钟及调整诊断。不要用排序抹掉原证据。
- 精确价格、数量与费用常以字符串保存，计算用 `Decimal`，不先转 `float`。
- Poly元数据可能有 `feeSchedule`、`feesEnabled`、`feeType`、`makerBaseFee`、
  `takerBaseFee`；Predict可能有 `feeRateBps`、`makerFeeBps`。
- 成交费用列为 `leg, leg_index, fee_rate_bps, fee_amount_native, fee_unit, fee_status`。
  未报告不等于零。实际扣份额/扣现金、逐撮合舍入、返佣和净对冲份额仍需执行模型验证。

## 4. 安装与完整下载

目前通过真实数据验证的是 **Windows x64 + Python 3.13.14**。
针对目标Mac，另提供官方 **Apple Silicon ARM64** wheel与独立锁文件。
引擎为 `2.0.0rc8.dev20261025`，实际源码提交
`b23139911fd9fed82aed2811c5a8a373202b9480`，wheel SHA256为
`ef2391d1b03b96c8231cc8ff20490a21e195cdbb2fad6ffcfd9a51e87379e7c7`。
`runtime.compatibility.lock.json` 是实际运行锁，`engine.lock.json` 是另一个较新的目标，
不能混用。验证器检查安装文件的RECORD哈希及二进制完整commit标记。

Linux机器不能安装这份Windows wheel，也不能把换装Linux wheel称作同一已验证运行时。
Linux需要独立构建/安装固定引擎、记录对应平台锁并重新完成保真验证。
数据格式和大部分转换源码可移植，但本交接不宣称已完成Linux验收。

### Mac Apple Silicon 安装与目标机小样本验收

先运行 `uname -m`，原生M系列Mac应为 `arm64`；Python也必须是原生ARM64，
不能在Rosetta下误用x86_64环境。官方固定版本索引提供的Mac包为
`nautilus_trader-2.0.0rc8.dev20261025-cp313-cp313-macosx_11_0_arm64.whl`，
SHA256为 `be9ca8e364ec0f276dc302ebf77a0afc7c51b7092a374062430e471eec837561`。
本次索引未列出该版本的Intel Mac wheel，Intel机器需要另做固定源码构建及验收；
工具会明确拒绝，不自动换版本。

[Mac运行锁](../runtime.compatibility.macos-arm64.lock.json)与Windows使用相同引擎源码commit，
但二进制哈希不同。wheel的 `11_0` tag不代表我们已验收macOS 11，实际导入及数据处理
仍须在目标机验证。官方平台说明见
[Nautilus安装文档](https://nautilustrader.io/docs/latest/getting_started/installation/)。

准备原生Python 3.13.14后，在Mac终端执行下列命令。`/Volumes/MarketData`替换成
实际外置盘挂载点；Mac没有Windows的E盘盘符。建议本地APFS磁盘；不要把同一SQLite
写入目录放在多机共享网盘上。

```bash
set -euo pipefail
git clone https://github.com/chenencc/prediction-market-strategies.git
cd prediction-market-strategies
git checkout --detach <交付commit>
python3.13 -m venv .venv
source .venv/bin/activate
python -c "import platform; assert platform.python_version() == '3.13.14'; assert platform.machine() == 'arm64'"
python -m pip install -r requirements-hf-sync-macos.txt
python -m pip install --no-deps -e .
DATA_ROOT='/Volumes/MarketData/metaculus/hf-v3/e0a7982-portable'
REV='e0a7982b34e4b66c3254e58024e2f9efdd7497cf'
TOKEN_FILE="$HOME/.config/forecastagent/hf-read-token.txt"

# 先在Mac原生环境验证两个分块，不直接启动全量。
python -B tools/hf_v3_sync.py --root "$DATA_ROOT" --ref "$REV" \
  --token-file "$TOKEN_FILE" --max-chunks 2 --catalog-workers 8 \
  --delete-converted-downloads
python -B tools/verify_v3_snapshot.py --root "$DATA_ROOT" --allow-prefix --full-files
python -B tools/verify_v3_replay_sample.py --root "$DATA_ROOT"

# 只有上面每条命令都成功，尤其逐事件指纹一致，才单独执行以下全量命令。
python -B tools/pair_history_sync.py --root "$DATA_ROOT" --revision "$REV" \
  --token-file "$TOKEN_FILE" --workers 8
python -B tools/verify_v3_snapshot.py --root "$DATA_ROOT"
```

Mac路径自动选择Mac运行锁，验证 `.so` 的完整引擎commit标记和安装文件RECORD。
两个分块应产生243,236个原生事件；验证脚本逐事件比较精确数值、时间、方向、
序列与成交身份的指纹，预期值在交付回执中。标准层逐行核对及完整深度比较同时由
转换器完成。如果失败，保留本地失败回执和源缓存，不放宽精度或跳过检查继续。
**此仓库交付时Mac原生运行仍待目标机执行，不能把Windows小样本通过写成Mac通过。**
已完成的官方Mac包静态核对见 [Mac wheel回执](validation/macos-wheel-static.json)。

### Windows安装与完整下载

先clone本仓库并checkout交付的**完整Git commit**，不要长期跟随浮动分支。
以下命令从仓库根目录执行。将 `<交付commit>` 替换为交付链接中的40位SHA。

```powershell
git clone https://github.com/chenencc/prediction-market-strategies.git
Set-Location prediction-market-strategies
git checkout --detach <交付commit>
py -3.13 -m venv .venv
$py = (Resolve-Path .venv/Scripts/python.exe).Path
& $py -c "import platform; assert platform.python_version() == '3.13.14'"
& $py -m pip install -r requirements-hf-sync.txt
& $py -m pip install --no-deps -e .
$root = 'E:/metaculus_data/backtests/hf-v3/e0a7982-portable'
$rev = 'e0a7982b34e4b66c3254e58024e2f9efdd7497cf'
$tokenFile = 'C:/private/hf-read-token.txt'

# 新根目录先转换首块，创建与当前源码/引擎绑定的config.lock.json。
& $py -B tools/hf_v3_sync.py --root $root --ref $rev `
  --token-file $tokenFile --max-chunks 1 --catalog-workers 8 `
  --delete-converted-downloads

# 再下载并转换这一固定版本的全部剩余核心分块。
& $py -B tools/pair_history_sync.py --root $root --revision $rev `
  --token-file $tokenFile --workers 8

# 完成后核对全部1,174个源绑定与保真回执。
& $py -B tools/verify_v3_snapshot.py --root $root
```

token文件由新机器本地准备，不上传GitHub、不放进数据目录，也不在命令参数写token内容。
第一条sync命令也支持省略 `--token-file` 使用HF本地标准凭据；全量worker使用显式token文件。
公开代码不包含token，也不授予他人私有数据集权限。

下载可并发1至8个worker。转换按源分块顺序推进，单分块内部可并发写独立instrument
catalog，不能把有检查点依赖的分块任意并行。使用本地SSD；派生文件量显著大于2.03GB
压缩源，且有大量小文件。12GiB空闲空间是停止下限，不是完整处理所需容量承诺。
先看首批的实际空间增长，再预留全量空间。

全量worker会在每块保真通过且写入durable ACK后删除其**本地下载ZIP缓存**。
标准Parquet、Nautilus数据、metadata、质量证据、receipt和检查点继续保留。
HF文件不会删除。默认不带 `--research-output`，因此下载完成不会自动启动策略实验。

## 5. 输出与读取

```text
<root>/
  config.lock.json
  index.sqlite
  cache/<sha256>.zip                 # 未处理或显式保留的ZIP
  chunks/<first>-<last>/
    source-manifest.json
    standard/
      source_events/                # compact记录，非官方原始报文
      definitions/                  # 市场、instrument、元数据、费用策略
      book_baselines/ book_snapshots/ book_updates/
      quotes/ observations/
      trade_payloads/ trade_occurrences/
      checkpoint.json.gz
    nautilus/
      catalog/
      event-index.parquet
      exact-overflow.parquet        # 按需存在，精度溢出数据必须合并读
      checkpoint.json.gz
    fee-summary.json
    quality/                        # 只有源中实际引用的证书
    receipt.json
  snapshots/<revision>/status.json  # 区分download_complete与conversion_complete
  latest-run.json
```

`standard`为 `market-standard-release/2`，`nautilus`为 `nautilus-standard-replay/1`。
保留两层；转换成功不授权删除标准层。每个分块先核对ZIP大小/SHA256，再逐行比较compact
header与记录、原生冷读回及完整盘口深度，成功后才提交SQLite索引与缓存删除ACK。

从已checkout仓库、已安装editable包的环境中读取：

```python
from itertools import islice
from prediction_market_strategies.data.postprocess_runtime import configure_postprocess
configure_postprocess()  # 验证并加载本仓库的固定reader依赖
from prediction_market_strategies.data.hf_backtest import BacktestDataset

dataset = BacktestDataset('E:/metaculus_data/backtests/hf-v3/e0a7982-portable')
print(dataset.status())
events = dataset.events(max_buffered_events=1_000_000)
for event in islice(events, 10):
    print(type(event).__name__, event.instrument_id, event.ts_event, event.ts_init)

# 分别流式读取，不要把全部26小时事件直接list()进内存。
definitions = dataset.context('definitions')
trade_payloads = dataset.context('trade_payloads')
observations = dataset.context('observations')
instruments = dataset.instruments()
```

`events()`合并重叠分块并保持跨块可用时间顺序。内存上限不足会明确报错，不隐式丢数据。
只读 `catalog` 会漏掉 `exact-overflow.parquet`；必须使用完整读取器。
`dataset.events(order='source')`只用于源顺序审计，可能出现短时钟逆序。
pair在 `observations` 中 `kind=compact_header` 的 `detail_json` 内，保持其有效期与结果映射；
不能用全历史pair集合提前赋予早期事件可交易关系。

## 6. 重启、完整性和相同数据的判定

- 一次只能有一个全量worker写同一root；检查真实进程及status后再恢复，不能只看日志安静。
- 相同固定命令可恢复，已完成的receipt不会重复下载。失败源ZIP保留；不要删锁或改索引强行继续。
- 更换引擎、converter或参数导致config lock不符时，使用新root并重新验收，不手改旧锁。
- `verify_v3_snapshot.py`默认要求全部分块，否则报 `snapshot_incomplete`。
  小样本可用 `--allow-prefix`，结果明确标为partial；`--full-files`会额外重算所有派生文件哈希。
- 验证器确认源清单、manifest/blob、payload哈希、序列及转换保真回执；默认不会重复扫描
  全部Parquet内容，报告会注明 `derived_files_rehashed=false`。
- 本机历史数据是逐步下载的，因此早期receipt可能记录早于目标revision的上传版本。
  只要同一目标快照中的manifest/blob/payload哈希一致，源输入相同。
- 新机器重新转换的receipt、release或Parquet字节不保证与旧机逐字节相等，因为旧机经历过
  converter修复和不同来源revision。比较源身份、语义记录、精确深度及冷读回，不比较墙钟字段。
- 本快照已被用于研究，不能再称为未见过的holdout。策略执行、费用净份额、真实成交完整性、
  无单腿风险与盈利均需单独实验。

## 7. GitHub代码导航

| 路径 | 职责 |
| --- | --- |
| [hf_v3_sync.py](../tools/hf_v3_sync.py) | 初始化、增量下载、固定runtime验证 |
| [pair_history_sync.py](../tools/pair_history_sync.py) | 固定版本全清单、并发下载、顺序转换、恢复 |
| [verify_v3_snapshot.py](../tools/verify_v3_snapshot.py) | 新机器全量/部分输入绑定核对 |
| [verify_v3_replay_sample.py](../tools/verify_v3_replay_sample.py) | Mac目标机真实两块原生事件与基线逐项指纹比较 |
| [hf_sync.py](../src/prediction_market_strategies/data/hf_sync.py) | HF只读传输、hash、SQLite ACK、缓存回收 |
| [v3_incremental.py](../src/prediction_market_strategies/data/v3_incremental.py) | 单块标准化、前块检查点、完整深度保真 |
| [hf_backtest.py](../src/prediction_market_strategies/data/hf_backtest.py) | 完整事件与上下文读取、跨块时钟合并 |
| [postprocess_runtime.py](../src/prediction_market_strategies/data/postprocess_runtime.py) | 依赖包哈希验证、防止混用私有checkout |
| [SOURCE.json](../vendor/v3_postprocess/SOURCE.json) | 随附依赖来源及逐文件哈希 |
| [backtest_spool.py](../vendor/v3_postprocess/services/mixed_recorder/backtest_spool.py) | V3解码、列定义、精确盘口重建 |
| [compact_standard.py](../vendor/v3_postprocess/services/local_postprocess/compact_standard.py) | 标准层schema与转换 |
| [nautilus.py](../vendor/v3_postprocess/services/local_postprocess/nautilus.py) | Nautilus导出、溢出保留、冷读回 |

交付验收记录见 [v3-portable-handoff.json](validation/v3-portable-handoff.json)。
它区分本机已有完整快照验证与本次独立代码的真实小样本重建，不把小样本写成新机器全量验收。
