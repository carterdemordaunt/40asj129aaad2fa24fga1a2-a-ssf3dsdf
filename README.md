# proxyip

定期从 <https://zip.cm.edu.kg> 下载代理 IP 列表并通过 CI 自动解压、整理、验证、提交回仓库；当前自动发布只维护 GitHub Runner 实测的欧洲清单。整个流程零第三方 Python 依赖，仅需标准库。

[![Unique Proxies](https://img.shields.io/badge/dynamic/json?url=https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/stats.json&query=unique&label=Unique%20Proxies&color=blue)](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/download/all.txt)
[![Alive](https://img.shields.io/badge/dynamic/json?url=https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/stats.json&query=alive&label=Alive&color=green)](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/valid/all.txt)
[![Alive Rate](https://img.shields.io/badge/dynamic/json?url=https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/stats.json&query=alive_rate&label=Alive%20Rate&color=orange)](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/valid/meta.json)
[![Updated](https://img.shields.io/badge/dynamic/json?url=https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/stats.json&query=updated_ago&label=Updated&color=informational)](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/stats.json)
[![Status](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/badge.json)](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/stats.json)

![Trend](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/chart_combo.svg)
<details>
<summary>📈 趋势与存活（点击展开 3 图）</summary>

![Country distribution](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/chart_country.svg)
![Port distribution](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/chart_port.svg)
![Churn](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/chart_churn.svg)

</details>
<details>
<summary>🌍 地理与速度分布（点击展开 4 图）</summary>

![Latency & Speed distribution](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/chart_latency_speed.svg)
![Sets](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/chart_sets.svg)
![Country speed distribution](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/chart_country_speed.svg)
![Within-country speed spread](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/chart_speed_spread.svg)

</details>
<details>
<summary>🔍 出口与质量（点击展开 7 图）</summary>

![Exit IP family](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/chart_family.svg)
![Exit country top 15](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/chart_exit.svg)
![Entry CC label audit](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/chart_entry_audit.svg)
![IP type distribution](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/chart_ip_type.svg)
![IP source availability](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/chart_source_avail.svg)
![IP source stats](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/chart_source_stats.svg)
![Reputation score](https://raw.githubusercontent.com/Xiaobei09/proxyip/main/data/output/chart_rep.svg)

</details>

## 功能特性

- **自动抓取整理**：下载上游 `all.json`（失败自动回退 zip）→ 按端口/国家/常用集合/全量多维度汇总，去重合并；并把上游真实出口 IP、ASN、地理等元数据落盘为 `data/quality/upstream_meta.json` 供下游消费
- **可用性验证**：TLS 握手检测，asyncio 高并发测活，并在判活连接内做**稳态下载测速**（丢弃慢启动爬坡、仅对稳态窗口计时，MB/s）；非限量输出**按延迟升序**，**`_ltd` 限量清单按实测速度取每国最快**
- **欧洲侧实测清单**（独立 CI）：从 GitHub 托管的 `ubuntu-latest` Runner 对候选逐条执行 TLS 握手和 HTTP GET，以已有测速 `≥5 MB/s` 过滤，并维护滚动稳定性。
- **更新差异**：每次更新自动对比上一版，产出 `added`/`removed` 并归档
- **统计与趋势**：保留历史统计脚本与数据工件；当前节点筛选以 `data/valid/all_eu.txt` 和 `data/valid/all_eu_stable.txt` 为准
- **结构化索引**：`valid/index.json` 提供每存活代理的延迟与检测方法索引，`valid/speed.json` 提供实测速度索引，便于程序直接消费
- **CI 自动化**：每 2 小时定时触发下载→验证→欧洲侧实测→提交，无需人工干预；提交前自动跑测试套件（stdlib `unittest`）。
- **浏览器指纹生成**：生成内部自洽、同一设备配置的 UA/分辨率/时区/WebGL 等指纹

## 快速开始

### 系统要求

- Python 3.11+（使用了 `asyncio.timeout` 与 `dict[str, ...]` 等新语法）
- 无任何第三方依赖，仅标准库

### 运行

```bash
git clone https://github.com/Xiaobei09/proxyip.git
cd proxyip

python3 -m unittest discover -s tests -v    # 0. 运行测试套件（可选）
python3 scripts/download_proxies.py          # 1. 下载解压整理
python3 scripts/validate_proxies.py             # 2. 连通性验证与测速（默认不设时间限制，跑完为止；上一轮未存活条目先经 TCP 预筛除朽尸）
python3 scripts/europe_check.py             # 3. GitHub Runner 欧洲侧 TLS/HTTP 检查
```

### 消费数据

**我该用哪个文件？**

| 需求 | 首选文件 | 说明 |
|---|---|---|
| 欧洲低延迟稳定使用 | `data/valid/all_eu_stable.txt` | GitHub 托管 runner 实测低延迟/速度候选；滚动合格率≥80% 且连续合格≥2轮 |
| 欧洲当前可达 | `data/valid/all_eu.txt` | 本轮 GitHub Runner TLS + HTTP 检查通过且已有测速达到门槛 |
| 未验证全量 | `data/download/all.txt` | 去重清单，IP 数字序 |
| 程序化消费 | `data/valid/all.json`、`speed.json`、`index.json` | 结构化导出 + 速度/延迟索引 |

未验证目录统一 `ip:port#国家` 格式（如 `1.2.3.4:443#US`），按 IP 数字序排列；`data/valid/` 内为
`ip:port#🇺🇸US-120ms-0.44MB/s`（国旗+国家-延迟毫秒-速度 MB/s，测速失败时省略速度段），**按延迟升序**（`_ltd` 按速度降序）。被质量 CI 检测后行内追加 `→` 出口地区与备注段（见下方格式）。

```bash
head -1 data/valid/all.txt                  # 当前延迟最低的存活代理（延迟升序）
data/valid/all_ltd.txt                      # 每国按实测速度最快的 20 条限量清单
data/valid/countries/US/all.txt            # 仅美国的存活代理（含延迟/速度）
data/valid/countries/US/ltd.txt            # 该国限量（每国最快 20 条，速度降序）
data/valid/countries/US/v4.txt             # 该国出口为 IPv4 的代理（仅 v4-only，不含双栈）
data/valid/countries/US/46.txt             # 该国出口为双栈（v4+v6）的代理
data/valid/countries/US/cn.txt             # 该国大陆可达的代理
data/valid/countries/US/cn4.txt            # 该国大陆可达且出口为 IPv4 的代理
data/valid/countries/US/rep.txt            # 该国按信誉分降序（质量 CI 生成）
data/valid/all_eu.txt                       # 本轮从欧洲服务器完成 TLS+HTTP 实测可达
data/valid/all_eu_stable.txt                # 欧洲侧滚动稳定清单（默认至少 3 个样本）
data/valid/all_good.txt                     # 全局综合最优（CN 可达 + 信誉≥80 + 非高风险，综合分降序，CN 视图）
data/valid/all_premium.txt                  # 全局高端优质（CN 可达 + 信誉≥95 + 真实住宅IP，综合分降序，CN 视图）
data/valid/all_premium_v4.txt               # 高端优质（出口为 IPv4 的家族分支，另有 _v6 / _46）
data/valid/countries/US/premium.txt         # 该国高端优质（质量 CI 生成）
data/valid/sets/hot/premium.txt             # 热门集合高端优质（质量 CI 生成）
data/valid/sets/europe/all.txt             # 欧洲集合存活代理（集合也是目录多件套）
data/valid/all_46.txt                      # 全部出口为双栈的代理（根级分组）
data/valid/ports/443.txt                    # 仅 443 端口的存活代理
data/valid/speed.json                       # 每存活代理的实测速度（MB/s，按速度降序）
data/valid/sets/hot/all.txt                 # 热门国家集合（验证后）
data/download/all.txt                       # 全量去重清单（未验证）
```

> 欧洲清单的 `5 MB/s` 条件使用验证阶段已有的测速结果；欧洲 Runner 的实时延迟只用于排序，
> 不作为硬性淘汰条件。

## 欧洲清单使用

- `data/valid/all_eu.txt`：本轮从 GitHub Runner 完成 TLS + HTTP 实测，且已有测速达到 `5 MB/s`。
- `data/valid/all_eu_stable.txt`：滚动历史中至少 3 次采样、合格率至少 80%、连续合格至少 2 轮。
- 延迟只用于排序，不设置 TLS 延迟硬阈值；全轮成功率异常过低时保留上一轮结果，避免网络故障清空订阅。
- CN、good、premium 和 PCB 相关文件仅作为历史数据/代码保留，不再由自动 workflow 维护。

## 文档

| 文档 | 看什么 |
|---|---|
| [数据规范与数据文件参考](docs/data-spec.md) | 行格式与备注段、限量版规则、国家集合、验证算法、各数据文件字段 |
| [脚本与 CLI 参考](docs/scripts.md) | 全部入口脚本的参数/默认值/行为、`common.py` 与拆分子模块说明 |
| [检测逻辑](docs/logic.md) | 质量/信誉/大陆连通判定算法、证据分级、逆向来源记录 |
| [数据目录浏览](data/README.md) | 数据层级视图与工件索引（从数据侧进入） |


## CI 自动更新

当前自动生产链只有两条：

- `.github/workflows/update-proxies.yml`：每 2 小时下载并验证上游代理，包含补充来源抓取，生成 `data/valid/all_ltd_verified.txt` 等候选文件。
- `.github/workflows/europe-check.yml`：在 GitHub 托管的 `ubuntu-latest` Runner 上执行 TLS 握手和 HTTP GET，过滤已有测速低于 `5 MB/s` 的节点，维护 `all_eu.txt` 与 `all_eu_stable.txt`。

欧洲检查的稳定条件是至少 3 次采样、滚动合格率至少 80%、连续合格至少 2 轮。延迟只用于排序，没有 TLS 延迟硬阈值；当整轮成功率异常低时，脚本保留上一轮历史和清单，避免 Runner 网络故障清空订阅。

CN、quality、good/premium、出口家族、分类和深测 workflow 已停用，因此不会再触发 reputation/PCB fallback。对应脚本和历史数据暂保留，供历史数据读取与测试使用，但不再是当前发布链的一部分。

## 目录结构

```
.github/workflows/update-proxies.yml     CI 自动更新（下载、验证、提交）
scripts/download_proxies.py              下载与解压整理
scripts/validate_proxies.py              可用性验证与测速
scripts/generate_stats.py                统计与趋势图
scripts/europe_check.py                  欧洲侧 TLS/HTTP 检查与滚动稳定性
scripts/common.py                        共享常量与助手（data 布局、HTTP/JSON 探测）

data/download/                           下载产出（原始代理列表）
data/download/all.txt                    全量去重 ip:port#国家（IP 数字序；同一 ip:port 多国标签可并存）
data/download/all_ltd.txt                全部国家每国限量后的并集
data/download/countries/<CC>.txt         按国家汇总（跨端口去重）
data/download/ports/<port>.txt           按端口汇总（跨国家去重）
data/download/sets/<集合>.txt            常用国家集合（europe、asia、hot、cn_common 等）
data/download/sets/<集合>_ltd.txt        限量版（每国 --per-country-limit 条）

data/valid/                              验证后代理数据（valid/ 下与 quality/ 同名的 *.json 为历史镜像：无写入者，仅存档查询）
data/valid/all.txt                       存活代理（按延迟排序）
data/valid/all_ltd.txt                   限量版（每国最快 20 条，速度降序）
data/valid/all_cn*.txt                   变体（cn、cn4、cn6、cn46、cn46_ltd 等）
data/valid/all_cn_http.txt / all_cn_stable.txt   大陆可达可靠性子集（`-CN` 可达 / `-CNH` 应用层可信；对应确认级为空时文件不落盘）
data/valid/all_good_verified.txt / all_good_stable.txt   可靠-good 变体（全链路验证 / 中国两轮稳定）
data/valid/tiers/<tier>/                 speed 分档目录（fast/mid/slow；good 全量镜像）
data/valid/all_ipv4.txt                  出口为 IPv4 的代理清单（exit-family CI，双栈双入）
data/valid/all_ipv6.txt                  出口为 IPv6 的代理清单（exit-family CI，双栈双入）
data/valid/all_rep.txt                   信誉排行（按分数降序，质量 CI）
data/valid/all_good.txt                  综合最优清单（CN 可达 + 信誉≥80 + 非高风险，按综合分降序，质量 CI）
data/valid/all_good_ltd.txt              每国最快的优质子集（同套 good 标准基于 ltd 限量池筛选 + _verified/_stable）
data/valid/all_premium.txt               高端优质清单（CN 可达 + 信誉≥95 + 真实住宅IP，按综合分降序，质量 CI）
data/valid/all_cn.txt                    全量大陆可达清单（全量池，china-check CI）
data/valid/countries/<CC>/               按国家分组（all.txt、ltd.txt、v4.txt、v6.txt、46.txt、cn.txt、rep.txt、good.txt、good_ltd.txt、premium.txt 等）
data/valid/ports/<port>.txt              按端口分组
data/valid/sets/<name>/                  按集合分组
data/valid/index.json                    代理索引（延迟与检测方法）
data/valid/speed.json                    实测速度索引
data/valid/meta.json                     验证摘要
data/valid/history.jsonl                 验证历史

data/quality/                            质量/检测元数据
data/quality/ipinfo.json                 出口 IP 地理/类型/信誉分（质量 CI）
data/quality/node_seen.json              节点按轮存活日期（滚动 45 天窗口）
data/quality/uptime.json                 7d/30d 存活率
data/valid/all.json                      结构化代理池（ip/port/cc/延迟/速度/家族/信誉…）
data/valid/all_diverse.txt               出口多样性视图（每出口一条最优）
data/quality/alert_state.json            健康看门狗状态
data/quality/abuse.json                  滥用分结果（配置 key 时生成，质量 CI）
data/quality/quality_meta.json           质量检测汇总（质量 CI）
data/quality/reputation.json             信誉分索引（0-100，质量 CI）
data/quality/reputation_cache.json       信誉信号缓存（7 天 TTL）
data/quality/external_check.json         外部 API 验证结果
data/quality/deep_speed.json             深测结果（每周，keyed 含每流明细）
data/quality/china.json                  大陆连通性检测明细（keyed，顶层含 ts，china-check CI）
data/quality/exit_family.json            实际出口家族明细（keyed，含上游交叉验证，顶层含 ts，exit-family CI）
data/quality/upstream_meta.json          上游 all.json 逐 IP 元数据（真实出口 clientIp / ASN / 地理 / colo）
data/quality/source_stats.json           下载源统计（各源 IP 数与重叠）
data/quality/source_history.json         各源逐轮 unique 覆盖快照（告警基线，最多 14 轮）
data/quality/source_quality.json         源质量分析（来源依赖关系与来源质量打分）
data/output/source_quality_report.txt    源质量人类可读汇总表
data/quality/entry_audit.json            入口国标签三方交叉审计结果
data/quality/good_meta.json              good 候选镜像（good CI 构建用 IP 元数据）
data/quality/premium_meta.json           premium 候选镜像（premium CI 构建用）
data/quality/ip_sources.json             逐 IP 下载源归属（`ip:port#CC → 来源`，下载 CI；同站多文件只记一个来源）
data/quality/history.jsonl               更新历史记录（每行一条，最多 1000 条）

data/output/                             展示输出
data/output/stats.json                   统计汇总（供徽章与外部消费）
data/output/badge.json                   状态徽章（fresh/stale）
data/output/chart_combo.svg              代理计数 + 存活率双轴折线图（近 7 天窗口）
data/output/chart_country.svg            存活代理按国家 top-15 条形图
data/output/chart_port.svg               存活代理按端口条形图
data/output/chart_churn.svg              每次更新 added/removed 条形图（近 7 天窗口）
data/output/chart_latency_speed.svg      延迟与速度分桶双面板条形图
data/output/chart_sets.svg               各命名集合存活代理条形图
data/output/chart_cn.svg                 大陆连通性 verdict 分布条形图
data/output/chart_family.svg             实际出口 IP 家族分布条形图
data/output/chart_source_avail.svg       IP 来源可用性图表
data/output/chart_source_stats.svg       IP 来源统计图表
data/output/chart_country_speed.svg      各国速度分布（p25–p75 区间）
data/output/chart_speed_spread.svg       同国内速度分化（四分位差/中位数）
data/output/chart_entry_audit.svg        入口国标签审计汇总
data/output/chart_ip_type.svg            出口 IP 类型（机房/住宅/移动）分布
data/output/chart_exit.svg               出口国家 top-15
data/output/chart_rep.svg                信誉分分布条形图
data/output/country_speed.json           各国测速汇总（中位速度，stats 工作流）

data/raw/<port>/<CC>.txt                 下载中间产物（历史已入库；由 CI 周期生成重建）
data/diff/                               更新差异归档（历史已入库）
tests/                                   标准库 unittest 测试套件
archive/Check_Proxy.js                   遗留的单节点连通性检查脚本（已停用归档）
```

## 免责声明

本项目提供的代理 IP 列表来自公开来源，仅限学习与研究用途。使用代理访问网络时请遵守当地法律法规及目标网站的服务条款；本项目不对列表内容的可用性、合法性及由此产生的任何后果负责。附带的指纹生成工具同样仅供学习、测试与隐私研究使用，请勿用于规避访问控制或反欺诈检测。
