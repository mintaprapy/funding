# Funding 面板项目说明

## 正式上线最小命令

### 首次上线

```bash
cd /srv
git clone https://github.com/mintaprapy/funding.git
cd funding
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip requests

# 如需只启用部分交易所，先改配置
# vim config/exchanges.json

# 如需启用告警：先复制示例文件，再填写真实配置
cp config/alerts.example.json config/alerts.json
vim config/alerts.json

# 先空跑一轮，确认采集链路和数据库正常
python3 -m app.run_all_funding_stack --once --no-open-browser --host 0.0.0.0 --port 5000 --alert-config config/alerts.json

sudo cp systemd/funding-stack.service /etc/systemd/system/funding-stack.service
sudo systemctl daemon-reload
sudo systemctl enable --now funding-stack
```

### 已有服务器更新

```bash
cd /srv/funding
git pull
source .venv/bin/activate
pip install -U pip requests

# 如果改了交易所配置，改仓库内文件
# vim config/exchanges.json

# 如果改了告警配置，改服务器本地文件
# vim config/alerts.json

# 补跑一轮再重启服务
python3 -m app.run_all_funding_stack --once --skip-dashboard --no-open-browser --host 0.0.0.0 --port 5000 --alert-config config/alerts.json

sudo cp systemd/funding-stack.service /etc/systemd/system/funding-stack.service
sudo systemctl daemon-reload
sudo systemctl restart funding-stack
```

### 上线后立刻验收

```bash
curl -s http://127.0.0.1:5000/api/data | python3 -c 'import sys,json; x=json.load(sys.stdin); print(len(x["items"]), len(x["exchanges"]))'
sudo journalctl -u funding-stack -n 100 --no-pager
sudo systemctl status funding-stack --no-pager
```

### 常用服务命令

```bash
# 查看当前是否正在运行
sudo systemctl status funding-stack --no-pager
sudo systemctl is-active funding-stack

# 立即停止服务
sudo systemctl stop funding-stack

# 取消开机自启，但不停止当前服务
sudo systemctl disable funding-stack

# 同时停止服务并取消开机自启
sudo systemctl disable --now funding-stack

# 启动服务
sudo systemctl start funding-stack

# 恢复开机自启，但不立即启动
sudo systemctl enable funding-stack

# 立即启动服务，并恢复开机自启
sudo systemctl enable --now funding-stack
```

说明：
- 如果你要只启用部分交易所，先编辑 [config/exchanges.json](/Users/m2/Desktop/Codex2026/Funding/config/exchanges.json)
- 如果你要启用告警，先复制 [config/alerts.example.json](/Users/m2/Desktop/Codex2026/Funding/config/alerts.example.json) 为服务器本地的 `config/alerts.json`，再填写真实 webhook
- `disable` 只取消开机自启，不会停止当前已经在跑的服务；要停掉当前服务请用 `stop` 或 `disable --now`
- `enable` 只恢复开机自启，不会立刻启动当前服务；要立即启动请用 `start` 或 `enable --now`
- 更完整的部署说明见下文“GitHub 首页部署步骤”

## 1. 项目简介
本项目用于采集多个交易所永续合约资金费率数据，并写入本地 SQLite 数据库，再通过内置网页面板展示。

当前覆盖交易所（13 家）：
- Binance
- Bybit
- Aster
- Hyperliquid
- Backpack
- Ethereal
- GRVT
- StandX
- Lighter
- Gate
- Bitget
- Variational
- edgeX

## 2. 运行依赖
依赖很轻量：
- Python 3.10+（建议 3.11）
- 第三方包：`requests`
- 其余均为 Python 标准库

安装示例：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip requests
```

## 3. 目录与入口
- Shell 启动入口：`start_all_funding.sh`
- 应用入口目录：`app/`
- 公共模块目录：`core/`
- 交易所采集脚本目录：`exchanges/`
- 启用交易所配置：`config/exchanges.json`
- 本地辅助脚本目录：`scripts/`
- systemd 模板目录：`systemd/`
- 运行数据库：`funding.db`
- 运行日志目录：`logs/`

当前仓库根目录只保留 shell 入口、部署文件和文档；Python 代码已按职责拆到 `app/`、`core/`、`exchanges/`。部署说明统一以本 README 为准，避免多份文档重复维护。

简化后的结构如下：

```text
Funding/
├── README.md
├── start_all_funding.sh
├── app/
│   ├── __init__.py
│   ├── run_all_funding_stack.py
│   └── allfunding_dashboard.py
├── core/
│   ├── __init__.py
│   ├── funding_exchanges.py
│   └── common_funding.py
├── exchanges/
│   ├── bincance_funding/
│   ├── bybit_funding/
│   ├── aster_funding/
│   ├── hyperliquid_funding/
│   ├── backpack_funding/
│   ├── ethereal_funding/
│   ├── grvt_funding/
│   ├── standx_funding/
│   ├── lighter_funding/
│   ├── gate_funding/
│   ├── bitget_funding/
│   ├── variational_funding/
│   └── edgex_funding/
├── config/
│   ├── exchanges.json
│   └── alerts.example.json
├── scripts/
│   └── cleanup_logs.py
├── logs/
├── funding.db
└── systemd/
```

职责说明：
- `app/`：总调度器和总 dashboard 的入口模块
- `core/`：公共 SQLite、限速、交易所注册表等共享代码
- `exchanges/`：各交易所的 `baseinfo`、`history`、单交易所 dashboard
- `config/exchanges.json`：控制启动时启用哪些交易所
- `config/alerts.example.json`：告警配置示例文件，可复制为服务器本地的 `config/alerts.json`
- `scripts/`：本地辅助脚本，例如清理长时间测试生成的日志控制文件
- `systemd/`：线上部署用服务模板
- `start_all_funding.sh`：面向人工执行的统一入口

启动链路：
- `start_all_funding.sh`
- `python3 -m app.run_all_funding_stack`
- `core/funding_exchanges.py`
- `config/exchanges.json`
- `config/alerts.json`（服务器本地真实文件） / `FUNDING_ALERT_CONFIG` / `--alert-config`

如果你要控制只启用部分交易所，直接编辑 [config/exchanges.json](/Users/m2/Desktop/Codex2026/Funding/config/exchanges.json)：

```json
{
  "enabled_exchanges": ["binance", "bybit", "gate"]
}
```

说明：
- 配置文件不存在时，默认启用全部交易所
- 仓库当前自带这份文件，默认启用全部 13 家交易所
- 这里的 key 必须使用小写：如 `binance`、`grvt`、`variational`、`edgex`
- 调度器和总 dashboard 会共用这份配置

## 4. 一键启动（推荐）
在项目根目录执行：

```bash
./start_all_funding.sh --no-open-browser --host 0.0.0.0 --port 5000
```

说明：
- 默认会先跑一轮启动批次，再进入定时循环。
- 默认调度时间：
  - `baseinfo`：每小时 `1,11,21,31,41,51` 分
  - `history`：每小时 `3` 分
- 面板地址：`http://<服务器IP>:5000`

后台运行示例（Linux）：

```bash
nohup ./start_all_funding.sh --no-open-browser --host 0.0.0.0 --port 5000 > /tmp/funding_stack.log 2>&1 &
```

本地调试推荐后台运行方式（macOS / 本机）：

```bash
cd /Users/m2/Desktop/Codex2026/Funding
ts=$(date +%Y%m%d_%H%M%S)
log=/Users/m2/Desktop/Codex2026/Funding/logs/local_runtime_${ts}.log
ln -sfn "$log" /Users/m2/Desktop/Codex2026/Funding/logs/local_runtime_latest.log
nohup /bin/zsh -lc 'cd /Users/m2/Desktop/Codex2026/Funding && ./start_all_funding.sh --no-open-browser --host 127.0.0.1 --port 5000' >>"$log" 2>&1 &
```

本地查看日志：

```bash
tail -f /Users/m2/Desktop/Codex2026/Funding/logs/local_runtime_latest.log
```

也可以直接使用模块入口：

```bash
python3 -m app.run_all_funding_stack --no-open-browser --host 0.0.0.0 --port 5000
```

## 5. 常用参数
查看全部参数：

```bash
python3 -m app.run_all_funding_stack --help
```

常用参数：
- `--baseinfo-minutes "1,11,21,31,41,51"`
- `--history-minutes "3"`
- `--skip-dashboard`（仅采集，不启动网页）
- `--once`（只跑启动批次，跑完退出）
- `--script-timeout-seconds 1800`（单脚本超时，秒；`<=0` 关闭）
- `--script-max-attempts 8`
- `--script-retry-wait 5`
- `--lock-file .run_all_funding_stack.lock`（单实例锁文件）
- `--exchange-config config/exchanges.json`（控制启用哪些交易所）
- `--alert-config config/alerts.json`（控制阈值告警和通知渠道；也可用 `FUNDING_ALERT_CONFIG` 指定）
- `--alert-minutes "0,5,10,15,20,25,30,35,40,45,50,55"`（告警检查分钟）
- `--disable-alerts`（禁用告警任务）

常见调试命令：

```bash
python3 -m app.allfunding_dashboard --host 127.0.0.1 --port 5000
python3 -m app.run_all_funding_stack --once --skip-dashboard --no-run-on-start
```

当前运行说明：
- 新上市或历史窗口未成熟的交易对，`24h / 3d / 7d / 15d / 30d` 会显示 `—`，不会显示误导性的 `0`
- `Binance` 的 `history` 脚本中途可能出现约 `7~8` 分钟无新日志输出，这是为规避 API 限频而设计的等待窗口；单独回补时不要误判为卡死
- `edgeX` 的 `baseinfo` 首轮会比其他交易所慢一些，因为它需要逐合约拉取 ticker
- `Variational` 的 `funding_rate` 来自年化值，代码会自动换算成单结算周期资金费率后再展示
- `config/exchanges.json` 可以控制只启用部分交易所，调度器和总 dashboard 会共用这份配置
- 调度器支持按阈值发 Telegram / 飞书通知；仓库建议只提交示例文件，服务器本地维护真实 `config/alerts.json`
- 搜索框末尾加 `/` 表示精确搜索，例如 `BTC/`
- 交易所筛选支持多选，`24h / 3d / 7d / 15d / 30d` 的 `—` 在排序时统一排在最后

## 6. 告警通知
项目支持按阈值发送资金费率告警，通知渠道支持：
- Telegram Bot
- 飞书自定义机器人

默认配置文件：
- 仓库内示例文件：[config/alerts.example.json](/Users/m2/Desktop/Codex2026/Funding/config/alerts.example.json)
- 生产环境真实文件：`config/alerts.json`（不提交到 GitHub）

建议流程：
- 服务器拉取代码后，复制 `config/alerts.example.json` 为 `config/alerts.json`
- 在 `config/alerts.json` 中填写真实 webhook / bot token
- 不要把真实 `config/alerts.json` 提交到 GitHub

示例：

```json
{
  "enabled": true,
  "cooldown_minutes": 120,
  "max_items_per_run": 20,
  "open_interest_min_musd": 3,
  "latest_abs_pct_gte": 0.5,
  "h4_abs_pct_gte": 1.0,
  "providers": {
    "telegram": {
      "enabled": true,
      "bot_token": "123456:ABC",
      "chat_id": "-100xxxxxxxxxx",
      "message_thread_id": null
    },
    "feishu": {
      "enabled": false,
      "webhook_url": "",
      "secret": ""
    }
  },
  "state_path": "logs/alert_state.json"
}
```

规则说明：
- `open_interest_min_musd`：持仓量最小值，单位是 `M$`；只有持仓量大于等于这个值的交易对，才会参与告警判断
- `latest_abs_pct_gte`：最新单次资金费率绝对值达到这个百分比时告警
- `h4_abs_pct_gte`：过去 4 小时累计资金费率绝对值达到这个百分比时告警
- `cooldown_minutes`：同一条告警在冷却时间内不会重复发送
- `max_items_per_run`：单次通知最多展开多少条命中项

先本地 dry-run 验证：

```bash
python3 app/funding_alerts.py --config config/alerts.json --dry-run --force
```

如果你想单独指定配置文件：

```bash
python3 app/funding_alerts.py --config /abs/path/to/alerts.json --dry-run --force
```

调度器启动后会自动带上告警任务；如果临时不想跑告警：

```bash
python3 -m app.run_all_funding_stack --disable-alerts
```

## 7. 停止服务
停止主调度/面板：

```bash
pkill -f app.run_all_funding_stack
pkill -f app.allfunding_dashboard
```

本地改完代码后，通常按“停止 -> 再用上面的 `nohup` 命令重启”即可。

## 8. 快速自检命令
查看关键进程：

```bash
ps -Ao pid,etime,command | grep -E 'app.run_all_funding_stack|app.allfunding_dashboard' | grep -v grep
```

检查总面板接口：

```bash
curl -s http://127.0.0.1:5000/api/data | python3 -c 'import sys,json; x=json.load(sys.stdin); print(len(x["items"]), len(x["exchanges"]))'
```

检查 13 家交易所 `baseinfo` 关键字段空值：

```bash
sqlite3 funding.db "
SELECT 'binance',  SUM(CASE WHEN markPrice IS NULL OR TRIM(markPrice)='' THEN 1 ELSE 0 END), SUM(CASE WHEN lastFundingRate IS NULL OR TRIM(lastFundingRate)='' THEN 1 ELSE 0 END), SUM(CASE WHEN openInterest IS NULL OR TRIM(openInterest)='' THEN 1 ELSE 0 END) FROM binance_funding_baseinfo
UNION ALL
SELECT 'bybit',    SUM(CASE WHEN markPrice IS NULL OR TRIM(markPrice)='' THEN 1 ELSE 0 END), SUM(CASE WHEN lastFundingRate IS NULL OR TRIM(lastFundingRate)='' THEN 1 ELSE 0 END), SUM(CASE WHEN openInterest IS NULL OR TRIM(openInterest)='' THEN 1 ELSE 0 END) FROM bybit_funding_baseinfo
UNION ALL
SELECT 'aster',    SUM(CASE WHEN markPrice IS NULL OR TRIM(markPrice)='' THEN 1 ELSE 0 END), SUM(CASE WHEN lastFundingRate IS NULL OR TRIM(lastFundingRate)='' THEN 1 ELSE 0 END), SUM(CASE WHEN openInterest IS NULL OR TRIM(openInterest)='' THEN 1 ELSE 0 END) FROM aster_funding_baseinfo
UNION ALL
SELECT 'hyperliquid', SUM(CASE WHEN markPrice IS NULL OR TRIM(markPrice)='' THEN 1 ELSE 0 END), SUM(CASE WHEN lastFundingRate IS NULL OR TRIM(lastFundingRate)='' THEN 1 ELSE 0 END), SUM(CASE WHEN openInterest IS NULL OR TRIM(openInterest)='' THEN 1 ELSE 0 END) FROM hyperliquid_funding_baseinfo
UNION ALL
SELECT 'backpack', SUM(CASE WHEN markPrice IS NULL OR TRIM(markPrice)='' THEN 1 ELSE 0 END), SUM(CASE WHEN lastFundingRate IS NULL OR TRIM(lastFundingRate)='' THEN 1 ELSE 0 END), SUM(CASE WHEN openInterest IS NULL OR TRIM(openInterest)='' THEN 1 ELSE 0 END) FROM backpack_funding_baseinfo
UNION ALL
SELECT 'ethereal', SUM(CASE WHEN markPrice IS NULL OR TRIM(markPrice)='' THEN 1 ELSE 0 END), SUM(CASE WHEN lastFundingRate IS NULL OR TRIM(lastFundingRate)='' THEN 1 ELSE 0 END), SUM(CASE WHEN openInterest IS NULL OR TRIM(openInterest)='' THEN 1 ELSE 0 END) FROM ethereal_funding_baseinfo
UNION ALL
SELECT 'grvt',     SUM(CASE WHEN markPrice IS NULL OR TRIM(markPrice)='' THEN 1 ELSE 0 END), SUM(CASE WHEN lastFundingRate IS NULL OR TRIM(lastFundingRate)='' THEN 1 ELSE 0 END), SUM(CASE WHEN openInterest IS NULL OR TRIM(openInterest)='' THEN 1 ELSE 0 END) FROM grvt_funding_baseinfo
UNION ALL
SELECT 'standx',   SUM(CASE WHEN markPrice IS NULL OR TRIM(markPrice)='' THEN 1 ELSE 0 END), SUM(CASE WHEN lastFundingRate IS NULL OR TRIM(lastFundingRate)='' THEN 1 ELSE 0 END), SUM(CASE WHEN openInterest IS NULL OR TRIM(openInterest)='' THEN 1 ELSE 0 END) FROM standx_funding_baseinfo
UNION ALL
SELECT 'lighter',  SUM(CASE WHEN markPrice IS NULL OR TRIM(markPrice)='' THEN 1 ELSE 0 END), SUM(CASE WHEN lastFundingRate IS NULL OR TRIM(lastFundingRate)='' THEN 1 ELSE 0 END), SUM(CASE WHEN openInterest IS NULL OR TRIM(openInterest)='' THEN 1 ELSE 0 END) FROM lighter_funding_baseinfo
UNION ALL
SELECT 'gate',     SUM(CASE WHEN markPrice IS NULL OR TRIM(markPrice)='' THEN 1 ELSE 0 END), SUM(CASE WHEN lastFundingRate IS NULL OR TRIM(lastFundingRate)='' THEN 1 ELSE 0 END), SUM(CASE WHEN openInterest IS NULL OR TRIM(openInterest)='' THEN 1 ELSE 0 END) FROM gate_funding_baseinfo
UNION ALL
SELECT 'bitget',   SUM(CASE WHEN markPrice IS NULL OR TRIM(markPrice)='' THEN 1 ELSE 0 END), SUM(CASE WHEN lastFundingRate IS NULL OR TRIM(lastFundingRate)='' THEN 1 ELSE 0 END), SUM(CASE WHEN openInterest IS NULL OR TRIM(openInterest)='' THEN 1 ELSE 0 END) FROM bitget_funding_baseinfo
UNION ALL
SELECT 'variational', SUM(CASE WHEN markPrice IS NULL OR TRIM(markPrice)='' THEN 1 ELSE 0 END), SUM(CASE WHEN lastFundingRate IS NULL OR TRIM(lastFundingRate)='' THEN 1 ELSE 0 END), SUM(CASE WHEN openInterest IS NULL OR TRIM(openInterest)='' THEN 1 ELSE 0 END) FROM variational_funding_baseinfo
UNION ALL
SELECT 'edgex',    SUM(CASE WHEN markPrice IS NULL OR TRIM(markPrice)='' THEN 1 ELSE 0 END), SUM(CASE WHEN lastFundingRate IS NULL OR TRIM(lastFundingRate)='' THEN 1 ELSE 0 END), SUM(CASE WHEN openInterest IS NULL OR TRIM(openInterest)='' THEN 1 ELSE 0 END) FROM edgex_funding_baseinfo;
"
```

## 9. 部署建议（Linux）
当前项目依赖极少，建议优先“直接在 Linux 服务器运行”：
- 部署简单
- 进程与日志排查方便
- 对单机采集场景足够稳定

如果后续有以下需求，再考虑 Docker：
- 多环境一致性强需求
- 需要 CI/CD 镜像发布
- 需要容器编排（如 Kubernetes）

## 10. GitHub 首页部署步骤
下面这套命令按顺序执行即可，适合直接放到服务器上从 GitHub 拉代码部署。
建议优先走这条路径：`git clone -> 安装依赖 -> 先跑一次 --once -> systemd 启动`。

### 10.1 拉取代码
HTTPS 方式（公开仓库，推荐）：

```bash
cd /srv
git clone https://github.com/mintaprapy/funding.git
cd funding
```

SSH 方式（如果服务器已经配置好 GitHub SSH key）：

```bash
cd /srv
git clone git@github.com:mintaprapy/funding.git
cd funding
```

### 10.2 安装依赖

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip requests
```

### 10.3 首次空库预热
首次部署建议先跑一轮空库预热，确认采集链路和数据库都正常：

```bash
source .venv/bin/activate
python -m app.run_all_funding_stack --once --no-open-browser --host 0.0.0.0 --port 5000
```

说明：
- 这一步建议保留，不要跳过
- 首次空库全量回填耗时较长，属于正常现象
- 如果你已经手动跑过这一轮，并且不想让 `systemd` 首次启动时再重复跑一次启动批次，可以在 `ExecStart` 末尾追加 `--no-run-on-start`

### 10.4 用 systemd 启动（推荐）
先安装服务文件：

```bash
sudo cp systemd/funding-stack.service /etc/systemd/system/funding-stack.service
sudo systemctl daemon-reload
```

按服务器实际情况修改这些字段：
- `User`（模板默认值：`root`）
- `Group`（模板默认值：`root`）
- `WorkingDirectory`
- `Environment=FUNDING_EXCHANGE_CONFIG=...`（可选，默认就是 `/srv/funding/config/exchanges.json`）
- `Environment=FUNDING_ALERT_CONFIG=...`（默认可用 `/srv/funding/config/alerts.json`）
- `Environment=FUNDING_DB_PATH=...`
- `ExecStart`

然后启动：

```bash
sudo systemctl enable --now funding-stack
sudo systemctl status funding-stack
```

常用控制命令：

```bash
sudo systemctl status funding-stack --no-pager
sudo systemctl stop funding-stack
sudo systemctl disable funding-stack
sudo systemctl disable --now funding-stack
sudo systemctl start funding-stack
sudo systemctl enable funding-stack
sudo systemctl enable --now funding-stack
sudo journalctl -u funding-stack -f
```

说明：
- `disable` 只取消开机自启，不会停止当前服务
- `enable` 只恢复开机自启，不会立即启动当前服务
- 如果你之前执行过 `mask`，恢复前先执行 `sudo systemctl unmask funding-stack`

### 10.5 后续更新代码
服务器上后续更新代码时，直接执行：

```bash
cd /srv/funding
git pull
source .venv/bin/activate
pip install -U pip requests
sudo systemctl restart funding-stack
```

### 10.6 上线后验收

```bash
ps -Ao pid,etime,command | grep -E 'app.run_all_funding_stack|app.allfunding_dashboard' | grep -v grep
curl -s http://127.0.0.1:5000/api/data | python3 -c 'import sys,json; x=json.load(sys.stdin); print(len(x["items"]), len(x["exchanges"]))'
sudo journalctl -u funding-stack -n 200 --no-pager
```

说明：
- 健康检查不要使用 `HEAD /`，建议使用 `GET /` 或 `GET /api/data`。
- 单个交易所脚本也可以直接运行，路径统一位于 `exchanges/<exchange>/`。

## 11. systemd 服务
项目内已提供主服务模板：[systemd/funding-stack.service](/Users/m2/Desktop/Codex2026/Funding/systemd/funding-stack.service)

安装步骤：

```bash
cd /srv/funding
sudo cp systemd/funding-stack.service /etc/systemd/system/funding-stack.service
sudo systemctl daemon-reload
sudo systemctl enable --now funding-stack
sudo systemctl status funding-stack
sudo journalctl -u funding-stack -f
```

启用前请按服务器实际情况修改以下字段：
- `User`（模板默认值：`root`）
- `Group`（模板默认值：`root`）
- `WorkingDirectory`
- `Environment=FUNDING_EXCHANGE_CONFIG=...`（可选）
- `Environment=FUNDING_ALERT_CONFIG=...`（默认可用 `/srv/funding/config/alerts.json`）
- `Environment=FUNDING_DB_PATH=...`
- `ExecStart`

常用运维命令：

```bash
sudo systemctl status funding-stack --no-pager
sudo systemctl is-active funding-stack
sudo systemctl stop funding-stack
sudo systemctl disable funding-stack
sudo systemctl disable --now funding-stack
sudo systemctl start funding-stack
sudo systemctl enable funding-stack
sudo systemctl enable --now funding-stack
sudo journalctl -u funding-stack -n 200 --no-pager
sudo journalctl -u funding-stack -f
```

补充说明：
- `disable` 不会停止当前已经在跑的实例，只会取消开机自启
- `enable` 不会自动把当前服务拉起，只会恢复开机自启
- 需要立即停掉当前服务时，用 `stop` 或 `disable --now`
- 需要立即启动并恢复自启时，用 `start` 或 `enable --now`

## 12. 本地长跑日志清理
如果你在本地做过 `10h / 12h` 这类长时间测试，`logs/` 目录里会保留报告、日志和快照。

为了避免历史控制脚本影响整仓检查，可以在测试结束后执行：

```bash
python3 scripts/cleanup_logs.py
```

清理规则：
- 没有 `report.md` 的失败尝试目录：直接删除
- 有 `report.md` 的运行目录：保留报告、日志、快照，删除 `monitor_local_run.py`、`controller.sh`、`*.pid`、`*.plist` 等控制文件

清理后再次做整仓编译检查：

```bash
python3 -m compileall -q .
```
