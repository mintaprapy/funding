# Funding 面板项目说明

## 1. 项目简介
本项目用于采集多个交易所永续合约资金费率数据，并写入本地 SQLite 数据库，再通过内置网页面板展示。

当前覆盖交易所（9 家）：
- Binance
- Bybit
- Aster
- Hyperliquid
- Backpack
- Ethereal
- GRVT
- StandX
- Lighter

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
- systemd 模板目录：`systemd/`
- 运行数据库：`funding.db`
- 运行日志目录：`logs/`

当前仓库根目录只保留 shell 入口、部署文件和文档；Python 代码已按职责拆到 `app/`、`core/`、`exchanges/`，GitHub 首页会明显更干净。

简化后的结构如下：

```text
Funding/
├── README.md
├── DEPLOY_DAY_CHECKLIST.md
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
│   └── lighter_funding/
├── logs/
├── funding.db
└── systemd/
```

职责说明：
- `app/`：总调度器和总 dashboard 的入口模块
- `core/`：公共 SQLite、限速、交易所注册表等共享代码
- `exchanges/`：各交易所的 `baseinfo`、`history`、单交易所 dashboard
- `systemd/`：线上部署用服务模板
- `start_all_funding.sh`：面向人工执行的统一入口

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

常见调试命令：

```bash
python3 -m app.allfunding_dashboard --host 127.0.0.1 --port 5000
python3 -m app.run_all_funding_stack --once --skip-dashboard --no-run-on-start
```

## 6. 停止服务
停止主调度/面板：

```bash
pkill -f app.run_all_funding_stack
pkill -f app.allfunding_dashboard
```

## 7. 快速自检命令
查看关键进程：

```bash
ps -Ao pid,etime,command | grep -E 'app.run_all_funding_stack|app.allfunding_dashboard' | grep -v grep
```

检查总面板接口：

```bash
curl -s http://127.0.0.1:5000/api/data | python3 -c 'import sys,json; x=json.load(sys.stdin); print(len(x["items"]), len(x["exchanges"]))'
```

检查 9 家交易所 `baseinfo` 关键字段空值：

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
SELECT 'lighter',  SUM(CASE WHEN markPrice IS NULL OR TRIM(markPrice)='' THEN 1 ELSE 0 END), SUM(CASE WHEN lastFundingRate IS NULL OR TRIM(lastFundingRate)='' THEN 1 ELSE 0 END), SUM(CASE WHEN openInterest IS NULL OR TRIM(openInterest)='' THEN 1 ELSE 0 END) FROM lighter_funding_baseinfo;
"
```

## 8. 部署建议（Linux）
当前项目依赖极少，建议优先“直接在 Linux 服务器运行”：
- 部署简单
- 进程与日志排查方便
- 对单机采集场景足够稳定

如果后续有以下需求，再考虑 Docker：
- 多环境一致性强需求
- 需要 CI/CD 镜像发布
- 需要容器编排（如 Kubernetes）

## 9. GitHub 首页部署步骤
下面这套命令按顺序执行即可，适合直接放到服务器上从 GitHub 拉代码部署。
建议优先走这条路径：`git clone -> 安装依赖 -> 先跑一次 --once -> systemd 启动`。

### 9.1 拉取代码
SSH 方式：

```bash
cd /srv
git clone git@github.com:mintaprapy/funding.git
cd funding
```

HTTPS 方式：

```bash
cd /srv
git clone https://github.com/mintaprapy/funding.git
cd funding
```

### 9.2 安装依赖

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip requests
```

### 9.3 首次空库预热
首次部署建议先跑一轮空库预热，确认采集链路和数据库都正常：

```bash
source .venv/bin/activate
python -m app.run_all_funding_stack --once --no-open-browser --host 0.0.0.0 --port 5000
```

说明：
- 这一步建议保留，不要跳过
- 首次空库全量回填耗时较长，属于正常现象

### 9.4 用 systemd 启动（推荐）
先安装服务文件：

```bash
sudo cp systemd/funding-stack.service /etc/systemd/system/funding-stack.service
sudo systemctl daemon-reload
```

按服务器实际情况修改这些字段：
- `User`
- `Group`
- `WorkingDirectory`
- `Environment=FUNDING_DB_PATH=...`
- `ExecStart`

然后启动：

```bash
sudo systemctl enable --now funding-stack
sudo systemctl status funding-stack
```

### 9.5 后续更新代码
服务器上后续更新代码时，直接执行：

```bash
cd /srv/funding
git pull
source .venv/bin/activate
pip install -U pip requests
sudo systemctl restart funding-stack
```

### 9.6 上线后验收

```bash
ps -Ao pid,etime,command | grep -E 'app.run_all_funding_stack|app.allfunding_dashboard' | grep -v grep
curl -s http://127.0.0.1:5000/api/data | python3 -c 'import sys,json; x=json.load(sys.stdin); print(len(x["items"]), len(x["exchanges"]))'
sudo journalctl -u funding-stack -n 200 --no-pager
```

说明：
- 健康检查不要使用 `HEAD /`，建议使用 `GET /` 或 `GET /api/data`。
- 单个交易所脚本也可以直接运行，路径统一位于 `exchanges/<exchange>/`。

## 10. systemd 服务
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
- `User`
- `Group`
- `WorkingDirectory`
- `Environment=FUNDING_DB_PATH=...`
- `ExecStart`
