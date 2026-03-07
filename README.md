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
- 一键启动入口：`start_all_funding.sh`
- 主调度器：`run_all_funding_stack.py`
- 网页面板：`allfunding_dashboard.py`
- 交易所共享配置：`funding_exchanges.py`
- 长时监督压测：`long_run_supervisor.py`
- 定期健康巡检（可选）：`periodic_health_monitor.py`
- 数据库文件：`funding.db`

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

## 5. 常用参数
查看全部参数：

```bash
python3 run_all_funding_stack.py --help
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

## 6. 5 小时后台压测运行
用于“全量后台压测 + 失败统计”：

```bash
python3 long_run_supervisor.py --duration-hours 5 --until-clean --max-rounds 8 --no-open-browser
```

输出文件：
- 轮次日志：`/tmp/funding_supervisor_round*.log`
- 汇总结果：`/tmp/funding_supervisor_summary.json`

## 7. 健康巡检（可选）
该脚本会定期检查：
- 核心进程是否存在
- 数据库关键字段是否为空
- 最新监督日志是否出现失败关键字

前台运行：

```bash
python3 periodic_health_monitor.py --db funding.db --log-dir /tmp --interval-sec 600
```

后台运行：

```bash
nohup python3 periodic_health_monitor.py --db funding.db --log-dir /tmp --interval-sec 600 > /tmp/funding_periodic_monitor.log 2>&1 &
```

注意：该巡检脚本默认期望 `long_run_supervisor.py`、`run_all_funding_stack.py`、`allfunding_dashboard.py` 三个进程都在运行。

## 8. 停止服务
停止主调度/面板：

```bash
pkill -f run_all_funding_stack.py
pkill -f allfunding_dashboard.py
pkill -f long_run_supervisor.py
```

停止巡检：

```bash
pkill -f periodic_health_monitor.py
```

## 9. 快速自检命令
查看关键进程：

```bash
ps -Ao pid,etime,command | grep -E 'run_all_funding_stack.py|allfunding_dashboard.py|long_run_supervisor.py' | grep -v grep
```

查看监督日志中的失败关键词：

```bash
latest=$(ls -t /tmp/funding_supervisor_round*.log | head -n 1)
grep -nE '\[fail\]|脚本失败|Traceback|ERROR' "$latest"
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

## 10. 部署建议（Linux）
当前项目依赖极少，建议优先“直接在 Linux 服务器运行”：
- 部署简单
- 进程与日志排查方便
- 对单机采集场景足够稳定

如果后续有以下需求，再考虑 Docker：
- 多环境一致性强需求
- 需要 CI/CD 镜像发布
- 需要容器编排（如 Kubernetes）

## 11. 服务器部署命令
以下示例假设项目部署目录为 `/srv/funding`。

安装依赖：

```bash
cd /srv/funding
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip requests
```

首次空库预热一轮：

```bash
cd /srv/funding
source .venv/bin/activate
python run_all_funding_stack.py --once --no-open-browser --host 0.0.0.0 --port 5000
```

正式后台运行：

```bash
cd /srv/funding
nohup ./.venv/bin/python run_all_funding_stack.py \
  --no-open-browser \
  --host 0.0.0.0 \
  --port 5000 \
  > /tmp/funding_stack.log 2>&1 &
```

如果数据库需要放到单独可写目录：

```bash
cd /srv/funding
nohup ./.venv/bin/python run_all_funding_stack.py \
  --no-open-browser \
  --host 0.0.0.0 \
  --port 5000 \
  --db-path /data/funding/funding.db \
  > /tmp/funding_stack.log 2>&1 &
```

上线后验收：

```bash
ps -Ao pid,etime,command | grep -E 'run_all_funding_stack.py|allfunding_dashboard.py' | grep -v grep
curl -s http://127.0.0.1:5000/api/data | python3 -c 'import sys,json; x=json.load(sys.stdin); print(len(x[\"items\"]), len(x[\"exchanges\"]))'
grep -nE '\[error\]|\[warn\].*exited with code|Traceback' /tmp/funding_stack.log
```

说明：
- 首次空库全量回填耗时较长，属于正常现象。
- 健康检查不要使用 `HEAD /`，建议使用 `GET /` 或 `GET /api/data`。
- 如果线上不运行 `long_run_supervisor.py`，则不建议直接依赖 `periodic_health_monitor.py` 的进程告警结果。

## 12. systemd 服务
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
