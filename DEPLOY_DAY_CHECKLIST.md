# Deploy Day Checklist

## 1. 登录服务器
```bash
ssh <user>@<server>
cd /srv/funding
```

## 2. 确认代码与环境
```bash
pwd
python3 --version
find . -maxdepth 1 -type d | sort
test -d .venv || python3 -m venv .venv
source .venv/bin/activate
pip install -U pip requests
```

确认仓库结构至少包含：
- `app/`
- `core/`
- `exchanges/`
- `systemd/`
- `start_all_funding.sh`

## 3. 检查 systemd 配置
确认以下字段已按服务器实际情况修改：
- `User`
- `Group`
- `WorkingDirectory`
- `Environment=FUNDING_DB_PATH=...`
- `ExecStart`

参考文件：
- `systemd/funding-stack.service`

## 4. 首次空库预热一轮
如果是首次部署，或你准备从空库开始：

```bash
cd /srv/funding
source .venv/bin/activate
python -m app.run_all_funding_stack --once --no-open-browser --host 0.0.0.0 --port 5000
```

说明：
- 首次全量回填时间较长，属于正常现象。
- Binance history 中途长时间静默属于当前已知行为。

## 5. 安装并启动服务
```bash
cd /srv/funding
sudo cp systemd/funding-stack.service /etc/systemd/system/funding-stack.service
sudo systemctl daemon-reload
sudo systemctl enable --now funding-stack
```

## 6. 查看服务状态
```bash
sudo systemctl status funding-stack
sudo journalctl -u funding-stack -f
```

## 7. 上线验收
进程检查：

```bash
ps -Ao pid,etime,command | grep -E 'app.run_all_funding_stack|app.allfunding_dashboard' | grep -v grep
```

接口检查：

```bash
curl -s http://127.0.0.1:5000/api/data | python3 -c 'import sys,json; x=json.load(sys.stdin); print(len(x["items"]), len(x["exchanges"]))'
```

日志检查：

```bash
sudo journalctl -u funding-stack -n 200 --no-pager
```

数据库检查：

```bash
sqlite3 /srv/funding/funding.db "
SELECT 'binance', COUNT(*) FROM binance_funding_baseinfo
UNION ALL SELECT 'bybit', COUNT(*) FROM bybit_funding_baseinfo
UNION ALL SELECT 'aster', COUNT(*) FROM aster_funding_baseinfo
UNION ALL SELECT 'hyperliquid', COUNT(*) FROM hyperliquid_funding_baseinfo
UNION ALL SELECT 'backpack', COUNT(*) FROM backpack_funding_baseinfo
UNION ALL SELECT 'ethereal', COUNT(*) FROM ethereal_funding_baseinfo
UNION ALL SELECT 'grvt', COUNT(*) FROM grvt_funding_baseinfo
UNION ALL SELECT 'standx', COUNT(*) FROM standx_funding_baseinfo
UNION ALL SELECT 'lighter', COUNT(*) FROM lighter_funding_baseinfo;
"
```

## 8. 健康检查注意事项
- 不要用 `HEAD /` 做健康检查，建议用 `GET /` 或 `GET /api/data`
- 如果只想确认模块入口可执行，可以运行 `python -m app.run_all_funding_stack --help`

## 9. 回滚/停止
停止服务：

```bash
sudo systemctl stop funding-stack
```

禁止开机启动：

```bash
sudo systemctl disable funding-stack
```

如果需要手动杀进程：

```bash
pkill -f app.run_all_funding_stack
pkill -f app.allfunding_dashboard
```
