# Options Dashboard

Dash application for option percentile, volatility, solver, and strategy views.

## Local Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python index.py
```

Open `http://localhost:8050` unless you change `OPTIONS_PORT`.

## Data Location

The app does not store option data in Git. Set `OPTIONS_DATA_ROOT` to a directory containing:

```text
master/
percentile_master/
strategies_master/
raw_percent/
percentile/
strategies/
logs/
```

Example:

```bash
export OPTIONS_DATA_ROOT=/opt/options_data
```

## Production Run

```bash
source .venv/bin/activate
gunicorn -b 0.0.0.0:8050 index:server
```

## Azure VM Deployment Sketch

```bash
sudo apt update
sudo apt install -y python3-venv python3-pip git nginx

sudo mkdir -p /opt/options_dashboard /opt/options_data
sudo chown -R "$USER":"$USER" /opt/options_dashboard /opt/options_data

git clone https://github.com/lejams/options_dashboard.git /opt/options_dashboard
cd /opt/options_dashboard

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env` and set the real data path and credentials if API scripts will run on the VM.

For a persistent service, copy `deploy/options-dashboard.service` to `/etc/systemd/system/options-dashboard.service`, then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable options-dashboard
sudo systemctl start options-dashboard
sudo systemctl status options-dashboard
```

## Docker

```bash
docker build -t options-dashboard .
docker run --rm -p 8050:8050 --env-file .env -v /opt/options_data:/opt/options_data options-dashboard
```
