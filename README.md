
# TianGong AI Multi-agents

## Env Preparing

Install `Python 3.12`

```bash
sudo apt update
sudo apt install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt install python3.12
```

Setup `venv`:

```bash
sudo apt install python3.12-venv
python3.12 -m venv .venv
source .venv/bin/activate
```

Install requirements:

❗️ **Only used for coding with intelligent hints, it is NOT the runtime environment!** ❗️

```bash
pip install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
pip install -r requirements.txt --upgrade

pip freeze > requirements_freeze.txt
```

Compose up Airflow:

Optioncally, you can modify the following command to download an latest Airflow server.

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.0/docker-compose.yaml'
```

```bash

mkdir -p ./dags ./logs ./plugins ./config
echo -e "\nAIRFLOW_UID=$(id -u)" >> .env

docker compose up airflow-init

docker compose up -d
```

### Variable settings

<http://localhost:8080/login/>

localhost:8080 -> Admin -> Variables -> Add new record
Key: FAST_API_TOKEN #an example
Val: #your token defined in server
