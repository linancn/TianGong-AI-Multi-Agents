
# TianGong AI Multi-agents

## Env Preparing

Install `Python 3.11`

```bash
sudo apt update
sudo apt install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt install python3.11
```

Setup `venv`:

```bash
sudo apt install python3.11-venv
python3.11 -m venv .venv
source .venv/bin/activate
```

Install requirements:

```bash
pip install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
pip install -r requirements.txt --upgrade

pip freeze > requirements_freeze.txt
```

Compose up Airflow:

```bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "\nAIRFLOW_UID=$(id -u)" >> .env

docker compose up airflow-init

docker compose up -d
```

### Variable settings
```
localhost:8080 -> Admin -> Variables -> Add new record
Key: FAST_API_TOKEN #an example
Val: #your token defined in server
```


### Auto Build

The auto build will be triggered by pushing any tag named like release-v$version. For instance, push a tag named as v0.0.1 will build a docker image of 0.0.1 version.

```bash
#list existing tags
git tag
#creat a new tag
git tag v0.0.1
#push this tag to origin
git push origin v0.0.1
```
