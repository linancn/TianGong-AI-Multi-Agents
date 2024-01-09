from functools import partial

import httpx
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator as DummyOperator
from httpx import RequestError

BASE_URL = "http://host.docker.internal:8000"
TOKEN = Variable.get("FAST_API_TOKEN")


def post_request(
    post_url: str,
    formatter: callable,
    ti: any,
    data_to_send: dict = None,
):
    if not data_to_send:
        data_to_send = formatter(ti)
    headers = {"Authorization": f"Bearer {TOKEN}"}
    with httpx.Client(base_url=BASE_URL, headers=headers, timeout=120) as client:
        try:
            response = client.post(post_url, json=data_to_send)

            response.raise_for_status()

            data = response.json()

            return data

        except RequestError as req_err:
            print(f"An error occurred during the request: {req_err}")
        except Exception as err:
            print(f"An unexpected error occurred: {err}")


agent_url = "/openai_agent/invoke"
openai_url = "/openai/invoke"


def agent_formatter(ti):
    data = ti.xcom_pull(task_ids="openai")
    content = data["output"]["content"]
    formatted_data = {
        "input": {"input": content},
        "config": {"configurable": {"session_id": ""}},
    }
    return formatted_data


def openai_formatter(data):
    pass


agent = partial(post_request, post_url=agent_url, formatter=agent_formatter)
openai = partial(post_request, post_url=openai_url, formatter=openai_formatter)


default_args = {
    "owner": "airflow",
}


with DAG(
    dag_id="lca_agent_dag",
    default_args=default_args,
    description="LCA Agent DAG",
    schedule_interval=None,
    tags=["lca_agent"],
    catchup=False,
) as dag:

    task_1 = PythonOperator(
        task_id="openai",
        python_callable=openai,
        op_kwargs={"data_to_send": {"input": "翻译成英文：今天北京天气如何?"}},
    )
    task_2 = PythonOperator(
        task_id="agent",
        python_callable=agent,
    )
    # task_3 = DummyOperator(
    #     task_id="start1",
    # )
    # task_4 = DummyOperator(
    #     task_id="start2",
    # )
    # task_5 = DummyOperator(
    #     task_id="end",
    # )


    task_1 >> task_2
    # task_1 >> task_2 >> [task_3, task_4] >> task_5
