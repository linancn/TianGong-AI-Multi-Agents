import json
import time

import httpx
import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule


def get_content(topic: str, topK: int, extK: int):
    url = Variable.get("URL_SCI")
    headers = {
        "email": Variable.get("EMAIL"),
        "password": Variable.get("PASSWORD"),
        "Authorization": "Bearer " + Variable.get("TOKEN"),
        "Content-Type": "application/json",
    }
    payload = {"query": topic, "topK": topK, "extK": extK}

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        try:
            return response.json()
        except json.JSONDecodeError:
            print("Error decoding JSON response")
            return None
    else:
        print(f"Request failed with status code {response.status_code}")
        return None
    pass


with DAG(
    dag_id="EXTRACT_CARBON_FOOTPRINT",
    description="A simple DAG to extract the carbon footprint from the given topic.",
    schedule_interval=None,
    tags=["LCA TASKS"],
    catchup=False,
) as dag:

    @task
    def get_contents():
        contents = get_content(
            topic="LCA analysis of cement production", topK=10, extK=5
        )
        return contents

    @task
    def process_content(content):
        print("---------- PROCESS: GET DATA ----------")
        print("---- CONTENT ----")
        print(content)

        headers = {
            "email": Variable.get("EMAIL"),
            "password": Variable.get("PASSWORD"),
            "Authorization": "Bearer " + Variable.get("TOKEN"),
            "Content-Type": "application/json",
        }

        payload = {"query": content}

        BASE_URL = "http://host.docker.internal:8000"
        POST_URL = "/main/meta"

        with httpx.Client(base_url=BASE_URL, headers=headers, timeout=120) as client:
            try:
                response = client.post(POST_URL, json=payload)
                print("---- response ----")
                print(response.json())
                print("---- flows ----")
                flows_json = response.json()
                flows = flows_json["kwargs"]["tool_calls"][0]["args"]["data"]
                flows_ls = []
                for flow in flows:
                    flow["source"] = content  # 如果需要，可以替换为实际的 source
                    flows_ls.append(flow)
                return flows_ls  # 返回处理后的数据
            except httpx.RequestError as exc:
                print(f"An error occurred while requesting: {exc}")
                return None

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def output_data(processed_contents_list):
        print("---------- PROCESS: OUTPUT DATA ----------")

        researchObject = []
        location = []
        functionalUnit = []
        outputAmount = []
        processMethods = []
        climateChangeImpact = []
        source = []

        for flows in processed_contents_list:
            if flows:
                for flow in flows:
                    researchObject.append(flow.get("researchObject"))
                    location.append(flow.get("location"))
                    functionalUnit.append(flow.get("functionalUnit"))
                    outputAmount.append(flow.get("outputAmount"))
                    processMethods.append(flow.get("processMethods"))
                    climateChangeImpact.append(flow.get("climateChangeImpact"))
                    source.append(flow.get("source"))

        now = time.strftime("%Y%m%d%H%M%S", time.localtime(time.time()))
        filename = "./results/carbon_footprint_" + now + ".csv"

        pd.DataFrame(
            {
                "researchObject": researchObject,
                "location": location,
                "functionalUnit": functionalUnit,
                "outputAmount": outputAmount,
                "processMethods": processMethods,
                "climateChangeImpact": climateChangeImpact,
                "source": source,
            }
        ).to_csv(filename, index=False)

    contents = get_contents()
    processed_contents = process_content.expand(content=contents)
    output_data(processed_contents)
