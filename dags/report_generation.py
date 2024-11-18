import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests
import time
from datetime import timedelta

# Function to make the API request
def call_agent_towrite(**op_kwargs):
    url = Variable.get("URL_ESG_GRAPH")
    email = Variable.get("email")
    password = Variable.get("password")
    headers = {
        "email": email,
        "password": password,
        "Content-Type": "application/json"
    }
    if 'input' in op_kwargs:
        contents = op_kwargs['ti'].xcom_pull(task_ids=op_kwargs['upstream_task_id'], key = 'input')
        context = "\n\n" + 'Content: ' + str(contents)
    else:
        context = ""

    payload = {
        "query":str(op_kwargs['prompts']) + context,
    }  
    print("---------- Start Work!  ----------")  
    print(payload)
    response = requests.post(url, headers=headers, json=payload)
    return response.json()


def process_output(**op_kwargs):    
    ti = op_kwargs['ti']
    task_id = ti.task_id
    rag_output = ""
    if task_id == 'generate_report':      
        print("---------- report output  ----------")
        for task in op_kwargs['upstream_task_id']:
            rag_output += str(ti.xcom_pull(task_ids=task,key='input')) + "\n"
        now = time.strftime("%Y%m%d%H%M%S",time.localtime(time.time())) 
        with open("esgreport_" + str(now) + ".md", mode="w+") as f:
            f.write(rag_output)
        print("---------- Finished!  ----------")
        f.close()
        return rag_output
    else:
        for task in op_kwargs['upstream_task_id']:
            rag_output += str(ti.xcom_pull(task_ids=task)['kwargs']['content']) + "\n\n\n"
        ti.xcom_push(key='input', value=str(rag_output)) # 将输出传递给下游任务，用input作为key进行获取

# Read the prompts from the prompts.json file
prompts_dic = {
    "introduction1": {
        "prompts": "As ESG expert, write a sub-section of Introduction to introduce the circular economy and its significance in achieving sustainable development goals, particularly in carbon emission reduction, and then use the latest examples with exact statistics of innovative policies, regulations, and standards to illustrate the positive impacts of circular economy actions (e.g., global and regional efforts) and most importanly highlight China's efforts."
    },
    "introduction2": {
        "prompts": "As ESG expert, write a sub-section of Introduction to provide an overview of Xianyu Platform, describe its role in the circular economy, and highlight its ESG commitments,  referencing to the retrieved information on the introduction to the Xianyu platform and the CEO speeches."
    },
    "introduction3": {
        "prompts": "As ESG expert, write a sub-section of Introduction to detail how Xianyu promotes carbon emission avoidance through second-hand trading and recycling efforts, highlight the alignment of Xianyu efforts to the worldwide-accepted guideline, and explain how Xianyu actions align with Alibaba initiatives of Scope 3+ emission reduction goals, referencing to 《Alibaba scope3+ Emissions Resudction 2022-A new methodology for corporate climate actions beyond value chains》 and 《Guidance on avoided emissions.\n Following the guidelines."    },
    "result1": {
        "prompts": "As a professor experting in writing academic paper, please write a paragraph of the Results section to briefly define the system boundary, including time, space, research objectives and other important information, referencing to《2024财年范围3+减排量报告书》."
    },
    "result2": {
        "prompts": "As a professor experting in writing academic paper, please write a part of Results with a sub-title of key findings to elaborate on the Scope 3+ Emission Reduction by Xianyu efforts on trading second-hand goods, referencing to the retrieved report of《2024财年范围3+减排量报告书》. Please using specific data points, tables, and figures to analyze and compare the carbon emission with Baseline scenarios by goods and highlight the avoided emissions by Xianyu."
    },
    "result3": {
        "prompts": "As a professor experting in writing academic paper, please write a part of Results with a sub-title of key findings to elaborate on the Scope 3+ Emission Reduction by Xianyu efforts on recycling end-of-life products, referencing to the retrieved report of《2024财年范围3+减排量报告书》.\n Please using specific data points, tables, and figures to analyze and compare the carbon emission with Baseline scenarios by end-of-life products and highlight the avoided emissions by Xianyu. \n "
    },
    "methodology1": {
        "prompts": "As a professor experting in writing academic paper, follow the requirements of making credible avoided emissions claims by WBSCD《Guidance on avoided emissions》, to write a part of the Methodology. Please reference to 《互联网闲置物品交易碳排放量化方法学研究报告》, T/CACE 087.1—2023 and 《Scope 3+ Emissions Reduction》, to specify assumptions, models of emission reduction calculation, price convention, etc. with corresponding formulas, product categories, emission factors, and other relevant data to provide a detailed explanation of the methodology used to calculate avoided emissions through trading second-hand goods on the Xianyu platform."
        },
    "methodology2": {
        "prompts": "As a professor experting in writing academic paper, follow the requirements of making credible avoided emissions claims by WBSCD《Guidance on avoided emissions》, to write a sub-section of the Methodology. Referencing to T/CACE 034.3—2023 and 《Scope 3+ Emissions Reduction》, specify assumptions, models with corresponding formulas, product categories, emission factors, and other relevant data to provide a detailed explanation of the methodology used to calculate avoided emissions through recycling end-of-life products on the Xianyu platform."
        },
    "discussion1":{
        "prompts":"As a professor experting in writing academic paper, according to the 'Results' section, write a sub-section of the Discussion to compare the current report disclosures with the avoided emissions criteria outlined in the the retrieved report titled 《Guidance on avoided emissions》published by wbcsd."
        },
    "discussion2":{
        "prompts":"As a professor experting in writing academic paper, write a sub-section of the Discussion referencing to the retrieved report titled《互联网闲置物品交易碳排放量化方法学研究报告》, which analyze the data sensitivity and limitations related to Scope 3+ with focuses on system boundary, factor selection, emission calculation models, etc.\n"
        },
    "discussion3":{
        "prompts":"As a professor experting in writing academic paper, according to the 'Results' section, write a sub-section of the Discussion to provide future outlooks and recommendations."
    },
    "summary":{
        "prompts":" Summarize this report, focusing on the following aspects:\n 1. Briefly introduce the nexus of climate change, carbon emissions and the significance of circular economy actions on reducing carbon emissions. Then, introduce scope3+ carbon emissions and Xianyu efforts and contributions to their reductions.\n 2. Summarize the pathways of Xianyu to reduce carbon emissions through second-hand trading and recycling end-of-life products according to the results of avoided carbon emission, and highlight the value of Xianyu company.\n 3. Outlook: picturing the future, provide recommendations for Xianyu to further enhance its circular economy initiatives and carbon emission reduction efforts, and suggest potential areas for improvement and growth."
    },
    "Additional_considerations":{
        "prompts":"According to the generated report, compose a section of additional considerations to create greater comparability and consistency and minimize any misstatement risks, according to the following details: \n 1. Rationale behind the chosen reference scenario(s) (e.g., new/existing demand, improvement/replacement, led by legislation). \n 2. Whether attributional or consequential approaches were used for the assessment. \n3. Sources and key hypotheses used to defineand calculate the life cycle GHG emissions of the reference scenario and solution, including the solution’s lifespan. \n 4. A quantitative estimate or qualitative description of the uncertainty of the results, listing key assumptions and limitations associated with the calculations. \n 5. A qualitative and quantitative assessment of the specificity score.\n 6. Any potential materiality threshold used in the calculation process"
    }
}


# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'execution_timeout': timedelta(minutes=60),
}

# Define the DAG
with DAG(
    dag_id="xianyu_reports",
    default_args=default_args,
    description="A simple DAG to generate esg reports",
    schedule=None,
    tags=["esg_reports"],
    catchup=False,
) as dag:
    # Create a task to call the RAG service
    introduction1 = PythonOperator(
        task_id='introduction1',
        python_callable=call_agent_towrite,
        op_kwargs={'prompts': prompts_dic.get('introduction1')['prompts']},
    )
    introduction2 = PythonOperator(
        task_id='introduction2',
        python_callable=call_agent_towrite,
        op_kwargs={'prompts': prompts_dic.get('introduction2')['prompts']},
    )
    introduction3 = PythonOperator(
        task_id='introduction3',
        python_callable=call_agent_towrite,
        op_kwargs={'prompts': prompts_dic.get('introduction3')['prompts']},
    )

    result1 = PythonOperator(
        task_id='result1',
        python_callable=call_agent_towrite,
        op_kwargs={'prompts': prompts_dic.get('result1')['prompts']},
    )
    result2 = PythonOperator(
        task_id='result2',
        python_callable=call_agent_towrite,
        op_kwargs={'prompts': prompts_dic.get('result2')['prompts']},
    )
    result3 = PythonOperator(
        task_id='result3',
        python_callable=call_agent_towrite,
        op_kwargs={'prompts': prompts_dic.get('result3')['prompts']},
    )

    discussion1 = PythonOperator(
        task_id='discussion1',
        python_callable=call_agent_towrite,
        op_kwargs={'prompts': prompts_dic.get('discussion1')['prompts']},
    )
    discussion2 = PythonOperator(
        task_id='discussion2',
        python_callable=call_agent_towrite,
        op_kwargs={'prompts': prompts_dic.get('discussion2')['prompts']},
    )
    discussion3 = PythonOperator(
        task_id='discussion3',
        python_callable=call_agent_towrite,
        op_kwargs={'prompts': prompts_dic.get('discussion3')['prompts']},
    )

    methodology1 = PythonOperator(
        task_id='methodology1',
        python_callable=call_agent_towrite,
        op_kwargs={'prompts': prompts_dic.get('methodology1')['prompts']},
    )
    methodology2 = PythonOperator(
        task_id='methodology2',
        python_callable=call_agent_towrite,
        op_kwargs={'prompts': prompts_dic.get('methodology2')['prompts']},
    )

    # appendix = PythonOperator(
    #     task_id='appendix',
    #     python_callable=call_agent_towrite,
    #     op_kwargs={'prompts': prompts_dic.get('appendix')['prompts']},
    # )

    introduction = PythonOperator(
        task_id='introduction',
        python_callable=process_output,
        op_kwargs={'upstream_task_id': list(['introduction1', 'introduction2', 'introduction3'])},
        # op_kwargs={'upstream_task_id': list(['introduction', 'result'])},
    )

    result = PythonOperator(
        task_id='result',
        python_callable=process_output,
        op_kwargs={'upstream_task_id': list(['result1', 'result2', 'result3'])},
        # op_kwargs={'upstream_task_id': list(['introduction', 'result'])},
    )

    discussion = PythonOperator(
        task_id='discussion',
        python_callable=process_output,
        op_kwargs={'upstream_task_id': list(['discussion1', 'discussion2', 'discussion3'])},
        # op_kwargs={'upstream_task_id': list(['introduction', 'result'])},
    )
    methodology = PythonOperator(
        task_id='methodology',
        python_callable=process_output,
        op_kwargs={'upstream_task_id': list(['methodology1', 'methodology2'])},
        # op_kwargs={'upstream_task_id': list(['introduction', 'result'])},
    )

    summary0 = PythonOperator(
        task_id='summary0',
        python_callable=call_agent_towrite,
        op_kwargs=
        {
            'prompts': prompts_dic.get('summary')['prompts'],
            'upstream_task_id': list(['introduction',  'methodology', 'result', 'discussion']),
        },
    )

    summary = PythonOperator(
        task_id='summary',
        python_callable=process_output,
        op_kwargs=
        {
            'upstream_task_id': list(['summary0']),
        },
    )

    report_output = PythonOperator(
        task_id='generate_report',
        python_callable=process_output,
        op_kwargs={
            'upstream_task_id': list(['summary','introduction',  'methodology', 'result', 'discussion']),},
    )

    # 设置任务依赖关系
    [introduction1, introduction2] >> introduction3
    [introduction1, introduction2, introduction3] >> introduction
    [result1, result2, result3] >> result
    [methodology1, methodology2] >> methodology
    result >> [discussion1, discussion2, discussion3] >> discussion
    methodology >> [discussion1, discussion2, discussion3]
    [discussion1, discussion2, discussion3] >> discussion
    [introduction, result, methodology, discussion] >> summary0 >> summary
    [summary, introduction, result, methodology, discussion] >> report_output
