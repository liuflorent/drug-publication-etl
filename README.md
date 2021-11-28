# Date pipeline Project
### _The drug publication analytis with pyspark_

## Description
The objective is to build a data pipeline to process the data defined in the data ressources in order to generate the result with two goals:

- This oject is to create a JSON output which represents a link graph between the different drugs and their respective mentions in the different PubMed publications, the scientific publications and finally the newpapers with the date associated with each of these mentions. 
- Find the name fo one or serveral newpapers from the json produced by the data pipeline that mention the most different drugs. 
> Two rules have to be respected:
> A drug is considered mentioned in a PubMed article or clinical trial if it is mentioned in the title of the publication.
> A drug is considered mentioned by a newspaper if it is mentioned in a publication issued by that newspaper.

##  Data Resources

- drugs.csv
- clinical_trials.csv
- pubmed.csv
- pubmed.json

## Features

- Build a mvc pipeline architecture
-- app: view
-- service: controler
-- model: dao
- Build output data which represents a link graph between the different drugs and their respective mentions in the different PubMed publications, the scientific publications and finally the newpapers with the date associated with each of these mentions

>Demo:

| drug | journal | clinical_trial | pubmed  | date | 
| ------ | ------ | ------ | ------ | ------ |
| DIPHENHYDRAMINE | Journal of emerge... |yes | no | 1 January 2020 |
| BETAMETHASONE | Hôpitaux Universi... |yes | no | 27 April 202 |
| ... | ... |... | ... |... | ... |

- Build output data which mentions the name fo one or serveral newpapers from the json produced by the data pipeline that mention the most different drugs

>Demo:

| drugs_count | journal |
| ------ | ------ | 
| 2 | Journal of emerge... |
| 2 | Psychopharmacology |


## Run & Installation Features
- Running as a Python Project
>Command-line
```
        python main.py input/drugs.csv \
                       input/clinical_trials.csv \
                       input/pubmed.csv \
                       input/pubmed.json \
                       output
```
- Running as a Spark Standalone job
-- Create an egg file by running python setup.py bdist_egg
-- Submit your spark application: spark-submit --py-files=drug-publication-etl-0.1.egg main.py with 5 argv
- Running as a task which executes Spark app in the DAG in Apache Airflow with the differents choices:
-- SparkSubmitOperator
-- SparkKubernetesOperator

>SparkSubmitOperator example:
```
spark_job_task = SparkSubmitOperator(task_id='task_id',
        conn_id='spark_local',
        application=f'{pyspark_app_home}/main.py',
        total_executor_cores=4,
        packages="{pyspark_packages}",
        executor_cores=2,
        executor_memory='5g',
        driver_memory='5g',
        name='spark_job_task',
        execution_timeout=timedelta(minutes=120),
        dag=dag
)
```
> SparkKubernetesOperator example:
```
spark_pi_submit = SparkKubernetesOperator(
    task_id='spark_pi_submit',
    namespace="default",
    application_file="spark_kubernetes_spark_pi.yaml",
    do_xcom_push=True,
    dag=dag,
)

spark_pi_monitor = SparkKubernetesSensor(
    task_id='spark_pi_monitor',
    namespace="default",
    application_name="{{ task_instance.xcom_pull(task_ids='spark_pi_submit')['metadata']['name'] }}",
    dag=dag,
)
spark_pi_submit >> spark_pi_monitor
```
## Unittest
- Tests are implemented in tests folder using pytest framework
- How to Run Test cases
-- Create a virtual environment: "virtualenv pyspark-venv" or "conda create -n pyspark-venv python=3.x"
-- Activate the virtual environment: "source pyspark-ven/bin/activate" or "conda activate pyspark-venv"
-- Install the test's dependency: "pip install -r requirements.txt"
-- Run the test: "pytest"

## Tech

This project uses a number of open source projects to work properly:
- [Python3] 
- [PySpark]
- [Apache Spark]