from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG(
    'Executando-notebook-etl',
    start_date=datetime(2024, 8, 21),
    schedule_interval="0 9 * * *"
) as dag_executando_notebook_extracao:
    extraindo_dados = DatabricksRunNowOperator(
        task_id='Extraindo-conversoes',
        databricks_conn_id='databricks_default',
        job_id=1069484085430183,
        notebook_params={"data_execucao": '{{ data_interval_end.strftime("%Y-%m-%d") }}'}
    )
    
    transformando_dados = DatabricksRunNowOperator(
        task_id='Transformando-dados',
        databricks_conn_id='databricks_default',
        job_id=355237088238219,
    )
    
    enviando_relatorio = DatabricksRunNowOperator(
        task_id='Enviando-relatorio',
        databricks_conn_id='databricks_default',
        job_id=603930067998198,
    )

    extraindo_dados >> transformando_dados >> enviando_relatorio
