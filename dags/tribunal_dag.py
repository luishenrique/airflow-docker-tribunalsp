import sys
sys.path.append("airflow-docker")

from airflow.models import DAG
from operators.tribunal_operator import TribunalOperator
from os.path import join
from airflow.utils.dates import days_ago
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
                  

with DAG(dag_id = "Tribunal_DAG", start_date=days_ago(2)) as dag:
        municipio = "hortolandia"
        exercicio = 2023
        jan = TribunalOperator(exercicio=exercicio,
                               mes="1", 
                               municipio=municipio,
                               task_id="Janeiro_run")
        fev = TribunalOperator( exercicio=exercicio,
                               mes="2", 
                               municipio=municipio,
                               task_id="Fevereiro_run")
        mar = TribunalOperator( exercicio=exercicio,
                               mes="3", 
                               municipio=municipio,
                               task_id="Março_run")
        abr = TribunalOperator( exercicio=exercicio,
                               mes="4", 
                               municipio=municipio,
                               task_id="Abril_run")
        
jan >> fev >> mar >> abr


