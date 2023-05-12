import sys
sys.path.append("airflow-docker")

from airflow.models import BaseOperator
from pathlib import Path
from hook.tribunal_hook import TribunalHook
import json
import os 
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator


class TribunalOperator(BaseOperator):

    template_fields = ["exercicio", "mes", "municipio"]

    def __init__ (self, mes, exercicio, municipio, **kwargs):
        self.mes = mes
        self.exercicio = exercicio
        self.municipio = municipio
        super().__init__(**kwargs)
    
    def create_parent_folder(self):
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True) 
    
    def insert_mssql_hook(self, row, exercicio, mes ):
        mssql_hook = MsSqlHook(mssql_conn_id="sql_default", schema="tcesp")
        target_fields = ["exercicio", "mes_int","orgao", "mes","evento","nr_empenho", "id_fornecedor","nm_fornecedor","dt_emissao_despesa","vl_despesa"]
        for r in row:              
            lista_tuplas = []
            for dicionario in r:
                tupla = (exercicio, mes, dicionario['orgao'], dicionario['mes'], dicionario['evento'], dicionario['nr_empenho'], dicionario['id_fornecedor'], dicionario['nm_fornecedor'], dicionario['dt_emissao_despesa'], dicionario['vl_despesa'])
                lista_tuplas.append(tupla)
            mssql_hook.insert_rows(table="Despesas", rows=lista_tuplas, target_fields=target_fields)
                
        #(os.mkdir(path=self.file_path, parents=True, exist_ok=True))      

    def execute(self, context):
             pg = TribunalHook(self.municipio, self.mes, self.exercicio).run()
             despesa = json.dumps(pg, indent=4, sort_keys=False)
             despesa = json.loads(despesa)
             self.insert_mssql_hook(despesa,self.exercicio, self.mes)
                  
