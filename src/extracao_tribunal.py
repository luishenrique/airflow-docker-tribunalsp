import requests
import json
import pyodbc
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator


municipio = "hortolandia"
mes = 3
exercicio = 2023

url_municipios = "https://transparencia.tce.sp.gov.br/api/json/municipios"
url_desp = f"https://transparencia.tce.sp.gov.br/api/json/despesas/{municipio}/{exercicio}/{mes}"

def connect_to_endpoint(url):
    response = requests.get(url)    
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def insert_mssql(rows):
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=GOVBR7564;'
                      'Database=TCESP;'
                      'Trusted_Connection=no;'
                      'UID=PRONIM;'
                      'PWD=senha')
    cursor = conn.cursor()
    for row in rows: 
        cursor.execute("INSERT INTO Municipios (municipio, municipio_extenso) values(?,?)" , row["municipio"], row["municipio_extenso"])
        conn.commit()
    cursor.close()

def main():
    json_response = connect_to_endpoint(url_desp)
    list = json.dumps(json_response, indent=4, sort_keys=True)
    #list = json.loads(list)
    print(list)
    #insert_mssql(list)


if __name__ == "__main__":
    main()

#{'municipio': 'adamantina', 'municipio_extenso': 'Adamantina'}