from airflow.providers.http.hooks.http import HttpHook
import requests


class TribunalHook(HttpHook):

    def __init__(self, municipio, mes, exercicio,  conn_id=None):
        self.municipio = municipio
        self.mes = mes
        self.exercicio = exercicio
        self.conn_id = conn_id or "tribunal_default"
        super().__init__(http_conn_id=self.conn_id)
    
    def create_url(self):
        url_raw = f"{self.base_url}/api/json/despesas/{self.municipio}/{self.exercicio}/{self.mes}"
        return url_raw
    
    def connect_to_endpoint_api(self, url, session):
        request = requests.Request("GET", url)
        prep = session.prepare_request(request)
        self.log.info(f"URL: {url}")
        return self.run_and_check(session, prep, {})

    def paginate(self, url_raw, session):        
        lista_json_response = []
        #imprimir json
        response = self.connect_to_endpoint_api(url_raw, session)
        json_response = response.json()
        lista_json_response.append(json_response)
        return lista_json_response    
        
    def run(self):
        session = self.get_conn()
        url_raw = self.create_url()
        return self.paginate(url_raw, session)


