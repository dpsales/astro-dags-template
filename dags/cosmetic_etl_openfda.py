from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
# from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import json
import requests
import logging

# Função para validar os dados inseridos
def validate_data_in_postgres(**context):
    """
    Valida se a contagem de registros na tabela de metadados corresponde à contagem na tabela de resultados.
    """
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    execution_date = context['ds']
    
    # Obter a contagem de registros da tabela de metadados
    meta_sql = "SELECT total_records FROM openfda_cosmetic_meta WHERE execution_date = %s"
    meta_records = postgres_hook.get_first(meta_sql, parameters=(execution_date,))
    
    if not meta_records:
        raise ValueError(f"Nenhum metadado encontrado para a data de execução: {execution_date}")
        
    expected_records = meta_records[0]
    
    # Obter a contagem de registros da tabela de resultados
    results_sql = "SELECT COUNT(*) FROM openfda_cosmetic_results WHERE execution_date = %s"
    inserted_records = postgres_hook.get_first(results_sql, parameters=(execution_date,))[0]
    
    logging.info(f"Validação para {execution_date}: Esperado={expected_records}, Inserido={inserted_records}")
    
    if expected_records != inserted_records:
        raise ValueError(f"Falha na validação: {expected_records} registros esperados, mas {inserted_records} foram inseridos.")
    
    logging.info("Validação de dados concluída com sucesso!")

# Configurações padrão do DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': True,
    'start_date': datetime(2024, 11, 4),  # Segunda-feira da primeira semana de novembro 2019
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Definir o DAG
dag = DAG(
    'openfda_cosmetic_events_weekly',
    default_args=default_args,
    description='Coleta semanal de dados da OpenFDA Cosmetic Event API',
    schedule='0 2 * * 1',  # Todo segunda-feira às 2:00 AM
    catchup=False,  # Executar para datas passadas
    max_active_runs=1,
    tags=['openfda', 'cosmetic', 'api', 'weekly']
)

# Função para extrair dados da API OpenFDA
def extract_openfda_data(**context):
    """
    Extrai dados da API OpenFDA Cosmetic Event para um período específico
    prev_start_date_success"""
    execution_date = context.get('start_date', datetime.now())

    data_interval_start = context['prev_start_date_success'] if context['prev_start_date_success'] else context['dag'].start_date
    data_interval_end = context['data_interval_start']
    
    # Converter datas para formato YYYY-MM-DD
    start_date = data_interval_start.strftime('%Y-%m-%d') 
    end_date = data_interval_end.strftime('%Y-%m-%d')
    
    logging.info(f"Coletando dados entre {start_date} e {end_date}")
    
    # URL base da API
    base_url = "https://api.fda.gov/cosmetic/event.json"
    
    # Configurar parâmetros de busca por intervalo de datas
    # Usando latest_received_date como campo de data para filtrar
    search_query = f"initial_received_date:[{start_date} TO {end_date}]"
    
    # Parâmetros da API
    params = {
        'search': search_query,
        'limit': 1000,  # Máximo permitido por requisição
        'skip': 0
    }
    
    # Opcional: Adicionar API key se disponível
    # params['api_key'] = 'YOUR_API_KEY_HERE'
    
    all_results = []
    meta_info = None
    
    try:
        while True:            
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Salvar meta informações da primeira requisição
            if meta_info is None:
                meta_info = data.get('meta', {})
            
            # Adicionar resultados
            results = data.get('results', [])
            if not results:
                break
                
            all_results.extend(results)
            
            # Verificar se há mais dados
            if len(results) < 1000:
                break
                
            # Próxima página
            params['skip'] += 1000
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao fazer requisição para API: {e}")
        raise 
    except Exception as e:
        logging.error(f"Erro inesperado: {e}")
        raise 
    
    logging.info(f"Total de registros coletados: {len(all_results)}")
    
    # Retornar dados para próxima task
    return {
        'results': all_results,
        'meta': meta_info,
        'execution_date': execution_date,
        'total_records': len(all_results)
    }

# Função para inserir dados no PostgreSQL
def load_to_postgres(**context):
    """
    Carrega os dados extraídos no PostgreSQL
    """
    # Obter dados da task anterior
    ti = context['ti']
    data = ti.xcom_pull(task_ids='extract_data')
    
    if not data or not data['results']:
        logging.info("Nenhum dado para inserir")
        return
    
    # Conectar ao PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # Inserir dados de meta
        meta_sql = """
        INSERT INTO openfda_cosmetic_meta (
            execution_date, 
            disclaimer, 
            terms, 
            license, 
            last_updated,
            total_records
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (execution_date) DO UPDATE SET
            disclaimer = EXCLUDED.disclaimer,
            terms = EXCLUDED.terms,
            license = EXCLUDED.license,
            last_updated = EXCLUDED.last_updated,
            total_records = EXCLUDED.total_records;
        """
        
        meta = data['meta']
        postgres_hook.run(meta_sql, parameters=[
            data['execution_date'],
            meta.get('disclaimer', ''),
            meta.get('terms', ''),
            meta.get('license', ''),
            meta.get('last_updated', ''),
            data['total_records']
        ])
        
        # Inserir dados dos resultados
        results_sql = """
        INSERT INTO openfda_cosmetic_results (
            execution_date,
            report_id,
            receivedate,
            data_json
        ) VALUES (%s, %s, %s, %s)
        ON CONFLICT (execution_date, report_id) DO UPDATE SET
            receivedate = EXCLUDED.receivedate,
            data_json = EXCLUDED.data_json;
        """
        
        # Preparar dados para inserção em lote
        batch_data = []
        for result in data['results']:
            # Gerar ID único para o relatório baseado nos dados disponíveis
            report_id = result.get('report_number', result.get('id', hash(json.dumps(result, sort_keys=True))))
            
            batch_data.append([
                data['execution_date'],
                str(report_id),
                datetime.strptime(result['latest_received_date'], '%Y%m%d') if result['latest_received_date'] else None,
                json.dumps(result)
            ])
        
        # Inserir em lotes para melhor performance
        # batch_size = 100

        # for i in range(0, len(batch_data), batch_size):
        #     batch = batch_data[i:i+batch_size]

        # with postgres_hook.get_cursor() as c:
        #     c.execute(results_sql, batch_data)
        #     c.commit()

        for row in batch_data:
            postgres_hook.run(results_sql, parameters=row)
        
        logging.info(f"Inseridos {len(batch_data)} registros no PostgreSQL")
        
    except Exception as e:
        logging.error(f"Erro ao inserir dados no PostgreSQL: {e}")
        raise

# Task para criar tabelas se não existirem
create_tables_task = PostgresOperator(
    task_id='create_tables',
    conn_id='postgres_default',
    sql="""
    -- Tabela para armazenar metadados
    CREATE TABLE IF NOT EXISTS openfda_cosmetic_meta (
        id SERIAL PRIMARY KEY,
        execution_date DATE UNIQUE NOT NULL,
        disclaimer TEXT,
        terms TEXT,
        license TEXT,
        last_updated TEXT,
        total_records INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Tabela para armazenar os resultados
    CREATE TABLE IF NOT EXISTS openfda_cosmetic_results (
        id SERIAL PRIMARY KEY,
        execution_date DATE NOT NULL,
        report_id TEXT NOT NULL,
        receivedate DATE NOT NULL,
        data_json JSONB NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(execution_date, report_id)
    );
    
    -- Índices para melhor performance
    CREATE INDEX IF NOT EXISTS idx_cosmetic_results_execution_date 
        ON openfda_cosmetic_results(execution_date);
    CREATE INDEX IF NOT EXISTS idx_cosmetic_results_receivedate 
        ON openfda_cosmetic_results(receivedate);
    CREATE INDEX IF NOT EXISTS idx_cosmetic_results_data_json 
        ON openfda_cosmetic_results USING GIN(data_json);
    """,
    dag=dag
)

# Task para extrair dados da API
extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_openfda_data,
    dag=dag
)

# Task para carregar dados no PostgreSQL
load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_to_postgres,
    dag=dag
)

# # Task para validar dados inseridos
# validate_data_task = PythonOperator(
#     task_id='validate_data',
#     python_callable=validate_data_in_postgres,
#     dag=dag
# )

# Definir dependências
create_tables_task >> extract_data_task >> load_data_task  #>> validate_data_task

if __name__ == "__main__":
    dag.test()  # Permite testar o DAG localmente
     