from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, inspect, text

# Define os caminhos dos arquivos CSV
caminho_receita = r'C:\\Users\\brusan\\Desktop\\Anotações\\Espera\\gdvReceitasExcel.csv'
caminho_despesas = r'C:\\Users\\brusan\\Desktop\\Anotações\\Espera\\gdvDespesasExcel.csv'

# Conectar ao banco de dados SQL Server
engine = create_engine('mssql+pyodbc://ops:M4v&r!ck@10.15.5.92/ProfessionalServices01?driver=ODBC+Driver+17+for+SQL+Server')

# Verificar a existência das tabelas e remover se existirem
inspector = inspect(engine)
tables_to_drop = ['TabelaReceitas', 'TabelaDespesas', 'TabelaFinal']
for table in tables_to_drop:
    if inspector.has_table(table):
        with engine.connect() as conn:
            conn.execute(text(f'DROP TABLE {table}'))

def carregar_e_exportar_para_sql():
    # Função para carregar os arquivos CSV e exportar para o banco de dados SQL Server
    def carregar_csv(caminho):
        try:
            df = pd.read_csv(caminho, encoding='ISO-8859-1')
            # Remover a última linha, que contém o total
            df = df.iloc[:-1]
            print(f"Arquivo CSV {caminho} carregado com sucesso usando a codificação ISO-8859-1.")
            return df
        except UnicodeDecodeError:
            print(f"Não foi possível abrir o arquivo CSV {caminho} usando a codificação ISO-8859-1.")

    # Carregar dados do arquivo de receitas e despesas
    receita_df = carregar_csv(caminho_receita)
    despesas_df = carregar_csv(caminho_despesas)

    # Exportar os dados para tabelas separadas no banco de dados SQL Server
    receita_df.to_sql('TabelaReceitas', engine, if_exists='replace', index=False)
    despesas_df.to_sql('TabelaDespesas', engine, if_exists='replace', index=False)

def criar_tabela_final():
    # Query da tabela final
    sql_query = """
    SELECT 
        FORMAT(SUM(CONVERT(NUMERIC(18,2), REPLACE(REPLACE(R.[Arrecadado até 02/02/2024], '.', ''), ',', '.'))), 'N2') AS TOTAL_RECEITAS,
        FORMAT(SUM(CONVERT(NUMERIC(18,2), REPLACE(REPLACE(D.[Liquidado], '.', ''), ',', '.'))), 'N2') AS TOTAL_DESPESAS,
        R.[FONTE DE RECURSOS] AS FONTE_DE_RECURSOS,
        D.[DESPESA] AS TIPO_DE_DESPESA
    FROM [ProfessionalServices01].[dbo].[TabelaDespesas] D WITH (NOLOCK)
    LEFT JOIN [ProfessionalServices01].[dbo].[TabelaReceitas] R WITH (NOLOCK) ON D.[FONTE DE RECURSOS] = R.[FONTE DE RECURSOS]
    GROUP BY 
        R.[FONTE DE RECURSOS], D.[DESPESA]
    ORDER BY 
        SUM(CONVERT(NUMERIC(18,2), REPLACE(REPLACE(R.[Arrecadado até 02/02/2024], '.', ''), ',', '.'))) DESC,
        SUM(CONVERT(NUMERIC(18,2), REPLACE(REPLACE(D.[Liquidado], '.', ''), ',', '.'))) DESC
    """

    # Executar a consulta SQL e salvar os resultados em um DataFrame
    result_df = pd.read_sql(sql_query, engine)
    
    # Definir o nome da nova tabela
    nova_tabela_nome = "TabelaFinal"
    
    # Salvar o DataFrame como uma nova tabela no banco de dados
    result_df.to_sql(nova_tabela_nome, engine, if_exists='replace', index=False)
    print(f"Tabela '{nova_tabela_nome}' criada com sucesso no banco de dados.")

# Define a DAG
with DAG('carregar_e_criar_tabela_final', start_date=datetime(2024, 2, 10), schedule_interval=None) as dag:
    # Tarefa para carregar os arquivos CSV e exportar para o banco de dados SQL Server
    load_and_export_task = PythonOperator(
        task_id='load_and_export_to_sql',
        python_callable=carregar_e_exportar_para_sql
    )

    # Tarefa para criar a tabela final
    create_final_table_task = PythonOperator(
        task_id='create_final_table',
        python_callable=criar_tabela_final
    )
    

    # Define a ordem de execução das tarefas
    load_and_export_task >> create_final_table_task
