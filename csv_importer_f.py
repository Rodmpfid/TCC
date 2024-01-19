import pandas as pd
from sqlalchemy import create_engine
import mysql.connector

def replace_csv_data_in_db(csv_file, db_username, db_password, db_host, db_port, db_name, table_name):
    # Criar conexão para SQLAlchemy engine
    connection_string = f"mysql+mysqlconnector://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(connection_string)

    # Ler CSV file com delimiter correto
    df_csv = pd.read_csv(csv_file, delimiter=';')

    # Conectar ao banco de dados
    conn = mysql.connector.connect(user=db_username, password=db_password, host=db_host, port=db_port, database=db_name)
    cursor = conn.cursor()

    # Excluir dados existentes na tabela
    cursor.execute(f"DELETE FROM {table_name}")
    conn.commit()

    # Importar os novos dados para tabela
    if not df_csv.empty:
        df_csv.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"{len(df_csv)} records imported successfully.")
    else:
        print("No records to import.")

    cursor.close()
    conn.close()

# Parâmetros de conexão ao banco de dados
db_username = 'root'  
db_password = ''
db_host = 'localhost'
db_port = '3306'
db_name = 'db_financeiro'
table_name = 'tb_folha'
csv_file = 'C:/users/dell/documents/csv/folha.csv'

# Chamar função para substituir dados
replace_csv_data_in_db(csv_file, db_username, db_password, db_host, db_port, db_name, table_name)
