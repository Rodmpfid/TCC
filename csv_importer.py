import pandas as pd
from sqlalchemy import create_engine
import mysql.connector

def import_unique_csv_to_db(csv_file, db_username, db_password, db_host, db_port, db_name, table_name, id_column):
    # Criar conexao para SQLAlchemy engine
    connection_string = f"mysql+mysqlconnector://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"

    # Criar um SQLAlchemy engine
    engine = create_engine(connection_string)

    # Ler CSV file com delimiter correto
    df_csv = pd.read_csv(csv_file, delimiter=';')

    # Conectar ao banco de dados e buscar ids existentes
    conn = mysql.connector.connect(user=db_username, password=db_password, host=db_host, port=db_port, database=db_name)
    cursor = conn.cursor()
    cursor.execute(f"SELECT {id_column} FROM {table_name}")
    existing_ids = {row[0] for row in cursor.fetchall()}
    cursor.close()
    conn.close()



    # Filtrar records que já existem no database
    df_new = df_csv[~df_csv[id_column].isin(existing_ids)]

    print(df_new.columns)
    print(df_new)

    # Importar os novos dados para tabela
    if not df_new.empty:
        df_new.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"{len(df_new)} new records imported successfully.")
    else:
        print("No new records to import.")

# Database parametros de conexao XAMPP
db_username = 'root'  # default XAMPP username 
db_password = ''  # default XAMPP sem senha
db_host = 'localhost'
db_port = '3306'
db_name = 'db_financeiro'
table_name = 'tb_despesa'
csv_file = 'C:/users/dell/documents/csv/fincsv.csv'
id_column = 'id'



# Chamar função para importar dados
import_unique_csv_to_db(csv_file, db_username, db_password, db_host, db_port, db_name, table_name, id_column)
