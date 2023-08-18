import re
import csv
import chardet
import unidecode
import unicodedata
import psycopg2.extras
from datetime import datetime



####################################################################################################
# Connection step
####################################################################################################

db_params = {
    "host": "127.0.0.1",
    "database": "transactions",
    "user": "admin",
    "password": "admin"
}

# Dados dos arquivos CSV
csv_files = {
    "transacoes": "db_cartoes.transacoes.csv",
    "associado": "db_pessoa.associado.csv",
    "agencia": "db_entidade.agencia.csv"
}

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']


####################################################################################################
# Data Model
####################################################################################################

'''
Utilizamos para a nossa solução a modelagem dimensional dos dados, este processo nos
ajuda a lidar de forma mais otimizada com dados que têm relações complexas e necessidade 
de muitas consultas analíticas.
'''

# Scripts SQL para criação da tabela dimensão Associado:
create_associado_table = """
    CREATE TABLE IF NOT EXISTS Associado (
        num_cpf_cnpj VARCHAR(15) PRIMARY KEY,
        des_nome_associado VARCHAR(255),
        dat_associacao DATE,
        cod_faixa_renda VARCHAR(10),
        des_faixa_renda VARCHAR(100)
    );
"""

# Scripts SQL para criação da tabela dimensão Agencia:
"""
Nesse caso, a coluna agencia_id será a chave primária e será composta 
pela combinação do código da cooperativa e do código da agência. 
"""
create_agencia_table = """
    CREATE TABLE IF NOT EXISTS Agencia (
        agencia_id VARCHAR(15) PRIMARY KEY,
        cod_cooperativa VARCHAR(10),
        cod_agencia VARCHAR(10),
        des_nome_cooperativa VARCHAR(255),
        des_nome_agencia VARCHAR(255)
    );
"""

# Scripts SQL para criação da tabela fato Transacoes
create_transacoes_table = """
    CREATE TABLE IF NOT EXISTS Transacoes (
        transacao_id SERIAL PRIMARY KEY,
        num_plastico VARCHAR(20),
        dat_transacao TIMESTAMP,
        vlr_transacao NUMERIC,
        nom_modalidade VARCHAR(10),
        nom_cidade_estabelecimento VARCHAR(100),
        associado_id VARCHAR(15) REFERENCES Associado(num_cpf_cnpj),
        agencia_id VARCHAR(20) REFERENCES Agencia(agencia_id)
    );
"""

# Função para criar tabelas

def create_tables(connection, cursor):
    cursor.execute(create_associado_table)
    cursor.execute(create_agencia_table)
    cursor.execute(create_transacoes_table)
    connection.commit()


####################################################################################################
# Data Sanitization
####################################################################################################

'''
Aqui definimos a config de todas as regras e atributos em que serão aplicadas
'''
sanitization_rules = {
    "agencia": {
        "cod_cooperativa": ["zfill"],
        "des_nome_cooperativa": ["uppercase"]
    },
    "associado": {
        "des_nome_associado": ["uppercase"]
    },
    "transacoes": {
        "cod_cooperativa": ["zfill"],
        "nom_modalidade": ["decode","remove_accents"]
    }
}

'''
Aqui são escritas as logicas de sanitização dos dados
'''

def remove_accents(data):

    nfkd = unicodedata.normalize('NFKD', data)
    field_data = u"".join([c for c in nfkd if not unicodedata.combining(c)])

    # Usa expressão regular para retornar a palavra apenas com números, letras e espaço
    return re.sub('[^a-zA-Z0-9 \\\]', '', field_data)

def sanitize_data(data, rules, column):
    sanitized_data = data
    for rule in rules:
        if rule == "zfill":
            sanitized_data = sanitized_data.rjust(4, '0')
        elif rule == "uppercase":
            sanitized_data = sanitized_data.upper()
        elif rule == "decode":
            sanitized_data = sanitized_data.encode('latin1').decode('utf-8').title()
        elif rule == "remove_accents":
            sanitized_data = remove_accents(sanitized_data)
    return sanitized_data


####################################################################################################
# ETL Step
####################################################################################################

# Função para realizar o processo de ETL para a tabela Associado
def etl_associado(connection, cursor, csv_file):
    table_name = 'associado'
    print("Processing Associado...")
    csv_encoding = detect_encoding(csv_file)

    with open(csv_file, "r", encoding=csv_encoding) as file:
        csv_reader = csv.DictReader(file, delimiter=';')
        
        insert_query = "INSERT INTO Associado (num_cpf_cnpj, des_nome_associado, dat_associacao, cod_faixa_renda, des_faixa_renda) VALUES %s;"
        for row in csv_reader:
            # Converte a data para o formato YYYY-MM-DD
            dat_associacao = datetime.strptime(row["dat_associacao"], "%d/%m/%Y").strftime("%Y-%m-%d")
            
            new_row = {
                "num_cpf_cnpj": row["num_cpf_cnpj"],
                "des_nome_associado": row["des_nome_associado"],
                "dat_associacao": dat_associacao,
                "cod_faixa_renda": row["cod_faixa_renda"],
                "des_faixa_renda": row["des_faixa_renda"]
            }

            # Aplicar regras de sanitização se a tabela estiver no dicionário
            if table_name in sanitization_rules:
                rules = sanitization_rules[table_name]
                for column, rules_list in rules.items():
                    new_row[column] = sanitize_data(new_row[column], rules_list, column)

            values = tuple(new_row.values())
            psycopg2.extras.execute_values(cursor, insert_query, [values])
            connection.commit()
    
    print("Associado processed.")

# Função para realizar o processo de ETL para a tabela Agencia
def etl_agencia(connection, cursor, csv_file):
    table_name = 'agencia'
    print("Processing Agencia...")
    csv_encoding = detect_encoding(csv_file)

    with open(csv_file, "r", encoding=csv_encoding) as file:
        csv_reader = csv.DictReader(file, delimiter=';')
        
        insert_query = """
        INSERT INTO Agencia 
        (agencia_id, cod_cooperativa, cod_agencia, des_nome_cooperativa, des_nome_agencia)
        VALUES %s;
        """
        
        values = []
        for row in csv_reader:
            
            # Aplicar regras de sanitização se a tabela estiver no dicionário
            if table_name in sanitization_rules:
                rules = sanitization_rules[table_name]
                for column, rules_list in rules.items():
                    row[column] = sanitize_data(row[column], rules_list, column)
            
            cooperativa_id = row["cod_cooperativa"]
            agencia_id = f"{cooperativa_id}-{row['cod_agencia']}"  # Geramos uma chave composta para atender as regras de negocio

            values.append((agencia_id, cooperativa_id, row["cod_agencia"], row["des_nome_cooperativa"], row["des_nome_agencia"]))
        
        psycopg2.extras.execute_values(cursor, insert_query, values)
        connection.commit()
    
    print("Agencia processed.")

# Função para realizar o processo de ETL para a tabela Transacoes
def etl_transacoes(connection, cursor, csv_file):
    table_name = 'transacoes'
    print("Processing Transacoes...")

    with open(csv_file, "r", encoding="ISO-8859-1") as file:
        csv_reader = csv.DictReader(file, delimiter=';')
        
        insert_query = "INSERT INTO Transacoes (num_plastico, dat_transacao, vlr_transacao, nom_modalidade, nom_cidade_estabelecimento, associado_id, agencia_id) VALUES %s;"
        for row in csv_reader:
            
            # Aplicar regras de sanitização se a tabela estiver no dicionário
            if table_name in sanitization_rules:
                rules = sanitization_rules[table_name]
                for column, rules_list in rules.items():
                    row[column] = sanitize_data(row[column], rules_list, column)

            agencia_id = f"{row['cod_cooperativa']}-{row['cod_agencia']}"  # Formato agencia_id
            
            new_row = {
                "num_plastico": row["num_plastico"],
                "dat_transacao": datetime.strptime(row["dat_transacao"], "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S"),
                "vlr_transacao": row["vlr_transacao"].replace(',', '.'),
                "nom_modalidade": row["nom_modalidade"],
                "nom_cidade_estabelecimento": row["nom_cidade_estabelecimento"],
                "associado_id": row["num_cpf_cnpj"],
                "agencia_id": agencia_id
            }

            values = tuple(new_row.values())
            psycopg2.extras.execute_values(cursor, insert_query, [values])
            connection.commit()
    
    print("Transacoes processed.")

def main():
    try:
        # Conectando ao banco de dados
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Criando tabelas
        create_tables(connection, cursor)

        # Realizando o ETL para cada tabela
        etl_associado(connection, cursor, csv_files["associado"])
        etl_agencia(connection, cursor, csv_files["agencia"])
        etl_transacoes(connection, cursor, csv_files["transacoes"])

        print("-----> FINISHED")

    except psycopg2.Error as e:
        print("Erro ao interagir com o banco de dados:", e)

    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main()
