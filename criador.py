import psycopg2

#Estabelecendo a conexão
conn = psycopg2.connect(
   database="postgres", user='postgres', password='senha123', host='127.0.0.1', port= '5432'
)
conn.autocommit = True

#Criando o cursor
cursor = conn.cursor()

#Criando o banco de dados grupo_15
sql = '''CREATE database grupo_15''';

cursor.execute(sql)
print("Database created successfully........")

#Fechando a conexão para essa transação
conn.close()

#######Criação das tabelas do banco de dados#################
conn2 =  psycopg2.connect(
database="grupo_15", user='postgres', password='senha123', host='127.0.0.1', port= '5432'
)
conn2.autocommit = True
cur = conn2.cursor()

cur.execute('''CREATE TABLE cidade(
	cod_ibge INTEGER NOT NULL,
	nome_cidade VARCHAR(25),
	estado VARCHAR(15),
	populacao INTEGER,
    CONSTRAINT ibge_pk PRIMARY KEY(cod_ibge)
);''')

cur.execute('''CREATE TABLE casos_covid(
    cod_ibge INTEGER,
    ordem_registro INTEGER,
	data_coleta DATE,
	semana_epidemiologica SMALLINT,
	data_dado DATE,
    eh_atual BOOLEAN,
	confirm_ultimo_dia INTEGER,
	mort_ultimo_dia INTEGER,
	novas_mortes INTEGER,
	tax_mort REAL,
	novos_confirmados INTEGER,
	repetido BOOLEAN,
	casos_100mil REAL,
    CONSTRAINT data_pk PRIMARY KEY(data_coleta,cod_ibge,ordem_registro)
);''')
##Criação da chave estrangeira
cur.execute('''ALTER TABLE casos_covid ADD CONSTRAINT cod_fk FOREIGN KEY (cod_ibge)
              REFERENCES cidade(cod_ibge) ON DELETE CASCADE;''')
              
###Fechando a conexão
conn2.close()