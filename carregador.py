import pandas as pd
import psycopg2
import psycopg2.extras as extras
import sys


########Conexão ao banco de dados##########
param_dic = {
    "host"      : "127.0.0.1",
    "database"  : "grupo_15",
    "user"      : "postgres",
    "password"  : "senha123"
}


#importação dos dados e renomeação das colunas
arquivo_csv = "D:/Users/gilmar/Documents/Pos_graduacao/Especialização em Ciência de Dados/Linguagens de programação para Ciência de Dados/covid19_casos_brasil.csv"
dados_covid = pd.read_csv(arquivo_csv)
dados_covid.rename(columns = {'city': 'nome_cidade',
                     'city_ibge_code':'cod_ibge',
                     'date':'data_dado',
                     'epidemiological_week': 'semana_epidemiologica',
                     'estimated_population_2019':'populacao',
                     'is_last':'eh_atual',
                     'is_repeated': 'repetido',
                     'last_available_confirmed':'confirm_ultimo_dia',
                     'last_available_confirmed_per_100k_inhabitants': 'casos_100mil',
                     'last_available_date': 'data_coleta',
                     'last_available_death_rate':'tax_mort',
                     'last_available_deaths':'mort_ultimo_dia',
                     'order_for_place':'ordem_registro',
                     'place_type':'tipo_lugar',
                     'state': 'estado', 
                     'new_confirmed':'novos_confirmados',
                     'new_deaths':'novas_mortes'}, inplace = True)

######Seleção das cidades#########
selecao_cidades = dados_covid.query('cod_ibge == 3550308 or cod_ibge == 3300407 or cod_ibge == 3304557 or cod_ibge == 2910800 or cod_ibge == 3547304')

###########Construção dos dataframes para carregamento no banco de dados##############
cidades = selecao_cidades[['cod_ibge','nome_cidade','estado','populacao']]
cidade = cidades.drop_duplicates()
casos_covid = selecao_cidades[['cod_ibge','ordem_registro','data_coleta','semana_epidemiologica','data_dado','eh_atual','confirm_ultimo_dia',
                               'mort_ultimo_dia','novas_mortes','tax_mort','novos_confirmados','repetido','casos_100mil']]


#######Inserção de dados nas tabelas#########
def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    print("Connection successful")
    return conn
conn = connect(param_dic)


def execute_values(conn, df, table):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()
    
 ######Carregando os dataframes no banco de dados#######   
execute_values(conn,cidade,'cidade')
execute_values(conn,casos_covid,'casos_covid')
