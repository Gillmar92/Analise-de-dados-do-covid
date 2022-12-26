import pandas as pd
import psycopg2
from matplotlib import pyplot as plt

###############Conexão ao banco de dados#########################
conn = psycopg2.connect("dbname=grupo_15 user=postgres password=senha123")
cur = conn.cursor()

cidade = pd.read_sql("SELECT * FROM cidade",conn)

dados_risco_feira = pd.read_sql('''SELECT c.cod_ibge,cv.semana_epidemiologica,SUM(cv.novos_confirmados) as confirmacoes_semana,SUM(cv.casos_100mil) as casos_100mil,AVG(cv.tax_mort) as media_taxa_mort
								FROM cidade as c, casos_covid as cv
                            	WHERE c.cod_ibge = cv.cod_ibge AND
                                c.cod_ibge = 2910800 AND
                                semana_epidemiologica >= 14 AND
                                semana_epidemiologica <= 28
								GROUP BY c.cod_ibge,semana_epidemiologica
                            	ORDER BY c.cod_ibge,semana_epidemiologica
								
                            ''', conn);
                            
dados_risco_sp = pd.read_sql('''SELECT c.cod_ibge,cv.semana_epidemiologica,SUM(cv.novos_confirmados) as confirmacoes_semana,SUM(cv.casos_100mil) as casos_100mil,AVG(cv.tax_mort) as media_taxa_mort
								FROM cidade as c, casos_covid as cv
                            	WHERE c.cod_ibge = cv.cod_ibge AND
                                c.cod_ibge = 3550308 AND
                                semana_epidemiologica >= 14 AND
                                semana_epidemiologica <= 28
								GROUP BY c.cod_ibge,semana_epidemiologica
                            	ORDER BY c.cod_ibge,semana_epidemiologica
								
                            ''', conn);                            
                    
dados_risco_brmansa = pd.read_sql('''SELECT c.cod_ibge,cv.semana_epidemiologica,SUM(cv.novos_confirmados) as confirmacoes_semana,SUM(cv.casos_100mil) as casos_100mil,AVG(cv.tax_mort) as media_taxa_mort
								FROM cidade as c, casos_covid as cv
                            	WHERE c.cod_ibge = cv.cod_ibge AND
                                c.cod_ibge = 3300407 AND
                                semana_epidemiologica >= 14 AND
                                semana_epidemiologica <= 28
								GROUP BY c.cod_ibge,semana_epidemiologica
                            	ORDER BY c.cod_ibge,semana_epidemiologica
								
                            ''', conn);
                            
dados_risco_rj = pd.read_sql('''SELECT c.cod_ibge,cv.semana_epidemiologica,SUM(cv.novos_confirmados) as confirmacoes_semana,SUM(cv.casos_100mil) as casos_100mil,AVG(cv.tax_mort) as media_taxa_mort
								FROM cidade as c, casos_covid as cv
                            	WHERE c.cod_ibge = cv.cod_ibge AND
                                c.cod_ibge = 3304557 AND
                                semana_epidemiologica >= 14 AND
                                semana_epidemiologica <= 28
								GROUP BY c.cod_ibge,semana_epidemiologica
                            	ORDER BY c.cod_ibge,semana_epidemiologica
								
                            ''', conn);

dados_risco_parnaiba = pd.read_sql('''SELECT c.cod_ibge,cv.semana_epidemiologica,SUM(cv.novos_confirmados) as confirmacoes_semana,SUM(cv.casos_100mil) as casos_100mil,AVG(cv.tax_mort) as media_taxa_mort
								FROM cidade as c, casos_covid as cv
                            	WHERE c.cod_ibge = cv.cod_ibge AND
                                c.cod_ibge = 3547304 AND
                                semana_epidemiologica >= 14 AND
                                semana_epidemiologica <= 28
								GROUP BY c.cod_ibge,semana_epidemiologica
                            	ORDER BY c.cod_ibge,semana_epidemiologica
								
                            ''', conn);
                            
##Normalização do número de novos confirmados
def minmax_norm(df_input):
    return (df_input - df_input.min()) / ( df_input.max() - df_input.min())

dados_risco_brmansa['confirmacoes_semana'] = minmax_norm(dados_risco_brmansa['confirmacoes_semana'])    
dados_risco_feira['confirmacoes_semana'] = minmax_norm(dados_risco_feira['confirmacoes_semana'])   
dados_risco_parnaiba['confirmacoes_semana'] = minmax_norm(dados_risco_parnaiba['confirmacoes_semana'])   
dados_risco_sp['confirmacoes_semana'] = minmax_norm(dados_risco_sp['confirmacoes_semana'])   
dados_risco_rj['confirmacoes_semana'] = minmax_norm(dados_risco_rj['confirmacoes_semana'])   

########Gráficos para acompanhar o avanço dos parâmetros#############

# População de cada cidade
#Avanço dos casos por 100 mil habitantes
fig, ax1 = plt.subplots(1, 1,figsize =(12,4))
plt.bar(cidade['nome_cidade'],cidade['populacao'])
ax1.set_ylabel("Número de habitantes")
plt.title("Número da habitantes em cada cidade") 
plt.legend()  
ax1.locator_params(nbins=20)  

#Avanço dos novos confirmados   
fig, ax = plt.subplots(1, 1,figsize =(12,4))
ax.plot(dados_risco_feira['semana_epidemiologica'],dados_risco_feira['confirmacoes_semana'],color = 'red',label = 'Feira de Santana')
ax.plot(dados_risco_sp['semana_epidemiologica'],dados_risco_sp['confirmacoes_semana'],color = 'green',label = 'São Paulo')
ax.plot(dados_risco_brmansa['semana_epidemiologica'],dados_risco_brmansa['confirmacoes_semana'],color = 'blue',label = 'Barra Mansa')
ax.plot(dados_risco_rj['semana_epidemiologica'],dados_risco_rj['confirmacoes_semana'],color = 'yellow',label = 'Rio de Janeiro')
ax.plot(dados_risco_parnaiba['semana_epidemiologica'],dados_risco_parnaiba['confirmacoes_semana'],color = 'orange',label = 'Santana de Parnaíba')
ax.set_ylabel("Número de casos")
ax.set_xlabel("Semana epidemiológica")
plt.title("Avanço dos casos ao longo das semanas") 
plt.legend()  
ax.locator_params(nbins=16)     

#Avanço da taxa de mortes
fig, ax2 = plt.subplots(1, 1,figsize =(12,4))
ax2.plot(dados_risco_feira['semana_epidemiologica'],dados_risco_feira['media_taxa_mort'],color = 'red',label = 'Feira de Santana')
ax2.plot(dados_risco_sp['semana_epidemiologica'],dados_risco_sp['media_taxa_mort'],color = 'green',label = 'São Paulo')
ax2.plot(dados_risco_brmansa['semana_epidemiologica'],dados_risco_brmansa['media_taxa_mort'],color = 'blue',label = 'Barra Mansa')
ax2.plot(dados_risco_rj['semana_epidemiologica'],dados_risco_rj['media_taxa_mort'],color = 'yellow',label = 'Rio de Janeiro')
ax2.plot(dados_risco_parnaiba['semana_epidemiologica'],dados_risco_parnaiba['media_taxa_mort'],color = 'orange',label = 'Santana de Parnaíba')       
ax2.set_ylabel("Taxa de mortes")
ax2.set_xlabel("Semana epidemiológica")
plt.title("Avanço da taxa de mortes") 
plt.legend()  
ax2.locator_params(nbins= 16)      

#Avanço dos casos por 100 mil habitantes
fig, ax3 = plt.subplots(1, 1,figsize =(12,4))
ax3.plot(dados_risco_feira['semana_epidemiologica'],dados_risco_feira['casos_100mil'],color = 'red',label = 'Feira de Santana')
ax3.plot(dados_risco_sp['semana_epidemiologica'],dados_risco_sp['casos_100mil'],color = 'green',label = 'São Paulo')
ax3.plot(dados_risco_brmansa['semana_epidemiologica'],dados_risco_brmansa['casos_100mil'],color = 'blue',label = 'Barra Mansa')
ax3.plot(dados_risco_rj['semana_epidemiologica'],dados_risco_rj['casos_100mil'],color = 'yellow',label = 'Rio de Janeiro')
ax3.plot(dados_risco_parnaiba['semana_epidemiologica'],dados_risco_parnaiba['casos_100mil'],color = 'orange',label = 'Santana de Parnaíba')       
ax3.set_ylabel("Número de casos")
ax3.set_xlabel("Semana epidemiológica")
plt.title("Casos por 100 mil habitantes") 
plt.legend()  
ax3.locator_params(nbins=16)  

