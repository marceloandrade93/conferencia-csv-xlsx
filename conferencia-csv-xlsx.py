###### ------- DEFINIR FUSO HORÁRIO / TIMEZONE DE SÃO PAULO [OK]
import pandas as pd
from datetime import datetime
import numpy as np
import pytz
from pathlib import Path


tz_Brasilia = pytz.timezone('America/Sao_Paulo')
data = datetime.now(tz_Brasilia)
hoje = data.strftime('%y%m%d-%H%M.xlsx')
ncpoe = 'Conferência Net-A'

csvclientes = f'{Path()}/Entrada/CRM - Clientes Net-A.csv'
xlsxfaturamento = f'{Path()}/Entrada/Faturamento Net-A.xlsx'
csvpedidosnovos = f'{Path()}/Entrada/CRM - Pedidos Net-A.csv'
csvcancelamentos = f'{Path()}/Entrada/CRM - Cancelamentos Net-A.csv'
xlsxsaida = f'{Path()}/Saida/'

dfcrm = pd.read_csv(csvclientes, encoding = 'UTF-8', delimiter = ',') #importar df
dfneta = pd.read_excel(xlsxfaturamento) #importar df

###### ------- REMOVER CARACTERES INDESEJÁVEIS DOS ATRIBUTOS [OK]
dfcrm = dfcrm.rename(columns={'Razão Social CNPJ': 'Razão Social', 'CPF/CNPJ': 'Documento'})
dfneta = dfneta.rename(columns={'CNPJ (Cliente)': 'Documento', 'Razao Social (Cliente)': 'Razão Social', 'Produto': 'Produto Net-A', 'Data do Pedido': 'Data Pedido Net-A', 'Status': 'Status Net-A'})

###### ------- EXCLUIR ATRIBUTOS INDESEJADOS [OK]
dfneta = dfneta.drop(columns=['Nº Pedido', 'Revenda - CNPJ', 'Revenda - Razao Social', 'Valor Total', 'Quantidade'])

###### ------- INCLUIR ATRIBUTOS NECESSÁRIOS [OK]
dfneta['Provedor'] = 'Net-A'

###### ------- MERGE (COMPARAR) DOIS DATAFRAMES (NET-A X CRM) [OK]
rsneta = pd.merge(dfneta, dfcrm, on=['Documento'], how='left', indicator='MERGE') #MERGE NET-A (left_only = só na NET-A)
rscrm = pd.merge(dfneta, dfcrm, on=['Documento'], how='right', indicator='MERGE') #MERGE CRM (right_only = só no CRM)


###### ------- MERGE NET-A [OK]
rsneta = rsneta.drop(columns=['Provedor_y', 'Razão Social_y'])
rsneta = rsneta.rename(columns={'MERGE': 'REVISAR', 'Provedor_x': 'Provedor', 'Razão Social_x': 'Razão Social'}) #renomeou o Merge
rsneta['Produto CRM'] = rsneta['IP Maquina'].map({'192.192.19.1': 'Conexão Online'})
rsneta['REVISAR'] = rsneta['REVISAR'].map({'both': 'OK', 'left_only': 'Sobra Net-A', 'right_only': 'Sobra CRM'}, na_action=None)
### ---- SOBRA NET-A [OK]
sobraneta = rsneta.loc[(rsneta['REVISAR'] == 'Sobra Net-A')]
rsneta.drop(rsneta[rsneta['REVISAR'] == 'Sobra Net-A'].index, inplace = True) #excluir esses do DF original


###### ------- MERGE CRM [OK]
rscrm = rscrm.drop(columns=['Provedor_x', 'Razão Social_x'])
rscrm = rscrm.rename(columns={'MERGE': 'REVISAR', 'Provedor_y': 'Provedor', 'Razão Social_y': 'Razão Social'}) #renomeou o Merge
rscrm['Produto CRM'] = rscrm['IP Maquina'].map({'192.192.19.1': 'Conexão Online'})
rscrm['REVISAR'] = rscrm['REVISAR'].map({'both': 'OK', 'left_only': 'Sobra Net-A', 'right_only': 'Sobra CRM'}, na_action=None)
### ---- SOBRA CRM [OK]
sobracrm = rscrm.loc[(rscrm['REVISAR'] == 'Sobra CRM')]
rscrm.drop(rscrm[rscrm['REVISAR'] == 'Sobra CRM'].index, inplace = True) #excluir esses do DF original


###### ------- CONCATENAR DATAFRAME RESULTADO FINAL [OK]
resultado = pd.concat([sobraneta, sobracrm, rsneta[rsneta[:] != 0]])
resultado['Chamado de Cancelamento'] = ''

### ---- ORDENANDO COLUNAS [OK]
sortcolres = ['Documento', 'Razão Social', 'Provedor',  'Produto Net-A', 'Produto CRM', 'Data Pedido Net-A', 'Status Net-A','REVISAR']
resultado = resultado[sortcolres]
dfcountres = resultado


###### ------- CRIAR DATA FRAME PEDIDOS NOVOS [OK]
### ---- IMPORTAR PEDIDOS NOVOS [--]
dfpedidosnovos = pd.read_csv(csvpedidosnovos, encoding = 'UTF-8', delimiter = ',') #importar df
### ---- AJUSTAR ATRIBUTOS [--]
dfpedidosnovos = dfpedidosnovos.rename(columns={'Organizações CPF/CNPJ': 'Documento', 'Pedidos de Vendas Data/Hora Criação': 'Data/Hora Criação', 'Pedidos de Vendas Data/Hora Modificação': 'Data/Hora Modificação', 'Pedidos de Vendas Status': 'Status'})
dfpedidosnovos = dfpedidosnovos.drop(columns={'Organizações Razão Social CNPJ', 'Pedidos de Vendas Produto'})
### ---- MERGE RESULTADO X PEDIDOS NOVOS [--]
dfpedidosnovos = pd.merge(resultado, dfpedidosnovos, on=['Documento'], how='left', indicator='Divergência') #left_only = resultado || both = 'Chamado de Cancelamento
### ---- ALTERAR RESULTADO BOTH E LEFT_ONLY [OK]
dfpedidosnovos['Divergência'] = dfpedidosnovos['Divergência'].map({'both': 'Pedido Novo', 'left_only': '', 'right_only': ''}) #renomar both para sim
dfpedidosnovos.drop(dfpedidosnovos[dfpedidosnovos['Divergência'] == ''].index, inplace = True)


###### ------- CRIAR DATA FRAME CHAMADOS DE CANCELAMENTOS [OK]
### ---- IMPORTAR CHAMADOS CANCELAMENTOS [OK]
dfchcanc = pd.read_csv(csvcancelamentos, encoding = 'UTF-8', delimiter = ',') #importar df
### ---- AJUSTAR ATRIBUTOS [OK]
dfchcanc = dfchcanc.rename(columns={'Organizações CPF/CNPJ': 'Documento', 'Chamados Data/Hora Criação': 'Data/Hora Criação', 'Chamados Data/Hora Modificação': 'Data/Hora Modificação', 'Chamados Status': 'Status'})
dfchcanc = dfchcanc.drop(columns={'Organizações Razão Social CNPJ', 'Chamados Produto'})
### ---- COMPARAR RESULTADO E CANCELAMENTOS [OK]
dfchcanc = pd.merge(resultado, dfchcanc, on=['Documento'], how='left', indicator='Divergência') #left_only = resultado || both = 'Chamado de Cancelamento
### ---- ALTERAR RESULTADO BOTH E LEFT_ONLY [OK]
dfchcanc['Divergência'] = dfchcanc['Divergência'].map({'both': 'Cancelamento', 'left_only': '', 'right_only': ''}) #renomar both para sim
dfchcanc = dfchcanc.drop(columns={'Chamados Motivo do Cancelamento', 'Chamados Categorias', 'Status'})
dfchcanc.drop(dfchcanc[dfchcanc['Divergência'] == ''].index, inplace = True)


###### ------- CRIAR DATA FRAME SEM DIVERGÊNCIA [OK]
### ---- IMPORTAR CHAMADOS CANCELAMENTOS [OK]
dfsemdiv = pd.concat([dfpedidosnovos,dfchcanc]) #dfsemdiv recebe pedidos e cancelamentos
dfsemdivcnpj = dfsemdiv.drop(columns={'Razão Social', 'Provedor', 'Produto Net-A', 'Produto CRM', 'Data Pedido Net-A', 'Status Net-A', 'REVISAR', 'Data/Hora Criação', 'Data/Hora Modificação', 'Divergência'})
resultado = pd.merge(resultado, dfsemdivcnpj, on=['Documento'], how='left', indicator='Divergência')
resultado.drop(resultado[resultado['Divergência'] == 'both'].index, inplace = True)
resultado = resultado.drop(columns={'Divergência'})


###### ------- DISTRIBUIR DATAFRAMES ENTRE NET-A, CRM E OK [OK]

### ---- OK / GERAL [OK]
dfok = pd.concat([resultado, dfsemdiv])
### ---- ORDENAR [OK]
sortfinal = ['Documento', 'Razão Social', 'Provedor', 'Produto Net-A', 'Produto CRM', 'Data Pedido Net-A', 'Status Net-A', 'REVISAR', 'Divergência', 'Data/Hora Criação', 'Data/Hora Modificação']
dfok = dfok[sortfinal]
dfok = dfok.sort_values('REVISAR')
### ---- SOBRA NET-A / [OK]
dfsobraneta = dfok.loc[(dfok['REVISAR'] == 'Sobra Net-A')]
dfsobraneta = dfsobraneta.sort_values('Documento') #ordenar por CNPJ
### ---- SOBRA CRM / [OK]
dfsobracrm = dfok.loc[(dfok['REVISAR'] == 'Sobra CRM')]
dfsobracrm = dfsobracrm.sort_values('Documento') #ordenar por CNPJ


###### ------- CONTAGEM POR REVISÃO [OK]
calcsobracrm = dfcountres.loc[(dfcountres['REVISAR'] == 'Sobra CRM')] #armazena total CRM
calcsobraneta = dfcountres.loc[(dfcountres['REVISAR'] == 'Sobra Net-A')] #armazena total Net-A
calcok = dfcountres.loc[(dfcountres['REVISAR'] == 'OK')] #armazena total OK
calcdesc = ['Sobra CRM', 'Sobra Net-A', 'OK']
calctot = [calcsobracrm.count(0)['REVISAR'], calcsobraneta.count(0)['REVISAR'], calcok.count(0)['REVISAR']]

sheetresumoS = {
    'Descrição': calcdesc,
    'Totais': calctot,
}

sheetresumoF = pd.DataFrame(sheetresumoS)


###### ------- SALVAR PARA .XLSX [OK]
with pd.ExcelWriter(f'{xlsxsaida}{ncpoe} - {hoje}') as writer:
  dfok.to_excel(writer, sheet_name='OK', index=False)
  dfsobraneta.to_excel(writer, sheet_name='Sobra Net-A', index=False) #Sobra Net-A
  dfsobracrm.to_excel(writer, sheet_name='Sobra CRM', index=False)
  sheetresumoF.to_excel(writer, sheet_name='Resumo', index=False)

#https://techoverflow.net/2021/03/05/how-to-auto-fit-pandas-pd-to_excel-xlsx-column-width/

###### ------- REMOVER HASHTAG PARA VER RESULTADOS AQUI ------- ######
#resultado.columns
#print(resultado) #ver dataframe em formato de planilha com resultado
#rscrm['REVISAR'].unique()
#sobraneta['REVISAR'].value_counts()
###### ------- REMOVER HASHTAG PARA VER RESULTADOS AQUI ------- ######

