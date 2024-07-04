import os
import sys
from datetime import datetime as dt
import streamlit as st
import pandas as pd
from functools import reduce
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
from fpdf import FPDF

sys.path.insert(0, os.path.abspath(os.curdir))
from config.system import *
from db.database_bigquery import DatabaseBigQuery

# Função para formatar valores no estilo "R$ 1.000,00"
def currency_format(val):
    if val is not None:
        return f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return val

# Função para carregar e preparar os dados
def load_data(file_path):
    df = pd.read_excel(file_path)
    # Remover espaços em branco das colunas
    df.columns = df.columns.str.strip()
    # Converter valores para float
    df['Valor no Centro de Custo'] = df['Valor no Centro de Custo'].astype(float)
    # Converter a coluna de data de string para datetime
    df['Data movimento'] = pd.to_datetime(df['Data movimento'], format='%d/%m/%Y', errors='coerce')
    df["Data movimento"] = df["Data movimento"].dt.normalize()
    return df

# -------------------------------------------------------------- Configurando Página

st.set_page_config(
    layout="wide",
    page_title="PACTUS",
    page_icon=":bar_chart:",
)

st.sidebar.image("teste.png", width=200)

st.title("Relatório de lançamentos por centro de custo")

# -------------------------------------------------------------- Carregando Base de Dados

# Carregar o arquivo Excel
file_path = 'novo_extrato_financeiro.xlsx'

# Carregar e preparar os dados
df = load_data(file_path)

# -------------------------------------------------------------- Configurando Filtro no Sidebar

df = df.sort_values(by="Tipo Operação", ascending=True)

agenda = ["TODOS"] + list(df["Centro de Custo"].unique())
agenda = st.sidebar.multiselect("Selecione os Centros de Custos", agenda, default="TODOS")

# Configura filtro de Data Inicial e Data Final
hoje = pd.to_datetime(dt.today()).normalize()
inicio_mes = dt(hoje.year, hoje.month, 1)

data_inicial = st.sidebar.date_input("Data Inicial", inicio_mes, format="DD/MM/YYYY")
data_final = st.sidebar.date_input("Data Final", hoje, format="DD/MM/YYYY")

data_inicial = dt.strftime(data_inicial, "%d/%m/%Y") + " 00:00"
data_inicial = pd.to_datetime(data_inicial, format="%d/%m/%Y %H:%M")

data_final = dt.strftime(data_final, "%d/%m/%Y") + " 00:00"
data_final = pd.to_datetime(data_final, format="%d/%m/%Y %H:%M")

# ----------------------------------------------------------------------- Validação dos Filtros

# Verifica se "TODOS" está selecionado junto com outras opções
if "TODOS" in agenda and len(agenda) > 1:
    st.error("Selecione 'TODOS' ou os Centros de Custos.")
    st.stop()
else:
    agenda_selecionadas = ','.join([f'"{item}"' for item in agenda])
    lista_agenda = [item.strip('"') for item in agenda_selecionadas.split(',')]

# valide se a data final é maior que a data inicial
if data_final < data_inicial:
    st.error("Data Final deve ser maior que a Data Inicial.")
    st.stop()

# -------------------------------------------------------------- Filtra Base de Dados Agendadas

condicoes = []

condicoes.append(df["Data movimento"] >= data_inicial)
condicoes.append(df["Data movimento"] <= data_final)
if "TODOS" not in agenda_selecionadas: 
    condicoes.append(df["Centro de Custo"].isin(lista_agenda))

df = df[reduce(lambda x, y: x & y, condicoes)]

# Validar se a base de dados está vazia
if df.empty:
    st.warning("Nenhum registro encontrado para os filtros selecionados.")
    st.stop()

# -------------------------------------------------------------- Calcular o saldo

df_credito = df['Valor no Centro de Custo'].apply(lambda x: x if x > 0 else 0)
df_debito = df['Valor no Centro de Custo'].apply(lambda x: x if x < 0 else 0)
total_debito = df_debito.sum()
total_credito = df_credito.sum()
saldo = total_credito + total_debito

# Configurar Cards
col1, col2, col3 = st.columns(3, gap="small")

with col1:
    st.metric(
        label="Total de Crédito",
        value=currency_format(total_credito)
    )
with col2:
    st.metric(
        label="Total de Débito",
        value=currency_format(total_debito)
    )    
with col3:
    st.metric(
        label="Saldo",
        value=currency_format(saldo)
    )

# Configurar as opções do AgGrid
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_column("Data movimento", header_name="Data", type=["dateColumnFilter", "customDateTimeFormat"], custom_format_string='dd/MM/yyyy', pivot=True)
gb.configure_column(
    "Valor no Centro de Custo", 
    header_name="Valor", 
    type=["numericColumn", "numberColumnFilter", "customNumericFormat"], 
    precision=2, 
    aggFunc='sum',
    valueFormatter=JsCode("""
        function(params) {
            if (params.value != undefined) {
                return 'R$ ' + params.value.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2});
            } else {
                return '';
            }
        }
    """),
    cellStyle=JsCode("""
        function(params) {
            if (params.value < 0) {
                return {color: 'red'};
            } else {
                return {color: 'blue'};
            }
        }
    """),
)

gb.configure_column("Descrição", header_name="Descrição", hide=True)
gb.configure_column("Tipo Operação", header_name="Tipo", hide=True)
gb.configure_column("Centro de Custo", header_name="Centro de Custo", hide=True)
gb.configure_column("Categoria", header_name="Categoria", hide=True)
gb.configure_column("Data Original Vencimento", header_name="Descrição", hide=True)
gb.configure_column("Data de competência", header_name="Tipo", hide=True)

gb.configure_default_column(
    resizable=True,
    filterable=True,
    sortable=True,
    editable=False,
)

gb.configure_grid_options(domLayout='autoHeight')

gridOptions = gb.build()

gridOptions["defaultColDef"] = {
    "flex": 1,
}

gridOptions["autoGroupColumnDef"] = {
    "headerName": 'Centros de Custo',
    "minWidth": 580,
    "cellRendererParams": {
        "suppressCount": True,
    },
}

gridOptions["treeData"] = True
gridOptions["animateRows"] = True
gridOptions["groupDefaultExpanded"] = 1
gridOptions["getDataPath"] = JsCode("""
    function(data) {
        return [                      
            data['Tipo Operação'],
            data['Centro de Custo'],                                       
            data['Categoria'],
            data['Descrição']
        ];
    }
""").js_code

# Exibir o DataFrame e o AgGrid
r = AgGrid(
    df[['Tipo Operação', 'Centro de Custo', 'Categoria', 'Descrição', 'Valor no Centro de Custo', 'Data movimento']],
    gridOptions=gridOptions,
    sizeToFit=True,
    height=2000,
    allow_unsafe_jscode=True,
    enable_enterprise_modules=True,
    update_mode=GridUpdateMode.SELECTION_CHANGED,
    theme="alpine",
    tree_data=True
)