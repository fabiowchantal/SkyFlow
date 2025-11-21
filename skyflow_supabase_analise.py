# ============================================================
# skyflow_supabase_analise.py
# ============================================================
import os
import pandas as pd
import numpy as np
import streamlit as st               # ⬅️ IMPORTANTE
from sqlalchemy import create_engine
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud
from scipy.stats import ttest_ind
from matplotlib.backends.backend_pdf import PdfPages


# ------------------------------------------------------------
# 1. CONFIGURAÇÃO DA CONEXÃO COM O SUPABASE
# ------------------------------------------------------------

# ❌ NÃO usar mais URL fixa com usuário/senha no código
# DB_URL = "postgresql+psycopg2://postgres:SENHA@host:5432/postgres?sslmode=require"
# engine = create_engine(DB_URL)

@st.cache_resource
def get_engine():
    """
    Cria e reutiliza o engine SQLAlchemy usando as credenciais
    definidas em st.secrets['supabase'] no Streamlit Cloud.
    
    Em .streamlit/secrets.toml (ou painel do Streamlit Cloud), você deve ter algo assim:

    [supabase]
    host = "seu_host.supabase.co"
    port = "5432"
    database = "postgres"
    user = "postgres"
    password = "SUA_SENHA_AQUI"
    """
    cfg = st.secrets["supabase"]

    url = (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}?sslmode=require"
    )

    engine = create_engine(url)
    return engine


# cria o engine uma vez (e cacheia no Streamlit)
engine = get_engine()

# teste rápido de conexão (vai para os logs do Streamlit Cloud)
try:
    with engine.connect() as conn:
        print("Conectou ao banco Supabase com sucesso!")
except Exception as e:
    print("Erro de conexão com o Supabase:", e)
    # Em app Streamlit, opcional:
    st.error("Erro ao conectar no banco de dados. Verifique as credenciais em Secrets.")
    st.stop()
