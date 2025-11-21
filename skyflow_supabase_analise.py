# ============================================================
# skyflow_supabase_analise.py
# Análise de dados SkyFlow Mobility (Global Solution – Fase 7)
#
# Este script:
# - Conecta ao banco PostgreSQL (Supabase)
# - Carrega tabelas principais do projeto
# - Faz limpeza e normalização de dados
# - Gera estatísticas descritivas
# - Calcula correlações
# - Executa teste de hipótese (DRONE vs EVTOL)
# - Realiza mineração de texto simples (NLP) em alertas
# - Gera um PDF A4 com margens de 1 cm contendo:
#     * Capa (título, grupo, tabela de integrantes)
#     * Todos os gráficos + análises
#     => skyflow_relatorio_graficos.pdf
# ============================================================
import os
import pandas as pd
import numpy as np
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

DB_URL = "postgresql+psycopg2://postgres:Fwc2025Fiap@db.yxeweiwnctswkvjokkqh.supabase.co:5432/postgres?sslmode=require"

engine = create_engine(DB_URL)

try:
    engine.connect()
    print("Conectou ao banco Supabase com sucesso!")
except Exception as e:
    print("Erro de conexão com o Supabase:", e)

# ------------------------------------------------------------
# 2. FUNÇÕES AUXILIARES: PÁGINAS DO PDF (A4, ~1 cm de margem)
# ------------------------------------------------------------

def add_cover_page(pdf):
    """
    Capa com margens aproximadas de 1 cm.
    """
    fig = plt.figure(figsize=(8.27, 11.69))  # A4
    margin = 0.39  # ~1 cm

    ax = fig.add_subplot(111)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    # Título
    ax.text(
        0.5,
        1 - margin / fig.get_figheight(),
        "SkyFlow Mobility",
        ha="center",
        va="top",
        fontsize=28,
        fontweight="bold",
    )

    # Grupo
    ax.text(
        0.5,
        1 - (margin / fig.get_figheight()) - 0.06,
        "Grupo: Síntese",
        ha="center",
        va="top",
        fontsize=16,
        fontweight="bold",
    )

    # Tabela de integrantes
    table_data = [
        ["FABIO WANDENKOLK DE CHANTAL", "562514"],
        ["JOZUÉ ALVES DE AZEVEDO JUNIOR", "562174"],
        ["THIAGO CAMPANILLE REIS", "561839"],
    ]
    col_labels = ["Nome", "RM"]

    table = ax.table(
        cellText=table_data,
        colLabels=col_labels,
        loc="center",
        cellLoc="left",
        colWidths=[0.75, 0.25],
        bbox=[0.10, 0.35, 0.80, 0.20],
    )

    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 1.5)

    pdf.savefig(fig)
    plt.close(fig)


def add_chart_page(pdf, title, description, plot_func):
    """
    Página com gráfico + texto + margens de 1 cm.
    Gráficos comuns têm o eixo encolhido para não encostar nas bordas.
    Heatmaps de correlação criam seus próprios eixos e ignoram esse ajuste.
    """
    fig = plt.figure(figsize=(8.27, 11.69))  # A4
    margin = 0.39  # 1 cm

    gs = fig.add_gridspec(
        2,
        1,
        height_ratios=[1.4, 1],      # gráfico menor que o texto
        top=1 - margin / fig.get_figheight() - 0.02,
        bottom=margin / fig.get_figheight(),
        left=margin / fig.get_figwidth(),
        right=1 - margin / fig.get_figwidth(),
        hspace=0.48,
    )

    ax_chart = fig.add_subplot(gs[0])

    # Título
    fig.suptitle(
        title,
        fontsize=15,
        fontweight="bold",
        y=1 - (margin / fig.get_figheight()) * 0.70,
    )

    # Gera o gráfico no eixo principal
    plot_func(ax_chart)

    # 🔹 Encolher o eixo do gráfico em ~10% na largura e um pouco na altura
    #    Isso evita que fique colado nas bordas, mesmo em viewers que cortam perto.
    pos = ax_chart.get_position()
    new_x0 = pos.x0 + 0.05
    new_y0 = pos.y0 + 0.02
    new_width = pos.width - 0.10
    new_height = pos.height - 0.04
    ax_chart.set_position([new_x0, new_y0, new_width, new_height])

    # Texto explicativo
    ax_text = fig.add_subplot(gs[1])
    ax_text.axis("off")
    ax_text.text(
        0,
        1,
        description,
        ha="left",
        va="top",
        wrap=True,
        fontsize=11,
        linespacing=1.4,
    )

    pdf.savefig(fig)
    plt.close(fig)


def add_text_page(pdf, title, text):
    """
    Página só de texto com margens de ~1 cm.
    """
    fig = plt.figure(figsize=(8.27, 11.69))  # A4
    margin = 0.39  # 1 cm

    ax = fig.add_subplot(111)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    ax.text(
        0.5,
        1 - margin / fig.get_figheight(),
        title,
        ha="center",
        va="top",
        fontsize=16,
        fontweight="bold",
    )

    ax.text(
        margin / fig.get_figwidth(),
        1 - margin / fig.get_figheight() - 0.08,
        text,
        ha="left",
        va="top",
        wrap=True,
        fontsize=11,
        linespacing=1.4,
    )

    pdf.savefig(fig)
    plt.close(fig)

# ------------------------------------------------------------
# 3. CARREGAR TABELAS
# ------------------------------------------------------------

def carregar_tabelas():
    df_aeronaves = pd.read_sql("SELECT * FROM tb_aeronaves", engine)
    df_voos = pd.read_sql("SELECT * FROM tb_voos_ativos", engine)
    df_alertas = pd.read_sql("SELECT * FROM tb_alertas_criticos", engine)
    df_clima = pd.read_sql("SELECT * FROM tb_clima_tempo_real", engine)
    df_malha = pd.read_sql("SELECT * FROM tb_historico_malha_aerea", engine)
    return df_aeronaves, df_voos, df_alertas, df_clima, df_malha


df_aeronaves, df_voos, df_alertas, df_clima, df_malha = carregar_tabelas()

print("Aeronaves:\n", df_aeronaves.head(), "\n")
print("Voos:\n", df_voos.head(), "\n")
print("Alertas:\n", df_alertas.head(), "\n")
print("Clima:\n", df_clima.head(), "\n")
print("Malha aérea:\n", df_malha.head(), "\n")

# ------------------------------------------------------------
# 4. LIMPEZA E NORMALIZAÇÃO
# ------------------------------------------------------------

def limpar_dados(df_aeronaves, df_voos, df_alertas, df_clima, df_malha):
    if 'status_voo' in df_voos.columns:
        df_voos['status_voo'] = df_voos['status_voo'].astype(str).str.upper().str.strip()

    if 'tipo' in df_aeronaves.columns:
        df_aeronaves['tipo'] = df_aeronaves['tipo'].astype(str).str.upper().str.strip()

    if 'risco_climatico' in df_clima.columns:
        df_clima['risco_climatico'] = df_clima['risco_climatico'].astype(str).str.upper().str.strip()

    if 'descricao' in df_alertas.columns:
        df_alertas['descricao'] = df_alertas['descricao'].fillna("")

    col_coords = ['origem_latitude', 'origem_longitude',
                  'destino_latitude', 'destino_longitude']
    col_coords = [c for c in col_coords if c in df_voos.columns]
    if col_coords:
        df_voos_limpo = df_voos.dropna(subset=col_coords)
    else:
        df_voos_limpo = df_voos

    return df_aeronaves, df_voos_limpo, df_alertas, df_clima, df_malha


df_aeronaves, df_voos, df_alertas, df_clima, df_malha = limpar_dados(
    df_aeronaves, df_voos, df_alertas, df_clima, df_malha
)

print("Após limpeza, total de voos:", len(df_voos))

# ------------------------------------------------------------
# 5. ESTATÍSTICA DESCRITIVA
# ------------------------------------------------------------

colunas_numericas_voos = [c for c in ['altitude_atual', 'velocidade_atual'] if c in df_voos.columns]
print("\n--- Estatística Descritiva - Voos (altitude e velocidade) ---\n")
if colunas_numericas_voos:
    print(df_voos[colunas_numericas_voos].describe())
else:
    print("Colunas numéricas esperadas não encontradas em df_voos.")

if 'id_aeronave' in df_voos.columns and 'id_aeronave' in df_aeronaves.columns:
    df_voos_aero = df_voos.merge(df_aeronaves, on='id_aeronave', how='left')
else:
    df_voos_aero = df_voos.copy()
    df_voos_aero['tipo'] = 'DESCONHECIDO'

media_altitude = float(df_voos['altitude_atual'].mean()) if 'altitude_atual' in df_voos.columns else None
media_velocidade = float(df_voos['velocidade_atual'].mean()) if 'velocidade_atual' in df_voos.columns else None

# ------------------------------------------------------------
# 6. GERAR PDF NA MESMA PASTA
# ------------------------------------------------------------

SCRIPT_PATH = os.path.dirname(os.path.abspath(__file__))
pdf_filename = os.path.join(SCRIPT_PATH, "skyflow_relatorio_graficos.pdf")

with PdfPages(pdf_filename) as pdf:

    # 6.0 CAPA
    add_cover_page(pdf)

    # 6.1 Histograma de Altitude
    if 'altitude_atual' in df_voos.columns:
        def plot_altitude_hist(ax):
            ax.hist(df_voos['altitude_atual'], bins=30)
            ax.set_xlabel("Altitude (m)")
            ax.set_ylabel("Frequência")

        if media_altitude is not None:
            desc_alt = (
                f"Este gráfico mostra a distribuição da altitude dos voos monitorados. "
                f"A altitude média é aproximadamente {media_altitude:.2f} metros. "
                "É possível observar em que faixa de altitude a maioria dos voos se concentra, "
                "identificando padrões de operação e possíveis outliers em altitudes muito baixas ou muito altas."
            )
        else:
            desc_alt = (
                "Este gráfico mostra a distribuição da altitude dos voos monitorados. "
                "É possível observar em que faixa de altitude a maioria dos voos se concentra, "
                "identificando padrões de operação e possíveis outliers."
            )

        add_chart_page(
            pdf,
            "Distribuição da Altitude Atual dos Voos",
            desc_alt,
            plot_altitude_hist
        )

    # 6.2 Histograma de Velocidade
    if 'velocidade_atual' in df_voos.columns:
        def plot_velocidade_hist(ax):
            ax.hist(df_voos['velocidade_atual'], bins=30)
            ax.set_xlabel("Velocidade (km/h ou m/s)")
            ax.set_ylabel("Frequência")

        if media_velocidade is not None:
            desc_vel = (
                f"Este gráfico apresenta a distribuição da velocidade atual dos voos. "
                f"A velocidade média é aproximadamente {media_velocidade:.2f} unidades. "
                "A curva de frequência permite identificar se a maioria das aeronaves está operando "
                "em uma faixa estreita de velocidade (padrão estável) ou se há grande variabilidade."
            )
        else:
            desc_vel = (
                "Este gráfico apresenta a distribuição da velocidade atual dos voos. "
                "A curva de frequência permite identificar se a maioria das aeronaves está operando "
                "em uma faixa estreita de velocidade ou se há grande variabilidade."
            )

        add_chart_page(
            pdf,
            "Distribuição da Velocidade Atual dos Voos",
            desc_vel,
            plot_velocidade_hist
        )

    # 6.3 Boxplot Altitude x Tipo de Aeronave
    if 'tipo' in df_voos_aero.columns and 'altitude_atual' in df_voos_aero.columns:
        def plot_box_alt_tipo(ax):
            sns.boxplot(data=df_voos_aero, x='tipo', y='altitude_atual', ax=ax)
            ax.set_xlabel("Tipo de aeronave")
            ax.set_ylabel("Altitude (m)")

        desc_box = (
            "O boxplot compara a distribuição de altitude entre os diferentes tipos de aeronave "
            "(por exemplo, DRONE e EVTOL). Cada caixa mostra a mediana, os quartis e possíveis outliers. "
            "Assim, é possível visualizar quais tipos operam em altitudes mais altas ou mais baixas, "
            "auxiliando na definição de regras de tráfego aéreo urbano."
        )
        add_chart_page(
            pdf,
            "Altitude Atual por Tipo de Aeronave",
            desc_box,
            plot_box_alt_tipo
        )

    # 6.4 Scatter Velocidade x Altitude
    if 'altitude_atual' in df_voos.columns and 'velocidade_atual' in df_voos.columns:
        corr_val = df_voos[['altitude_atual', 'velocidade_atual']].corr().iloc[0, 1]

        def plot_scatter_alt_vel(ax):
            sns.scatterplot(data=df_voos, x='velocidade_atual', y='altitude_atual', ax=ax)
            ax.set_xlabel("Velocidade")
            ax.set_ylabel("Altitude")

        desc_scatter = (
            f"Este gráfico de dispersão mostra a relação entre velocidade e altitude dos voos. "
            f"O coeficiente de correlação entre as duas variáveis é aproximadamente {corr_val:.2f}. "
            "Valores próximos de 0 sugerem pouca correlação; valores próximos de 1 ou -1 indicam correlação "
            "forte positiva ou negativa, respectivamente."
        )
        add_chart_page(
            pdf,
            "Relação entre Velocidade e Altitude dos Voos",
            desc_scatter,
            plot_scatter_alt_vel
        )

    # 6.5 Heatmap de Correlação (Voos) - com colorbar separada
    col_corr = [c for c in ['altitude_atual', 'velocidade_atual'] if c in df_voos.columns]
    if len(col_corr) >= 2:
        df_corr = df_voos[col_corr].corr()

        def plot_heat_voos(ax):
            ax.set_visible(False)
            fig = ax.figure

            left = 0.18
            bottom = 0.50
            width = 0.55
            height = 0.35

            heat_ax = fig.add_axes([left, bottom, width, height])
            cbar_ax = fig.add_axes([left + width + 0.035, bottom, 0.03, height])

            sns.heatmap(
                df_corr,
                annot=True,
                fmt=".3f",
                cmap="viridis",
                vmin=-1,
                vmax=1,
                square=True,
                cbar=True,
                cbar_ax=cbar_ax,
                ax=heat_ax,
            )
            heat_ax.set_xticklabels(heat_ax.get_xticklabels(), rotation=45, ha="right")
            heat_ax.set_yticklabels(heat_ax.get_yticklabels(), rotation=0)
            heat_ax.set_xlabel("")
            heat_ax.set_ylabel("")

        desc_heat_v = (
            "O mapa de calor apresenta a matriz de correlação entre altitude e velocidade. "
            "Os valores em cada célula indicam a intensidade e o sentido da relação entre as variáveis. "
            "Nesta análise, vemos se mudanças na velocidade estão associadas a mudanças na altitude."
        )
        add_chart_page(
            pdf,
            "Correlação entre Altitude e Velocidade",
            desc_heat_v,
            plot_heat_voos
        )

    # 6.6 Heatmap de Correlação (Clima) - com colorbar separada
    if not df_clima.empty:
        possible_cols = ['temperatura_c', 'umidade_relativa', 'velocidade_vento']
        cols_clima = [c for c in possible_cols if c in df_clima.columns]
        if len(cols_clima) >= 2:
            df_corr_clima = df_clima[cols_clima].corr()

            def plot_heat_clima(ax):
                ax.set_visible(False)
                fig = ax.figure

                left = 0.18
                bottom = 0.50
                width = 0.55
                height = 0.35

                heat_ax = fig.add_axes([left, bottom, width, height])
                cbar_ax = fig.add_axes([left + width + 0.035, bottom, 0.03, height])

                sns.heatmap(
                    df_corr_clima,
                    annot=True,
                    fmt=".3f",
                    cmap="viridis",
                    vmin=-1,
                    vmax=1,
                    square=True,
                    cbar=True,
                    cbar_ax=cbar_ax,
                    ax=heat_ax,
                )
                heat_ax.set_xticklabels(heat_ax.get_xticklabels(), rotation=45, ha="right")
                heat_ax.set_yticklabels(heat_ax.get_yticklabels(), rotation=0)
                heat_ax.set_xlabel("")
                heat_ax.set_ylabel("")

            desc_heat_c = (
                "Este mapa de calor mostra a correlação entre variáveis climáticas, como temperatura, "
                "umidade relativa e velocidade do vento. Correlações relevantes ajudam a entender combinações "
                "de condições que podem aumentar o risco operacional para os voos."
            )
            add_chart_page(
                pdf,
                "Correlação entre Variáveis de Clima",
                desc_heat_c,
                plot_heat_clima
            )

    # 6.7 Teste de Hipótese (DRONE vs EVTOL)
    if 'tipo' in df_voos_aero.columns and 'velocidade_atual' in df_voos_aero.columns:
        drones = df_voos_aero[df_voos_aero['tipo'] == 'DRONE']['velocidade_atual'].dropna()
        evtols = df_voos_aero[df_voos_aero['tipo'] == 'EVTOL']['velocidade_atual'].dropna()

        if len(drones) > 5 and len(evtols) > 5:
            stat, pvalue = ttest_ind(drones, evtols, equal_var=False)
            texto_teste = (
                "Este teste de hipótese compara as velocidades médias entre aeronaves do tipo DRONE e EVTOL.\n\n"
                f"Estatística t: {stat:.4f}\n"
                f"p-valor: {pvalue:.4f}\n\n"
                "Se o p-valor for menor que 0,05, consideramos que existe evidência estatística de que as médias "
                "são diferentes. Caso contrário, não há evidência forte de diferença.\n\n"
            )
            if pvalue < 0.05:
                texto_teste += (
                    "Resultado: p-valor < 0,05. Rejeitamos H0 e concluímos que há diferença significativa "
                    "entre as velocidades médias de DRONE e EVTOL."
                )
            else:
                texto_teste += (
                    "Resultado: p-valor ≥ 0,05. Não rejeitamos H0 e concluímos que não há evidência forte "
                    "de diferença entre as velocidades médias de DRONE e EVTOL."
                )
        else:
            texto_teste = (
                "Não há dados suficientes (amostras de DRONE e EVTOL) para aplicar o teste de hipótese "
                "sobre a diferença de velocidade média entre os dois tipos de aeronave."
            )
    else:
        texto_teste = (
            "As colunas necessárias ('tipo' e 'velocidade_atual') não estão disponíveis para realizar o "
            "teste de hipótese DRONE vs EVTOL."
        )

    add_text_page(pdf, "Teste de Hipótese: DRONE vs EVTOL (Velocidade)", texto_teste)

    # 6.8 Nuvem de Palavras dos Alertas
    if not df_alertas.empty and 'descricao' in df_alertas.columns:
        texto_alertas = " ".join(df_alertas['descricao'].tolist())
        if texto_alertas.strip():
            def plot_wordcloud(ax):
                wc = WordCloud(width=800, height=400, background_color="white").generate(texto_alertas)
                ax.imshow(wc)
                ax.axis("off")

            desc_wc = (
                "A nuvem de palavras apresenta os termos mais frequentes nas descrições dos alertas críticos. "
                "Palavras maiores são mais recorrentes, ajudando a identificar rapidamente os tipos de riscos "
                "que mais aparecem no sistema."
            )
            add_chart_page(
                pdf,
                "Nuvem de Palavras dos Alertas Críticos",
                desc_wc,
                plot_wordcloud
            )

    # 6.9 Voos por Status
    if 'status_voo' in df_voos.columns:
        def plot_voos_status(ax):
            df_voos['status_voo'].value_counts().plot(kind='bar', ax=ax)
            ax.set_xlabel("Status do Voo")
            ax.set_ylabel("Quantidade")

        desc_status = (
            "Este gráfico mostra quantos voos existem em cada status (por exemplo, ATIVO, EM ALERTA, ATRASADO). "
            "Ele dá uma visão rápida do estado atual da malha aérea monitorada pela plataforma."
        )
        add_chart_page(
            pdf,
            "Quantidade de Voos por Status",
            desc_status,
            plot_voos_status
        )

    # 6.10 Alertas por Tipo
    if 'tipo_alerta' in df_alertas.columns:
        def plot_alertas_tipo(ax):
            df_alertas['tipo_alerta'].value_counts().plot(kind='bar', ax=ax)
            ax.set_xlabel("Tipo de Alerta")
            ax.set_ylabel("Quantidade")

        desc_alerta_tipo = (
            "Este gráfico apresenta a quantidade de alertas registrados por tipo. "
            "Ele permite identificar quais categorias de risco são mais frequentes, "
            "apoiando decisões de mitigação e revisão de rotas."
        )
        add_chart_page(
            pdf,
            "Quantidade de Alertas por Tipo",
            desc_alerta_tipo,
            plot_alertas_tipo
        )

    # 6.11 Linha do tempo de alertas (últimas 24h)
    if 'hora_alerta' in df_alertas.columns:
        df_alertas['hora_alerta'] = pd.to_datetime(df_alertas['hora_alerta'])
        df_alertas_24h = df_alertas[
            df_alertas['hora_alerta'] >= df_alertas['hora_alerta'].max() - pd.Timedelta(hours=24)
        ]
        if not df_alertas_24h.empty:
            alertas_por_hora = df_alertas_24h.set_index('hora_alerta').resample('1H').size()

            def plot_alertas_tempo(ax):
                alertas_por_hora.plot(kind='line', marker='o', ax=ax)
                ax.set_xlabel("Hora")
                ax.set_ylabel("Quantidade de Alertas")

            desc_tempo = (
                "Este gráfico de linha mostra quantos alertas foram registrados a cada hora nas últimas 24 horas. "
                "Com ele, é possível identificar horários de pico de eventos críticos e reforçar o monitoramento "
                "nesses períodos."
            )
            add_chart_page(
                pdf,
                "Alertas nas Últimas 24 Horas",
                desc_tempo,
                plot_alertas_tempo
            )

print(f"\n==== FIM DA ANÁLISE SKYFLOW (ITEM 3.1) ====\nRelatório salvo em: {pdf_filename}\n")
