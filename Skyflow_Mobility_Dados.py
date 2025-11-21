import pandas as pd
import folium
import json
import streamlit as st
from streamlit_folium import st_folium

from sqlalchemy import create_engine
import psycopg2  # precisa estar instalado: pip install psycopg2-binary


# ==============================
# CONFIGURAÇÃO DE CONEXÃO – SUPABASE (POSTGRES)
# ==============================

# Pegue esses dados em:
# Supabase -> Project -> Settings -> Database -> Connection string

DB_URL = "postgresql+psycopg2://postgres:Fwc2025Fiap@db.yxeweiwnctswkvjokkqh.supabase.co:5432/postgres?sslmode=require"

# Cria engine global para reuso
engine = create_engine(DB_URL)


# ==============================
# TABELAS DO PROJETO SKYFLOW
# ==============================

TABLES = [
    "tb_aeronaves",
    "tb_voos_ativos",
    "tb_rotas_simuladas",
    "tb_zonas_proibidas",
    "tb_clima_tempo_real",
    "tb_alertas_criticos",
    "tb_historico_malha_aerea",
]


# ==============================
# FUNÇÕES DE BANCO
# ==============================

def get_connection():
    """
    Cria e retorna uma conexão com o banco Supabase (PostgreSQL).
    """
    # engine.connect() retorna uma Connection do SQLAlchemy
    return engine.connect()


def read_table_to_dataframe(conn, table_name: str) -> pd.DataFrame:
    """
    Lê TODOS os registros de uma tabela e retorna como DataFrame do pandas.
    """
    query = f'SELECT * FROM {table_name}'
    df = pd.read_sql(query, con=conn)
    return df


def load_skyflow_tables() -> dict:
    """
    Carrega todas as tabelas do Supabase em DataFrames.
    Retorna um dicionário: {nome_tabela: DataFrame}.
    """
    conn = None
    tabelas_df: dict[str, pd.DataFrame] = {}

    try:
        conn = get_connection()
        for table in TABLES:
            try:
                df = read_table_to_dataframe(conn, table)
                tabelas_df[table] = df
            except Exception as e_t:
                st.warning(f"Erro ao ler a tabela {table}: {e_t}")
        return tabelas_df
    finally:
        if conn is not None:
            try:
                conn.close()
            except:  # noqa: E722
                pass


# ==============================
# FUNÇÕES DE MAPA
# ==============================

def _obter_centro_mapa(
    df_voos: pd.DataFrame | None,
    df_clima: pd.DataFrame | None = None
) -> tuple[float, float]:
    """
    Define o centro do mapa com base em voos ou clima.
    (Agora usando nomes de colunas em minúsculo, padrão Postgres.)
    """
    latitudes: list[float] = []
    longitudes: list[float] = []

    if df_voos is not None and not df_voos.empty:
        if {"origem_latitude", "origem_longitude"}.issubset(df_voos.columns):
            latitudes.extend(df_voos["origem_latitude"].astype(float).tolist())
            longitudes.extend(df_voos["origem_longitude"].astype(float).tolist())
        if {"destino_latitude", "destino_longitude"}.issubset(df_voos.columns):
            latitudes.extend(df_voos["destino_latitude"].astype(float).tolist())
            longitudes.extend(df_voos["destino_longitude"].astype(float).tolist())

    if (not latitudes or not longitudes) and df_clima is not None and not df_clima.empty:
        if {"latitude", "longitude"}.issubset(df_clima.columns):
            latitudes.extend(df_clima["latitude"].astype(float).tolist())
            longitudes.extend(df_clima["longitude"].astype(float).tolist())

    if latitudes and longitudes:
        return sum(latitudes) / len(latitudes), sum(longitudes) / len(longitudes)

    # fallback: centro de São Paulo
    return -23.5500, -46.6330


def _cor_por_status_voo(status: str | None) -> str:
    """
    Define cor do marcador com base no status do voo.
    """
    if status is None:
        return "gray"
    status = str(status).upper()
    if status == "EM ROTA":
        return "blue"
    if status == "ATRASADO":
        return "orange"
    if status == "EMERGENCIA":
        return "red"
    if status == "FINALIZADO":
        return "green"
    return "gray"


def criar_mapa_skyflow(
    df_voos: pd.DataFrame,
    df_rotas: pd.DataFrame | None,
    df_zonas: pd.DataFrame | None,
    df_clima: pd.DataFrame | None,
    voo_selecionado: int | None = None,
    aeronave_selecionada: int | None = None,
) -> folium.Map:
    """
    Cria o mapa Folium com:
      - voos (todos ou filtrados por aeronave)
      - voo selecionado destacado (origem/destino + linha)
      - zonas proibidas
      - clima em tempo real
    """

    # Filtra voos pela aeronave, se houver seleção
    if aeronave_selecionada is not None and "id_aeronave" in df_voos.columns:
        df_voos_plot = df_voos[df_voos["id_aeronave"] == aeronave_selecionada].copy()
    else:
        df_voos_plot = df_voos.copy()

    # Centro do mapa
    center_lat, center_lon = _obter_centro_mapa(df_voos_plot, df_clima)

    # Mapa responsivo
    mapa = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=12,
        control_scale=True,
        width="100%",
        height="600px",
    )

    # --- Camada: Zonas Proibidas ---
    if df_zonas is not None and not df_zonas.empty:
        layer_zonas = folium.FeatureGroup(name="Zonas Proibidas", show=True)

        for _, row in df_zonas.iterrows():
            try:
                geojson_str = row.get("poligono_area_geojson")
                if isinstance(geojson_str, str):
                    geojson_obj = json.loads(geojson_str)

                    nome_zona = row.get("nome_zona", "Zona")
                    tipo_zona = row.get("tipo_zona", "Zona")
                    alt_max = row.get("altitude_maxima_permitida", None)

                    popup_text = f"""
                    <b>Zona:</b> {nome_zona}<br>
                    <b>Tipo:</b> {tipo_zona}<br>
                    <b>Altura máx. permitida:</b> {alt_max} m<br>
                    """

                    folium.GeoJson(
                        geojson_obj,
                        name=nome_zona,
                        tooltip=f"{nome_zona} ({tipo_zona})",
                        popup=folium.Popup(popup_text, max_width=300),
                        style_function=lambda x: {
                            "fillColor": "red",
                            "color": "red",
                            "weight": 2,
                            "fillOpacity": 0.3,
                        },
                    ).add_to(layer_zonas)
            except Exception as e:
                print(f"Erro ao plotar zona proibida: {e}")
                continue

        layer_zonas.add_to(mapa)

    # --- Camada: Clima Tempo Real ---
    if df_clima is not None and not df_clima.empty:
        layer_clima = folium.FeatureGroup(name="Clima Tempo Real", show=True)

        for _, row in df_clima.iterrows():
            try:
                lat = float(row["latitude"])
                lon = float(row["longitude"])
                cond = row.get("condicao_climatica", "Desconhecido")
                risco_clima = row.get("risco_climatico", "Desconhecido")
                temp = row.get("temperatura_c", None)
                umid = row.get("umidade_relativa", None)
                vento = row.get("velocidade_vento", None)
                data_hora = row.get("data_hora", None)

                popup_text = f"""
                <b>Condição climática:</b> {cond}<br>
                <b>Risco climático:</b> {risco_clima}<br>
                <b>Temperatura:</b> {temp} °C<br>
                <b>Umidade:</b> {umid} %<br>
                <b>Vento:</b> {vento} km/h<br>
                <b>Data/hora:</b> {data_hora}<br>
                """

                folium.Marker(
                    location=[lat, lon],
                    popup=folium.Popup(popup_text, max_width=300),
                    tooltip=f"Clima: {cond} (Risco: {risco_clima})",
                    icon=folium.Icon(icon="cloud", prefix="fa"),
                ).add_to(layer_clima)
            except Exception as e:
                print(f"Erro ao plotar clima: {e}")
                continue

        layer_clima.add_to(mapa)

    # --- Camada: Voos (todos ou filtrados) ---
    if df_voos_plot is not None and not df_voos_plot.empty:
        layer_voos = folium.FeatureGroup(name="Voos Ativos", show=True)

        for _, row in df_voos_plot.iterrows():
            try:
                lat_o = float(row["origem_latitude"])
                lon_o = float(row["origem_longitude"])
                status = row.get("status_voo", "Desconhecido")
                altitude = row.get("altitude_atual", None)
                velocidade = row.get("velocidade_atual", None)
                hora_inicio = row.get("hora_inicio", None)
                hora_prevista = row.get("hora_prevista_chegada", None)
                id_voo = row.get("id_voo", None)

                # Se for o voo selecionado → destaca em outra camada
                if voo_selecionado is not None and id_voo == voo_selecionado:
                    continue

                popup_text = f"""
                <b>Voo ID:</b> {id_voo}<br>
                <b>Status:</b> {status}<br>
                <b>Altitude atual:</b> {altitude} m<br>
                <b>Velocidade atual:</b> {velocidade} km/h<br>
                <b>Início:</b> {hora_inicio}<br>
                <b>Previsto Chegada:</b> {hora_prevista}<br>
                """

                folium.CircleMarker(
                    location=[lat_o, lon_o],
                    radius=5,
                    popup=folium.Popup(popup_text, max_width=300),
                    tooltip=f"Voo {id_voo} - {status}",
                    color=_cor_por_status_voo(status),
                    fill=True,
                    fill_opacity=0.7,
                ).add_to(layer_voos)
            except Exception as e:
                print(f"Erro ao plotar voo: {e}")
                continue

        layer_voos.add_to(mapa)

    # --- Destaque do Voo Selecionado ---
    if voo_selecionado is not None and not df_voos.empty:
        df_voo_sel = df_voos[df_voos["id_voo"] == voo_selecionado]
        if not df_voo_sel.empty:
            row = df_voo_sel.iloc[0]
            try:
                lat_o = float(row["origem_latitude"])
                lon_o = float(row["origem_longitude"])
                lat_d = float(row["destino_latitude"])
                lon_d = float(row["destino_longitude"])
                status = row.get("status_voo", "Desconhecido")
                altitude = row.get("altitude_atual", None)
                velocidade = row.get("velocidade_atual", None)
                hora_inicio = row.get("hora_inicio", None)
                hora_prevista = row.get("hora_prevista_chegada", None)

                popup_text_origem = f"""
                <b>Voo ID:</b> {voo_selecionado}<br>
                <b>Ponto:</b> Origem<br>
                <b>Status:</b> {status}<br>
                <b>Altitude atual:</b> {altitude} m<br>
                <b>Velocidade atual:</b> {velocidade} km/h<br>
                <b>Início:</b> {hora_inicio}<br>
                <b>Previsto Chegada:</b> {hora_prevista}<br>
                """

                popup_text_destino = f"""
                <b>Voo ID:</b> {voo_selecionado}<br>
                <b>Ponto:</b> Destino<br>
                <b>Status:</b> {status}<br>
                <b>Altitude atual:</b> {altitude} m<br>
                <b>Velocidade atual:</b> {velocidade} km/h<br>
                <b>Previsto Chegada:</b> {hora_prevista}<br>
                """

                # Marcador de origem
                folium.Marker(
                    location=[lat_o, lon_o],
                    popup=folium.Popup(popup_text_origem, max_width=300),
                    tooltip=f"Origem do voo {voo_selecionado}",
                    icon=folium.Icon(color="green", icon="plane-departure", prefix="fa"),
                ).add_to(mapa)

                # Marcador de destino
                folium.Marker(
                    location=[lat_d, lon_d],
                    popup=folium.Popup(popup_text_destino, max_width=300),
                    tooltip=f"Destino do voo {voo_selecionado}",
                    icon=folium.Icon(color="red", icon="plane-arrival", prefix="fa"),
                ).add_to(mapa)

                # Linha representando o trajeto do voo (origem -> destino)
                folium.PolyLine(
                    locations=[[lat_o, lon_o], [lat_d, lon_d]],
                    popup=f"Trajeto do voo {voo_selecionado}",
                    tooltip=f"Trajeto do voo {voo_selecionado}",
                    weight=4,
                    color="yellow",
                ).add_to(mapa)
            except Exception as e:
                st.warning(f"Erro ao destacar voo selecionado: {e}")

    folium.LayerControl().add_to(mapa)
    return mapa


# ==============================
# APP STREAMLIT
# ==============================

def main():
    st.set_page_config(
        page_title="SkyFlow Mobility - Mapa Aéreo Urbano 4D",
        layout="wide",
    )

    st.title("🛩 SkyFlow Mobility – Mapa Aéreo Urbano 4D")
    st.markdown(
        "Visualização interativa dos voos, zonas proibidas e clima, "
        "com filtros por **aeronave** e **voo** no painel à esquerda."
    )

    # Carrega dados do banco
    with st.spinner("Carregando dados do Supabase (PostgreSQL)..."):
        tabelas = load_skyflow_tables()

    df_aeronaves = tabelas.get("tb_aeronaves", pd.DataFrame())
    df_voos = tabelas.get("tb_voos_ativos", pd.DataFrame())
    df_rotas = tabelas.get("tb_rotas_simuladas", pd.DataFrame())
    df_zonas = tabelas.get("tb_zonas_proibidas", pd.DataFrame())
    df_clima = tabelas.get("tb_clima_tempo_real", pd.DataFrame())

    if df_voos.empty or df_aeronaves.empty:
        st.error("Não foi possível carregar tb_voos_ativos ou tb_aeronaves. Verifique o banco.")
        st.stop()

    # ==========================
    # SIDEBAR – FILTROS
    # ==========================
    st.sidebar.header("Filtros")

    # Opções de aeronave (ID + nome)
    if "nome_modelo" in df_aeronaves.columns and "id_aeronave" in df_aeronaves.columns:
        df_aeronaves_sorted = df_aeronaves.sort_values("nome_modelo")
    else:
        df_aeronaves_sorted = df_aeronaves.copy()

    opcoes_aeronave: list[tuple[int, str]] = []
    for _, row in df_aeronaves_sorted.iterrows():
        try:
            id_aer = int(row["id_aeronave"])
            nome_mod = row.get("nome_modelo", "Modelo")
            opcoes_aeronave.append(
                (id_aer, f"{id_aer} - {nome_mod}")
            )
        except Exception:
            continue

    label_por_id_aer = {id_: label for id_, label in opcoes_aeronave}
    label_list = [label for _, label in opcoes_aeronave]

    aeronave_label_sel = st.sidebar.selectbox(
        "Aeronave",
        options=label_list,
        index=0 if label_list else None,
    )

    aeronave_id_sel: int | None = None
    if aeronave_label_sel:
        for id_, label in label_por_id_aer.items():
            if label == aeronave_label_sel:
                aeronave_id_sel = id_
                break

    # Filtra voos pela aeronave escolhida
    df_voos_aer = df_voos.copy()
    if aeronave_id_sel is not None and "id_aeronave" in df_voos.columns:
        df_voos_aer = df_voos[df_voos["id_aeronave"] == aeronave_id_sel].copy()

    # Opções de voo
    opcoes_voo: list[tuple[int, str]] = []
    for _, row in df_voos_aer.sort_values("id_voo").iterrows():
        try:
            id_voo = int(row["id_voo"])
            status = row.get("status_voo", "Sem status")
            opcoes_voo.append(
                (id_voo, f"{id_voo} - {status}")
            )
        except Exception:
            continue

    voo_id_sel: int | None = None
    if opcoes_voo:
        label_voos = [label for _, label in opcoes_voo]
        voo_label_sel = st.sidebar.selectbox(
            "Voo",
            options=label_voos,
            index=0,
        )
        for id_v, label in opcoes_voo:
            if label == voo_label_sel:
                voo_id_sel = id_v
                break
    else:
        st.sidebar.info("Nenhum voo encontrado para essa aeronave.")

    st.sidebar.markdown("---")
    st.sidebar.caption("Selecione uma aeronave e um voo para destacar no mapa.")

    # ==========================
    # MAPA
    # ==========================
    mapa = criar_mapa_skyflow(
        df_voos=df_voos,
        df_rotas=df_rotas,
        df_zonas=df_zonas,
        df_clima=df_clima,
        voo_selecionado=voo_id_sel,
        aeronave_selecionada=aeronave_id_sel,
    )

    st_folium(mapa, width="100%", height=600)


if __name__ == "__main__":
    main()
