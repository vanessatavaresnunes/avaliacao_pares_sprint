import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
load_dotenv()
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}
QUERY = """
    SELECT DISTINCT ON (id_avaliador)
        id_avaliador,
        array_to_json(
            ARRAY(
                SELECT CAST(trim(unnest(string_to_array(REPLACE(REPLACE(REPLACE(ids, '[', ''), ']', ''), '''', ''), ','))) AS NUMERIC)
            )
        )::jsonb AS ids_json,
        array_to_json(
            ARRAY(
                SELECT CAST(trim(unnest(string_to_array(REPLACE(REPLACE(REPLACE(notas_e1, '[', ''), ']', ''), '''', ''), ','))) AS NUMERIC)
            )
        )::jsonb AS notas_e1_json,
        array_to_json(
            string_to_array(
                REPLACE(REPLACE(REPLACE(comentarios_e1, '[', ''), ']', ''), '''', ''), ', '
            )
        )::jsonb AS comentarios_e1_json,
        array_to_json(
            ARRAY(
                SELECT CAST(trim(unnest(string_to_array(REPLACE(REPLACE(REPLACE(notas_e2, '[', ''), ']', ''), '''', ''), ','))) AS NUMERIC)
            )
        )::jsonb AS notas_e2_json,
        array_to_json(
            string_to_array(
                REPLACE(REPLACE(REPLACE(comentarios_e2, '[', ''), ']', ''), '''', ''), ', '
            )
        )::jsonb AS comentarios_e2_json,
        array_to_json(
            ARRAY(
                SELECT CAST(trim(unnest(string_to_array(REPLACE(REPLACE(REPLACE(notas_e3, '[', ''), ']', ''), '''', ''), ','))) AS NUMERIC)
            )
        )::jsonb AS notas_e3_json,
        array_to_json(
            string_to_array(
                REPLACE(REPLACE(REPLACE(comentarios_e3, '[', ''), ']', ''), '''', ''), ', '
            )
        )::jsonb AS comentarios_e3_json
    FROM inteli_avaliacao_individual
    WHERE timestamp BETWEEN %s AND %s
      AND id_avaliador = ANY(%s)
      AND curso = %s
      AND turma = %s
      AND modulo = %s
    GROUP BY
        id_avaliador,
        timestamp,
        ids_json,
        notas_e1_json,
        comentarios_e1_json,
        notas_e2_json,
        comentarios_e2_json,
        notas_e3_json,
        comentarios_e3_json
    ORDER BY id_avaliador, timestamp DESC;
"""
def fetch_peer_ratings_df(início_dt, fim_dt, id_avaliadores, curso, turma, modulo):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(QUERY, (início_dt, fim_dt, id_avaliadores, curso, turma, modulo))
        columns = [desc[0] for desc in cur.description]
        peer_ratings = []
        while True:
            row = cur.fetchone()
            if row is None:
                break
            peer_ratings.extend([[row[0], *rating] for rating in zip(*row[1:])])
        cur.close()
        conn.close()
        return pd.DataFrame(peer_ratings, columns=columns)
    except psycopg2.Error as e:
        print(f"Erro ao conectar ao banco: {e}")
def get_peer_feedback(df):
    peer_feedback_df = (
        df[["ids_json", "comentarios_e1_json", "comentarios_e2_json", "comentarios_e3_json"]]
        .sort_values("ids_json")
    )
    peer_feedback_df.rename(
        columns = {
            "ids_json": "id",
            "comentarios_e1_json": "comentarios_e1",
            "comentarios_e2_json": "comentarios_e2",
            "comentarios_e3_json": "comentarios_e3"
        },
        inplace = True
    )
    return peer_feedback_df.groupby("id").agg(list).reset_index()
    #return peer_feedback_df
def compute_student_points_df(df):
    student_points_df = (
        df[["ids_json", "notas_e1_json", "notas_e2_json", "notas_e3_json"]]
        .groupby("ids_json", as_index=False)
        .sum()
    )
    student_points_df.rename(
        columns = {
            "ids_json": "id",
            "notas_e1_json": "points_e1",
            "notas_e2_json": "points_e2",
            "notas_e3_json": "points_e3"
        },
        inplace = True
    )
    return student_points_df
def compute_student_index_df(df):
    df["pontos"] = df["points_e1"] + df["points_e2"] + df["points_e3"]
    mean = df["pontos"].mean()
    max = df["pontos"].max()
    min = df["pontos"].min()
    range = max - min
    range = range if range != 0 else 1  # Avoid division by zero
    print(mean)
    print(max)
    print(min)
    print(range)
    df["índice"] = ((df["pontos"] - mean) / (0.6 * range)).round(1)
if __name__ == "__main__":
    início_dt = "2025-03-14"
    fim_dt = "2025-03-18"
    # Grupo 1
    #--------
    #  3: Anna Giulia Marques Riciopo
    #  6: Daniel Augusto de Araújo Gonçalves
    # 13: João Victor de Souza Campos
    # 20: Lucas Paiva Brasil
    # 26: Nataly de Souza Cunha
    # 28: Otavio de Carvalho Vasconcelos
    # 37: Thiago Gomes de Almeida
    grupo_1 = [3, 6, 13, 20, 26, 28, 37]
    # Grupo 2
    #--------
    #  1: Ana Carolina de Jesus Pacheco da Silva
    #  7: Davi D'avila Versan
    # 12: João Guilherme de Jesus Salomão
    # 19: Lucas Matheus Nunes
    # 29: Paulo Henrique Ribeiro
    # 35: Tainá de Paiva Cortez
    # 38: Thiago Martins Volcati de Almeida
    grupo_2 = [1, 7, 12, 19, 29, 35, 38]
    # Grupo 3
    #--------
    #  2: André Eduardo Lobo de Paula"
    # 10: Gabriel Santos do Nascimento
    # 11: Iasmim Santos Silva de Jesus
    # 27: Nicolas Ramon da Silva
    # 30: Rafael Furtado Victor dos Santos
    # 36: Thalyta da Silva Viana
    # 41: Vinicius dos Reis Savian 
    grupo_3 = [2, 10, 11, 27, 30, 36, 41]
    # Grupo 4
    #--------
    # 14: Karine Victoria Rosa da Paixão
    # 18: Lucas Cozzolino Tort
    # 22: Marco Ruas Sales Peixoto
    # 31: Rafael Rocha Barbosa
    # 34: Ryan Botelho Gartlan
    # 39: Vinicius Gomes Ibiapina
    # 43: Yasmin Ingrid Silva Minário 
    grupo_4 = [14, 18, 22, 31, 34, 39, 43]
    # Grupo 5
    #--------
    #  5: Calebe Yan Veras Matias
    #  9: Fernando Tavares Bertholdo
    # 15: Kauan Massuia
    # 17: Larissa dos Santos Temoteo
    # 23: Marlos do Carmo Guedes
    # 24: Matheus Ribeiro dos Santos
    # 32: Renan Sabino dos Reis
    # 40: Vinicius Maciel Flor  
    grupo_5 = [5, 9, 15, 17, 23, 24, 32, 40]
    # Grupo 6
    #--------
    #  4: Arthur Bretas Oliveira
    #  8: Felipe Gutierres Zillo
    # 16: Kauã Rodrigues dos Santos
    # 21: Lucca Henrique Pereira
    # 25: Milena Aparecida Vieira Castro
    # 33: Rodrigo Hu Tchie Lee
    # 42: Vitor Margarido Balbo 
    grupo_6 = [4, 8, 16, 21, 25, 33, 42]

    id_avaliadores = grupo_6
    curso = 3
    turma = 13
    modulo = 5
    df = fetch_peer_ratings_df(início_dt, fim_dt, id_avaliadores, curso, turma, modulo)
    print(df)
    feedback_df = get_peer_feedback(df)
    print(feedback_df)
    feedback_df.to_csv("arquivo.csv", sep=";", index=False)
    df = compute_student_points_df(df)
    print(df)
    compute_student_index_df(df)
    print(df)