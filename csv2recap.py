import os
import pandas as pd
import ollama
import mysql.connector
from mysql.connector import errorcode
from transformers import AutoTokenizer, AutoModel
import torch
import random
import csv
from pymilvus import  (
        connections, FieldSchema, CollectionSchema, DataType, 
        Collection, utility, AnnSearchRequest, WeightedRanker
)
from FlagEmbedding import BGEM3FlagModel
import numpy as np

# --- Configuration ---
MILVUS_HOST = "localhost"
MILVUS_PORT = "19530"


model = BGEM3FlagModel('BAAI/bge-m3', use_fp16=True)

def generate_embeddings(texts):
    # return_dense=True, return_sparse=True, return_colbert_vecs=False
    output = model.encode(texts, return_dense=True, return_sparse=True)
    
    dense_vectors = output['dense_vecs'].astype(np.float32)
    
    # Milvus용 Sparse 포맷 변환: {단어ID: 가중치} 형태의 Dictionary
    # BGE-M3의 sparse output은 이미 {id: weight} 형태입니다.
    sparse_vectors = output['lexical_weights'] 
    
    return dense_vectors, sparse_vectors

def setup_milvus(collection_name):
    if utility.has_collection(collection_name):
        return Collection(collection_name)
    
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="filename", dtype=DataType.VARCHAR, max_length=256),

        # [Dense Vector] 의미 검색용 (BGE-M3는 1024차원)
        FieldSchema(name="dense_vector", dtype=DataType.FLOAT_VECTOR, dim=1024),

        # [Sparse Vector] 키워드 검색용 (SPARSE_FLOAT_VECTOR 타입 사용)
        FieldSchema(name="sparse_vector", dtype=DataType.SPARSE_FLOAT_VECTOR),

        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=2000),
    ]
    schema = CollectionSchema(fields, "File description embeddings")
    return Collection(collection_name, schema)


def read_csv_smart(file_path, encoding='utf-8'):
    """
    조건에 따라 CSV 파일을 다르게 읽어들이는 함수
    
    Args:
        file_path (str): CSV 파일 경로
        encoding (str): 파일 인코딩 (기본: utf-8)
        
    Returns:
        pd.DataFrame: 조건에 맞춰 로드된 데이터프레임
    """
    
    # 1. 파일의 메타데이터(컬럼 수, 전체 행 수) 파악
    with open(file_path, 'r', encoding=encoding) as f:
        # csv reader로 헤더 파싱하여 컬럼 수 확인
        reader = csv.reader(f)
        try:
            header = next(reader)
            col_count = len(header)
        except StopIteration:
            # 빈 파일인 경우
            return pd.DataFrame()

        # 남은 줄 수를 세어 데이터 행 수 확인 (헤더 제외)
        row_count = sum(1 for row in f)

    # 2. 조건에 따른 임계값(Threshold) 설정
    # limit_threshold: 해당 줄 수 이상이면 샘플링을 시작하는 기준이자, 최대 읽기 허용 줄 수
    if col_count <= 10:
        limit_threshold = 100
    elif 10 < col_count <= 30:
        limit_threshold = 30
    else:  # col_count > 30
        limit_threshold = 10

    # 3. 읽기 전략 결정 및 실행
    if row_count <= limit_threshold:
        # 기준 줄 수 이하이면 모두 읽기
        return pd.read_csv(file_path, encoding=encoding)
    else:
        # 기준 줄 수 초과이면 1% Random Sampling (단, limit_threshold를 넘지 않음)
        
        # 1% 계산 (최소 1줄은 읽도록 설정)
        target_sample_size = int(row_count * 0.01)
        if target_sample_size < 1:
            target_sample_size = 1
            
        # 최종 읽을 줄 수는 1%와 limit_threshold 중 작은 값
        final_count = min(target_sample_size, limit_threshold)
        
        # 전체 데이터 인덱스(1 ~ row_count) 중에서 무작위로 final_count 개 선택
        # 0번 인덱스는 헤더이므로 제외하고 1부터 시작
        indices_to_keep = set(random.sample(range(1, row_count + 1), final_count))
        
        # skiprows에 적용할 함수 정의
        # x가 0(헤더)이거나, 선택된 인덱스(indices_to_keep)에 포함되면 건너뛰지 않음(False)
        # 그 외에는 건너뜀(True)
        def skip_logic(x):
            if x == 0: 
                return False # 헤더는 유지
            return x not in indices_to_keep # 선택되지 않은 행은 스킵

        return pd.read_csv(file_path, skiprows=skip_logic, encoding=encoding)


def recap_csv_files(directory):
    # dbname is the directory name
    milvus_db_name = os.path.basename(os.path.normpath(directory))
    # ---  Milvus Setup ---
    connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT)
    milvus_col = setup_milvus(milvus_db_name)

    # - --  Processing Files for MILVUS --
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)
            table_name = filename.replace(".csv", "")
            print(f"{table_name} ============================ ")

            # Read first 20 lines
            df_sample = read_csv_smart(file_path)
            #dF_sample = pd.read_csv(file_path, nrows=20)
            csv_snippet = df_sample.to_string()

            # Request 1: Analysis & Embedding

            prompt1 = f"""{csv_snippet}\n\n {table_name} 이름으로된 csv의 일부이다.
                    파일은 무엇을 담고 있는지 100자 내외로 설명하라.
                    파일 이름에 date를 의미하는 부분이 포함될 수 있으니
                    csv 파일 내용과 결부해서 date를 년월일을 구분해서 표기하라.
                    모든 열의 헤더만 설명없이 나열하라. """
            response1 = ollama.generate(model="exaone3.5:32b", prompt=prompt1)['response']
            print(f"{response1}")

            # Vectorize and Insert to Milvus
            dense_vecs, sparse_vecs = generate_embeddings([response1])
            entities = [
                [table_name],
                dense_vecs,
                sparse_vecs,
                [response1]
            ]
            milvus_col.insert(entities)

    milvus_col.flush()
    milvus_col.create_index("dense_vector",
            {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128}})
    milvus_col.create_index("sparse_vector",
            {"index_type": "SPARSE_INVERTED_INDEX", "metric_type": "IP", "params": {"drop_ratio_build": 0.2}})
    milvus_col.load()

