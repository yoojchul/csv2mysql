import os
import pandas as pd
import ollama
import mysql.connector

import re

def resolve_token_type(input_str: str):
    """
    입력된 문자열의 토큰들을 분석하여 단일 대표 타입을 반환합니다.
    - 날짜형이 단독으로 쓰이면 해당 타입 유지, 섞이면 datetime 반환
    - 숫자는 double > float > int 우선순위 적용
    - varchar는 가장 긴 길이 적용
    """
    if not input_str:
        return None

    # 1. 정규표현식: 타입 이름 + 선택적 괄호
    token_pattern = re.compile(
        r'(int|double|float|decimal|varchar|text|date|datetime|time|timestamp)(?:\((.*?)\))?', 
        re.IGNORECASE
    )

    matches = list(token_pattern.finditer(input_str))
    
    # 2. 유효성 검사: 허용되지 않은 문자 포함 여부 확인
    last_end = 0
    parsed_tokens = []
    
    for m in matches:
        start, end = m.span()
        if input_str[last_end:start].strip() != "":
            return None # 토큰 사이에 이상한 문자가 섞임
        
        t_type = m.group(1).lower()
        t_param = m.group(2) if m.group(2) else ""
        parsed_tokens.append((t_type, t_param))
        last_end = end
        
    if input_str[last_end:].strip() != "":
        return None # 끝부분에 이상한 문자가 남음
        
    if not parsed_tokens:
        return None

    # 3. 그룹별 로직 처리
    type_set = set(t[0] for t in parsed_tokens)

    numeric_group = {'int', 'double', 'float', 'decimal'}
    varchar_group = {'varchar'}
    text_group = {'text'}
    datetime_group = {'date', 'datetime', 'time', 'timestamp'}

    # (1) 숫자형 (Numeric)
    if type_set.issubset(numeric_group):
        if 'double' in type_set:
            return 'double'
        if 'float' in type_set or 'decimal' in type_set:
            return 'float'
        return 'int'

    # (2) 가변 문자열 (Varchar)
    elif type_set.issubset(varchar_group):
        max_len = 0
        for _, param in parsed_tokens:
            if param.isdigit():
                max_len = max(max_len, int(param))
        return f"varchar({max_len})"

    # (3) 텍스트 (Text)
    elif type_set.issubset(text_group):
        return 'text'

    # (4) 날짜/시간 (Date/Time) - [수정된 부분]
    elif type_set.issubset(datetime_group):
        # A. Format 일치 여부 확인
        base_format = parsed_tokens[0][1]
        for _, param in parsed_tokens:
            if param != base_format:
                return None # 포맷 불일치
        
        # B. 타입 결정 로직 수정
        # 종류가 1개뿐이면(예: date만 3번) -> 해당 타입(date) 반환
        # 종류가 섞여있으면(예: date + time) -> datetime 반환
        if len(type_set) == 1:
            final_type = list(type_set)[0]
        else:
            final_type = 'datetime'

        # C. 결과 반환
        if base_format:
            return f"{final_type}({base_format})"
        else:
            return final_type

    # (5) 서로 다른 그룹 혼용 (예: int + varchar)
    else:
        return None


# --- Configuration ---
DIRECTORY_PATH = "/var/lib/mysql-files/seoul_transport"  # Path to your CSV folder
MYSQL_CONFIG = {
    'user': 'root',
    'password': 'Password',
    'host': '127.0.0.1',
    'allow_local_infile': True  # Required for LOAD DATA LOCAL
}

def get_optimal_types(df):
    """Sends 20 lines to Ollama to get comma-separated MySQL types."""
    columns = df.columns.tolist()
    results = []
    var_name = "@temp"
    fields = "("
    set_stm = ""
    for i in range(len(columns)):
        prompt = f"{df.iloc[:, i].to_string(header=False, index=False)}. \n  문자열들은 csv file의 한 열이다. {columns[i]}는 이들 문자열의 제목인데  Mysql로 변환할 때 적당한 타입만 표시하라.  제목에 연월일, 시간, date 등이 포함되면  타입으로 DATE, DATETIME, TIME등을 사용하라. date, datetime, time, timestamp를  선택할 때는 문자열들을 근거로 년,월, 일 순서를  파악하고 타입 다음에 시간 형식도 같이 출력하라.  연도월일 순서이면 (%Y%m%d), 일이 없으면 (%Y%m),  연도-월-일 순이면 (%Y-%m-%d)를 출력하고, 월-일-년도 순서이면 (%m-%d-%Y) 로 출력한다.  추가로 primary, field 이름, 설명이나 comment는 넣지 마라. VARCHAR type은 반드시 크기를 지정하라. TINYINT, SMALLINT, MEDIUMINT 대신에 INT를 사용하라. "

        response = ollama.generate(model="gpt-oss:20b", prompt=prompt, options={'temperature': 0})
        typ =  response['response'].strip().replace("\n", "").replace(" ", "")
        print(typ)
        ret = resolve_token_type(typ)
        if ret is None:
            results.append("TEXT") 
            fields += columns[i] + ','
        elif "DATE" in ret or "date" in ret or "TIME" in ret or "time" in ret:
            tm = ret.split("(")[0].strip()
            fmt = ret.split("(")[1].strip()
            fmt = "'"+fmt.replace(")", "'")
            if not "%d" in fmt or not "%Y" in fmt or not "%m" in fmt:   # for example, %Y%m or illegal form 
                results.append("VARCHAR(10)") # as string  
                fields += columns[i] + ','
            else:
                results.append(tm)
                temp_var = var_name + str(i)
                fields += temp_var + ','
                set_stm += f" {columns[i]} = STR_TO_DATE({temp_var}, {fmt}),"
        else:
            fields += columns[i] + ','
            results.append(ret)

    return results, fields[:-1]+")\n", set_stm[:-1]


def process_directory(directory):
    # Database name is the directory name
    db_name = os.path.basename(os.path.normpath(directory))
    
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        # 1. Create and Use Database
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        cursor.execute(f"USE `{db_name}`")
 
        cursor.execute("SET SESSION sql_mode = 'STRICT_ALL_TABLES'")
        
        for filename in os.listdir(directory):
            if filename.endswith(".csv"):
                file_path = os.path.abspath(os.path.join(directory, filename))
                table_name = os.path.splitext(filename)[0]
                
                # 2. Read first 20 lines for LLM
                df_sample = pd.read_csv(file_path, nrows=20, index_col=False)
                csv_text = df_sample.to_csv(index=False)
                
                # 3. Get Types from Ollama
                column_names = df_sample.columns.tolist()
                sql_types, fields, set_stm = get_optimal_types(df_sample)
                #sql_types = get_optimal_types(csv_text)
                print(f"LLM returns {sql_types}")
                
                # Validation: ensure LLM returned enough types for the columns
                if len(sql_types) != len(column_names):
                    # Fallback to VARCHAR if LLM response length mismatches
                    sql_types = ["TEXT"] * len(column_names)

                # 4. Create Table Script
                col_definitions = [f"`{name}` {dtype}" for name, dtype in zip(column_names, sql_types)]
                create_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(col_definitions)});"
                
                cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
                cursor.execute(create_query)
                
                # 5. Load Data via LOAD DATA LOCAL INFILE
                # Note: replace backslashes for Windows compatibility in SQL string
                formatted_path = file_path.replace('\\', '/')
                load_query = f"""
                LOAD DATA INFILE '{formatted_path}'
                INTO TABLE `{table_name}`
                FIELDS TERMINATED BY ','
                ENCLOSED BY '\"'
                LINES TERMINATED BY '\\n'
                IGNORE 1 ROWS
                {fields}
                SET {set_stm};
                """
                print(load_query)            
                attempt = 1
                while True:
                    try:
                        print(f"[{attempt}차 시도] 데이터 로딩 시작...")
                        cursor.execute(load_query)
                        conn.commit()
                        print("✅ 데이터 로딩 성공!")
                        break

                    except mysql.connector.Error as err:
                        # 1406 에러: Data too long for column 'column_name'
                        if err.errno == 1406:
                            error_msg = str(err)
                            print(f"❌ 에러 발생: {error_msg}")
            
                            # 에러 메시지에서 컬럼명 추출 (예: Data too long for column 'email' at row 1)
                            match = re.search(r"column '(.+?)'", error_msg)
                            if match:
                                col_name = match.group(1)
                             
                                # 1. 현재 VARCHAR 크기 확인
                                cursor.execute(f"""
                                    SELECT CHARACTER_MAXIMUM_LENGTH 
                                    FROM information_schema.COLUMNS 
                                    WHERE  
                                    TABLE_NAME = '{table_name}' 
                                    AND COLUMN_NAME = '{col_name}'
                                """)
                                current_size = cursor.fetchone()[0]
                             
                                if current_size is None:
                                    print("사이즈를 확인할 수 없는 컬럼 타입입니다.")
                                    break
            
                                # 2. 크기를 2배로 확장
                                new_size = current_size * 2
                                print(f"🔧 컬럼 '{col_name}' 크기 변경: {current_size} -> {new_size}")
                             
                                alter_query = f"ALTER TABLE `{table_name}` MODIFY {col_name} VARCHAR({new_size})"
                                cursor.execute(alter_query)
                    
                                attempt += 1
                                continue # 루프 재시작 (재시도)
                            else:
                                print("컬럼명을 추출하지 못했습니다.")
                                raise
                        elif err.errno == 1265:   # Data truncated for column 'column_name'
                            error_msg = str(err)
                            print(f"❌ 에러 발생: {error_msg}")

                            # 에러 메시지에서 컬럼명 추출 (예: Data truncated for column 'email' at row 428)
                            match = re.search(r"column '(.+?)'", error_msg)
                            if match:
                                col_name = match.group(1)
                                # error where usually character data is given to int type, for example, 100A
                                print(f"🔧 컬럼 '{col_name}' type change:  int -> varchar(10)")

                                alter_query = f"ALTER TABLE `{table_name}` MODIFY {col_name} VARCHAR(10)"
                                cursor.execute(alter_query)
                    
                                attempt += 1
                                continue # 루프 재시작 (재시도)
                        elif err.errno == 1366:   # Incorrect integer value: 'N61' for column 'column_name'
                            error_msg = str(err)
                            print(f"❌ 에러 발생: {error_msg}")

                            # 에러 메시지에서 컬럼명 추출 (예: Incorrect integer value: 'N61' for column '노선번호'
                            match = re.search(r"column '(.+?)'", error_msg)
                            if match:
                                col_name = match.group(1)
                                # for example, N61
                                print(f"🔧 컬럼 '{col_name}' type change:  int -> varchar(10)")

                                alter_query = f"ALTER TABLE `{table_name}` MODIFY {col_name} VARCHAR(10)"
                                cursor.execute(alter_query)
                    
                                attempt += 1
                                continue # 루프 재시작 (재시도)
                        else:
                            print(f"기타 MySQL 에러: {err}")
                            raise
                # end of while loop
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    process_directory(DIRECTORY_PATH)
