# CSV to Mysql

csv 파일을 읽어 Mysql table로 변환시키는 파이썬 프로그램입니다. csv 파일이 위치한 디렉토리는 Mysql의 database가 되고 csv 파일 이름은 table, csv header는 table의 column이 되도록 합니다.   아래 프롬프트를 제미나이에게 보내는데 프로그램을 구성하게 했습니다. 

```
Build a python code to execute sql command for mysql with conditions.  

1) A directory is given and it has many csv files.  

2) The name of directory becomes the name of database on mysql. 

3) A table of mysql is made with one csv file. The name of table is the same of the file name except ".csv".   <br>
 ".csv" should be removed from the table name.  

4) The script commands create tables where the column of the file becomes the field name and fields are as many as columns.  

5) To optimize type of fields, compose the following prompt with the first 20 lines of each file and send it to "exaone3.5:32b" on ollama.
"This is a part of csv file. List only optimal types of fields for mysql table.  They are separated by comma in normal text, not in json. Don't mention primary, field name, explanation and comment. And VARCHAR type should be with size.".

6) The script must include loading data using “LOAD DATA LOCAL INFILE”.
```

Mysql의 table 구성시에 column의 type를 지정해야 하는데 type를 LLM(ollama 기반의 gpt-oss:20b, deepseek-r1:32b, exaone3.5:32b 등)이 결정하도록 합니다.

변경 1) column의 갯수가 50개가 넘을 때 LLM이 주는 type의 갯수가 column보다 작음.    <br>
=> 매 column 단위로 프롬프트를 작성해서 LLM에 보내고 type를 결정하도록 함.

변경 2) LLM이 돌려 주는 type이 "INT"가 아니라 "INTINTINTINT..." 로 오거나 엉뚱한 답변이 오는 경우가 있음   <br>
=> 제마나이에게 아래 프롬프트를 보내서 결과를 파싱하는 함수를 작성케함.
```
"string 입력을 받아서 int, double, float, decimal([M], [D]), varchar([num]), text, date([format]), datetime([format]), time([format]), timestamp([format]) 중에 하나이면 해당 token를  return하고 아니면 None를 return하는  python function을 구성하라.
이중에 하나 token이 반복적으로 나오는 것도 허용하지만 섞인 것은 허용하지 않는다.
varchar는 길이가 달라도 같은 토큰으로 보고 긴 숫자를 포함한 것은 return한다
date datetime time timestamp가 섞어서 나오면 datetime으로 return할 수 있도록 하나 format는 일치해야 한다
decimal는 float로 간주하고 {int, float, double}이 섞이면 우선 순위는 double > float > int 순서이다."
```

변경 3) MySQL LOAD DATA 실행시에 type에 맞지 않는 데이터가 있어 에러가 발생.    <br>
=> 제마나이에게 아래 프롬프트를 보내서 exception을 대처하는 루틴을 작성케함
```
"MySQL LOAD DATA로 실행시 ER_DATA_TOO_LONG 에러만 받아서 size가 부족한 field를 확인하고 해당 field의 varchar의 크기를 확인한다. 이를 두배로 키워서 alter table로 변경한 다음 다시 LOAD DATA를 실행. 또 같은 문제가 있으면 같은 동작을 반복하도록 파이썬 프로그램을 작성하라"
```
  <br>
sameple인 seoul_transport directory는 /var/lib/mysql-files에 있어야 합니다. 

## 환경
파이썬 3.12.3   <br>
Mysql 8.4.7    <br>
ollama 0.13.5  <br> 
Gemini 3 Pro    <br>
RTX 3090 
