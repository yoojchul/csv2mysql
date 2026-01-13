# CSV to Mysql

csv 파일을 읽어 Mysql table로 변환시키는 파이썬 프로그램입니다. csv 파일이 위치한 디렉토리는 Mysql의 database가 되고 csv 파일 이름은 table, csv header는 table의 column이 되도록 합니다.   <br>

Mysql의 table 구성시에 column의 type를 지정해야 하는데 type를 LLM(ollama 기반의 gpt-oss:20b, deepseek-r1:32b, exaone3.5:32b 등)이 결정하도록 합니다.

  <br>
sameple인 seoul_transport directory는 /var/lib/mysql-files에 있어야 합니다. 

## 환경
파이썬 3.12.3   <br>
Mysql 8.4.7    <br>
ollama 0.13.5  <br> 
Gemini 3 Pro    <br>
RTX 3090 
