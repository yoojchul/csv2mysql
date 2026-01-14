# CSV to Mysql

특정 디렉토리의 모든 csv 파일을 읽어 Mysql table로 자동 변환시키는 파이썬 프로그램입니다. csv 파일이 위치한 디렉토리는 Mysql의 database가 되고 csv 파일 이름은 table, csv header는 table의 column이 되도록 합니다.   <br>


* column 단위로 20줄을 읽어 ollama 위에서 실행하는 gpt-oss:20b에 프롬프트를 던져서 해당 column의 type를 결정
* resolve_token_type()를 통해 LLM의 답변을 정제
* "LOAD DATA INFILE" 실행시 발생하는 1406, 1265, 1366 에러는 mysql의 column의 type를 변경해서 자동 해결


  <br>
sameple인 seoul_transport directory는 /var/lib/mysql-files에 있어야 합니다. 

## 환경
파이썬 3.12.3   <br>
Mysql 8.4.7    <br>
ollama 0.13.5  <br> 
Gemini 3 Pro    <br>
RTX 3090 
