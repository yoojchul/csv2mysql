# CSV to Mysql

특정 디렉토리의 모든 csv 파일을 읽어 Mysql DB로 자동 변환시키는 파이썬 프로그램입니다. csv 파일이 위치한 디렉토리는 milvus의 collection과 Mysql의 database가 되고 csv 파일 이름은 table, csv header는 table의 column이 되도록 합니다.   <br>

* csv 파일 일부를 읽어  ollama 위에서 실행하는  exaone3.5:32b를 이용해서 요약.  csv 파일 특성인 숫자 검색을 강화하기 위하여 BGE-M3 embbeding를 사용하고 요약 내용은 milvus에 저장.  
* column 단위로 20줄을 읽어 ollama 위에서 실행하는 gpt-oss:20b에 프롬프트를 던져서 해당 column의 type를 결정
* resolve_token_type()를 통해 LLM의 답변을 정제 및 변환
* "LOAD DATA INFILE" 실행시 발생하는 1406, 1265, 1366 에러는 mysql의 column의 type를 자동 변경해서 해결


  <br>
sample인 seoul_transport directory는 /var/lib/mysql-files에 있어야 합니다. 

To upload csv files to Mysql
```
# python3 main.py   ; 
```
<br>

그리고 Mysql를 조회하는 프로그램은 아래 절차 대로입니다.

* table를 조회하는 유저 입력을 받아 milvus에 query하고 가장 적당한 table를 선택
* 유저 입력과 선택한 table로 부족한 table이 있는지 ollama(gpt-oss:20b)에게 문의하는 prompt를 작성
* 부족한 table이 있다는 ollama 결론이 나오면 milvus query를 ollama로부터 받아 milvus에 조회. 만족한 결론이 나올 때까지 반복 수행.
* table 리스트를 확보한 다음 mysql query를 통해 각 table의 field를 확인
* 조회하는 유저 입력과 field로 프롬프트를 작성해서 ollama(gpt-oss:20b)에 보내 mysql query문을 작성케하고 이를 실행하는 파이썬 프로그램을 작성하고 실행

```
# python3 serch.py
...
user query : 2024년5월과 6월  지하철 망포 총승차승객수는?

[RESULT] rows=2
{'month': '2024-05', 'total': Decimal('467064')}
{'month': '2024-06', 'total': Decimal('430759')}
```

## 환경
파이썬 3.12.3   <br>
torch 2.9.1<br>
flagEmbedding 1.3.5 <br>
pymilvus 2.6.6 <br>
ollama 0.6.1 <br>
mysql-connector-python 9.5.0 <br>
<br>
Milvus 2.6.4 <br>
Mysql 8.4.7    <br>
Ollama 0.13.5  <br> 
RTX 3090 
