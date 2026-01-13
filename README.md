# CSV to Mysql

csv 파일을 읽어 Mysql table로 변환시키는 파이썬 프로그램입니다. csv 파일이 위치한 디렉토리는 Mysql의 database가 되고 csv 파일 이름은 table, csv header는 table의 column이 되도록 합니다.   아래 프롬프트를 제미나이에게 보내는데 프로그램을 구성하게 했습니다. 

'''
Build a python code to execute sql command for mysql with conditions.  

1) A directory is given and it has many csv files.  

2) The name of directory becomes the name of database on mysql. 

3) A table of mysql is made with one csv file. The name of table is the same of the file name except ".csv".  ".csv" should be removed from the table name.  

4) The script commands create tables where the column of the file becomes the field name and fields are as many as columns.  

5) To optimize type of fields, compose the following prompt with the first 20 lines of each file and send it to "exaone3.5:32b" on ollama.
"This is a part of csv file. List only optimal types of fields for mysql table.  They are separated by comma in normal text, not in json. Don't mention primary, field name, explanation and comment. And VARCHAR type should be with size.". 

6) The script must include loading data using “LOAD DATA LOCAL INFILE”.
'''
