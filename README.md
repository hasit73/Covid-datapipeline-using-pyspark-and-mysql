# Covid-datapipeline-using-pyspark-and-mysql
Created covid data pipeline using PySpark and MySQL that collected data stream from API and do some processing and store it into MYSQL database.

### Tools used :  PySpark , MySQL 

### Procedure 

1) Fetch latest data from API using requests & pandas module of python.

2) Apply some data processing and filtering to generate summarized information.

3) Store that summarized information into database using MySQL.

To build above pipeline i had used pyspark


### How to use

1) clone **Covid-datapipeline-using-pyspark-and-mysql** repo.

2) start MySQL server 

3) execute following command

```
  python main.py
```

### Results:

command line output:
![image](https://user-images.githubusercontent.com/69752829/141651642-defb93e2-4a62-42f2-bbb9-ea0919be8618.png)

Database status after execution:
![image](https://user-images.githubusercontent.com/69752829/141654147-c99e0785-367e-4a80-8a81-0f01f4089da0.png)



