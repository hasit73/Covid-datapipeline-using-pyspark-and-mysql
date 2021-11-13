import pyspark
from pyspark.sql.types import *
import requests
import pandas as pd
import json
import pyspark.sql.functions as func
from io import StringIO
from pyspark import SparkConf,SparkContext,SQLContext,HiveContext
import json


## read config file
config = json.loads(open("covid-config.json").read())



## start spark master node
conf = SparkConf() \
    .setAppName(config['spark-app-name']) \
    .setMaster(config['master']) \
    .set("spark.driver.extraClassPath",config['mysql-connector-path'])\
    

sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)


spark = sqlContext.sparkSession
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
covid_data = None
print("Spark Master node is started ...")

def get_data_from_api(URL):
    global covid_data
    response = requests.get(URL)
    pandas_df = pd.read_csv(StringIO(response.text))
    covid_data = spark.createDataFrame(pandas_df)
    print("Covid Data loaded fetch from API")
    

def data_preprocessing():
    ## type cast string to datetime
    global covid_data

    covid_data = covid_data.withColumn("First_Dose",func.round(covid_data['First Dose Administered']).cast("integer"))
    covid_data = covid_data.withColumn("Second_Dose",func.round(covid_data['Second Dose Administered']).cast("integer"))
    covid_data = covid_data.withColumn("Total_Dose",func.round(covid_data['Total Doses Administered']).cast("integer"))
    covid_data = covid_data.withColumnRenamed("Vaccinated As of","Registeration_Date")
    covid_data = covid_data.select("Registeration_Date","State","First_Dose","Second_Dose","Total_Dose")
    
    ## remove nans values
    covid_data = covid_data.na.drop(how="any",subset=['First_Dose','State'])

    print("Data preprocessing completed")


def get_weekly_stats():
    """ This function compute weekly stats for input state"""

    data = []
    for state_data in covid_data.select('State').distinct().collect():
        state = state_data.State
        statedata = covid_data.where((covid_data['State']==state))
        info = statedata.tail(7)
        
        lower = info[0].First_Dose
        higher = info[-1].First_Dose
        weekly_incresed_first_dose = (higher-lower)/lower
        
        lower = info[0].Second_Dose
        higher = info[-1].Second_Dose
        weekly_incresed_second_dose = (higher-lower)/lower
        
        updated_date = info[-1].Registeration_Date
        fully_vaccinated = info[-1].Second_Dose 
        data.append([updated_date,state,weekly_incresed_first_dose*100,weekly_incresed_second_dose*100,fully_vaccinated])
    
    d1 = pd.DataFrame(data,columns = ['Date','State','Weekly_Gain_First','Weekly_Gain_Second','Fully_Vaccinated'])
    summerized_df = spark.createDataFrame(d1)
    print("Weekly insights are computed.. ")
    
    
    return summerized_df

def push_data_into_db(data):
    jdbcUrl = f"jdbc:mysql://localhost:3306/{config['db_data']['db_name']}"
    table_name = config['db_data']['db_table']
    user = config['db_data']['db_user']
    password = config['db_data']['db_password']

    data.write.format("jdbc")\
    .mode("overwrite")\
    .option("url", jdbcUrl) \
      .option("dbtable", table_name) \
      .option("user", user) \
      .option("password", password) \
      .save()

    print("Data successfully written into DB")
    



if __name__=="__main__":

    get_data_from_api(config['api-link'])
    data_preprocessing()
    data = get_weekly_stats()
    push_data_into_db(data)


