from selenium import webdriver
from selenium.webdriver.common.by import By
import psycopg2
from psycopg2 import OperationalError
import time
import pandas as pd
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
import pendulum
from datetime import timedelta

local_tz = pendulum.timezone('Asia/Kathmandu')


def Collect_data():
    try:
        driver = webdriver.Chrome()
        driver.get("https://www.nepalipaisa.com/company/NABIL")
        time.sleep(2)

        price_history_button = driver.find_element(By.XPATH, "//a[@data-tab='price-history' and @href='#c-price']")
        driver.execute_script("arguments[0].scrollIntoView();", price_history_button)
        driver.execute_script("arguments[0].click();", price_history_button)

        data_from = driver.find_element(By.XPATH, "//input[@id='txtFromDate']")
        driver.execute_script("arguments[0].scrollIntoView();", data_from)
        time.sleep(1)
        data_from.clear()
        data_from.send_keys('2010-01-01')
        search_button = driver.find_element(By.XPATH, "//button[@id='btnSearch']")
        driver.execute_script("arguments[0].click();", search_button)
        time.sleep(0.1)

        no_of_pages = 306
        final_data = []
        for i in range(no_of_pages):
            try:
                next_button = driver.find_element(By.XPATH, "//div[@id='divPricePager']//ul//li[contains(@class,'next')]/a")
                table_rows = driver.find_elements(By.XPATH, "//table[@id='tblPriceHistory']//tbody//tr")
                for row in table_rows:
                    try:
                        data = {}
                        data_ = row.text.split()
                        data['S.N.'] = data_[0]
                        data['Date'] = data_[1]
                        data['Txns'] = data_[2].replace(',', '')
                        data['High'] = data_[3].replace(',', '')
                        data['Low'] = data_[4].replace(',', '')
                        data['Close'] = data_[5].replace(',', '')
                        data['Volume'] = data_[6].replace(',', '')
                        data['Turnover'] = data_[7].replace(',', '')
                        data['Previuos Close'] = data_[8].replace(',', '')
                        data['change in Rs.'] = data_[9].replace(',', '')
                        data['Percent Change'] = data_[10].replace(',', '')
                        final_data.append(data)
                    except Exception as e:
                        print(f"Error occurred while processing row: {e}")
                driver.execute_script("arguments[0].click();", next_button)
                time.sleep(0.7)
            except Exception as e:
                print(f"Error occurred while navigating pages: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        driver.quit()

    if final_data:
        df = pd.DataFrame(final_data)
        df.to_csv("Nabil.csv", index=False)
        print(f"Data Collection Successful. Total {len(final_data)} data Collected")
    else:
        print("No data was collected.")


def Clean_data():
    #read and preprocess data
    df = pd.read_csv("Nabil.csv")
    df["Date"]=pd.to_datetime(df["Date"])
    #drop duplicates
    df.drop_duplicates()
    #if there is any null value remove it
    if df.isnull().values.any():
        print("DataFrame contains null values.")
        df.dropna(inplace=True)
        print("Null values removed from the data")
    else:
        print("DataFrame does not contain any null values.")

    #select only relevant data and remove unnecessary column
    features = ['Date','High','Low','Close','Volume','Percent Change']
    cleaned_data = df[features]
    # sort the data by date in ascending order
    cleaned_data.sort_values('Date', inplace=True)
    cleaned_data.to_csv("Nabil_cleaned.csv",index=True)
    print("Data Cleaning Successful.")
    print(cleaned_data.head())
    print(len(cleaned_data))

host = 'localhost'
dbname = 'stock_data'
user = 'bibek'
password = 'bibek'

def load_data():
    try:
        conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password)
        conn.autocommit = True
        cursor = conn.cursor()
        table_name = 'Nabil_Bank'
        drop_table_query = f'''DROP TABLE IF EXISTS {table_name}'''
        create_table_query = f'''CREATE TABLE {table_name}
            (date DATE,
             high DECIMAL,
             low DECIMAL,
             close DECIMAL,
             volume INT,
             percent_change DECIMAL)'''
        cursor.execute(drop_table_query)
        cursor.execute(create_table_query)
        df = pd.read_csv('Nabil_cleaned.csv')

        for index, row in df.iterrows():
            cursor.execute(f"INSERT INTO {table_name} (date, high, low, close, volume, percent_change) VALUES(%s, %s, %s, %s, %s, %s)",
                           (row['Date'],row['High'], row['Low'], row['Close'], row['Volume'], row['Percent Change']))

        print("Data inserted successfully.")

    except OperationalError as e:
        print(f"The error '{e}' occurred.")

    finally:
        if conn:
            cursor.close()
            conn.close()


default_args = {
    'retries':1,
    'retry_delay':timedelta(minutes=2),
    'start_date':local_tz.datetime(2023,10,18)
}

dag = DAG(
    "ETL_DAG",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

collect_data = PythonOperator(
    task_id = 'collect_data_from_web',
    python_callable=Collect_data,
    dag = dag
)

clean_data = PythonOperator(
    task_id = 'data_cleaning',
    python_callable=Clean_data,
    dag = dag
)

data_load= PythonOperator(
    task_id = 'Load_data',
    python_callable=load_data,
    dag = dag
)

collect_data>>clean_data>>data_load

