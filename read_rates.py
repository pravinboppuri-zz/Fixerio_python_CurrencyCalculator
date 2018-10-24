import json
import sys
import pandas as pd
from pandas.io import sql
import requests
import sqlite3

bases =['USD','EUR']




def currency_rates():
    try:
        for base in bases:
            # For now only considered API Call to get latest dates as the timeseries API is restricted to my account.
            url = 'http://data.fixer.io/api/latest?access_key=70642aa874dd1f11a014f73ef421944c&base=' + base
            response = requests.get(url)

        if response.status_code != 200:
            print('N/A')
            return response
        else:
            # Response is flattened using pandas framework and dataframe is created

            read_json = response.json()
            data = json.dumps(read_json)
            df = pd.read_json(data)
            dataset1 = df["rates"]
            s = pd.Series(dataset1, name='rates')
            s.index.name = 'currency_code'
            sd = s.reset_index()

            cols1 = ["currency_code", "rates"]
            dataset_1 = pd.DataFrame(sd, columns=cols1)
            dataset2 = pd.DataFrame(df)
            dataset3 = pd.merge(dataset_1, dataset2)
            rows = dataset3.drop(['success', 'timestamp'], axis=1)
#             print(rows)
            return rows


    except requests.ConnectionError as error:
        print (error)
    sys.exit(1)


def create_connection(dbfile):
    try:
        conn = sqlite3.connect(dbfile)
        return conn
    except ConnectionError as e:
        print(e)

    return None


def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except ConnectionError as e:
        print(e)


def insert_into_db(dbfile):

    rates = currency_rates()

    try:

        con = create_connection(dbfile)
        cur = con.cursor()
        rates.to_sql('currency_rates', con, if_exists='append', index=False)
        cur.close()

    except ConnectionError as e:
        print(e)


def find_rates(xdate):
    database = "/home/bella/rates.db"
    try:
        con = sqlite3.connect(database)
        # con.row_factory = sqlite3.Row
        cur =con.cursor()

        sql = "select * from currency_rates where date(date)='{}'".format(xdate)
#
        cur.execute(sql)
        rows =cur.fetchall()
        return rows
    except ConnectionError as e:
        print(e)

def get_avg(currency,sdate,edate):

    database = "/home/bella/rates.db"
    try:
        con = sqlite3.connect(database)

        param = "'" + currency + "' and date(date) between '" + sdate + "' and '" + edate + "' "
        sql2 = "select  currency_code,round(avg(rates),3) as avg_rate from currency_rates where currency_code= "+param
        cur = con.cursor()
        cur.execute(sql2)
        rows2 =cur.fetchall()
        return rows2
        # print (rows)
    except ConnectionError as e:
        print(e)


def main():
    database = "/home/bella/rates.db"
    conn = create_connection(database)

    create_table_rates = '''CREATE TABLE IF NOT EXISTS currency_rates(
                             currency_code varchar(5),
                             rates float,
                             base varchar(5),
                             date date )'''

    if conn is not None:

        create_table(conn, create_table_rates)
        insert_into_db(database)

    else:
        print("Error! cannot create the database connection.")


if __name__=='__main__':

    currency_rates()
    main()
    rates = input('PLease Enter the Date you want to find Rates for in format YYYY-MM-DD :')
    cur = input("enter Currency Code in CAPITALS:")
    stdate=input("enter Start date in format YY-MM-DD :")
    edate =input("enter End date in format YY-MM-DD:")
    # Avg =input('PLease Enter the currency & Date range you want to find the average Rates for in format YYYY-MM-DD :')
    print (find_rates(str(rates)))
    print ("-----------Average Currency Rates Below--------------------")
    print ("Average Currency Rates : ",get_avg(cur,stdate,edate))


