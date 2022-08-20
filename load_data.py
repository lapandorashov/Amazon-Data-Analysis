import requests
import pandas as pd
from mysql.connector import Error
import sqlalchemy
import pyodbc
import csv
import argparse
import sys

def main():
    # PARAMETERS
    # MY SQL Server Confidentials
    host='msba-bootcamp-prod.cneftpdd0l3q.us-east-1.rds.amazonaws.com'
    user='HSHOU3'
    password = open("mysql_pass.txt","r").readline().strip()

    # databases
    Database = "MSBA_Team7"
    table = "try_and_error"
    final_table = "reviews_raw"
    create_table = f'''
                    CREATE TABLE IF NOT EXISTS {table} (
                        marketplace varchar(256),
                        customer_id INT,
                        review_id varchar(256),
                        product_id varchar(256),
                        product_parent INT,
                        product_title varchar(256),
                        product_category varchar(256),
                        star_rating INT,
                        helpful_votes INT,
                        total_votes INT,
                        vine varchar(256),
                        verified_purchase varchar(256),
                        review_headline varchar(1000),
                        review_body varchar(1000),
                        review_date DATE)
    '''
    insert_function = f'''
                INSERT INTO {table}
                (marketplace, customer_id, review_id,
                product_id,product_parent,product_title,product_category,
                star_rating,helpful_votes,total_votes,vine,verified_purchase,
                review_headline,review_body,review_date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                '''

    possible_categories = {'Wireless','Watches','Video_Games','Video_DVD',
                'Video','Toys','Tools','Sports','Software','Shoes','Pet_Products',
                'Personal_Care_Appliances','PC','Outdoors','Office_Products',
                'Musical_Instruments','Music','Mobile_Electronics','Mobile_Apps',
                'Major_Appliances','Luggage','Lawn_and_Garden','Kitchen',
                'Jewelry','Home_Improvement','Home_Entertainment','Home',
                'Health_Personal_Care','Grocery','Gift_Card','Furniture','Electronics',
                'Digital_Video_Games','Digital_Video_Download','Digital_Software',
                'Digital_Music_Purchase','Digital_Ebook_Purchase','Digital_Ebook_Purchase',
                'Camera','Books','Beauty','Baby','Automotive','Apparel'}

    #Arguments
    parser = argparse.ArgumentParser(description='Return data for the given product category.')
    parser.add_argument('-c', '--category',
                    help='Input category name like "Firstword_Secondword_Thirdword" to get data')
    args = parser.parse_args()

    #filter out the non-existing product categories
    if args.category:
        if args.category not in possible_categories:
            sys.exit("Please input a valid product category or check your input format.")
        else:
            category = args.category

    #url and file names
    url = f"https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_{category}_v1_00.tsv.gz"
    filename = f"{category}_reviews.tsv.gz"
    # filename = "gift_reviews_copy.csv"  #TESTING




    #Starting the Process
    print("\nStep 1: Downloading the data...")
    download(filename,url)

    #Read the data & Preview
    print("\nStep 2: Reading the data...")
    data = read_data(filename)

    #Use pyodbc to connect to SQL Server
    print(f"\nStep 3: Connecting to {Database} Database...")
    try:
        params = 'DRIVER={MySQL ODBC 8.0 Unicode Driver};SERVER='+host +';PORT=3306;DATABASE=' + Database + ';UID=' + user + ';PWD=' + password
        conn = pyodbc.connect(params)
        cursor = conn.cursor()
        cursor.execute(f"USE {Database}")
        print("Success!")
    except Error as e:
        if e.__class__ == pyodbc.ProgrammingError:
            conn == reinit()
            cursor = conn.cursor()

    #Delete and recreate the table if it exists
    print("\nStep 4: creating a temporary table...")
    delete(cursor,table)
    create(cursor,create_table)
    print("Success!")

    #Upload data to MySQL Server
    print("\nStep 5: upload the data to temporary table...")
    try:
        engine = sqlalchemy.create_engine(f"mysql://{user}:{password}@{host}:3306/{Database}")
        data.to_sql(table,engine,index=False,if_exists="append")
        print("Success!")

        #Transfer everything from temporary table to final table
        print("\nStep 6: transferring everything to final table...")
        delete(engine,final_table)
        engine.execute(f"CREATE TABLE {final_table} LIKE {table}")
        engine.execute(f"INSERT INTO {final_table} SELECT * FROM {table}")
        print(pd.DataFrame(engine.execute(f"SELECT * FROM {final_table}").fetchall()))
        print("\nCongratulations! You have successfully uploaded the data to SQL!")
    except Error as e:
        print("Error occurred during data uploading:",e)

    #delete temporary table
    delete(cursor,table)
    print("Temporary table deleted.")


#download the tsv.gz file
def download(filename,url):
    try:
        with open(filename, "wb") as f:
            r = requests.get(url)
            f.write(r.content)
        print("Success!")
    except error as e:
        print("Problem occurred during downloading data:",e)


#reading the data
def read_data(filename):
    try:
        data = pd.read_csv(filename,sep='\t', compression='gzip')
        #data = pd.read_csv(filename)   # TESTING
        #convert NaN data to None values
        data = data.where((pd.notnull(data)), None)
        #data = data.iloc[: , 1:]  #TESING: use if csv have 1 column of index
        print("Success!")
        return data
    except error as e:
        print("\nProblem occurred during downloading data:",e)

#delete and recreate a new table for testing the code
def delete(cursor,table):
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    except Error as e:
        print("error in deleting the table.")

#delete and recreate a new table for testing the code
def create(cursor,create_table):
    try:
        cursor.execute(create_table)
    except Error as e:
        print("error in creating new table.")


if __name__ == "__main__":main()
