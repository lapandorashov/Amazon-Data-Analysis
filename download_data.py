from importlib import metadata
from multiprocessing.sharedctypes import Array
import sqlalchemy as db
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://insert_username:insert_password@msba-bootcamp-prod.cneftpdd0l3q.us-east-1.rds.amazonaws.com/MSBA_Team7")
cnxn = engine.connect()
print(engine.execute("SELECT * FROM reviews_raw").fetchall())
metadata = db.MetaData()
reviews_raw = db.Table('reviews_raw', metadata, autoload=True, autoload_with=engine)

df = pd.DataFrame(reviews_raw)
print('DataFrame:\n',df)

csv_data = df.to_csv()
print('n\CSV String:\n', csv_data)

#organizing into a dataframe using PANDAS
#columns = ['customer_id', 'review_id', 'product_id', 'product_parent', 'product_title', 'product_category', 'star_rating', 'helpful_votes', 'total_votes', 'vine', 'verified_purchase', 'review_headline', 'review_body', 'review_date', 'y_year']
