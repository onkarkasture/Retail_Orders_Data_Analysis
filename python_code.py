import pandas as pd
import sqlalchemy as sal #importing required libs and modules

df = pd.read_csv('orders.csv', na_values=['Not Available','unknown']) # Read data with pandas and treat given values as NAN/NULL

df.columns = df.columns.str.lower()
df.columns = df.columns.str.replace(' ', '_') # Making the columns names code friendly

df['discount'] =  df['list_price']*df['discount_percent']/100
df['sale_price'] = df['list_price'] - df['discount']
df['profit'] = round(df['sale_price'] -  df['cost_price'],2) # Adding new columns for better insights

df.drop(columns = ['list_price', 'cost_price', 'discount_percent'], inplace = True) # Removing unnecessary columns

df['order_date'] = pd.to_datetime(df['order_date'], format = '%Y-%m-%d') # Converting object datatype of date to datetime datatype

engine = sal.create_engine("postgresql://postgres:162045@localhost/global_mart") 
conn = engine.connect() # Connecting the SQL database

df.to_sql('orders', con = conn, index = False, if_exists= 'replace') # Adding the changes made onto the database through SQL.