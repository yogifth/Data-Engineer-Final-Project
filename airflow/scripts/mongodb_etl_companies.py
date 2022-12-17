from pymongo import MongoClient
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


# connect to mongodb 
client = MongoClient("mongodb+srv://admin:admin@cluster0.etjsmyx.mongodb.net/test")

# Get the "sample_training" database and read "companies" collection a.k.a table
db = client["sample_training"]
companies = db["companies"]

# Exclude nested columns
exc_cols = [
    'offices','image', 'products', 'relationships', 'competitions', 'providerships', 
    'funding_rounds', 'investments','acquisition','acquisitions','milestones','video_embeds',
    'screenshots','external_links','partners', 'ipo'
]

# Initiate pymongo cursor
cursor_comp = companies.aggregate([
    {"$addFields": {
        "office": {"$first": "$offices"}
        }},
        {"$unset" : exc_cols}], allowDiskUse=True)

# Load data into dataframe
companies_df = pd.DataFrame.from_dict(list(cursor_comp))

# Fill empty office value
empty_comp = {
    'description': '',
    'address1': '',
    'address2': '',
    'zip_code': '',
    'city': '',
    'state_code': '',
    'country_code': '',
    'latitude': None,
    'longitude': None
}

companies_df['office'] = np.where(companies_df['office'].notna(), companies_df['office'], empty_comp)

# Flatten office columns
office = pd.DataFrame(companies_df['office'].tolist())
companies_df = pd.concat([companies_df, office], axis=1)

# Drop office column 
companies_df.drop(['office'], axis=1, inplace=True)
companies_df.rename(columns={'_id': 'id'}, inplace=True)

# Change dtypes ObjectId in column 'id' to string
companies_df['id'] = companies_df['id'].astype(str)

# Slice value 'id' to the last 5 digit
companies_df['id'] = companies_df['id'].str.strip().str[-5:]
print(companies_df.tail())

# Import DataFrame to Postgres
engine = create_engine("postgresql://postgres:password@localhost:5433/final_project")

try:
    companies_df.to_sql("sample_training_companies", index=False, con=engine, if_exists="replace")
    print("Import data success")
except:
    print("Import data failed")