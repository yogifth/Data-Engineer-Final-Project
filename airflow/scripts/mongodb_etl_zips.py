from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine

# connect to mongodb 
client = MongoClient("mongodb+srv://admin:admin@cluster0.etjsmyx.mongodb.net/test")

# Get the "sample_training" database and read "zips" collection a.k.a table
db = client["sample_training"]
zips = db["zips"]

# Initiate pymongo cursor
zips_coll = zips.find()

# Load data into DataFrame
df_zips = pd.DataFrame.from_dict(list(zips_coll))

# Flatten nested 'loc' column
loc = pd.DataFrame(df_zips["loc"].tolist())
zips_df = pd.concat([df_zips, loc], axis=1)
zips_df.drop(["loc"], axis=1, inplace=True)
zips_df.rename(columns={"_id": "id", "y": "latitude", "x": "longitude"}, inplace=True)

# Change dtypes ObjectId in columns 'id' into string
zips_df["id"] = zips_df["id"].astype(str)

# Slice value 'id' to the last 5 digit 
zips_df["id"] = zips_df["id"].str.strip().str[-5:]
print(zips_df.tail())


# Import DataFrame to Postgres
engine = create_engine("postgresql://postgres:password@localhost:5433/final_project")

try:
    zips_df.to_sql("sample_training_zips", index=False, con=engine, if_exists="replace")
    print("Import data success")
except:
    print("Import data failed")
