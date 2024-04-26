import pandas as pd
import re
import psycopg2
from config import pg_host, pg_database, pg_user, pg_password
from watchBrands import watch_brands

# Function to find brand and price in title
def find_brand_and_price(title):
    brand = None
    for brand_name in watch_brands:
        if brand_name.lower() in title.lower():
            brand = brand_name
            break
    # Regex to find price patterns like $1000 or USD 1500
    price_pattern = r'\$(\d+)|USD\s(\d+)'
    price_matches = re.findall(price_pattern, title)
    if price_matches:
        # Flatten the list of tuples and filter out None values, then convert to int
        price = [int(num) for tup in price_matches for num in tup if num][0]
    else:
        price = None
    return brand, price

# Load CSV data
df = pd.read_csv('posts.csv')
if 'username' not in df.columns:
    df['username'] = "N/A"
# Apply function to find brand and price
df[['brand', 'price']] = df['title'].apply(lambda title: pd.Series(find_brand_and_price(title)))

# Insert data into PostgreSQL
def insert_posts(df):
    conn = psycopg2.connect(
        host=pg_host,
        database=pg_database,
        user=pg_user,
        password=pg_password)
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO watchexchange_posts (id, created_utc, username, num_comments, upvotes, title, brand, price)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """, (row['id'], row['created_utc'], row['username'], row['num_comments'], row['upvotes'], row['title'], row['brand'], row['price']))
    conn.commit()
    cur.close()
    conn.close()

insert_posts(df)
