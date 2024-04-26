import praw
import pandas as pd
from datetime import datetime, timezone
import re
from config import (
    reddit_client_id, reddit_client_secret, reddit_user_agent,
    reddit_username, reddit_password, pg_host, pg_database, pg_user, pg_password
)
from watchBrands import watch_brands
import psycopg2

def run_scrapper():
    reddit = praw.Reddit(
        client_id=reddit_client_id,
        client_secret=reddit_client_secret,
        user_agent=reddit_user_agent,
        username=reddit_username,
        password=reddit_password,
    )

    subreddit = reddit.subreddit("watchexchange")
    posts_data = []
    start_of_day = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    for submission in subreddit.new(limit=1000):
        submission_time = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)
        if submission_time >= start_of_day:
            post_details = {
                "id": submission.id,
                "created_utc": submission_time.strftime('%Y-%m-%d %H:%M:%S'),
                "username": submission.author.name if submission.author else "N/A",
                "num_comments": submission.num_comments,
                "upvotes": submission.score,
                "title": submission.title,
            }
            posts_data.append(post_details)

    posts_df = pd.DataFrame(posts_data)
    posts_df[['brand', 'price']] = posts_df['title'].apply(lambda title: pd.Series(find_brand_and_price(title)))

    # Insert data into PostgreSQL database
    insert_posts(posts_df)

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

if __name__ == "__main__":
    run_scrapper()
