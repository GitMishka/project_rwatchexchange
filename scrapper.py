# daily_scrapper.py

import praw
import pandas as pd
from datetime import datetime, timezone
from config import reddit_client_id, reddit_client_secret, reddit_user_agent, reddit_username, reddit_password
from watchBrands import watch_brands

def run_scrapper():
    reddit = praw.Reddit(
        client_id=reddit_client_id,
        client_secret=reddit_client_secret,
        user_agent=reddit_user_agent,
        username=reddit_username,
        password=reddit_password,
    )

    subreddit = reddit.subreddit("watches")

    posts_data = []
    start_of_day = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    for submission in subreddit.new(limit=None):  # Adjust limit as necessary
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

    # Add brand column 
    def find_brand(title):
        for brand in watch_brands:
            if brand.lower() in title.lower():
                return brand
        return None

    posts_df['brand'] = posts_df['title'].apply(find_brand)

    # Return the DataFrame
    return posts_df
