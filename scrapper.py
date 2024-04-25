import praw
import pandas as pd
from datetime import datetime, timezone
import re  # Regex module to find price patterns in titles
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

    subreddit = reddit.subreddit("watchexchange")  # Changed to watchexchange subreddit

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

    # Function to find brand and price in title
    def find_brand_and_price(title):
        brand = None
        for brand_name in watch_brands:
            if brand_name.lower() in title.lower():
                brand = brand_name
                break

        # Regex to find price patterns like $1000 or USD 1500
        price_pattern = r'\$\d+|USD\s\d+'
        price_matches = re.findall(price_pattern, title)
        price = price_matches[0] if price_matches else None

        return brand, price

    # Apply function to find brand and price
    posts_df[['brand', 'price']] = posts_df['title'].apply(lambda title: pd.Series(find_brand_and_price(title)))

    posts_df.to_csv('posts.csv')
    return posts_df

# Example usage
if __name__ == "__main__":
    result_df = run_scrapper()
    print(result_df)
