import praw
import requests
import pandas as pd
from datetime import datetime
from config import reddit_client_id, reddit_client_secret, reddit_user_agent


def get_posts_from_pushshift(subreddit, start_time, end_time):
    url = f"https://api.pushshift.io/reddit/search/submission/?subreddit={subreddit}&after={start_time}&before={end_time}&size=500"
    response = requests.get(url)
    posts = response.json()

    # Check if 'data' key exists in the response
    if 'data' not in posts:
        print("No data found in the API response:", posts)
        return []  # Return an empty list if no data is found

    return [post['id'] for post in posts['data']]


def fetch_data_with_praw(post_ids):
    reddit = praw.Reddit(
        client_id=reddit_client_id,
        client_secret=reddit_client_secret,
        user_agent=reddit_user_agent
    )

    posts_data = []
    for post_id in post_ids:
        submission = reddit.submission(id=post_id)
        post_details = {
            'title': submission.title,
            'created_utc': datetime.fromtimestamp(submission.created_utc),
            'num_comments': submission.num_comments,
            'upvotes': submission.score,
        }
        posts_data.append(post_details)
    return pd.DataFrame(posts_data)

# Provide the subreddit name, start time, and end time in epoch format.
post_ids = get_posts_from_pushshift('watchexchange', '2022-04-25', '2024-04-25')
data_frame = fetch_data_with_praw(post_ids)
print(data_frame)
