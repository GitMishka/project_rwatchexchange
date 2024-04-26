import praw
import pandas as pd
from datetime import datetime, timedelta
from config import reddit_client_id, reddit_client_secret, reddit_user_agent

def fetch_recent_posts(subreddit_name, days_ago):
    """Fetch recent posts from a subreddit up to 'days_ago'."""
    reddit = praw.Reddit(
        client_id=reddit_client_id,
        client_secret=reddit_client_secret,
        user_agent=reddit_user_agent
    )

    subreddit = reddit.subreddit(subreddit_name)
    limit_time = datetime.utcnow() - timedelta(days=days_ago)
    
    posts_data = []
    for submission in subreddit.new(limit=None):
        submission_time = datetime.utcfromtimestamp(submission.created_utc)
        if submission_time < limit_time:
            break
        posts_data.append({
            'id': submission.id,
            'title': submission.title,
            'created_utc': submission_time,
            'num_comments': submission.num_comments,
            'upvotes': submission.score
        })
    return pd.DataFrame(posts_data)

# Example usage
data_frame = fetch_recent_posts('watchexchange', 100)  # Fetch posts from the last 30 days
print(data_frame)
data_frame.to_csv('posts.csv')
