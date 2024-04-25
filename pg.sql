CREATE TABLE subreddit_posts (
    id VARCHAR(20) PRIMARY KEY,
    created_utc TIMESTAMP,
    username VARCHAR(50),
    num_comments INT,
    upvotes INT,
    title TEXT,
    brand VARCHAR(50),
    price DECIMAL(10,2) 
);
