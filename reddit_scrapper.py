import os , time
import praw
import pandas as pd
from datetime import datetime
from dotenv import dotenv_values 
from tqdm import tqdm  

# --- CARGAR VARIABLES DESDE .env ---
config = dotenv_values(os.path.join(os.path.dirname(__file__), '.env'))

CLIENT_ID     = config.get("CLIENT_ID")
CLIENT_SECRET = config.get("CLIENT_SECRET")
USER_AGENT    = config.get("USER_AGENT")

SUBREDDITS = [
    "CrohnsDisease", "UlcerativeColitis", "IBD",
    "crohnsandcolitis", "CrohnsDiseaseDiet",
    "UlcerativeColitisRDLA", "IBDDiet", "Ulcerativecolitisdiet"
]

reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    user_agent=USER_AGENT
)

def scrape_subreddit(subreddit_name, limit=1000):
    posts = []
    subreddit = reddit.subreddit(subreddit_name)
    # ver progress bar con tqdm
    for post in tqdm(subreddit.new(limit=limit),
                     
                     desc=f"Scrapeando r/{subreddit_name}",
                     unit="post"):
        post.comments.replace_more(limit=0)
        time.sleep(0.5)
        comments = " || ".join(c.body for c in post.comments)
        posts.append({
            "subreddit": subreddit_name,
            "id": post.id,
            "title": post.title,
            "selftext": post.selftext,
            "comments": comments,
            "author": str(post.author),
            "created_utc": datetime.fromtimestamp(post.created_utc),
            "score": post.score,
            "post_num_comments": post.num_comments
        })
    return posts

all_posts = []
for s in SUBREDDITS:
    all_posts.extend(scrape_subreddit(s, limit=1000))


df = pd.DataFrame(all_posts)
df.to_csv("reddit_ibd_posts.csv", index=False, encoding="utf-8")
print(f"Se guardaron {len(df)} posts en reddit_ibd_posts.csv")