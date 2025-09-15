import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import praw
from praw.models import MoreComments
from dotenv import dotenv_values
from tqdm import tqdm

# --- CARGA .env ---
BASE_DIR = os.path.dirname(__file__)
config = dotenv_values(os.path.join(BASE_DIR, ".env"))
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
    user_agent=USER_AGENT,
)

START = datetime(2015, 1, 1, tzinfo=timezone.utc)
END   = datetime.now(timezone.utc)
WINDOW_DAYS = 14

def fetch_new_within(subreddit_name, after_dt, before_dt, max_empty=2000):
    sr = reddit.subreddit(subreddit_name)
    rows = []
    empty_hits = 0
    for submission in sr.new(limit=None):
        created_dt = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)
        # detener cuando ya pasamos la ventana por abajo
        if created_dt < after_dt:
            break
        # saltar si es posterior a la ventana (porque new viene de reciente a antiguo)
        if created_dt >= before_dt:
            continue
        empty_hits = 0
        rows.append(submission)
        # backoff suave para respetar cuota
        if len(rows) % 50 == 0:
            time.sleep(0.6)
    return rows

def scrape_window(subreddit_name, after_dt, before_dt):
    rows = []
    submissions = fetch_new_within(subreddit_name, after_dt, before_dt)
    # Si la ventana es muy densa, subdividir recursivamente
    if len(submissions) >= 950:
        mid = after_dt + (before_dt - after_dt)/2
        rows += scrape_window(subreddit_name, after_dt, mid)
        rows += scrape_window(subreddit_name, mid, before_dt)
        return rows

    for submission in tqdm(submissions, desc=f"{subreddit_name} {after_dt.date()}..{before_dt.date()}", unit="post"):
        try:
            author = str(submission.author) if submission.author else None
            submission.comments.replace_more(limit=0)
            author_comments = []
            for c in submission.comments.list():
                if isinstance(c, MoreComments):
                    continue
                if author and c.author and str(c.author) == author:
                    author_comments.append(c.body)

            rows.append({
                "subreddit": subreddit_name,
                "id": submission.id,
                "title": submission.title or "",
                "selftext": getattr(submission, "selftext", "") or "",
                "author_comments": " || ".join(dict.fromkeys(author_comments)),
                "author": author or "",
                "created_utc": int(submission.created_utc),
                "score": submission.score,
                "post_num_comments": submission.num_comments,
                "url": submission.url,
            })
            time.sleep(0.4)
        except Exception:
            time.sleep(1.0)
    return rows

def scrape_subreddit_batched(subreddit_name, start_dt, end_dt, window_days=14):
    cur = end_dt  # vamos de reciente a antiguo para aprovechar new
    all_rows = []
    while cur > start_dt:
        prev = max(start_dt, cur - timedelta(days=window_days))
        batch_rows = scrape_window(subreddit_name, prev, cur)
        all_rows.extend(batch_rows)
        cur = prev
    return all_rows

def main():
    all_rows = []
    for sub in SUBREDDITS:
        print(f"\nScrapeando r/{sub} en ventanas de {WINDOW_DAYS} días ...")
        all_rows.extend(scrape_subreddit_batched(sub, START, END, WINDOW_DAYS))

    df = pd.DataFrame(all_rows)
    if not df.empty and "created_utc" in df.columns:
        df["created_dt"] = pd.to_datetime(df["created_utc"], unit="s", utc=True)
    else:
        df["created_dt"] = pd.NaT

    out = "reddit_ibd_posts_batched.csv"
    df.to_csv(out, index=False, encoding="utf-8")
    print(f"\nSe guardaron {len(df)} posts en {out}")

if __name__ == "__main__":
    main()
