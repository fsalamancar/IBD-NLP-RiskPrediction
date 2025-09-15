#Programa para scrapear posts y comentarios de los autores de subreddits relacionados con IBD
# y guardar los datos en archivos CSV.

#Datos extraidos de:
#2)Scrapping last 1000 post of each subreddit (14-sep-2025)
#    "CrohnsDisease", "UlcerativeColitis", "IBD",
#    "crohnsandcolitis", "CrohnsDiseaseDiet",
#    "UlcerativeColitisRDLA", "IBDDiet", "Ulcerativecolitisdiet"

import os
import time
import praw
import pandas as pd
from datetime import datetime
from dotenv import dotenv_values
from tqdm import tqdm

# --- CARGAR VARIABLES DESDE .env ---
# Asegura que .env tenga: CLIENT_ID, CLIENT_SECRET, USER_AGENT
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '.env')
config = dotenv_values(CONFIG_PATH)
CLIENT_ID     = config.get("CLIENT_ID")
CLIENT_SECRET = config.get("CLIENT_SECRET")
USER_AGENT    = config.get("USER_AGENT")

# --- Subreddits relacionados a IBD ---
SUBREDDITS = [
    "CrohnsDisease", "UlcerativeColitis", "IBD",
    "crohnsandcolitis", "CrohnsDiseaseDiet",
    "UlcerativeColitisRDLA", "IBDDiet", "Ulcerativecolitisdiet"
]

# --- Inicializar PRAW ---
reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    user_agent=USER_AGENT,
    check_for_async=False
)

# --- asegurar carpeta de salida ---
OUT_DIR = "output"
os.makedirs(OUT_DIR, exist_ok=True)

def scrape_subreddit(subreddit_name, limit=1000, replace_more_limit=0, sleep_s=0.5):
    """
    Extrae últimos 'limit' posts de r/subreddit_name.
    - replace_more_limit=0 elimina 'MoreComments' sin expandirlos (rápido).
      Usar None si se requieren todos los comentarios pero será mucho más lento. [5]
    - sleep_s: pausa corta para convivir mejor con rate limits. PRAW ya hace sleeps internos cuando falta header. [6]
    """
    posts = []
    subreddit = reddit.subreddit(subreddit_name)
    for post in tqdm(subreddit.new(limit=limit),
                     desc=f"Scrapeando r/{subreddit_name}",
                     unit="post"):
        try:
            post.comments.replace_more(limit=replace_more_limit)
        except Exception:
            # si falla el reemplazo de MoreComments, continúa con lo disponible
            pass

        # Comentarios del autor del post (submitter)
        try:
            author_name = post.author.name if post.author else None
        except Exception:
            author_name = None

        if author_name:
            try:
                author_comments = [
                    c.body for c in post.comments.list()
                    if getattr(c, "author", None) and getattr(c.author, "name", None) == author_name
                ]
            except Exception:
                author_comments = []
        else:
            author_comments = []

        comments_joined = " || ".join(author_comments)

        # Ensamblar registro
        posts.append({
            "subreddit": subreddit_name,
            "id": post.id,
            "title": getattr(post, "title", None),
            "selftext": getattr(post, "selftext", None),
            "author_comments": comments_joined,
            "author": str(post.author) if post.author else None,
            "created_utc": datetime.utcfromtimestamp(getattr(post, "created_utc", 0)),
            "score": getattr(post, "score", None),
            "post_num_comments": getattr(post, "num_comments", None),
            "permalink": f"https://www.reddit.com{getattr(post, 'permalink', '')}"
        })

        # pequeña pausa
        time.sleep(sleep_s)

    return posts

# --- Ejecutar y guardar CSV con patrón 'subreddit_last1000_YYYYMMDD.csv' ---
def run_and_save_all(subreddits, limit=1000):
    fecha_tag = datetime.utcnow().strftime("%Y%m%d")
    total = 0
    for s in subreddits:
        posts = scrape_subreddit(s, limit=limit, replace_more_limit=0, sleep_s=0.5)  # rápido: no expande todos los MoreComments [5]
        df = pd.DataFrame(posts)
        out_name = f"{s}_last{limit}_{fecha_tag}.csv"
        out_path = os.path.join(OUT_DIR, out_name)
        # to_csv maneja bien UTF-8 en pandas modernos
        df.to_csv(out_path, index=False, encoding="utf-8")
        print(f"Guardado {len(df)} posts de r/{s} en {out_path}")
        total += len(df)
    print(f"Total guardado: {total} posts en {OUT_DIR}/")

if __name__ == "__main__":
    run_and_save_all(SUBREDDITS, limit=1000)
