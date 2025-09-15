import os
from dotenv import dotenv_values
from pathlib import Path

# Ruta 
env_path = Path(__file__).resolve().parent / ".env"
print("Buscando .env en:", env_path)

config = dotenv_values(env_path)

print("ID:", config.get("CLIENT_ID"))
print("Secret:", config.get("CLIENT_SECRET"))
print("Agent:", config.get("USER_AGENT"))
