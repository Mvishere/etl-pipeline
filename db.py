import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def get_connection():
    try:
        db = psycopg2.connect(
            user=os.getenv("user"),
            password=os.getenv("password"),
            host=os.getenv("host"),
            port=os.getenv("port"),
            dbname=os.getenv("dbname")
        )
        return db
    except Exception as e:
        print("Error while connecting database", e)

if __name__ == "__main__":
    get_connection()