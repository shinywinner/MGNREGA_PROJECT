import psycopg2

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host="postgres.railway.internal",
            port="5432",
            dbname="railway",
            user="postgres",
            password="YERKhVsLYOxQLiblHZJvvizhKasCpjbb"
        )
        return conn
    except Exception as e:
        print("❌ Database connection failed:", e)
        return None
