import pandas as pd
from pandas import DataFrame
from db import get_connection

def extract() -> DataFrame:
    df: DataFrame = pd.read_csv("source_data.csv")
    return df

def transform(df: DataFrame) -> DataFrame:
    df = df.dropna()
    return df

def load(df):
    try:
        conn = get_connection()
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO employee_data (id, name, email, age, join_date, salary)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    row["id"],
                    row["name"],
                    row["email"],
                    int(row["age"]),
                    row["join_date"],
                    float(row["salary"]),
                )
            )

        conn.commit()
        print("Data inserted successfully.")

    except Exception as e:
        conn.rollback()
        print("Server Error:", e)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    df: DataFrame = extract()
    df = transform(df)
    load(df)