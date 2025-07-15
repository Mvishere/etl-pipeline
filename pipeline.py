import pandas as pd
from pandas import DataFrame
import requests
from sqlalchemy import create_engine

def extract() -> DataFrame:
    df: DataFrame = pd.read_csv("source_data.csv")
    return df

def transform(df: DataFrame) -> DataFrame:
    df = df.dropna()
    return df

def load(df: DataFrame) -> None:
    pass