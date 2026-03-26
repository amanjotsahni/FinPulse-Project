# explore_data.py
import pandas as pd 
FILE_PATH = r"C:\Users\amana\Downloads\Finance data\Finance_dataset.csv"
df = pd.read_csv(FILE_PATH)

print("Shape:", df.shape)
print("Columns:", df.columns.tolist())
print("First 5 rows:\n", df.head())
print("Data Types:\n", df.dtypes)
print("Null Values:\n", df.isnull().sum())
print("Basic Stats:\n", df.describe())