import pandas as pd
from sqlalchemy import create_engine

# Database credentials
db_user = 'postgres'
db_password = 'root'
db_host = 'localhost'
db_port = '5433'
db_name = 'netflix'

# Create SQLAlchemy engine
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Extract: Read data from the netflix_titles table
df = pd.read_sql_table('netflix_titles', engine)

# Transform: Count movies and TV shows by country
# Split the 'country' column into separate rows
df_countries = df.dropna(subset=['country']).copy()  # Drop rows where country is NaN
df_countries['country'] = df_countries['country'].str.split(', ')
df_countries = df_countries.explode('country')

# Group by country and type, and count the number of occurrences
df_counts = df_countries.groupby(['country', 'type']).size().reset_index(name='count')

# Pivot the table to have separate columns for movie and TV show counts
df_pivot = df_counts.pivot(index='country', columns='type', values='count').reset_index().fillna(0)

# Rename the columns for clarity
df_pivot.columns.name = None
df_pivot.columns = ['country', 'movie_count', 'tv_show_count']

# Load: Write the transformed data into a new table
df_pivot.to_sql('netflix_counts_by_country', engine, if_exists='replace', index=False)

print("ETL process completed successfully")
