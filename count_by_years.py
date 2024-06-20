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

# Transform: Count movies by release year
# Filter the DataFrame to include only movies
df_movies = df[df['type'] == 'Movie']

# Convert release_year to numeric
df_movies['release_year'] = pd.to_numeric(df_movies['release_year'], errors='coerce')

# Group by release_year and count the number of movies
df_movie_counts = df_movies.groupby('release_year').size().reset_index(name='movie_count')

# Load: Write the transformed data into a new table
df_movie_counts.to_sql('netflix_movie_counts_by_year', engine, if_exists='replace', index=False)

print("ETL process completed successfully")
