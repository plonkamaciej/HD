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

# Transform: Perform some transformations
# Example: Filter shows released after 2015 and add a new column with show length in minutes
df['release_year'] = pd.to_numeric(df['release_year'], errors='coerce')
df_filtered = df[df['release_year'] > 2015]

# Add a new column "duration_minutes"
def convert_duration(duration):
    if pd.isnull(duration) or 'Season' in str(duration):
        return None  # Skip TV shows with seasons or None values
    parts = str(duration).split()
    if len(parts) == 2 and parts[1] == 'min':
        return int(parts[0])
    return None

df_filtered['duration_minutes'] = df_filtered['duration'].apply(convert_duration)

# Drop the 'description' column
df_filtered.drop(columns=['description'], inplace=True)

# Sort the DataFrame by 'duration_minutes' in descending order
df_filtered.sort_values(by='duration_minutes', ascending=False, inplace=True)

# Load: Write the transformed data back into a new table
df_filtered.to_sql('netflix_titles_transformed', engine, if_exists='replace', index=False)

print("ETL process completed successfully")
