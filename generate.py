import pandas as pd

# Load the dataset
df = pd.read_csv('netflix_titles.csv')

# Open the SQL file in append mode
with open('init.sql', 'a') as f:
    for _, row in df.iterrows():
        # Convert row values to SQL compatible strings
        values = [f"""'{str(x).replace("'", "''")}'""" if pd.notnull(x) else 'NULL' for x in row]
        insert_statement = f"INSERT INTO netflix_titles VALUES ({', '.join(values)});\n"
        f.write(insert_statement)

print("SQL insert statements generated successfully")