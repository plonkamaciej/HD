import psycopg2
from psycopg2 import Error

def display_table():
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            user="postgres",
            password="root",
            host="localhost",
            port="5433",  
            database="netflix"
        )

        # Create a cursor object
        cursor = connection.cursor()

        # Execute the SQL query to fetch data from the table
        cursor.execute("SELECT * FROM netflix_titles_transformed LIMIT 10")

        # Fetch all rows from the result set
        rows = cursor.fetchall()

        # Display the fetched rows
        for row in rows:
            print(row)

    except (Exception, Error) as error:
        print("Error while fetching data from PostgreSQL:", error)

    finally:
        # Close the cursor and connection
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

# Call the function to display the table
display_table()
