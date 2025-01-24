import argparse
import mysql.connector
from datetime import datetime

def main():

    # Argument parsing (translating raw data into a format a computer can interpret)
    parser = argparse.ArgumentParser(description="Add a user if email doesn't exist")
    parser.add_argument('-f', '--file', type=str, required=True, help="Path to the CSV or data file")
    args = parser.parse_args()

    # Open the file
    filename = args.file
    with open(filename, "r") as file:
        # Connect to MySQL db (open the connection once, outside the loop)
        connection = mysql.connector.connect(
            host='18.117.154.172',
            user='dwarfmoon',
            password='astroidbelt',
            database='ceresdb'
        )
        cursor = connection.cursor()

        for line in file:
            name, email, department = line.strip().split(',')

            # Check if email exists
            cursor.execute("SELECT user_id FROM users WHERE email = %s", (email,))
            result = cursor.fetchone()

            if result:
                print(f"User with email {email} already exists.")
            else:
                # Insert user into the 'users' table (user_id is auto-incremented, no need to specify it)
                cursor.execute("""
                    INSERT INTO users (name, email, department)
                    VALUES (%s, %s, %s)
                """, (name, email, department))

                # Get the user_id of the newly inserted user
                user_id = cursor.lastrowid

                # Insert record into the 'user_reports' table with the current date
                date_now = datetime.now().strftime('%Y-%m-%d')
                cursor.execute("""
                    INSERT INTO user_reports (user_id, seen_date)
                    VALUES (%s, %s)
                """, (user_id, date_now))

                print(f"User {name} with email {email} added to database.")

        # Commit changes and close the database connection after processing all records
        connection.commit()
        connection.close()

if __name__ == "__main__":
    main()