import argparse
import mysql.connector
from datetime import datetime


def main():

    #arguement parsing (means translating raw data into a format a computer can interpret)
    parser = argparse.ArgumentParser(description="Add a user if email doesn't exist")
    parser.add_argument('-f', '--file', type=str, required=True, help="Path to the CSV or data file")
    args = parser.parse_args()

    #open the file
    filename = args.file
    with open(filename, "r") as file:
        for line in file:
            name, email, department = line.strip().split(',')

            #connect to MySQL db
            connection = mysql.connector.connect(
                host='ceres',
                user='dwarfmoon',
                password='astroidbelt',
                database='ceresdb'
            )
            cursor = connection.cursor()

            #check if email exists
            cursor.execute("SELECT 1 FROM users WHERE email = %s", (email,))
            result = cursor.fetchone()

            if result:
                print(f"User with email {email} already exists.")
            else:
                #insert user into the 'users' table
                cursor.execute("""
                    INSERT INTO users (user_id, name, email, department
                    VALUES (%s, %s, %s)
                """, (name, email, department))

                #insert record into the 'user_reports' table with the current date
                date_now = datetime.now().strftime('%Y-%m-%d')
                cursor.execute("""
                    INSERT INTO user_reports (user_email, seen_date)
                    VALUES (%s, %s)
                """, (email, date_now))

                print(f"User {name} with email {email} added to database.")

            #commit and close the database connection
            connection.commit()
            connection.close()

if __name__ == "__main__":
    main()