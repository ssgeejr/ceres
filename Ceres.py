import subprocess
import csv
import mysql.connector
from datetime import datetime
from Astroidbelt import DwarfMoon

class Ceres:
    def __init__(self):
        self.db_connection = None
        self.db_cursor = None

    def connect_to_db(self):
        """Establish a connection to the MySQL database."""
        try:
            dwarfmoon = DwarfMoon()

            self.db_connection = mysql.connector.connect(
                host=dwarfmoon.getServer(),         # Update with your MySQL server details
                user=dwarfmoon.getUsername(),              # Update with your MySQL username
                password=dwarfmoon.getPassword(),      # Update with your MySQL password
                database=dwarfmoon.getDB()
            )
            self.db_cursor = self.db_connection.cursor()
        except mysql.connector.Error as err:
            print("Error connecting to MySQL:", err)
            exit(1)

    def close_db_connection(self):
        """Close the database connection."""
        if self.db_cursor:
            self.db_cursor.close()
        if self.db_connection:
            self.db_connection.close()

    def read_csv_and_insert_into_db(self):
        """Read the CSV file and insert its contents into MySQL database."""
        # Open CSV file
        try:

            file_path = r"C:\Users\imidd\PycharmProjects\ceres\Test - Sheet1.csv"

            with open(file_path, "r") as file:
                loaded_records = 0
                batch_size = 100
                for line in file:
                    name, email, department = line.strip().split(',')

                    # Check if email exists
                    self.db_cursor.execute("SELECT user_id FROM users WHERE email = %s", (email,))
                    result = self.db_cursor.fetchone()

                    if result:
                        print(f"User with email {email} already exists.")
                    else:
                        # Insert user into 'users' table
                        self.db_cursor.execute("""
                            INSERT INTO users (name, email, department)
                            VALUES (%s, %s, %s)
                        """, (name, email, department))

                        # Get the user_id of the newly inserted user
                        user_id = self.db = self.db_cursor.lastrowid

                        # Insert into 'user_reports' table
                        seen_date = datetime.now().strftime('%m/%d/%Y')
                        self.db_cursor.execute("""
                        INSERT INTO user_reports (user_id, seen_date)
                        VALUES (%s, %s)
                        """, (user_id, seen_date))

                        loaded_records += 1

                        #commit after every batch_size records
                        if loaded_records % batch_size == 0:
                            self.db_connection.commit()
                            print(f"Committed {loaded_records} records.")

                #final commit for any remaining records
                self.db_connection.commit()
                print(f"Total records loaded: {loaded_records}")

        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except mysql.connector.Error as err:
            print("Error inserting into database:", err)

    def process(self):
        """Main process for the Ceres class."""
        self.connect_to_db()
        self.read_csv_and_insert_into_db()
        self.close_db_connection()

if __name__ == '__main__':
    ceres = Ceres()
    ceres.process()
    #buddy.run_batch_file()

