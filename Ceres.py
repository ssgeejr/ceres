import subprocess
import pandas as pd
import csv
import mysql.connector
import getopt
import sys
from datetime import datetime
from Astroidbelt import DwarfMoon

class Ceres:
    def __init__(self):
        self.db_connection = None
        self.db_cursor = None
        self.file_path = None

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

    def read_excel_and_insert_into_db(self):
        """Read the Excel file (without headers) with 4 columns and insert contents into MySQL database."""
        if not self.file_path:
            print("No file provided to read.")
            return

        try:
            # Load the file without headers and assign default column names
            print("Loading Excel file without headers.")
            df = pd.read_excel(self.file_path, header=None)

            # Ensure that the file contains at least 4 columns
            if df.shape[1] < 4:
                print("Excel file must contain at least 4 columns.")
                return

            # Assign default column names (the 1st column will be ignored)
            df.columns = ['Ignored_Column', 'Name', 'Email', 'Department']

            # Display the first few rows and columns for inspection
            print("First few rows of the Excel file:\n", df.head())
            print("Columns detected:", df.columns.tolist())

            loaded_records = 0
            batch_size = 100

            # Iterate through the DataFrame, skipping the first column
            for _, row in df.iterrows():
                name, email, department = row['Name'], row['Email'], row['Department']

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

                    # Insert into 'user_reports' table
                    user_id = self.db_cursor.lastrowid
                    seen_date = datetime.now().strftime('%m/%d/%Y')
                    self.db_cursor.execute("""
                        INSERT INTO user_reports (user_id, seen_date)
                        VALUES (%s, %s)
                    """, (user_id, seen_date))

                    loaded_records += 1

                    if loaded_records % batch_size == 0:
                        self.db_connection.commit()
                        print(f"Committed {loaded_records} records.")

            self.db_connection.commit()
            print(f"Total records loaded: {loaded_records}")

        except FileNotFoundError:
            print(f"File not found: {self.file_path}")
        except mysql.connector.Error as err:
            print("Error inserting into database:", err)
        except Exception as e:
            print(f"Unexpected error: {e}")

    def process(self):
        """Main process for the Ceres class."""
        self.parse_arguments(sys.argv[1:])
        self.connect_to_db()
        self.read_excel_and_insert_into_db()
        self.close_db_connection()

    def parse_arguments(self, argv):
        """Parse command-line arguments."""
        try:
            opts, args = getopt.getopt(argv, "d:hewmabcxf:", ["emails=", "excel"])
        except getopt.GetoptError as e:
            print('>>>> ERROR: %s' % str(e))
            sys.exit(2)

        for opt, arg in opts:
            if opt == '-h':
                print('---RUNTIME PARAMETERS---')
                print('python script.py -f filename  # set the input filename')
                # ... add other options
                sys.exit()
            elif opt == '-f':
                self.file_path = arg  # Store the filename passed with -f
                print(f'Using file: {self.file_path}')

if __name__ == '__main__':
    ceres = Ceres()
    ceres.process()
    #buddy.run_batch_file()

