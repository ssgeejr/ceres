import pandas as pd
import mysql.connector
import getopt
import sys
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

    def read_excel_and_insert_into_db(self):
        """Read the Excel file and insert data into the database."""
        if not self.file_path:
            print("No file provided to read.")
            return

        try:
            print("Loading Excel file without headers.")
            df = pd.read_excel(self.file_path, header=None)

            # Inspect the first few rows to see the data format
            print("First few rows from Excel file:")
            print(df.head())

            if df.shape[1] < 4:
                print("Excel file must contain at least 4 columns.")
                return

            # Assign default column names (the 1st column will be used as date, others for user details)
            df.columns = ['Date', 'Name', 'Email', 'Department']

            loaded_records = 0
            batch_size = 100

            # Iterate through the DataFrame
            for _, row in df.iterrows():
                date, name, email, department = row['Date'], row['Name'], row['Email'], row['Department']

                # Check if email exists in the users table
                self.db_cursor.execute("SELECT user_id FROM users WHERE email = %s", (email,))
                result = self.db_cursor.fetchone()

                if result:
                    user_id = result[0]
                    print(f"User with email {email} already exists with user_id {user_id}.")
                else:
                    # Insert user into 'users' table
                    self.db_cursor.execute(
                        """
                        INSERT INTO users (name, email, department)
                        VALUES (%s, %s, %s)
                        """,
                        (name, email, department)
                    )
                    user_id = self.db_cursor.lastrowid
                    print(f"Inserted new user: {name} with email {email} (user_id: {user_id}).")

                # Convert the date to an integer (assuming it represents a valid date)
                try:
                    seen_date = int(date)
                except ValueError:
                    print(f"Invalid date for row: {row}. Skipping.")
                    continue

                try:
                    # Check if this user_id already has this specific seen_date
                    self.db_cursor.execute(
                        """
                        SELECT COUNT(*) FROM user_reports WHERE user_id = %s AND seen_date = %s
                        """,
                        (user_id, seen_date)
                    )
                    count = self.db_cursor.fetchone()[0]

                    if count == 0:
                        # If this user_id and seen_date pair doesn't exist, insert it
                        self.db_cursor.execute(
                            """
                            INSERT INTO user_reports (user_id, seen_date)
                            VALUES (%s, %s)
                            """,
                            (user_id, seen_date)
                        )
                        print(f"Inserted new report for user_id {user_id} on {seen_date}.")
                    else:
                        print(f"Duplicate report found for user_id {user_id} on {seen_date}. Skipping insertion.")
                except mysql.connector.Error as err:
                    print(f"Error inserting report for user_id {user_id}: {err}")

                loaded_records += 1

                # Commit after every batch_size records
                if loaded_records % batch_size == 0:
                    self.db_connection.commit()
                    print(f"Committed {loaded_records} records.")

            # Final commit for any remaining records
            self.db_connection.commit()
            print(f"Total records loaded: {loaded_records}")

        except FileNotFoundError:
            print(f"File not found: {self.file_path}")
        except mysql.connector.Error as err:
            print("Error inserting into database:", err)
        except Exception as e:
            print(f"Unexpected error: {e}")

    def close_db_connection(self):
        """Close the database connection."""
        if self.db_cursor:
            self.db_cursor.close()
        if self.db_connection:
            self.db_connection.close()

    def process(self):
        """Main process for the Ceres class."""
        self.parse_arguments(sys.argv[1:])
        self.connect_to_db()
        self.read_excel_and_insert_into_db()
        self.close_db_connection()

    def parse_arguments(self, argv):
        """Parse command-line arguments."""
        try:
            opts, args = getopt.getopt(argv, "hf:", ["file="])  # Fixed -f option
        except getopt.GetoptError as e:
            print('>>>> ERROR: %s' % str(e))
            sys.exit(2)

        for opt, arg in opts:
            if opt == '-h':
                print('---RUNTIME PARAMETERS---')
                print('python script.py -f filename  # set the input filename')
                sys.exit()
            elif opt in ("-f", "--file"):
                self.file_path = arg
                print(f'Using file: {self.file_path}')

if __name__ == '__main__':
    ceres = Ceres()
    ceres.process()

