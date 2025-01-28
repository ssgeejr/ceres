import subprocess
import pandas as pd
import mysql.connector
import getopt
import sys
import logging
from datetime import datetime, timedelta
from Astroidbelt import DwarfMoon

# Helper function to parse and convert Excel date formats
def parse_excel_date(date_value):
    """Helper function to parse and convert Excel date formats."""
    if isinstance(date_value, str):
        date_value = date_value.strip()  # Remove leading/trailing spaces if it's a string
        if len(date_value) == 8:  # For text-based MMDDYYYY
            print(f"Text date found: {date_value}")
            return datetime.strptime(date_value, '%m%d%Y').strftime('%m%d%Y')
    elif isinstance(date_value, (int, float)):  # For Excel serial number
        try:
            print(f"Raw Excel date serial number: {date_value}")

            # Adding a range check for invalid Excel serial numbers.
            if date_value < 1 or date_value > 2958465:
                print(f"Out-of-range Excel serial number detected: {date_value}. Skipping this record.")
                return None

            # Excel's base date is actually December 30, 1899
            excel_base_date = datetime(1899, 12, 30)
            # Convert the serial number to a date
            excel_date = excel_base_date + timedelta(days=date_value)

            print(f"Intermediate Excel date: {excel_date}")

            # Ensure final formatted date is correct
            formatted_date = excel_date.strftime('%m%d%Y')
            print(f"Converted Excel date: {formatted_date}")  # Final print check
            return formatted_date
        except Exception as e:
            print(f"Error converting Excel date: {e}. Skipping record.")
            return None

    return None  # If no valid date format is found


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

    def insert_user_report(self, user_id, seen_date):
        """Insert a user report entry, allowing multiple seen_date entries for the same user_id."""
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
                logging.info(f"Inserted new report for user_id {user_id} on {seen_date}.")
            else:
                logging.info(f"Duplicate report found for user_id {user_id} on {seen_date}. Skipping insertion.")
        except mysql.connector.Error as err:
            logging.error(f"Error inserting report for user_id {user_id}: {err}")

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

            # Validate and convert the date column
            #df['Date'] = df['Date'].apply(parse_excel_date)

            # Drop rows with invalid or missing dates
            #df = df.dropna(subset=['Date'])

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

                # Insert into 'user_reports' table
                self.insert_user_report(user_id, seen_date)
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
            elif opt in ("-f", "--file"):  # Ensure -f option is working
                self.file_path = arg  # Store the filename passed with -f
                print(f'Using file: {self.file_path}')


if __name__ == '__main__':
    ceres = Ceres()
    ceres.process()