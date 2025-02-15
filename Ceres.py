import pandas as pd
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
        self.new_records_count = 0
        self.duplicate_records_count = 0

    def connect_to_db(self):
        try:
            dwarfmoon = DwarfMoon()
            self.db_connection = mysql.connector.connect(
                host=dwarfmoon.getServer(),
                user=dwarfmoon.getUsername(),
                password=dwarfmoon.getPassword(),
                database=dwarfmoon.getDB()
            )
            self.db_cursor = self.db_connection.cursor()
        except mysql.connector.Error as err:
            print("Error connecting to MySQL:", err)
            exit(1)

    def read_excel_and_insert_into_db(self):
        if not self.file_path:
            print("No file provided to read.")
            return

        try:
            df = pd.read_excel(self.file_path, header=None)
            if df.shape[1] < 4:
                print("Excel file must contain at least 4 columns.")
                return
            df.columns = ['Date', 'Name', 'Email', 'Department']

            for _, row in df.iterrows():
                date, name, email, department = row['Date'], row['Name'], row['Email'], row['Department']
                self.db_cursor.execute("SELECT user_id FROM users WHERE email = %s", (email,))
                result = self.db_cursor.fetchone()

                if result:
                    user_id = result[0]
                    self.duplicate_records_count += 1  # Increment duplicate counter
                else:
                    self.db_cursor.execute(
                        "INSERT INTO users (name, email, department) VALUES (%s, %s, %s)",
                        (name, email, department)
                    )
                    user_id = self.db_cursor.lastrowid
                    self.new_records_count += 1  # Increment new record counter

                try:
                    # Ensure the date is padded to exactly 8 characters (MMDDYYYY)
                    seen_date = str(date).zfill(8)

                    # If the date length is still less than 8, it is invalid and should be skipped
                    if len(seen_date) != 8:
                        continue

                    # Convert the seen_date into MMDDYYYY format
                    seen_date_display = datetime.strptime(seen_date, "%m%d%Y").strftime("%m-%d-%Y")

                    # Using STR_TO_DATE and LPAD directly in the SQL statement
                    self.db_cursor.execute(
                        "INSERT INTO user_reports (user_id, seen_date) "
                        "VALUES (%s, STR_TO_DATE(LPAD(%s, 8, '0'), '%m%d%Y'))",
                        (user_id, seen_date)  # Pass both user_id and seen_date as parameters
                    )

                except ValueError:
                    continue

            self.db_connection.commit()
            print(f"New records: {self.new_records_count}")
            print(f"Duplicate records: {self.duplicate_records_count}")

        except Exception as e:
            print(f"Error: {e}")

    def close_db_connection(self):
        if self.db_cursor:
            self.db_cursor.close()
        if self.db_connection:
            self.db_connection.close()

    def process(self):
        self.parse_arguments(sys.argv[1:])
        self.connect_to_db()
        self.read_excel_and_insert_into_db()
        self.close_db_connection()

    def parse_arguments(self, argv):
        try:
            opts, _ = getopt.getopt(argv, "hf:", ["file="])
        except getopt.GetoptError as e:
            print(f'ERROR: {e}')
            sys.exit(2)

        for opt, arg in opts:
            if opt in ("-f", "--file"):
                self.file_path = arg
                print(f'Using file: {self.file_path}')


if __name__ == '__main__':
    Ceres().process()

