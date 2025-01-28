import subprocess
import pandas as pd
import mysql.connector
import getopt
import sys
import logging
from datetime import datetime, timedelta
from Astroidbelt import DwarfMoon


# def parse_excel_date(date_value): #Defines a function to parse and convert dates from Excel formats
#     if isinstance(date_value, str): #Checks if the input is a string.
#         date_value = date_value.strip()  #Removes any leading or trailing whitespace from the string
#         if len(date_value) == 8:  #Checks if the string has exactly 8 characters, assuming it’s in the format MMDDYYYY.
#             print(f"Text date found: {date_value}") #Logs the detected text date for debugging.
#             return datetime.strptime(date_value, '%m%d%Y').strftime('%m%d%Y') #Converts the string into a datetime object using the specified format
#     elif isinstance(date_value, (int, float)):  #Checks if the input is a number (Excel serial date).
#         try:
#             print(f"Raw Excel date serial number: {date_value}") #Logs the raw Excel serial number.
#
#             if date_value < 1 or date_value > 2958465: #Validates that the serial number falls within a reasonable range for Excel dates.
#                 print(f"Out-of-range Excel serial number detected: {date_value}. Skipping this record.") #debugging text
#                 return None
#
#             excel_base_date = datetime(1899, 12, 30) #Sets the base date for Excel serial numbers (Excel's date system starts from December 30, 1899).
#             excel_date = excel_base_date + timedelta(days=date_value) #Converts the serial number into a datetime object by adding the number of days to the base date.
#
#             print(f"Intermediate Excel date: {excel_date}") #Date debugging
#
#             formatted_date = excel_date.strftime('%m%d%Y') #Formats the converted date into the MMDDYYYY format.
#             print(f"Converted Excel date: {formatted_date}")  # Final print debugging
#             return formatted_date #Returns the final formatted date as a string.
#         except Exception as e: #Catches any error that occurs during the conversion process.
#             print(f"Error converting Excel date: {e}. Skipping record.") #Logs the error and skips the problematic record.
#             return None #Returns None if the input date is invalid.
#
#     return None  #Returns None if the input date is invalid.


class Ceres: #Defines a class named Ceres to manage database connections and file processing.
    def __init__(self): #Defines the constructor for the Ceres class.
        self.db_connection = None #Initializes the database connection object as None.
        self.db_cursor = None #Initializes the database cursor as None.
        self.file_path = None #Initializes the file path for the Excel file as None.

    def connect_to_db(self): #Defines a method to connect to a MySQL database.
        """Establish a connection to the MySQL database."""
        try:
            dwarfmoon = DwarfMoon()

            self.db_connection = mysql.connector.connect( #Creates an instance of the DwarfMoon class to fetch database credentials.
                host=dwarfmoon.getServer(),         # Update with your MySQL server details
                user=dwarfmoon.getUsername(),              # Update with your MySQL username
                password=dwarfmoon.getPassword(),      # Update with your MySQL password
                database=dwarfmoon.getDB()
            )
            self.db_cursor = self.db_connection.cursor() #Creates a cursor object for executing SQL queries.
        except mysql.connector.Error as err: #Catches errors during the connection process.
            print("Error connecting to MySQL:", err)
            exit(1) #Exits the program with an error code if the connection fails.

    def read_excel_and_insert_into_db(self):
        """Read the Excel file and insert data into the database."""
        if not self.file_path:
            print("No file provided to read.")
            return

        try:
            print("Loading Excel file without headers.")
            df = pd.read_excel(self.file_path, header=None)  # Load the Excel file into a pandas DataFrame

            # Inspect the first few rows to see the data format
            print("First few rows from Excel file:")
            print(df.head())

            if df.shape[1] < 4:
                print("Excel file must contain at least 4 columns.")
                return

            # Assign default column names (the 1st column will be used as date, others for user details)
            df.columns = ['Date', 'Name', 'Email', 'Department']

            # Validate and convert the date column
            # df['Date'] = df['Date'].apply(parse_excel_date)

            # Drop rows with invalid or missing dates
            # df = df.dropna(subset=['Date'])

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

    def close_db_connection(self): #Closes the database connection and cursor.
        """Close the database connection."""
        if self.db_cursor:
            self.db_cursor.close()
        if self.db_connection:
            self.db_connection.close()

    def process(self): #Orchestrates the overall workflow (argument parsing, database connection, file processing).
        """Main process for the Ceres class."""
        self.parse_arguments(sys.argv[1:]) #Parses command-line arguments to get the file path.
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