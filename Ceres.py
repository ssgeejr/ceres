import subprocess
import pandas as pd

print(pd.__version__)
import csv
import mysql.connector
import getopt
import sys
from datetime import datetime, timedelta
from Astroidbelt import DwarfMoon


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
        # (Database connection code remains the same)

    def close_db_connection(self):
        """Close the database connection."""
        # (Closing code remains the same)

    def read_excel_and_insert_into_db(self):
        """Read the Excel file and insert into the database."""
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

            df.columns = ['Date', 'Name', 'Email', 'Department']

            # Apply the parsing function to the 'Date' column
            df['Date'] = df['Date'].apply(parse_excel_date)

            # Drop rows with invalid or missing dates
            df = df.dropna(subset=['Date'])

            # Database insertion code follows...

        except FileNotFoundError:
            print(f"File not found: {self.file_path}")
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

