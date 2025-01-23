import subprocess
import csv
import mysql.connector
from datetime import datetime
from Astroidbelt import DwarfMoon

class Ceres:
    def __init__(self):
        self.db_connection = None
        self.db_cursor = None

    def read_csv_and_insert_into_db(self):
        """Read the CSV file and insert its contents into MySQL database."""
        # Open CSV file
        try:
            loaded_records = 0
            # the database will be opened, see README
            # Open the first excel file
            # read the data in, in a loop
            # do the database required steps
            # you may need to use upsert or select and validate, I'm not sure
            # insert/upsert the record
            # after 100 records, commit them
            # print that you are committing 100 records
            # after the loop is finished commit one last time to get any missed records
            # print out the total records committed
            print(f'Total records loaded: {loaded_records}')
        except mysql.connector.Error as err:
            print("Error inserting into database:", err)

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

    def close_file(self, file):
        """Close the file."""
        file.close()

    def process(self):
        """Main process for the Ceres class."""
        self.connect_to_db()
        self.read_csv_and_insert_into_db()
        self.close_db_connection()

if __name__ == '__main__':
    ceres = Ceres()
    ceres.process()
    #buddy.run_batch_file()

