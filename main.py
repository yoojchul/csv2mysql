import csv2recap
import csv2mysql

# --- Configuration ---
DIRECTORY_PATH = "/var/lib/mysql-files/seoul_transport"  
# "LOAD DATA LOCAL INFILE" makes it easy to change the dirctory. But it doesn't check an error truncation of VARCHAR. 
# "LOAD DATA INFILE" needs "/var/lib/mysql-files".

if __name__ == "__main__":
    csv2recap.recap_csv_files(DIRECTORY_PATH)
    csv2mysql.process_directory(DIRECTORY_PATH)

