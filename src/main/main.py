import csv
import sqlite3

# Connect to the SQLite in-memory database
conn = sqlite3.connect(':memory:')

# A cursor object to execute SQL commands
cursor = conn.cursor()


def main():

    # users table
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        userId INTEGER PRIMARY KEY,
                        firstName TEXT,
                        lastName TEXT
                      )'''
                   )

    # callLogs table (with FK to users table)
    cursor.execute('''CREATE TABLE IF NOT EXISTS callLogs (
        callId INTEGER PRIMARY KEY,
        phoneNumber TEXT,
        startTime INTEGER,
        endTime INTEGER,
        direction TEXT,
        userId INTEGER,
        FOREIGN KEY (userId) REFERENCES users(userId)
    )''')

    # You will implement these methods below. They just print TO-DO messages for now.
    load_and_clean_users('../../resources/users.csv')
    load_and_clean_call_logs('../../resources/callLogs.csv')
    write_user_analytics('../../resources/userAnalytics.csv')
    write_ordered_calls('../../resources/orderedCalls.csv')

    # Helper method that prints the contents of the users and callLogs tables. Uncomment to see data.
    # select_from_users_and_call_logs()

    # Close the cursor and connection. main function ends here.
    cursor.close()
    conn.close()


# TODO: Implement the following 4 functions. The functions must pass the unit tests to complete the project.


# This function will load the users.csv file into the users table, discarding any records with incomplete data
def load_and_clean_users(file_path):
    with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if(len(row)==2):
                    first_name = row.get('firstName').strip()
                    last_name = row.get('lastName').strip()

                    if not first_name or not last_name:
                        continue

                    qr1="""insert into users (firstName, lastName) values(?, ?) """
                    cursor.execute(qr1,(first_name, last_name))



# This function will load the callLogs.csv file into the callLogs table, discarding any records with incomplete data
def load_and_clean_call_logs(file_path):
    with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                phone_number = row.get('phoneNumber').strip()
                start_time = row.get('startTime').strip()
                end_time = row.get('endTime').strip()
                direction = row.get('direction').strip()
                user_id = row.get('userId').strip()

                if not (phone_number and start_time.isdigit() and end_time.isdigit() and direction and user_id.isdigit()):
                    continue

                qr2="""insert into callLogs (phoneNumber, startTime, endTime, direction, userId) values (?, ?, ?, ?, ?)"""
                cursor.execute(qr2, (phone_number, int(start_time), int(end_time), direction, int(user_id)))


# This function will write analytics data to testUserAnalytics.csv - average call time, and number of calls per user.
# You must save records consisting of each userId, avgDuration, and numCalls
# example: 1,105.0,4 - where 1 is the userId, 105.0 is the avgDuration, and 4 is the numCalls.
def write_user_analytics(csv_file_path):
    user_analytics_data = []
    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['userId', 'avgDuration', 'numCalls']) 

        qr3="""select userId, avg(endTime - startTime) 
        as avgDuration, count(callId) 
        as numCalls from callLogs group by userId"""
        cursor.execute(qr3)
        for row in cursor.fetchall():
            writer.writerow(row)
            user_analytics_data.append(row)  


# This function will write the callLogs ordered by userId, then start time.
# Then, write the ordered callLogs to orderedCalls.csv
def write_ordered_calls(csv_file_path):
    ordered_calls_data = []
    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['callId', 'phoneNumber', 'startTime', 'endTime', 'direction', 'userId'])

        qr4="""select callId, phoneNumber, startTime, endTime, direction, userId
            from callLogs
            order by userId, startTime"""

        cursor.execute(qr4)
        for row in cursor.fetchall():
            writer.writerow(row)
            ordered_calls_data.append(row)



# No need to touch the functions below!------------------------------------------

# This function is for debugs/validation - uncomment the function invocation in main() to see the data in the database.
def select_from_users_and_call_logs():

    print()
    print("PRINTING DATA FROM USERS")
    print("-------------------------")

    # Select and print users data
    cursor.execute('''SELECT * FROM users''')
    for row in cursor:
        print(row)

    # new line
    print()
    print("PRINTING DATA FROM CALLLOGS")
    print("-------------------------")

    # Select and print callLogs data
    cursor.execute('''SELECT * FROM callLogs''')
    for row in cursor:
        print(row)


def return_cursor():
    return cursor


if __name__ == '__main__':
    main()
