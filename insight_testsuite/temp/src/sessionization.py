import sys
import csv
from datetime import datetime, date, time

def inactivity_period(arg):
    """
        :param argv: file_name as string
        :return: session time as int
    """
    try:
        with open(arg, 'r') as f:
            i = int(next(f))
            f.close()
        #Session time should range between 1 and 86400
        if(i>1 and i<86400):
            return i
        else:
            return "Incorrect session activity period"
    except IOError:
        return "There was an error opening the file!"

def parse_time(tm):
    """
        :param tm: time as string
        :return: time as datetime object
    """
    try:
        tm = datetime.strptime(tm, "%Y-%m-%d %H:%M:%S")
    except:
        try:
            tm = datetime.strptime(tm, "%y-%m-%d %H:%M:%S")
        except:
            try:
                tm = datetime.strptime(tm, "%m-%d-%y %H:%M:%S")
            except:
                tm = datetime.datetime.now()
    return tm

def time_difference(start, end):
    """
        Finds difference between two datetime objects in seconds
        :param start: datetime object
        :param end: datetime object
        :return: time in seconds as int

    """
    sec = (end - start).seconds
    return sec


def process_line(session_dict, row, ip_in, date_c_in, time_c_in, current_timestamp, st):
    """
        processes each line in the document
        1. extracts the required fields

        2. checks if current_timestamp is not equal to time in current log
            if not equal calls the write function
        
        3. checks if the ip address is already present in active sessions
            updates count and start_time_stp

        4. creates a new ip key if ip not in active session dictionary

        :param session_dict: associative array/dictionary having records of all active sessions
        :param row: list of values to be processed
        :param ip_in: index of the ip address
        :param date_c_in: index of date
        :param time_c_in: index of time
        :param current_timestamp: value of date time of last timestamp
        :return session_dict: associative array of updated session_dict with active sessions
    """
    ip = row[ip_in]
    date_time_c = row[date_c_in]+' '+row[time_c_in]
    dt = parse_time(date_time_c)

    #This will be true after every second
    #All the active sessions will be checked if they have expired
    if(current_timestamp != date_time_c):
        write_output(session_dict, out_file, dt, st)
        
        #time will be updated after every second
        current_timestamp = date_time_c

    #Adds ip address to current active sessions if not exist
    #maintains count of documents requested
    if(ip in active_sessions):
        #maintain a count of each time an ip makes a request
        session_dict[ip]['count'] += 1

        #update the most recent session time
        session_dict[ip]['start_time_stp'] = dt

    else:
        #if the ip session is starting
        session_dict[ip] = {}

        #maintain a count variable for no. of requests
        session_dict[ip]['count'] = 1

        #session start time
        session_dict[ip]['start_date'] = date_time_c

        #this is updated each time session renews
        session_dict[ip]['start_time_stp'] = dt

        #this is updates each time a session renews
        session_dict[ip]['end_date'] = date_time_c


    return session_dict, current_timestamp

def write_output(active_sessions, out_file, dt, st):
    """
       :param active_sessions: associative array/dictionary of all active ip sessions
       :param out_file: filepath name as string
       :param dt current time
    """
    #open file in append mode
    with open(out_file, "a") as fo:

        #The ips are will be printed in order of their start time
        #This condition will hold for all the ip that are written if a user session ends
        if(dt !=None):
            for i in list(active_sessions):
                
                #check if the period of inactivity is greater than 2 seconds

                if(time_difference(active_sessions[i]['start_time_stp'], dt) > st):
                    
                    #find the total time of session time
                    ses_end_time = parse_time(active_sessions[i]['start_date'])
                    session_time = time_difference(ses_end_time, active_sessions[i]['start_time_stp'])
                    
                    
                    #write the output to the file
                    wr = i+','+active_sessions[i]['start_date']+','+ active_sessions[i]['end_date']+','+str(session_time+1)+',' \
                        +str(active_sessions[i]['count'])+'\n'
                    fo.write(wr)

                    #remove the ip from current active session
                    active_sessions.pop(i)

        #This condition holds for ip address when end of file is reached and user session does not end
        else:
            for i in list(active_sessions):

                #find the total time of session time
                ses_end_time = parse_time(active_sessions[i]['start_date'])
                session_time = time_difference(ses_end_time, active_sessions[i]['start_time_stp'])

                #write the output to the file
                wr = i+','+active_sessions[i]['start_date']+','+ active_sessions[i]['end_date']+','+str(session_time+1)+',' \
                        +str(active_sessions[i]['count'])+'\n'
                fo.write(wr)
        fo.close()

if __name__ == "__main__":

    #find session time
    st = inactivity_period(sys.argv[2])
    if(type(st) == int):

        #input and file will be given as argument
        input_file = sys.argv[1]
        out_file = sys.argv[3]
        try:
            #open file in read mode
            with open(input_file, "r") as f:

                #extract all the required fields, read it using csv reader to avoid mistaken splitting by commas in the data fields
                #ip,date,time,cik,accession,extention
                header = list(csv.reader([next(f)], delimiter=',', quotechar="'"))[0]
                ip_in = header.index('ip')
                time_c_in = header.index('time')
                date_c_in = header.index('date')
                row = list(csv.reader([next(f)], delimiter=',', quotechar="'"))[0]
                row_len = len(row)
                #maintain a dictionary of active sessions
                active_sessions = dict()

                #Initialize the current time to any random integer
                current_timestamp = 0

                try:
                    while(row != ''):
                        #checks if the length of row is same as header lenghth else does not process row
                        if(len(row) == row_len):
                            #process each line in file in a streaming manner
                            active_sessions, current_timestamp = process_line(active_sessions, row, ip_in, date_c_in, time_c_in, current_timestamp, st)

                            #read the next row
                            row = list(csv.reader([next(f)], delimiter=',', quotechar="'"))[0]
                        else:
                            pass
                except:
                    pass
            f.close()
            #write the remaining logs into file
            write_output(active_sessions, out_file, None, st)
        except IOError:
            print("There was an error opening input file!")
    else:
        print(st)    



