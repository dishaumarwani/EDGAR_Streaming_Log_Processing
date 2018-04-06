import sys
import csv
from datetime import datetime, date, time

#inactivity period
def inactivity_period(arg):
    with open(arg) as st:
        for line in st:
            i = int(line)
        st.close()
    #Session time should range between 1 and 86400
    if(i>1 and i<86400):
        return i
    else:
        print("Incorrect session activity period")

def process_line(session_dict, row, ip_in, date_c_in, time_c_in, current_timestamp):
    ip = row[ip_in]
    date_time_c = row[date_c_in]+' '+row[time_c_in]
    dt = datetime.strptime(date_time_c, "%Y-%m-%d %H:%M:%S")

    #Adds ip address to current active sessions if not exist
    #maintains count of documents requested
    if(ip in curr_active_sessions):
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

    #This will be true after every second
    #All the active sessions will be checked if they have expired
    if(current_timestamp != date_time_c):

        write_output(session_dict, out_file, dt)
        
        #time will be updated after every second
        current_timestamp = date_time_c
    return session_dict, current_timestamp

def write_output(curr_active_sessions, out_file, dt):

    #open file in append mode
    with open(out_file, "a") as fo:
        
        #The ips are will be printed in order of their start time
        #This condition will hold for all the ip that are written if a user session ends
        if(dt !=None):
            for i in list(curr_active_sessions):
                
                #check if the period of inactivity is greater than 2 seconds
                if((dt - curr_active_sessions[i]['start_time_stp']).seconds > 2):
                    
                    #find the total time of session time
                    st = datetime.strptime(curr_active_sessions[i]['start_date'], "%Y-%m-%d %H:%M:%S")
                    session_time = (curr_active_sessions[i]['start_time_stp'] - st).seconds
                    
                    #write the output to the file
                    wr = i+','+curr_active_sessions[i]['start_date']+','+ curr_active_sessions[i]['end_date']+','+str(session_time+1)+',' \
                        +str(curr_active_sessions[i]['count'])+'\n'
                    fo.write(wr)

                    curr_active_sessions.pop(i)

        #This condition holds for ip address when end of file is reached and user session does not end
        else:
            for i in list(curr_active_sessions):

                #find the total time of session time
                st = datetime.strptime(curr_active_sessions[i]['start_date'], "%Y-%m-%d %H:%M:%S")
                session_time = (curr_active_sessions[i]['start_time_stp'] - st).seconds

                #write the output to the file
                wr = i+','+curr_active_sessions[i]['start_date']+','+ curr_active_sessions[i]['end_date']+','+str(session_time+1)+',' \
                        +str(curr_active_sessions[i]['count'])+'\n'
                fo.write(wr)
        fo.close()

if __name__ == "__main__":

    #find session time
    st = inactivity_period(sys.argv[2])
    #input and file will be given as argument
    input_file = sys.argv[1]
    out_file = sys.argv[3]
    
    #open file in read mode
    with open(input_file, "r") as f:

        #extract all the required fields, read it using csv reader to avoid mistaken splitting by commas in the data fields
        #ip,date,time,cik,accession,extention
        header = list(csv.reader([next(f)], delimiter=',', quotechar="'"))[0]
        ip_in = header.index('ip')
        time_c_in = header.index('time')
        date_c_in = header.index('date')
        row = list(csv.reader([next(f)], delimiter=',', quotechar="'"))[0]

        #maintain a dictionary of active sessions
        curr_active_sessions = dict()

        #Initialize the current time to any random integer
        current_timestamp = 0

        try:
            while(row != ''):

                #process each line in file in a streaming manner
                curr_active_sessions, current_timestamp = process_line(curr_active_sessions, row, ip_in, date_c_in, time_c_in, current_timestamp)

                #read the next row
                row = list(csv.reader([next(f)], delimiter=',', quotechar="'"))[0]
        except:
            pass
    f.close()

    #write the remaining logs into file
    write_output(curr_active_sessions, out_file, None)    



