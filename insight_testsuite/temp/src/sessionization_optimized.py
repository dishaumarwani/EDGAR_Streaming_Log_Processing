import sys
import csv
from datetime import datetime, date, time

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


#check if any timestamp has expired
def expired_timestamp(curr_active_session, current_timestamp, st):
    for time_stamp in list(curr_active_session):
        if((current_timestamp - time_stamp).seconds  > st):
            return time_stamp

#check if ip already present in a timestamp
def find_ip_address(current_active_session, ip):
    for i in list(current_active_session):
        if(ip in list(current_active_session[i])):
            return i
    return None

#writing the output to file at the end of an active session
def write_output(curr_active_sessions,out_file, et=None):
    with open(out_file, "a") as fo:
        if(et):
            for i in list(curr_act_sessions[et]):
                session_time = (parse_time(curr_act_sessions[et][i]['end_time'])\
                                -parse_time(curr_act_sessions[et][i]['start_time'])).seconds + 1
                wr = i+','+ curr_act_sessions[et][i]['start_time']+ ','+\
                curr_act_sessions[et][i]['end_time']+','+str(session_time)+','+str(curr_act_sessions[et][i]['count'])+'\n'
                fo.write(wr)
            curr_act_sessions.pop(et)        
        else:        
            for i in list(curr_act_sessions):
                for j in list(curr_act_sessions[i]):
                    session_time = (parse_time(curr_act_sessions[i][j]['end_time'])\
                                    -parse_time(curr_act_sessions[i][j]['start_time'])).seconds + 1
                    wr = j+','+ curr_act_sessions[i][j]['start_time']+','+\
                    curr_act_sessions[i][j]['end_time']+','+ str(session_time)+','+\
                    str(curr_act_sessions[i][j]['count'])+'\n'
                    fo.write(wr)
            curr_act_sessions.pop(i)
    fo.close()

def process_line(session_dict, row, ip, date, time, current_timestamp):
    #extract all the required fields
    ip = row[ip]
    date_time_c = row[date]+' '+row[time]
    dt = parse_time(date_time_c)

    #checks in both the timestamps if ip address is present
    tm = find_ip_address(curr_act_sessions, ip)

    if(tm != None):    
        curr_act_sessions[tm][ip]['count']  += 1
        curr_act_sessions[tm][ip]['end_time'] = date_time_c
        if(tm != dt):
            if(current_timestamp != dt):
                curr_act_sessions[dt] = {}
            curr_act_sessions[dt][ip] = curr_act_sessions[tm][ip]
            curr_act_sessions[tm].pop(ip)        
    else:
        if(current_timestamp != dt):
            curr_act_sessions[dt] = {}
        curr_act_sessions[dt][ip] = {}
        curr_act_sessions[dt][ip]['start_time'] = date_time_c
        curr_act_sessions[dt][ip]['end_time'] = date_time_c
        curr_act_sessions[dt][ip]['count'] = 1

    #This will be true after every second
    #All the timestamps mainly previous two will be compared if they have expired.
    if(current_timestamp != dt):
        et = expired_timestamp(curr_act_sessions, dt, st)
        if(et):
            write_output(curr_act_sessions,out_file, et)
        current_timestamp = dt
        
    return curr_act_sessions, current_timestamp

if __name__ == "__main__":
    st = inactivity_period(sys.argv[2])
    input_file = sys.argv[1]
    out_file = sys.argv[3]
    with open(input_file, "r") as f:
        header = next(f).split(',')
        ip = header.index('ip')
        time = header.index('time')
        date = header.index('date')
        row = next(f).split(',')
        curr_act_sessions = dict()
        current_timestamp = 0
        row_next = row
        try:
            while(row_next):
                row_next = next(f).split(',')
                curr_act_sessions, current_timestamp = process_line(curr_act_sessions, row, ip, date, time, current_timestamp)
                row = row_next
        except:
            curr_act_sessions, current_timestamp = process_line(curr_act_sessions, row, ip, date, time, current_timestamp)
            f.close()
    write_output(curr_act_sessions, out_file, None)
