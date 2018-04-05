import sys
import csv
from datetime import datetime, date, time

#inactivity period
def inactivity_period(arg):
    with open(arg) as st:
        for line in st:
            i = int(line)
    st.close()
    if(i>1 and i<86400):
        return i
    else:
        print("Incorrect session activity period")


#check if any timestamp has expired
def expired_timestamp(curr_active_session, current_timestamp, st):
    for time_stamp in list(curr_active_session):
        if((current_timestamp - time_stamp).seconds >= st):
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
            print(curr_act_sessions[et])
            for i in list(curr_act_sessions[et]):
                session_time = (datetime.strptime(curr_act_sessions[et][i]['end_time'], "%Y-%m-%d %H:%M:%S")\
                                -datetime.strptime(curr_act_sessions[et][i]['start_time'], "%Y-%m-%d %H:%M:%S")).seconds + 1
                wr = i+','+ curr_act_sessions[et][i]['start_time']+ ','+\
                curr_act_sessions[et][i]['end_time']+','+str(session_time)+','+str(curr_act_sessions[et][i]['count'])+'\n'
                fo.write(wr)

            curr_act_sessions.pop(et)        
        else:        
            for i in list(curr_act_sessions):
                for j in list(curr_act_sessions[i]):
                    session_time = (datetime.strptime(curr_act_sessions[i][j]['end_time'], "%Y-%m-%d %H:%M:%S")\
                                    -datetime.strptime(curr_act_sessions[i][j]['start_time'], "%Y-%m-%d %H:%M:%S")).seconds + 1
                    wr = j+','+ curr_act_sessions[i][j]['start_time']+','+\
                    curr_act_sessions[i][j]['end_time']+','+ str(session_time)+','+\
                    str(curr_act_sessions[i][j]['count'])+'\n'
                    fo.write(wr)
            curr_act_sessions.pop(i)
    fo.close()
        
if __name__ == "__main__":
    st = inactivity_period(sys.argv[2])
    input_file = sys.argv[1]
    out_file = sys.argv[3]
    with open(input_file, "r") as f:
        next(f)
        spamreader = csv.reader(f, delimiter=',', quotechar="'")
        curr_act_sessions = dict()
        current_timestamp = 0
        
        for row in spamreader:
            #extract all the required fields
            ip = row[0]
            date_time_c = row[1]+' '+row[2]
            dt = datetime.strptime(date_time_c, "%Y-%m-%d %H:%M:%S")
            document = str(row[4])+' '+str(row[5])+' '+str(row[6])
            
            #checks in both the timestamps if ip address is present
            tm = find_ip_address(curr_act_sessions, ip)

            if(tm != None):    
                curr_act_sessions[tm][ip]['count'] += 1
                curr_act_sessions[tm][ip]['end_time'] = date_time_c
                if(tm != dt):
                    if(current_timestamp != dt):
                        if(dt not in curr_act_sessions):
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
        f.close()
    write_output(curr_act_sessions, out_file, None)
