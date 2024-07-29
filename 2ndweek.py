from datetime import datetime, timedelta

date_time_format_string = "%Y-%m-%d %H:%M:%S"

#------- inputs for time calculation 

# old time data ---------------------
old_hold_start_time = datetime.strptime('2024-07-28 18:00:00', date_time_format_string)
old_end_start_time = datetime.strptime('2024-07-28 19:00:00', date_time_format_string)


old_end = datetime.strptime('2024-07-29 09:00:00', date_time_format_string)


working_time_report = datetime.strptime('2024-07-28 09:00:00', date_time_format_string)
completed_time_report = datetime.strptime('2024-07-29 19:00:00', date_time_format_string)
hold_time_start_report = datetime.strptime('2024-07-29 11:00:00', date_time_format_string)
hold_time_end_report = datetime.strptime('2024-07-29 11:15:00', date_time_format_string)
break_time_start_report = datetime.strptime('2024-07-29 13:00:00', date_time_format_string)
break_time_end_report = datetime.strptime('2024-07-29 13:30:00', date_time_format_string)
meeting_time_start_report = None
meeting_time_end_report = None
call_time_start_report = None
call_time_end_report = None
end_time_start_report = None
end_time_end_report = None

current_time_report = datetime.strptime('2024-07-29 15:00:00', date_time_format_string)


# print(working_time_report.time(),'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee')

#---------- first parameter greater time (second lesser time)

def time_difference(time1 , time2):

    if time1 and time2:
        try:
            print(time1,time2,'two times')
            return time1 - time2
        except :
            return timedelta(hours=0)
    elif time1:
        time2 = current_time_report
        try:
            print(time1,current_time_report,'time 2 absent')
            return time1 - current_time_report
        except :
            return timedelta(hours=0)
    else :
        time1 = current_time_report
        try:
            print(time1,current_time_report,'time 1 absent')
            return time1 - current_time_report
        except :
            return timedelta(hours=0)



if completed_time_report:

    if completed_time_report.date() == working_time_report.date():
        
        timei1 = working_time_report
        timei2 = completed_time_report
        timediff_f2 = time_difference(timei2,timei1)
        timei3 = time_difference(hold_time_end_report,hold_time_start_report)
        timei4 = time_difference(break_time_end_report,break_time_start_report)
        timei5 = time_difference(meeting_time_end_report,meeting_time_start_report)
        timei6 = time_difference(call_time_end_report,call_time_start_report)

        Totaltime = timediff_f2 - (timei3+timei4+timei5+timei6)

        print(Totaltime,'same day')
    else:
        timei1 = old_end
        timei2 = completed_time_report
        timediff_f2 = time_difference(timei2,timei1)
        timei3 = time_difference(hold_time_end_report,hold_time_start_report)
        timei4 = time_difference(break_time_end_report,break_time_start_report)
        timei5 = time_difference(meeting_time_end_report,meeting_time_start_report)
        timei6 = time_difference(call_time_end_report,call_time_start_report)

        Totaltime = timediff_f2 - (timei3+timei4+timei5+timei6)

        print(Totaltime,'different day')

elif working_time_report.date()==current_time_report.date():

        timei1 = working_time_report
        timei2 = current_time_report
        timediff_f2 = time_difference(timei2,timei1)
        timei3 = time_difference(hold_time_end_report,hold_time_start_report)
        timei4 = time_difference(break_time_end_report,break_time_start_report)
        timei5 = time_difference(meeting_time_end_report,meeting_time_start_report)
        timei6 = time_difference(call_time_end_report,call_time_start_report)

        Totaltime = timediff_f2 - (timei3+timei4+timei5+timei6)

        print(Totaltime,'sameday not finished')

else :

    if old_hold_start_time and old_end_start_time:
        if (old_hold_start_time > old_end_start_time) :

            timei1 = current_time_report
            timei2 = old_end
            timediff_f2 = time_difference(timei1,timei2)
            print(current_time_report-hold_time_end_report,'---------------------')
            timei3 = time_difference(hold_time_end_report,hold_time_start_report)
            timei4 = time_difference(break_time_end_report,break_time_start_report)
            timei5 = time_difference(meeting_time_end_report,meeting_time_start_report)
            timei6 = time_difference(call_time_end_report,call_time_start_report)
            print(timei3,timei4,timei5,timei6)
            Totaltime = timediff_f2 - (timei3+timei4+timei5+timei6)
            print(Totaltime,'hold')
        
        else:
            timei1 = current_time_report
            timei2 = old_end
        
            timediff_f2 = time_difference(timei1,timei2)
            print(timediff_f2)
            timei3 = time_difference(hold_time_end_report,hold_time_start_report)
            timei4 = time_difference(break_time_end_report,break_time_start_report)
            timei5 = time_difference(meeting_time_end_report,meeting_time_start_report)
            timei6 = time_difference(call_time_end_report,call_time_start_report)
            
            Totaltime = timediff_f2 - (timei3+timei4+timei5+timei6)
            print(Totaltime,'end')
    elif old_end_start_time:
        
        timei1 = current_time_report
        timei2 = old_end
     
        timediff_f2 = time_difference(timei1,timei2)
        print(timediff_f2)
        timei3 = time_difference(hold_time_end_report,hold_time_start_report)
        timei4 = time_difference(break_time_end_report,break_time_start_report)
        timei5 = time_difference(meeting_time_end_report,meeting_time_start_report)
        timei6 = time_difference(call_time_end_report,call_time_start_report)
        
        Totaltime = timediff_f2 - (timei3+timei4+timei5+timei6)
        print(Totaltime,'end')
    elif old_hold_start_time:
            timei1 = current_time_report
            timei2 = old_end
            timediff_f2 = time_difference(timei1,timei2)
            print(current_time_report-hold_time_end_report,'---------------------')
            timei3 = time_difference(hold_time_end_report,hold_time_start_report)
            timei4 = time_difference(break_time_end_report,break_time_start_report)
            timei5 = time_difference(meeting_time_end_report,meeting_time_start_report)
            timei6 = time_difference(call_time_end_report,call_time_start_report)
            print(timei3,timei4,timei5,timei6)
            Totaltime = timediff_f2 - (timei3+timei4+timei5+timei6)
            print(Totaltime,'hold')
