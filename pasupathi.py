

    elif reportoptions == "scope_subscope_natureofwork":

            finalre = {
                'estimated_time_with_add' : pendulum.duration(),
                'date': set(),
                'user': set(),
                'Service_ID': set(),
                'scope': set(),
                'created_at' : set(),
                'Completed_date' : set(),
                'subscopes': set(),
                'entity': set(),
                # 'no_of_entity' : set(),
                'status': set(),
                'type_of_activity': set(),
                'Nature_of_Work': set(),
                'gst_tan': set(),
                'estimated_d_o_d': set(),
                'estimated_time': set(),
                'member_name': set(),
                'end_time': pendulum.duration(),
                'hold': pendulum.duration(),
                'break': pendulum.duration(),
                'time_diff_work': pendulum.duration(),
                'call': pendulum.duration(),
                'meeting': pendulum.duration(),
                'in_progress': pendulum.duration(),
                'completed': pendulum.duration(),
                'third_report_data' : set(),
                'fourth_report' :  set(),
                'fourth_report2' : set(),
                'fifth_report' : set(),
                'no_of_items' : set(),
                'chargable' : set(),
                'non-chargable' : set(),
                'total-time' : set(),
                'idealname' : pendulum.duration()
            }

            date_time_formate_string = '%Y-%m-%d %H:%M:%S'
            list_data = []
            result_data = []
            


            d1 = picked_date
            d2 = to_date

            # Convert strings to datetime objects
            start_date = datetime.strptime(d1, '%Y-%m-%d')
            end_date = datetime.strptime(d2, '%Y-%m-%d')

            # Generate all dates in between and store as strings
            dates_list = []
            current_date = start_date

            while current_date <= end_date:
                
                dates_list.append(current_date.strftime('%Y-%m-%d'))
                current_date += timedelta(days=1)
                

            # dates_list contains all dates as strings
            # print(dates_list)
            for item in dates_list:
                # print(item)
                print('db')
                print(db)
                print('item')
                print(item)
                print('reportoptions')
                print(reportoptions)
                reportoptions="twenty"
                list_data.append(totaltime.user_wise_report(db,item,reportoptions))

                            
            list_data = [item for item in list_data if item]

            common =  set()
            print("list_data")
            print(list_data)  
            for report_list in list_data:
                for entry in report_list:
                        my_set = {str(x) for x in entry['Service_ID']} 
                        
                        common.add(my_set.pop())

                                
            for finalitems in common:
                for report_list in list_data:
                    for entry in report_list:
                        if int(finalitems) in entry['Service_ID']:
                            for key in finalre.keys():
                                        if key == 'end_time':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'estimated_time_with_add':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'hold':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'break':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'time_diff_work':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'call':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'meeting':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'in_progress':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'completed':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'idealname':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'no_of_items':
                                            try:
                                                
                                            
                                                finalre[key] = [finalre[key].pop()]+int(entry[key].pop())
                                                
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'chargable':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'non-chargable':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'total-time':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        else:
                                            finalre[key] = entry[key].union(finalre[key])

                result = {
                    'estimated_time_with_add' : set(),
                    'work_started_date' : set(),
                    'work_ended_date' : set(),
                    'number_of_days_taken' : set(),
                    'number_of_days_delayed' : set(),
                    'actual_date_of_delivery' : set(),
                    'estimated_date_of_delivery' : set(),
                    'number_of_entity' : set(),
                    'estimated_time_minus_chargable_time' : set(),
                    'date': set(),
                    'user': set(),
                    'Service_ID': set(),
                    'created_at' : set(),
                    'Completed_date' : set(),
                    'scope': set(),
                    'subscopes': set(),
                    'entity': set(),
                    # 'no_of_entity' : set(),
                    'status': set(),
                    'type_of_activity': set(),
                    'Nature_of_Work': set(),
                    'gst_tan': set(),
                    'estimated_d_o_d': set(),
                    'estimated_time': set(),
                    'member_name': set(),
                    'end_time': set(),
                    'hold': set(),
                    'break': set(),
                    'time_diff_work': set(),
                    'call': set(),
                    'meeting': set(),
                    'in_progress': set(),
                    'completed': set(),
                    'third_report_data' : set(),
                    'fourth_report' :  set(),
                    'fourth_report2' : set(),
                    'fifth_report' : set(),
                    'no_of_items' : set(),
                    'chargable' : set(),
                    'non-chargable' : set(),
                    'total-time' : set(),
                    'idealname' : set()
                }
                for key in finalre:
                    if isinstance(finalre[key], set):

                            cpof = finalre[key]
                            result[key]= cpof
                        
                            finalre[key] = set()

                    elif isinstance(finalre[key], int):
                        result[key] = finalre[key]
                        #   print(finalre[key],'hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh')
                    else:
                    
                        result[key].add(convert_to_duration(finalre[key]))
                        finalre[key] = pendulum.duration()
                


        #--------------------------- last calculation


                        

                # Define the set of date strings
                date_strings = result['date']
            
                # Convert the date strings to datetime objects
                dateslast = {datetime.strptime(date, "%Y-%m-%d").date() for date in date_strings}

                # Find the maximum date
                max_date = max(dateslast)

                min_date = min(dateslast)




                # Define the set of date strings
                date_strings_date = result['estimated_d_o_d']

                # Convert the date strings to datetime objects
                dateslast_date = {datetime.strptime(date, "%Y-%m-%d").date() for date in date_strings_date}

                # Find the maximum date
                max_date_in_dates = max(dateslast_date)

                        


        #--------------------------- last calculation


                dateesti =  max_date_in_dates.strftime("%Y-%m-%d")

                # Define the set of date strings
                date_strings = result['date']

                # Convert dateesti to a datetime object
                dateesti_dt = datetime.strptime(dateesti, "%Y-%m-%d").date()

                # Convert the set of date strings to a set of datetime objects
                dates = {datetime.strptime(date, "%Y-%m-%d").date() for date in date_strings}

                # Find and count the dates that are greater than dateesti_dt
                greater_dates = {date for date in dates if date > dateesti_dt}
                count_greater_dates = len(greater_dates)

                    

        #-----------------------------getting estimate time
                        

                estichar = result['estimated_time_with_add'].pop()
                hourses, minuteses, secondses = map(int, estichar.split(':'))

                # Create a pendulum Duration object
                durationes = pendulum.duration(hours=hourses, minutes=minuteses, seconds=secondses)

        #----------------------------------------- chargable time


                nchar = result['chargable'].pop()

                hours, minutes, seconds = map(int, nchar.split(':'))

                # Create a pendulum Duration object
                duration = pendulum.duration(hours=hours, minutes=minutes, seconds=seconds)
                # print(durationes-duration,'hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh')
#------------------------ late of date estimated
                result['chargable'] = nchar
                result['estimated_time_with_add'] = estichar
                result['work_started_date'] =  min_date
                result['work_ended_date'] = max_date
                result['number_of_days_taken'] = len(result['date'])
                result['number_of_days_delayed'] = count_greater_dates
                result['actual_date_of_delivery'] = max_date_in_dates 
                result['estimated_date_of_delivery'] = max_date_in_dates
                result['number_of_entity'] = len(result['entity'])
                result['estimated_time_minus_chargable_time'] = convert_to_duration(durationes-duration)

        #------------------------------ code end
                result_data.append(result)
                result = {}
                # print("result starts here =>")
                # print(result_data)
                # print("result end here =>")
            # CODE FOR COMBINING TOTAL-TIME STARTS HERE
            newObj1 = {}

            # print("hai")
            # print(result_data)
            
            for item in result_data: 
                #print(item)    

                # print("single item starts ")    

                # print(item)

                # print("single item ends ")    
                tempGroup = ''
                
                for scope in item['scope']:
                    tempGroup+=scope

                for subscopes in item['subscopes']:
                    tempGroup+=subscopes

                for Nature_of_Work in item['Nature_of_Work']:
                    tempGroup+=Nature_of_Work

                key_to_check = tempGroup

                if key_to_check in newObj1:
                    if isinstance(item['user'], set):   
                        newObj1[tempGroup]['user'].update(item['user'])  # Update with a set of users
                    else:
                        newObj1[tempGroup]['user'].add(item['user'])  # Add a single user

                    oldETWA = newObj1[tempGroup]['estimated_time_with_add']
                    currentETWA = item['estimated_time_with_add']

                    oldendtime = newObj1[tempGroup]['end_time']
                    currentendtime = item['end_time']

                    oldhold = newObj1[tempGroup]['hold']
                    currenthold = item['hold']

                    oldbreak = newObj1[tempGroup]['break']
                    currentbreak = item['break']

                    oldTDW = newObj1[tempGroup]['time_diff_work']
                    currentTDW = item['time_diff_work']

                    oldcall = newObj1[tempGroup]['call']
                    currentcall = item['call']

                    oldmeeting = newObj1[tempGroup]['meeting']
                    currentmeeting = item['meeting']

                    oldInProgress = newObj1[tempGroup]['in_progress']
                    currentInProgress = item['in_progress']

                    oldcompleted = newObj1[tempGroup]['completed']
                    currentcompleted = item['completed']

                    oldnoofitems = newObj1[tempGroup]['no_of_items']
                    currentnoofitems = item['no_of_items']

                    oldTime = newObj1[tempGroup]['total-time']
                    curentTime = item['total-time']
                    #--------------------------------------------------------------------------------------
                    #--------------------------Total-estimated_time_with_add-------------------------------
                    ETWAToAdd = {
                        'oldETWA': oldETWA,
                        'currentETWA' : currentETWA
                    }
                    print(oldETWA)
                    print(currentETWA)

                    total_ETWA = timedelta()

                    for times in ETWAToAdd:
                        time_value = ETWAToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)  # Add time to the set

                                # time_value = ""
                                # time_value += time
                        else:
                            time_values = time_value
                        
                    total_ETWA += time_str_to_timedelta(time_values)

                    total_ETWA_str1 = str(total_ETWA)

                    newObj1[tempGroup]['estimated_time_with_add'] = total_ETWA_str1 
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-end_time---------------------------------------------------
                    endtimeToAdd = {
                        'oldendtime': oldendtime,
                        'currentendtime' : currentendtime
                    }

                    print(oldendtime)
                    print(currentendtime)

                    total_endtime = timedelta()

                    for times in endtimeToAdd:
                        time_value = endtimeToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)  # Add time to the set

                                # time_value = ""
                                # time_value += time
                        else:
                            time_values = time_value
                        
                    total_endtime += time_str_to_timedelta(time_values)

                    total_endtime_str1 = str(total_endtime)

                    newObj1[tempGroup]['end_time'] = total_endtime_str1 
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-hold---------------------------------------------------
                    holdToAdd = {
                        'oldhold': oldhold,
                        'currenthold' : currenthold
                    }

                    print(oldhold)
                    print(currenthold)

                    total_hold = timedelta()

                    for times in holdToAdd:
                        time_value = holdToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)  # Add time to the set

                                # time_value = ""
                                # time_value += time
                        else:
                            time_values = time_value
                        
                    total_hold += time_str_to_timedelta(time_values)

                    total_hold_str1 = str(total_hold)

                    newObj1[tempGroup]['hold'] = total_hold_str1 
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-break------------------------------------------------------

                    breakToAdd = {
                        'oldbreak': oldbreak,
                        
                        'currentbreak' : currentbreak
                    }

                    print(oldbreak)
                    print(currentbreak)

                    total_break = timedelta()

                    for times in breakToAdd:
                        time_value = breakToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)  # Add time to the set

                                # time_value = ""
                                # time_value += time
                        else:
                            time_values = time_value
                        
                    total_break += time_str_to_timedelta(time_values)

                    total_break_str1 = str(total_break)

                    newObj1[tempGroup]['break'] = total_break_str1 
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-time_diff_work------------------------------------------------------

                    TDWToAdd = {
                        'oldTDW': oldTDW,
                        'currentTDW' : currentTDW
                    }

                    print(oldTDW)
                    print(currentTDW)

                    total_TDW = timedelta()

                    for times in TDWToAdd:
                        time_value = TDWToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)  # Add time to the set

                                # time_value = ""
                                # time_value += time
                        else:
                            time_values = time_value
                        
                    total_TDW += time_str_to_timedelta(time_values)

                    total_TDW_str1 = str(total_TDW)

                    newObj1[tempGroup]['time_diff_work'] = total_TDW_str1 
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-call------------------------------------------------------

                    callToAdd = {
                        'oldcall': oldcall,
                        'currentcall' : currentcall
                    }

                    print(oldcall)
                    print(currentcall)

                    total_call = timedelta()

                    for times in callToAdd:
                        time_value = callToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)  # Add time to the set

                                # time_value = ""
                                # time_value += time
                        else:
                            time_values = time_value

                        
                    total_call += time_str_to_timedelta(time_values)

                    total_call_str1 = str(total_call)

                    newObj1[tempGroup]['call'] = total_call_str1 
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-meeting------------------------------------------------------
                    meetingToAdd = {
                        'oldmeeting': oldmeeting,
                        'currentmeeting' : currentmeeting
                    }

                    print(oldmeeting)
                    print(currentmeeting)

                    total_meeting = timedelta()

                    for times in meetingToAdd:
                        time_value = meetingToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)  # Add time to the set

                                # time_value = ""
                                # time_value += time
                        else:
                            time_values = time_value
                        
                    total_meeting += time_str_to_timedelta(time_values)

                    total_meeting_str1 = str(total_meeting)

                    newObj1[tempGroup]['meeting'] =  total_meeting_str1
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-in_progress------------------------------------------------------

                    Inprogresstoadd = {
                        'oldInProgress': oldInProgress,
                        'currentInProgress' : currentInProgress
                    }

                    print(oldInProgress)
                    print(currentInProgress)

                    total_Inprogresstime = timedelta()

                    for times in Inprogresstoadd:
                        time_value = Inprogresstoadd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)  # Add time to the set

                                # time_value = ""
                                # time_value += time
                        else:
                            time_values = time_value
                        
                    total_Inprogresstime += time_str_to_timedelta(time_values)

                    total_time_str1 = str(total_Inprogresstime)

                    newObj1[tempGroup]['in_progress'] = total_time_str1 
                    #---------------------------------------------------------------------------------------
                    #-------------------------Total-completed------------------------------------------------------
                    completedToAdd = {
                        'oldcompleted': oldcompleted,
                        'currentcompleted' : currentcompleted

                    }

                    print(oldcompleted)
                    print(currentcompleted)

                    total_completed = timedelta()

                    for times in completedToAdd:
                        time_value = completedToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)  # Add time to the set

                                # time_value = ""
                                # time_value += time1
                        else:
                            time_values = time_value
                        
                    total_completed += time_str_to_timedelta(time_values)

                    total_completed_str1 = str(total_completed)

                    newObj1[tempGroup]['completed'] = total_completed_str1 

                    #------------------------------------------------------------------------------------
                    #-------------------------Total-no_of_items------------------------------------------------------
                    noofitemsdToAdd = {
                        'oldnoofitems': oldnoofitems,
                        'currentnoofitems' : currentnoofitems
                    }
                 
                    print("Old number of items:", oldnoofitems)
                    print("Current number of items:", currentnoofitems)

                    total_noofitems = 0

                    for num in noofitemsdToAdd:
                        num_value = noofitemsdToAdd[num]

                        if isinstance(num_value, set):  # If the value is a set, extract the single element
                            total_noofitems += sum(num_value)
                        elif isinstance(num_value, int):  # If the value is an integer, add it directly
                            total_noofitems += num_value

                    total_noofitems_str1 = str(total_noofitems)

                    # Print the results
                    print("Total number of items:", total_noofitems)

                    #---------------------------------------------------------------------------------------
                    #-------------------------Total-total_time------------------------------------------------------

                    timesToAdd = {
                        'oldTime': oldTime,
                        'curentTime': curentTime
                    }
                    print(oldTime)
                    print(curentTime)
                    # Initialize a timedelta object to store the total time
                    total_time = timedelta()

                    # Iterate through the keys to add the time
                    for times in timesToAdd:                        
                        #time_value = next(iter(timesToAdd[times]))  # Extract the single item from the set                        
                        time_value = timesToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)  # Add time to the set

                                # time_value += time
                        else:
                            time_values = time_value
                        
                    total_time += time_str_to_timedelta(time_values)

                    # Convert total_time back to a string in HH:MM:SS format
                    total_time_str = str(total_time)  

                    # newObj[tempGroup] = item     #-----(THIS LINE UPDATE THE RECENT RECORD TO THE RESPONSE)

                    newObj1[tempGroup]['total-time'] = total_time_str                    
                else:
                    if not isinstance(item['user'], set):
                            item['user'] = {item['user']}  # Convert the user to a set
                            newObj1[tempGroup] = item  
                    newObj1[tempGroup] = item               
                
            combinedArr = []

            for key, value in newObj1.items():
                combinedArr.append(value)

            result_data = combinedArr
            # CODE FOR COMBINING TOTAL-TIME ENDS HERE
            print("newObj1")
            print(newObj1)

            print("result data => sdfdfdfdfdfdfdfdfdf ")
            print(result_data)


            return result_data
        
    elif reportoptions == "scope_subscope":

            finalre = {
                'estimated_time_with_add' : pendulum.duration(),
                'date': set(),
                'user': set(),
                'Service_ID': set(),
                'scope': set(),
                'created_at' : set(),
                'Completed_date' : set(),
                'subscopes': set(),
                'entity': set(),
                # 'no_of_entity' : set(),
                'status': set(),
                'type_of_activity': set(),
                'Nature_of_Work': set(),
                'gst_tan': set(),
                'estimated_d_o_d': set(),
                'estimated_time': set(),
                'member_name': set(),
                'end_time': pendulum.duration(),
                'hold': pendulum.duration(),
                'break': pendulum.duration(),
                'time_diff_work': pendulum.duration(),
                'call': pendulum.duration(),
                'meeting': pendulum.duration(),
                'in_progress': pendulum.duration(),
                'completed': pendulum.duration(),
                'third_report_data' : set(),
                'fourth_report' :  set(),
                'fourth_report2' : set(),
                'fifth_report' : set(),
                'no_of_items' : set(),
                'chargable' : set(),
                'non-chargable' : set(),
                'total-time' : set(),
                'idealname' : pendulum.duration()
            }

            date_time_formate_string = '%Y-%m-%d %H:%M:%S'
            list_data = []
            result_data = []
            


            d1 = picked_date
            d2 = to_date

            # Convert strings to datetime objects
            start_date = datetime.strptime(d1, '%Y-%m-%d')
            end_date = datetime.strptime(d2, '%Y-%m-%d')

            # Generate all dates in between and store as strings
            dates_list = []
            current_date = start_date

            while current_date <= end_date:
                
                dates_list.append(current_date.strftime('%Y-%m-%d'))
                current_date += timedelta(days=1)
                

        #     # dates_list contains all dates as strings
            # print(dates_list)
            for item in dates_list:
                reportoptions="twenty"
                list_data.append(totaltime.user_wise_report(db,item,reportoptions))

                
            list_data = [item for item in list_data if item]

            common =  set()
            # print("list_data")
            # print(list_data)  
            for report_list in list_data:
                for entry in report_list:
                        my_set = {str(x) for x in entry['Service_ID']} 
                        
                        common.add(my_set.pop())

                                
            for finalitems in common:
                for report_list in list_data:
                    for entry in report_list:
                        if int(finalitems) in entry['Service_ID']:
                            for key in finalre.keys():
                                        if key == 'end_time':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'estimated_time_with_add':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'hold':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'break':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'time_diff_work':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'call':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'meeting':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'in_progress':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'completed':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'idealname':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'no_of_items':
                                            try:
                                        
                                    
                                                finalre[key] = [finalre[key].pop()]+int(entry[key].pop())
                                                
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'chargable':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'non-chargable':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'total-time':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        else:
                                            finalre[key] = entry[key].union(finalre[key])

                result = {
                    'estimated_time_with_add' : set(),
                    'work_started_date' : set(),
                    'work_ended_date' : set(),
                    'number_of_days_taken' : set(),
                    'number_of_days_delayed' : set(),
                    'actual_date_of_delivery' : set(),
                    'estimated_date_of_delivery' : set(),
                    'number_of_entity' : set(),
                    'estimated_time_minus_chargable_time' : set(),
                    'date': set(),
                    'user': set(),
                    'Service_ID': set(),
                    'created_at' : set(),
                    'Completed_date' : set(),
                    'scope': set(),
                    'subscopes': set(),
                    'entity': set(),
                    # 'no_of_entity' : set(),
                    'status': set(),
                    'type_of_activity': set(),
                    'Nature_of_Work': set(),
                    'gst_tan': set(),
                    'estimated_d_o_d': set(),
                    'estimated_time': set(),
                    'member_name': set(),
                    'end_time': set(),
                    'hold': set(),
                    'break': set(),
                    'time_diff_work': set(),
                    'call': set(),
                    'meeting': set(),
                    'in_progress': set(),
                    'completed': set(),
                    'third_report_data' : set(),
                    'fourth_report' :  set(),
                    'fourth_report2' : set(),
                    'fifth_report' : set(),
                    'no_of_items' : set(),
                    'chargable' : set(),
                    'non-chargable' : set(),
                    'total-time' : set(),
                    'idealname' : set()
                }
                for key in finalre:
                    if isinstance(finalre[key], set):

                            cpof = finalre[key]
                            result[key]= cpof
                        
                            finalre[key] = set()

                    elif isinstance(finalre[key], int):
                        result[key] = finalre[key]
                        #   print(finalre[key],'hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh')
                    else:
                    
                        result[key].add(convert_to_duration(finalre[key]))
                        finalre[key] = pendulum.duration()
                        


        #--------------------------- last calculation


                        

                # Define the set of date strings
                date_strings = result['date']
            
                # Convert the date strings to datetime objects
                dateslast = {datetime.strptime(date, "%Y-%m-%d").date() for date in date_strings}

                # Find the maximum date
                max_date = max(dateslast)

                min_date = min(dateslast)




                # Define the set of date strings
                date_strings_date = result['estimated_d_o_d']

                # Convert the date strings to datetime objects
                dateslast_date = {datetime.strptime(date, "%Y-%m-%d").date() for date in date_strings_date}

                # Find the maximum date
                max_date_in_dates = max(dateslast_date)

                        


        #--------------------------- last calculation


                dateesti =  max_date_in_dates.strftime("%Y-%m-%d")

                # Define the set of date strings
                date_strings = result['date']

                # Convert dateesti to a datetime object
                dateesti_dt = datetime.strptime(dateesti, "%Y-%m-%d").date()

                # Convert the set of date strings to a set of datetime objects
                dates = {datetime.strptime(date, "%Y-%m-%d").date() for date in date_strings}

                # Find and count the dates that are greater than dateesti_dt
                greater_dates = {date for date in dates if date > dateesti_dt}
                count_greater_dates = len(greater_dates)

                    

        #-----------------------------getting estimate time
                        

                estichar = result['estimated_time_with_add'].pop()
                hourses, minuteses, secondses = map(int, estichar.split(':'))

                # Create a pendulum Duration object
                durationes = pendulum.duration(hours=hourses, minutes=minuteses, seconds=secondses)

        #----------------------------------------- chargable time


                nchar = result['chargable'].pop()

                hours, minutes, seconds = map(int, nchar.split(':'))

                # Create a pendulum Duration object
                duration = pendulum.duration(hours=hours, minutes=minutes, seconds=seconds)
                # print(durationes-duration,'hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh')
#------------------------ late of date estimated
                result['chargable'] = nchar
                result['estimated_time_with_add'] = estichar
                result['work_started_date'] =  min_date
                result['work_ended_date'] = max_date
                result['number_of_days_taken'] = len(result['date'])
                result['number_of_days_delayed'] = count_greater_dates
                result['actual_date_of_delivery'] = max_date_in_dates 
                result['estimated_date_of_delivery'] = max_date_in_dates
                result['number_of_entity'] = len(result['entity'])
                result['estimated_time_minus_chargable_time'] = convert_to_duration(durationes-duration)

        #------------------------------ code end
                result_data.append(result)
                result = {}
                # print("result starts here =>")
                # print(result_data)
                # print("result end here =>")
            # CODE FOR COMBINING TOTAL-TIME STARTS HERE
            newObj1 = {}
            
            for item in result_data: 
                #print(item)    

                # print("single item starts ")    

                # print(item)

                # print("single item ends ")    
                tempGroup = ''
                
                for scope in item['scope']:
                    tempGroup+=scope

                for subscopes in item['subscopes']:
                    tempGroup+=subscopes

                key_to_check = tempGroup

                if key_to_check in newObj1:
                    if isinstance(item['user'], set):   
                        newObj1[tempGroup]['user'].update(item['user'])  # Update with a set of users
                    else:
                        newObj1[tempGroup]['user'].add(item['user'])  # Add a single user

                    oldETWA = newObj1[tempGroup]['estimated_time_with_add']
                    currentETWA = item['estimated_time_with_add']

                    oldendtime = newObj1[tempGroup]['end_time']
                    currentendtime = item['end_time']

                    oldhold = newObj1[tempGroup]['hold']
                    currenthold = item['hold']

                    oldbreak = newObj1[tempGroup]['break']
                    currentbreak = item['break']

                    oldTDW = newObj1[tempGroup]['time_diff_work']
                    currentTDW = item['time_diff_work']

                    oldcall = newObj1[tempGroup]['call']
                    currentcall = item['call']

                    oldmeeting = newObj1[tempGroup]['meeting']
                    currentmeeting = item['meeting']

                    oldInProgress = newObj1[tempGroup]['in_progress']
                    currentInProgress = item['in_progress']

                    oldcompleted = newObj1[tempGroup]['completed']
                    currentcompleted = item['completed']

                    oldnoofitems = newObj1[tempGroup]['no_of_items']
                    currentnoofitems = item['no_of_items']

                    oldTime = newObj1[tempGroup]['total-time']
                    curentTime = item['total-time']
#--------------------------------------------------------------------------------------
#--------------------------Total-estimated_time_with_add-------------------------------
                    ETWAToAdd = {
                        'oldETWA': oldETWA,
                        'currentETWA' : currentETWA
                    }
                    print(oldETWA)
                    print(currentETWA)

                    total_ETWA = timedelta()

                    for times in ETWAToAdd:
                        time_value = ETWAToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time) 
                        else:
                            time_values = time_value
                        
                    total_ETWA += time_str_to_timedelta(time_values)

                    total_ETWA_str1 = str(total_ETWA)

                    newObj1[tempGroup]['estimated_time_with_add'] = total_ETWA_str1 
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-end_time---------------------------------------------------
                    endtimeToAdd = {
                        'oldendtime': oldendtime,
                        'currentendtime' : currentendtime
                    }

                    print(oldendtime)
                    print(currentendtime)

                    total_endtime = timedelta()

                    for times in endtimeToAdd:
                        time_value = endtimeToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)
                        else:
                            time_values = time_value
                        
                    total_endtime += time_str_to_timedelta(time_values)

                    total_endtime_str1 = str(total_endtime)

                    newObj1[tempGroup]['end_time'] = total_endtime_str1 
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-hold---------------------------------------------------
                    holdToAdd = {
                        'oldhold': oldhold,
                        'currenthold' : currenthold
                    }

                    print(oldhold)
                    print(currenthold)

                    total_hold = timedelta()

                    for times in holdToAdd:
                        time_value = holdToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time) 
                        else:
                            time_values = time_value
                        
                    total_hold += time_str_to_timedelta(time_values)

                    total_hold_str1 = str(total_hold)

                    newObj1[tempGroup]['hold'] = total_hold_str1 
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-break------------------------------------------------------

                    breakToAdd = {
                        'oldbreak': oldbreak,
                        
                        'currentbreak' : currentbreak
                    }

                    print(oldbreak)
                    print(currentbreak)

                    total_break = timedelta()

                    for times in breakToAdd:
                        time_value = breakToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = ""
                                time_value += time
                        else:
                            time_values = time_value
                        
                    total_break += time_str_to_timedelta(time_values)

                    total_break_str1 = str(total_break)

                    newObj1[tempGroup]['break'] = total_break_str1 
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-time_diff_work------------------------------------------------------

                    TDWToAdd = {
                        'oldTDW': oldTDW,
                        'currentTDW' : currentTDW
                    }

                    print(oldTDW)
                    print(currentTDW)

                    total_TDW = timedelta()

                    for times in TDWToAdd:
                        time_value = TDWToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time) 
                        else:
                            time_values = time_value
                        
                    total_TDW += time_str_to_timedelta(time_values)

                    total_TDW_str1 = str(total_TDW)

                    newObj1[tempGroup]['time_diff_work'] = total_TDW_str1 
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-call------------------------------------------------------

                    callToAdd = {
                        'oldcall': oldcall,
                        'currentcall' : currentcall
                    }

                    print(oldcall)
                    print(currentcall)

                    total_call = timedelta()

                    for times in callToAdd:
                        time_value = callToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time) 
                        else:
                            time_values = time_value
                        
                    total_call += time_str_to_timedelta(time_values)

                    total_call_str1 = str(total_call)

                    newObj1[tempGroup]['call'] = total_call_str1 
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-meeting------------------------------------------------------
                    meetingToAdd = {
                        'oldmeeting': oldmeeting,
                        'currentmeeting' : currentmeeting
                    }

                    print(oldmeeting)
                    print(currentmeeting)

                    total_meeting = timedelta()

                    for times in meetingToAdd:
                        time_value = meetingToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time) 
                        else:
                            time_values = time_value
                        
                    total_meeting += time_str_to_timedelta(time_values)

                    total_meeting_str1 = str(total_meeting)

                    newObj1[tempGroup]['meeting'] =  total_meeting_str1
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-in_progress------------------------------------------------------

                    Inprogresstoadd = {
                        'oldInProgress': oldInProgress,
                        'currentInProgress' : currentInProgress
                    }

                    print(oldInProgress)
                    print(currentInProgress)

                    total_Inprogresstime = timedelta()

                    for times in Inprogresstoadd:
                        time_value = Inprogresstoadd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time) 
                        else:
                            time_values = time_value
                        
                    total_Inprogresstime += time_str_to_timedelta(time_values)

                    total_time_str1 = str(total_Inprogresstime)

                    newObj1[tempGroup]['in_progress'] = total_time_str1 
                    #---------------------------------------------------------------------------------------
                    #-------------------------Total-completed------------------------------------------------------
                    completedToAdd = {
                        'oldcompleted': oldcompleted,
                        'currentcompleted' : currentcompleted

                    }

                    print(oldcompleted)
                    print(currentcompleted)

                    total_completed = timedelta()

                    for times in completedToAdd:
                        time_value = completedToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time1 in time_value:
                                time_value = set()
                                time_value.add(time) 
                        else:
                            time_values = time_value
                        
                    total_completed += time_str_to_timedelta(time_values)

                    total_completed_str1 = str(total_completed)

                    newObj1[tempGroup]['completed'] = total_completed_str1 

                    #------------------------------------------------------------------------------------
                    #-------------------------Total-no_of_items------------------------------------------------------
                    noofitemsdToAdd = {
                        'oldnoofitems': oldnoofitems,
                        'currentnoofitems' : currentnoofitems
                    }
                    print(oldnoofitems)
                    print(oldnoofitems)

                    total_noofitems = 0

                    for num in noofitemsdToAdd:
                        num_value = noofitemsdToAdd[num]

                        if isinstance(num_value, set):  # If the value is a list, sum its elements
                            total_noofitems += sum(num_value)
                        elif isinstance(num_value, int):  # If the value is an integer, add it directly
                            total_noofitems += num_value

                    total_noofitems_str1 = str(total_noofitems)

                    newObj1[tempGroup]['no_of_items'] = total_noofitems_str1 

                    

                    #---------------------------------------------------------------------------------------
                    #-------------------------Total-total_time------------------------------------------------------

                    timesToAdd = {
                        'oldTime': oldTime,
                        'curentTime': curentTime
                    }
                    print(oldTime)
                    print(curentTime)
                    # Initialize a timedelta object to store the total time
                    total_time = timedelta()

                    # Iterate through the keys to add the time
                    for times in timesToAdd:                        
                        #time_value = next(iter(timesToAdd[times]))  # Extract the single item from the set                        
                        time_value = timesToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)
                        else:
                            time_values = time_value
                        
                    total_time += time_str_to_timedelta(time_values)

                    # Convert total_time back to a string in HH:MM:SS format
                    total_time_str = str(total_time)  

                    # newObj[tempGroup] = item     #-----(THIS LINE UPDATE THE RECENT RECORD TO THE RESPONSE)

                    newObj1[tempGroup]['total-time'] = total_time_str                    
                else:
                    if not isinstance(item['user'], set):
                            item['user'] = {item['user']}  # Convert the user to a set
                            newObj1[tempGroup] = item  
                    newObj1[tempGroup] = item               
                
            combinedArr = []

            for key, value in newObj1.items():
                combinedArr.append(value)

            result_data = combinedArr
            # CODE FOR COMBINING TOTAL-TIME ENDS HERE

            print("newObj1")
            print(newObj1)

            print("result data => sdfdfdfdfdfdfdfdfdf ")
            print(result_data)


            return result_data
        
    elif reportoptions == "natureofwork_membername":

            finalre = {
                'estimated_time_with_add' : pendulum.duration(),
                'date': set(),
                'user': set(),
                'Service_ID': set(),
                'scope': set(),
                'created_at' : set(),
                'Completed_date' : set(),
                'subscopes': set(),
                'entity': set(),
                # 'no_of_entity' : set(),
                'status': set(),
                'type_of_activity': set(),
                'Nature_of_Work': set(),
                'gst_tan': set(),
                'estimated_d_o_d': set(),
                'estimated_time': set(),
                'member_name': set(),
                'end_time': pendulum.duration(),
                'hold': pendulum.duration(),
                'break': pendulum.duration(),
                'time_diff_work': pendulum.duration(),
                'call': pendulum.duration(),
                'meeting': pendulum.duration(),
                'in_progress': pendulum.duration(),
                'completed': pendulum.duration(),
                'third_report_data' : set(),
                'fourth_report' :  set(),
                'fourth_report2' : set(),
                'fifth_report' : set(),
                'no_of_items' : set(),
                'chargable' : set(),
                'non-chargable' : set(),
                'total-time' : set(),
                'idealname' : pendulum.duration()
            }

            date_time_formate_string = '%Y-%m-%d %H:%M:%S'
            list_data = []
            result_data = []
            


            d1 = picked_date
            d2 = to_date

            # Convert strings to datetime objects
            start_date = datetime.strptime(d1, '%Y-%m-%d')
            end_date = datetime.strptime(d2, '%Y-%m-%d')

            # Generate all dates in between and store as strings
            dates_list = []
            current_date = start_date

            while current_date <= end_date:
                
                dates_list.append(current_date.strftime('%Y-%m-%d'))
                current_date += timedelta(days=1)
                

        #     # dates_list contains all dates as strings
            # print(dates_list)
            for item in dates_list:
                reportoptions="twenty"
                list_data.append(totaltime.user_wise_report(db,item,reportoptions))

                
            list_data = [item for item in list_data if item]

            common =  set()
            # print("list_data")
            # print(list_data)  
            for report_list in list_data:
                for entry in report_list:
                        my_set = {str(x) for x in entry['Service_ID']} 
                        
                        common.add(my_set.pop())

                                
            for finalitems in common:
                for report_list in list_data:
                    for entry in report_list:
                        if int(finalitems) in entry['Service_ID']:
                            for key in finalre.keys():
                                        if key == 'end_time':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'estimated_time_with_add':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'hold':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'break':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'time_diff_work':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'call':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'meeting':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'in_progress':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'completed':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'idealname':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'no_of_items':
                                        
                                            try:
                                                
                                            
                                                finalre[key] = [finalre[key].pop()]+int(entry[key].pop())
                                                
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'chargable':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'non-chargable':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        elif key == 'total-time':
                                            try:

                                                finalre[key] = finalre[key]+entry[key]
                                            except:
                                                finalre[key] = entry[key]
                                        else:
                                            finalre[key] = entry[key].union(finalre[key])

                result = {
                    'estimated_time_with_add' : set(),
                    'work_started_date' : set(),
                    'work_ended_date' : set(),
                    'number_of_days_taken' : set(),
                    'number_of_days_delayed' : set(),
                    'actual_date_of_delivery' : set(),
                    'estimated_date_of_delivery' : set(),
                    'number_of_entity' : set(),
                    'estimated_time_minus_chargable_time' : set(),
                    'date': set(),
                    'user': set(),
                    'Service_ID': set(),
                    'created_at' : set(),
                    'Completed_date' : set(),
                    'scope': set(),
                    'subscopes': set(),
                    'entity': set(),
                    # 'no_of_entity' : set(),
                    'status': set(),
                    'type_of_activity': set(),
                    'Nature_of_Work': set(),
                    'gst_tan': set(),
                    'estimated_d_o_d': set(),
                    'estimated_time': set(),
                    'member_name': set(),
                    'end_time': set(),
                    'hold': set(),
                    'break': set(),
                    'time_diff_work': set(),
                    'call': set(),
                    'meeting': set(),
                    'in_progress': set(),
                    'completed': set(),
                    'third_report_data' : set(),
                    'fourth_report' :  set(),
                    'fourth_report2' : set(),
                    'fifth_report' : set(),
                    'no_of_items' : set(),
                    'chargable' : set(),
                    'non-chargable' : set(),
                    'total-time' : set(),
                    'idealname' : set()
                }
                for key in finalre:
                    if isinstance(finalre[key], set):

                            cpof = finalre[key]
                            result[key]= cpof
                        
                            finalre[key] = set()

                    elif isinstance(finalre[key], int):
                        result[key] = finalre[key]
                        #   print(finalre[key],'hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh')
                    else:
                    
                        result[key].add(convert_to_duration(finalre[key]))
                        finalre[key] = pendulum.duration()
                


        #--------------------------- last calculation


                        

                # Define the set of date strings
                date_strings = result['date']
            
                # Convert the date strings to datetime objects
                dateslast = {datetime.strptime(date, "%Y-%m-%d").date() for date in date_strings}

                # Find the maximum date
                max_date = max(dateslast)

                min_date = min(dateslast)




                # Define the set of date strings
                date_strings_date = result['estimated_d_o_d']

                # Convert the date strings to datetime objects
                dateslast_date = {datetime.strptime(date, "%Y-%m-%d").date() for date in date_strings_date}

                # Find the maximum date
                max_date_in_dates = max(dateslast_date)

                        


        #--------------------------- last calculation


                dateesti =  max_date_in_dates.strftime("%Y-%m-%d")

                # Define the set of date strings
                date_strings = result['date']

                # Convert dateesti to a datetime object
                dateesti_dt = datetime.strptime(dateesti, "%Y-%m-%d").date()

                # Convert the set of date strings to a set of datetime objects
                dates = {datetime.strptime(date, "%Y-%m-%d").date() for date in date_strings}

                # Find and count the dates that are greater than dateesti_dt
                greater_dates = {date for date in dates if date > dateesti_dt}
                count_greater_dates = len(greater_dates)

                    

        #-----------------------------getting estimate time
                        

                estichar = result['estimated_time_with_add'].pop()
                hourses, minuteses, secondses = map(int, estichar.split(':'))

                # Create a pendulum Duration object
                durationes = pendulum.duration(hours=hourses, minutes=minuteses, seconds=secondses)

        #----------------------------------------- chargable time


                nchar = result['chargable'].pop()

                hours, minutes, seconds = map(int, nchar.split(':'))

                # Create a pendulum Duration object
                duration = pendulum.duration(hours=hours, minutes=minutes, seconds=seconds)
                # print(durationes-duration,'hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh')
#------------------------ late of date estimated
                result['chargable'] = nchar
                result['estimated_time_with_add'] = estichar
                result['work_started_date'] =  min_date
                result['work_ended_date'] = max_date
                result['number_of_days_taken'] = len(result['date'])
                result['number_of_days_delayed'] = count_greater_dates
                result['actual_date_of_delivery'] = max_date_in_dates 
                result['estimated_date_of_delivery'] = max_date_in_dates
                result['number_of_entity'] = len(result['entity'])
                result['estimated_time_minus_chargable_time'] = convert_to_duration(durationes-duration)

        #------------------------------ code end
                result_data.append(result)
                result = {}
                # print("result starts here =>")
                # print(result_data)
                # print("result end here =>")
            # CODE FOR COMBINING TOTAL-TIME STARTS HERE
            newObj1 = {}
            
            for item in result_data: 
                #print(item)    

                # print("single item starts ")    

                # print(item)

                # print("single item ends ")    
                tempGroup = ''
                
                for Nature_of_Work in item['Nature_of_Work']:
                    tempGroup+=Nature_of_Work

                for member_name in item['member_name']:
                    tempGroup+=member_name

                key_to_check = tempGroup

            
                if key_to_check in newObj1:
                    if isinstance(item['user'], set):   
                        newObj1[tempGroup]['user'].update(item['user'])  # Update with a set of users
                    else:
                        newObj1[tempGroup]['user'].add(item['user'])  # Add a single user

                    oldETWA = newObj1[tempGroup]['estimated_time_with_add']
                    currentETWA = item['estimated_time_with_add']

                    oldendtime = newObj1[tempGroup]['end_time']
                    currentendtime = item['end_time']

                    oldhold = newObj1[tempGroup]['hold']
                    currenthold = item['hold']

                    oldbreak = newObj1[tempGroup]['break']
                    currentbreak = item['break']

                    oldTDW = newObj1[tempGroup]['time_diff_work']
                    currentTDW = item['time_diff_work']

                    oldcall = newObj1[tempGroup]['call']
                    currentcall = item['call']

                    oldmeeting = newObj1[tempGroup]['meeting']
                    currentmeeting = item['meeting']

                    oldInProgress = newObj1[tempGroup]['in_progress']
                    currentInProgress = item['in_progress']

                    oldcompleted = newObj1[tempGroup]['completed']
                    currentcompleted = item['completed']

                    oldnoofitems = newObj1[tempGroup]['no_of_items']
                    currentnoofitems = item['no_of_items']

                    oldTime = newObj1[tempGroup]['total-time']
                    curentTime = item['total-time']
#--------------------------------------------------------------------------------------
#--------------------------Total-estimated_time_with_add-------------------------------
                    ETWAToAdd = {
                        'oldETWA': oldETWA,
                        'currentETWA' : currentETWA
                    }
                    print(oldETWA)
                    print(currentETWA)

                    total_ETWA = timedelta()

                    for times in ETWAToAdd:
                        time_value = ETWAToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)
                        else:
                            time_values = time_value
                        
                    total_ETWA += time_str_to_timedelta(time_values)

                    total_ETWA_str1 = str(total_ETWA)

                    newObj1[tempGroup]['estimated_time_with_add'] = total_ETWA_str1 
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-end_time---------------------------------------------------
                    endtimeToAdd = {
                        'oldendtime': oldendtime,
                        'currentendtime' : currentendtime
                    }

                    print(oldendtime)
                    print(currentendtime)

                    total_endtime = timedelta()

                    for times in endtimeToAdd:
                        time_value = endtimeToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)
                        else:
                            time_values = time_value
                        
                    total_endtime += time_str_to_timedelta(time_values)

                    total_endtime_str1 = str(total_endtime)

                    newObj1[tempGroup]['end_time'] = total_endtime_str1 
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-hold---------------------------------------------------
                    holdToAdd = {
                        'oldhold': oldhold,
                        'currenthold' : currenthold
                    }

                    print(oldhold)
                    print(currenthold)

                    total_hold = timedelta()

                    for times in holdToAdd:
                        time_value = holdToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)
                        else:
                            time_values = time_value
                        
                    total_hold += time_str_to_timedelta(time_values)

                    total_hold_str1 = str(total_hold)

                    newObj1[tempGroup]['hold'] = total_hold_str1 
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-break------------------------------------------------------

                    breakToAdd = {
                        'oldbreak': oldbreak,
                        'currentbreak' : currentbreak
                    }

                    print(oldbreak)
                    print(currentbreak)

                    total_break = timedelta()

                    for times in breakToAdd:
                        time_value = breakToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)
                        else:
                            time_values = time_value
                        
                    total_break += time_str_to_timedelta(time_values)

                    total_break_str1 = str(total_break)

                    newObj1[tempGroup]['break'] = total_break_str1 
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-time_diff_work------------------------------------------------------

                    TDWToAdd = {
                        'oldTDW': oldTDW,
                        'currentTDW' : currentTDW
                    }

                    print(oldTDW)
                    print(currentTDW)

                    total_TDW = timedelta()

                    for times in TDWToAdd:
                        time_value = TDWToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)
                        else:
                            time_values = time_value
                        
                    total_TDW += time_str_to_timedelta(time_values)

                    total_TDW_str1 = str(total_TDW)

                    newObj1[tempGroup]['time_diff_work'] = total_TDW_str1 
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-call------------------------------------------------------

                    callToAdd = {
                        'oldcall': oldcall,
                        'currentcall' : currentcall
                    }

                    print(oldcall)
                    print(currentcall)

                    total_call = timedelta()

                    for times in callToAdd:
                        time_value = callToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)
                        else:
                            time_values = time_value
                        
                    total_call += time_str_to_timedelta(time_values)

                    total_call_str1 = str(total_call)

                    newObj1[tempGroup]['call'] = total_call_str1 
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-meeting------------------------------------------------------
                    meetingToAdd = {
                        'oldmeeting': oldmeeting,
                        'currentmeeting' : currentmeeting
                    }

                    print(oldmeeting)
                    print(currentmeeting)

                    total_meeting = timedelta()

                    for times in meetingToAdd:
                        time_value = meetingToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)
                        else:
                            time_values = time_value
                        
                    total_meeting += time_str_to_timedelta(time_values)

                    total_meeting_str1 = str(total_meeting)

                    newObj1[tempGroup]['meeting'] =  total_meeting_str1
                    #------------------------------------------------------------------------------------------
                    #-------------------------Total-in_progress------------------------------------------------------

                    # Inprogresstoadd = {
                    #     'oldInProgress': oldInProgress,
                    #     'currentInProgress' : currentInProgress
                    # }

                    # print(oldInProgress)
                    # print(currentInProgress)

                    # total_Inprogresstime = timedelta()

                    # for times in Inprogresstoadd:
                    #     time_value = Inprogresstoadd[times]
                    #     time_values = ''

                    #     if isinstance(time_value, set):
                    #         for time in time_value:
                    #             time_value = ""
                    #             time_value += time
                    #     else:
                    #         time_values = time_value
                        
                    #     total_Inprogresstime += time_str_to_timedelta(time_value)

                    # total_time_str1 = str(total_Inprogresstime)

                    # newObj1[tempGroup]['in_progress'] = total_time_str1 

                    Inprogresstoadd = {
                        'oldInProgress': oldInProgress,
                        'currentInProgress' : currentInProgress
                    }

                    print(oldInProgress)
                    print(currentInProgress)

                    total_Inprogresstime = timedelta()

                    for times in Inprogresstoadd:
                        time_value = Inprogresstoadd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)
                        else:
                            time_values = time_value
                        
                    total_Inprogresstime += time_str_to_timedelta(time_values)

                    total_time_str1 = str(total_Inprogresstime)

                    newObj1[tempGroup]['in_progress'] = total_time_str1 
                    #---------------------------------------------------------------------------------------
                    #-------------------------Total-completed------------------------------------------------------
                    completedToAdd = {
                        'oldcompleted': oldcompleted,
                        'currentcompleted' : currentcompleted

                    }

                    print(oldcompleted)
                    print(currentcompleted)

                    total_completed = timedelta()

                    for times in completedToAdd:
                        time_value = completedToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time)
                        else:
                            time_values = time_value
                        
                    total_completed += time_str_to_timedelta(time_values)

                    total_completed_str1 = str(total_completed)

                    newObj1[tempGroup]['completed'] = total_completed_str1 

                    #------------------------------------------------------------------------------------
                    #-------------------------Total-no_of_items------------------------------------------------------

                    noofitemsdToAdd = {
                        'oldnoofitems': oldnoofitems,
                        'currentnoofitems' : currentnoofitems
                    }
                    print(oldnoofitems)
                    print(oldnoofitems)

                    total_noofitems = 0

                    for num in noofitemsdToAdd:
                        num_value = noofitemsdToAdd[num]

                        if isinstance(num_value, set):  # If the value is a list, sum its elements
                            total_noofitems += sum(num_value)
                        elif isinstance(num_value, int):  # If the value is an integer, add it directly
                            total_noofitems += num_value

                    total_noofitems_str1 = str(total_noofitems)

                    newObj1[tempGroup]['no_of_items'] = total_noofitems_str1 


                    #---------------------------------------------------------------------------------------
                    #-------------------------Total-total_time------------------------------------------------------

                    timesToAdd = {
                        'oldTime': oldTime,
                        'curentTime': curentTime
                    }
                    print(oldTime)
                    print(curentTime)
                    # Initialize a timedelta object to store the total time
                    total_time = timedelta()

                    # Iterate through the keys to add the time
                    for times in timesToAdd:                        
                        #time_value = next(iter(timesToAdd[times]))  # Extract the single item from the set                        
                        time_value = timesToAdd[times]
                        time_values = ''

                        if isinstance(time_value, set):
                            for time in time_value:
                                time_value = set()
                                time_value.add(time) 
                        else:
                            time_values = time_value
                        
                    total_time += time_str_to_timedelta(time_values)

                    # Convert total_time back to a string in HH:MM:SS format
                    total_time_str = str(total_time)  

                    # newObj[tempGroup] = item     #-----(THIS LINE UPDATE THE RECENT RECORD TO THE RESPONSE)

                    newObj1[tempGroup]['total-time'] = total_time_str                    
                else:
                    if not isinstance(item['user'], set):
                            item['user'] = {item['user']}  # Convert the user to a set
                            newObj1[tempGroup] = item  
                    newObj1[tempGroup] = item               
                
            combinedArr = []

            for key, value in newObj1.items():
                combinedArr.append(value)

            result_data = combinedArr
            # CODE FOR COMBINING TOTAL-TIME ENDS HERE

            print("newObj1")
            print(newObj1)

            print("result data => sdfdfdfdfdfdfdfdfdf ")
            print(result_data)


            return result_data
