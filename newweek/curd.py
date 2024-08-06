

def convert_to_duration(value):
        total_seconds = int(value.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        formatted_duration = f"{hours}:{minutes}:{seconds}"
        
        return formatted_duration


def lastfivereports(db: Session, picked_date: str, to_date: str, reportoptions: str ):
    
    if reportoptions == "userlist":
        

            finalre = {
                'estimated_time_with_add' : pendulum.duration(),
                'date': set(),
                'user': set(),
                'Service_ID': set(),
                'scope': set(),
                'subscopes': set(),
                'entity': set(),
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
                'total-time' : set()
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
                # print(item)
                list_data.append(totaltime.user_wise_report(db,item,reportoptions))
                
            list_data = [item for item in list_data if item]

            common =  set()

            for report_list in list_data:
                for entry in report_list:
                        my_set = {str(x) for x in entry['user']} 
                        common.add(my_set.pop())
            
            
            for finalitems in common:
                for report_list in list_data:
                    
                    for entry in report_list:
                        
                        if entry['user']=={finalitems}:
                            # print(entry['Service_ID'],'ggggggggggggggggggggggggggggggggggggggggggggg')

                            
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
                                        elif key == 'no_of_items':
                                        
                                            try:

                                                finalre[key] = finalre[key]+int(entry[key])
                                                
                                                
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
                    'date': set(),
                    'user': set(),
                    'Service_ID': set(),
                    'scope': set(),
                    'subscopes': set(),
                    'entity': set(),
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
                    'total-time' : set()
                }
                for key in finalre:
                    if isinstance(finalre[key], set):

                            cpof = finalre[key]
                            result[key]= cpof
                           
                            finalre[key] = set()

                    else:
                    
                        result[key].add(convert_to_duration(finalre[key]))
                        finalre[key] = pendulum.duration()
                print(result)
                result_data.append(result)
                result = {}


            return result_data

        

    elif reportoptions == "entitylist":

            finalre = {
                'estimated_time_with_add' : pendulum.duration(),
                'date': set(),
                'user': set(),
                'Service_ID': set(),
                'scope': set(),
                'subscopes': set(),
                'entity': set(),
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
                'total-time' : set()
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
                # print(item)
                list_data.append(totaltime.user_wise_report(db,item,reportoptions))
                
            list_data = [item for item in list_data if item]

            common =  set()

            for report_list in list_data:
                for entry in report_list:
                        my_set = {int(x) for x in entry['Service_ID']} 
                        common.add(my_set.pop())
            
            for finalitems in common:
                for report_list in list_data:
                    
                    for entry in report_list:
                        
                        if entry['Service_ID']=={finalitems}:
                            # print(entry['Service_ID'],'ggggggggggggggggggggggggggggggggggggggggggggg')

                            
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
                                        elif key == 'no_of_items':
                                        
                                            try:

                                                finalre[key] = finalre[key]+int(entry[key])
                                                
                                                
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
                    'date': set(),
                    'user': set(),
                    'Service_ID': set(),
                    'scope': set(),
                    'subscopes': set(),
                    'entity': set(),
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
                    'total-time' : set()
                }
                for key in finalre:
                    if isinstance(finalre[key], set):

                            cpof = finalre[key]
                            result[key]= cpof
                           
                            finalre[key] = set()

                    else:
                    
                        result[key].add(convert_to_duration(finalre[key]))
                        finalre[key] = pendulum.duration()
                print(result)
                result_data.append(result)
                result = {}


            return result_data

    elif reportoptions == "scopelist":
        
        

            finalre = {
                'estimated_time_with_add' : pendulum.duration(),
                'date': set(),
                'user': set(),
                'Service_ID': set(),
                'scope': set(),
                'subscopes': set(),
                'entity': set(),
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
                'total-time' : set()
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
                # print(item)
                list_data.append(totaltime.user_wise_report(db,item,reportoptions))
                
            list_data = [item for item in list_data if item]

            common =  set()

            for report_list in list_data:
                for entry in report_list:
                        my_set = {str(x) for x in entry['scope']} 
                        common.add(my_set.pop())
            
            
            for finalitems in common:
                for report_list in list_data:
                    
                    for entry in report_list:
                        
                        if entry['scope']=={finalitems}:
                            # print(entry['Service_ID'],'ggggggggggggggggggggggggggggggggggggggggggggg')

                            
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
                                        elif key == 'no_of_items':
                                        
                                            try:

                                                finalre[key] = finalre[key]+int(entry[key])
                                                
                                                
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
                    'date': set(),
                    'user': set(),
                    'Service_ID': set(),
                    'scope': set(),
                    'subscopes': set(),
                    'entity': set(),
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
                    'total-time' : set()
                }
                for key in finalre:
                    if isinstance(finalre[key], set):

                            cpof = finalre[key]
                            result[key]= cpof
                           
                            finalre[key] = set()

                    else:
                    
                        result[key].add(convert_to_duration(finalre[key]))
                        finalre[key] = pendulum.duration()
                print(result)
                result_data.append(result)
                result = {}


            return result_data

    elif reportoptions == "subscope":
        
            print('hjhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh')
        

            finalre = {
                'estimated_time_with_add' : pendulum.duration(),
                'date': set(),
                'user': set(),
                'Service_ID': set(),
                'scope': set(),
                'subscopes': set(),
                'entity': set(),
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
                'total-time' : set()
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
                # print(item)
                list_data.append(totaltime.user_wise_report(db,item,reportoptions))
                
            list_data = [item for item in list_data if item]

            common =  set()

            for report_list in list_data:
                for entry in report_list:
                        my_set = {str(x) for x in entry['subscopes']} 
                        common.add(my_set.pop())
            
            
            for finalitems in common:
                for report_list in list_data:
                    
                    for entry in report_list:
                        
                        if entry['subscopes']=={finalitems}:
                            # print(entry['Service_ID'],'ggggggggggggggggggggggggggggggggggggggggggggg')

                            
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
                                        elif key == 'no_of_items':
                                        
                                            try:

                                                finalre[key] = finalre[key]+int(entry[key])
                                                
                                                
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
                    'date': set(),
                    'user': set(),
                    'Service_ID': set(),
                    'scope': set(),
                    'subscopes': set(),
                    'entity': set(),
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
                    'total-time' : set()
                }
                for key in finalre:
                    if isinstance(finalre[key], set):

                            cpof = finalre[key]
                            result[key]= cpof
                           
                            finalre[key] = set()

                    else:
                    
                        result[key].add(convert_to_duration(finalre[key]))
                        finalre[key] = pendulum.duration()
                print(result)
                result_data.append(result)
                result = {}


            return result_data
    
    elif reportoptions == "nature":
        

        

            finalre = {
                'estimated_time_with_add' : pendulum.duration(),
                'date': set(),
                'user': set(),
                'Service_ID': set(),
                'scope': set(),
                'subscopes': set(),
                'entity': set(),
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
                'total-time' : set()
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
                # print(item)
                list_data.append(totaltime.user_wise_report(db,item,reportoptions))
                
            list_data = [item for item in list_data if item]

            common =  set()

            for report_list in list_data:
                for entry in report_list:
                        my_set = {str(x) for x in entry['Nature_of_Work']} 
                        common.add(my_set.pop())
            
            
            for finalitems in common:
                for report_list in list_data:
                    
                    for entry in report_list:
                        print('gggggggsdohfuisdhfuerhvijsdohveruiv dbojhhbvfjvneribv h ')
                        if entry['Nature_of_Work']=={finalitems}:
                            print(entry['Service_ID'],'ggggggggggggggggggggggggggggggggggggggggggggg')

                            
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
                                        elif key == 'no_of_items':
                                        
                                            try:

                                                finalre[key] = finalre[key]+int(entry[key])
                                                
                                                
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
                    'date': set(),
                    'user': set(),
                    'Service_ID': set(),
                    'scope': set(),
                    'subscopes': set(),
                    'entity': set(),
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
                    'total-time' : set()
                }
                for key in finalre:
                    if isinstance(finalre[key], set):

                            cpof = finalre[key]
                            result[key]= cpof
                           
                            finalre[key] = set()

                    else:
                    
                        result[key].add(convert_to_duration(finalre[key]))
                        finalre[key] = pendulum.duration()
                print(result)
                result_data.append(result)
                result = {}


            return result_data
    
