from sqlalchemy.orm import Session
from . import models, schemas
from fastapi import UploadFile,HTTPException
import pandas as pd
import json
from datetime import date,datetime,timedelta
import os
from zipfile import ZipFile
import tracemalloc
import shutil
from sqlalchemy import cast, or_,and_,func,Date
import csv
from io import BytesIO
import pendulum

def convert_to_duration(value):
        total_seconds = int(value.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        formatted_duration = f"{hours}:{minutes}:{seconds}"
        
        return formatted_duration


def commoncalculation(db: Session,db_res, date: str):

            list_data = []
            date_time1 = datetime.now()
            date_time_formate_string = '%Y-%m-%d %H:%M:%S'
            for row in db_res:
                data = {}
                data["date"] = datetime.strptime(row.working_time, '%Y-%m-%d %H:%M:%S').date()
                data["user"] = row._user_table1.username
                data["Service_ID"] = row.Service_ID
                data["scope"] = row.Scope
                data["subscopes"] = row.From
                data["entity"] = row.name_of_entity
                data["status"] = row.work_status
                data["type_of_activity"] = row.type_of_activity
                data["Nature_of_Work"] = row._nature_of_work.work_name
                data["gst_tan"] = row.gst_tan
                data["estimated_d_o_d"] =  row.estimated_d_o_d
                data["no_of_items"] = row.no_of_items
                data["estimated_time"] =  row.estimated_time
                data["member_name"] = row._user_table1.firstname +' '+ row._user_table1.lastname

#---------------------------------------------------------------------------------------------------------------------- timing code

                date_time2 = datetime.strptime(row.working_time, date_time_formate_string)
                time_diff = date_time1 - date_time2
                work_hour_hours_diff = time_diff



                a = timedelta(hours=0)
                b = timedelta(hours=0)
                if row.work_status == "Reallocated":
                    db_res2 = db.query(models.REALLOCATED).filter(
                            models.CALL.Service_ID == row.Service_ID,
                            models.CALL.call_time_start.like(f"%{date}%"),
                    ).all()

                    re_hour_diff = timedelta(hours=0)

                    for row2 in db_res2:
                        if row2.re_time_start and row2.re_time_end:
                            date_time2r = datetime.strptime(row2.re_time_start, date_time_formate_string)
                            re_time_diff = date_time1 - date_time2r
                            re_hour_diff += re_time_diff
                            data["reallocated"] = re_hour_diff
                    a = work_hour_hours_diff
                if row.work_status == "Completed":
                    a = work_hour_hours_diff
                else:
                    b = work_hour_hours_diff

                # ----- Hold Hour ------
                db_res2 = db.query(models.HOLD).filter(
                            models.CALL.Service_ID == row.Service_ID,
                            models.CALL.call_time_start.like(f"%{date}%"),
                ).all()

                hold_hour_diff = timedelta(hours=0)

                for row2 in db_res2:
                    if row2.hold_time_end and row2.hold_time_start:
                        date_time11 = datetime.strptime(row2.hold_time_end, date_time_formate_string)
                        date_time22 = datetime.strptime(row2.hold_time_start, date_time_formate_string)
                        time_diff = date_time11 - date_time22
                        hold_hour_diff += time_diff
                    else :
                        if not(row.work_status == "Completed" or row.work_status == "in_progress"):
                            time1 =  datetime.now() - datetime.strptime(row2.hold_time_start, date_time_formate_string)
                            hold_hour_diff +=  time1

                data["hold"] = hold_hour_diff

                # ----- Meeting Hour ------
                db_res2 = db.query(models.MEETING).filter(
                            models.CALL.Service_ID == row.Service_ID,
                            models.CALL.call_time_start.like(f"%{date}%"),
                ).all()

                meet_hour_diff = timedelta(hours=0)

                for row2 in db_res2:
                    if row2.meeting_time_end and row2.meeting_time_start:
                        date_time11 = datetime.strptime(row2.meeting_time_end, date_time_formate_string)
                        date_time22 = datetime.strptime(row2.meeting_time_start, date_time_formate_string)
                        time_diff = date_time11 - date_time22
                        meet_hour_diff += time_diff
                    else :
                        if not(row.work_status == "Completed" or row.work_status == "in_progress"):
                            time1 =  datetime.now() - datetime.strptime(row2.meeting_time_start, date_time_formate_string)
                            meet_hour_diff +=   time1



                data["meeting"] = meet_hour_diff

                # ----- Break Hour ------
                db_res2 = db.query(models.BREAK).filter(
                            models.CALL.Service_ID == row.Service_ID,
                            models.CALL.call_time_start.like(f"%{date}%"),
                ).all()

                break_hour_diff = timedelta(hours=0)

                for row2 in db_res2:
                    if row2.break_time_end and row2.break_time_start:
                        date_time11 = datetime.strptime(row2.break_time_end, date_time_formate_string)
                        date_time22 = datetime.strptime(row2.break_time_start, date_time_formate_string)
                        time_diff = date_time11 - date_time22
                        break_hour_diff += time_diff
                    else :
                        if not(row.work_status == "Completed" or row.work_status == "in_progress"):
                            time1 =  datetime.now() - datetime.strptime(row2.break_time_start, date_time_formate_string)
                        
                            break_hour_diff += time1

                data["break"] = break_hour_diff

                # ----- Call Hour ------
                db_res2 = db.query(models.CALL).filter(
                            models.CALL.Service_ID == row.Service_ID,
                            models.CALL.call_time_start.like(f"%{date}%"),
                ).all()

                call_hour_diff = timedelta(hours=0)

                for row2 in db_res2:
                    if row2.call_time_end and row2.call_time_start:
                        date_time11 = datetime.strptime(row2.call_time_end, date_time_formate_string)
                        date_time22 = datetime.strptime(row2.call_time_start, date_time_formate_string)
                        time_diff = date_time11 - date_time22
                        call_hour_diff += time_diff
                    else :
                        if not(row.work_status == "Completed" or row.work_status == "in_progress"):
                            time1 =  datetime.now() - datetime.strptime(row2.call_time_start, date_time_formate_string)
                        
                            call_hour_diff += time1

                data["call"] = call_hour_diff


                # -----end of the day
                temp = ''
                
                db_res2 = db.query(models.END_OF_DAY).filter(
                            models.CALL.Service_ID == row.Service_ID,
                            models.CALL.call_time_start.like(f"%{date}%"),
                ).all()

                count = db.query(models.END_OF_DAY).filter(
                            models.CALL.Service_ID == row.Service_ID,
                            models.CALL.call_time_start.like(f"%{date}%"),
                ).count()

                time_diff = timedelta(hours=0)
                if count >= 1 :
                    for rom in db_res2:
                        if rom.end_time_end == "":
                            temp = rom.end_time_start
                            parsed_date = datetime.strptime(str(temp), '%Y-%m-%d %H:%M:%S')
                            date_time22 = date_time1
                            time_diff += date_time22 - parsed_date
                        else:
                            temp = rom.end_time_start
                            parsed_date = datetime.strptime(str(temp), '%Y-%m-%d %H:%M:%S')
                            date_time11 = datetime.strptime(rom.end_time_end, date_time_formate_string)
                            time_diff += date_time11 - parsed_date
                data["end_of_day"] = time_diff
                
                if row.work_status == "Completed":
                    e_o_d = data["end_of_day"]
                    data["in_progress"] = timedelta(hours=0)
                    data["completed"] = (datetime.strptime(row.completed_time, date_time_formate_string) - datetime.strptime(row.working_time, date_time_formate_string)) - (call_hour_diff + break_hour_diff + meet_hour_diff + hold_hour_diff + e_o_d)
                    # data["completed"] = (a + b) - (call_hour_diff + break_hour_diff + meet_hour_diff + hold_hour_diff + e_o_d)
                else:
                    e_o_d = data["end_of_day"]
                    data["completed"] = timedelta(hours=0)
                    if row.work_status != "End Of Day":
                        data["in_progress"] = (a + b) - (call_hour_diff + break_hour_diff + meet_hour_diff + hold_hour_diff + e_o_d)
                    else:
                        data["in_progress"] = (a + b) - (call_hour_diff + break_hour_diff + meet_hour_diff + hold_hour_diff + e_o_d)
                    # print("in progress - ",data["in_progress"])

                # data["total_time_taken"] = call_hour_diff + break_hour_diff + meet_hour_diff + hold_hour_diff + data["completed"] + data["in_progress"]

                data["total_time_taken"] =  (data["in_progress"] + data["completed"] )
                # data["second_report_data"] = call_hour_diff + hold_hour_diff + data["completed"] + data["in_progress"]
                data["second_report_data"] =  data["completed"] + data["in_progress"]

#----------------------------------------------------------------------------------------------------- timing code

                str_temp = ""
                str_temper = ""

                if row.work_status == "Work in Progress":

                    data["third_report_data"] = ""

                elif row.work_status == "Hold":

                    db_res3 = db.query(models.HOLD).filter(
                        models.HOLD.Service_ID == row.Service_ID,
                    ).all()
                    for hold_obj in db_res3:
                        data["third_report_data"] = hold_obj.remarks
                        str_temper = hold_obj.remarks

                elif row.work_status == "Meeting":

                    db_res3 = db.query(models.MEETING).filter(
                        models.MEETING.Service_ID == row.Service_ID,
                    ).all()
                    for meet_obj in db_res3:
                        data["third_report_data"] = meet_obj.remarks


                elif row.work_status == "Break":
                    db_res3 = db.query(models.BREAK).filter(
                        models.BREAK.Service_ID == row.Service_ID,
                    ).all()
                    for break_obj in db_res3:
                      
                        data["third_report_data"] = break_obj.remarks
                        
                elif row.work_status == "Clarification Call":
                    db_res3 = db.query(models.CALL).filter(
                        models.CALL.Service_ID == row.Service_ID,
                    ).all()
                    for call_obj in db_res3:
                      
                        data["third_report_data"] = call_obj.remarks

                elif row.work_status == "Completed":
                    data["third_report_data"] = row.remarks

                if row.work_status == "Completed":
                    try:
                        db_res3 = db.query(models.HOLD).filter(
                            models.HOLD.Service_ID == row.Service_ID,
                        ).all()
                        for hold_obj in db_res3:
                            str_temp = str_temp + hold_obj.remarks + ","
                    except:
                        str_temp = ""
                    try:
                        db_res3 = db.query(models.MEETING).filter(
                            models.MEETING.Service_ID == row.Service_ID,
                        ).all()
                        for meet_obj in db_res3:

                            str_temp = str_temp + meet_obj.remarks + ","

                    except:
                        str_temp = ""
                    try:
                        db_res3 = db.query(models.BREAK).filter(
                            models.BREAK.Service_ID == row.Service_ID,
                        ).all()
                        for break_obj in db_res3:
                            str_temp = str_temp + break_obj.remarks + ","
                    except:
                        str_temp = ""
                    try:
                        db_res3 = db.query(models.CALL).filter(
                            models.CALL.Service_ID == row.Service_ID,
                        ).all()
                        for call_obj in db_res3:
                            str_temp = str_temp + call_obj.remarks + ","
                        str_temp = str_temp + row.remarks + ","
                    except:
                        str_temp = ""

                data["fourth_report"] = row.no_of_items
                data["fourth_report2"] = str_temp
                data["fifth_report"] = str_temper                    
                list_data.append(data)

                # break
            return list_data
#----------------------------------------------------------------------------


def common( db : Session , date : str , dataoptiopns   , option : str , statusdata : str):
            # dataoptiopns means entity , scope and sub scobe option means which report , statusdata means chargable or non-chargable
        # print(dataoptiopns,'000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000')
        
        if option == "userlist" and statusdata == 'CHARGABLE':
            
            db_res = db.query(models.TL).filter(
                models.TL.type_of_activity == 'CHARGABLE',
                models.TL.Assigned_To == dataoptiopns,
                or_(
                    models.TL.working_time.like(f"%{date}%"),
                    models.TL.reallocated_time.like(f"%{date}%")
                )
                ).all()
            
            return commoncalculation(db,db_res, date)
            
        elif option == "userlist" and statusdata == 'Non-Charchable':

            db_res = db.query(models.TL).filter(
                    models.TL.type_of_activity == 'Non-Charchable',
                    models.TL.Assigned_To == dataoptiopns,
                    or_(
                        models.TL.working_time.like(f"%{date}%"),
                        models.TL.reallocated_time.like(f"%{date}%")
                    )
                    ).all()

            return commoncalculation(db,db_res, date)
        
        elif option == "entitylist"  and statusdata == 'CHARGABLE':
            query = db.query(models.TL).filter(
                models.TL.type_of_activity == 'CHARGABLE',
                or_(
                    models.TL.working_time.like(f"%{date}%"),
                    models.TL.reallocated_time.like(f"%{date}%"),
                    models.TL.name_of_entity == "TESQUIREL SOLUTIONS PRIVATE LIMITED"
                )
            )
            print(query.statement.compile(compile_kwargs={"literal_binds": True}))
            db_res = query.all()
#             for tl in db_res:
#                print(tl.name_of_entity,'888888888888888888888888888888888888')
            
#             db_res = db.query(models.TL).filter(
#                 models.TL.type_of_activity == 'CHARGABLE',
#                 or_(
#                     models.TL.working_time.like(f"%{date}%"),
#                     models.TL.reallocated_time.like(f"%{date}%"),
#                     models.TL.name_of_entity == "TESQUIREL SOLUTIONS PRIVATE LIMITED"
#                 )
#                 ).all()
#             service_ids = [tl.Service_ID for tl in db_res]

# # Print or use the service_ids as needed
#             print(service_ids)
#             print(dataoptiopns,'uuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu')
            return commoncalculation(db,db_res, date)

        elif option == "entitylist"  and statusdata == 'Non-Charchable':
            
            db_res = db.query(models.TL).filter(
                models.TL.type_of_activity == 'Non-Charchable',
                or_(
                    models.TL.working_time.like(f"%{date}%"),
                    models.TL.reallocated_time.like(f"%{date}%"),
                    models.TL.name_of_entity == dataoptiopns
                )
                ).all()
            return commoncalculation(db,db_res, date)


        elif option == "scopelist" and statusdata == 'CHARGABLE':
            
            db_res = db.query(models.TL).filter(
                models.TL.type_of_activity == 'CHARGABLE',
                or_(
                    models.TL.working_time.like(f"%{date}%"),
                    models.TL.reallocated_time.like(f"%{date}%"),
                    models.TL.Scope == dataoptiopns
                )
                ).all()
            return commoncalculation(db,db_res, date)

        elif option == "scopelist" and statusdata == 'Non-Charchable':
            
            db_res = db.query(models.TL).filter(
                models.TL.type_of_activity == 'Non-Charchable',
                or_(
                    models.TL.working_time.like(f"%{date}%"),
                    models.TL.reallocated_time.like(f"%{date}%"),
                    models.TL.Scope == dataoptiopns
                )
                ).all()
            return commoncalculation(db,db_res, date)

        elif option == "subscope"  and statusdata == 'CHARGABLE':
            
            db_res = db.query(models.TL).filter(
                models.TL.type_of_activity == 'CHARGABLE',
                or_(
                    models.TL.working_time.like(f"%{date}%"),
                    models.TL.reallocated_time.like(f"%{date}%"),
                    models.TL.From == dataoptiopns
                )
                ).all()
            return commoncalculation(db,db_res, date)

        elif option == "subscope"  and statusdata == 'Non-Charchable':
            
            db_res = db.query(models.TL).filter(
                models.TL.type_of_activity == 'Non-Charchable',
                or_(
                    models.TL.working_time.like(f"%{date}%"),
                    models.TL.reallocated_time.like(f"%{date}%"),
                    models.TL.From == dataoptiopns
                )
                ).all()
            return commoncalculation(db,db_res, date)

def user_wise_report(db: Session,date: str,option: str):
    list_data = []
    db_res = db.query(models.TL).filter(
        models.TL.status == 1,
#        models.TL.Assigned_To == 6,
        or_(
            models.TL.working_time.like(f"%{date}%"),
            models.TL.reallocated_time.like(f"%{date}%") ,
        )
    ).all()

    finalfilter = set()

    Scopelist = set() # scope
    subscopeslist = set() # form
    userlist = set() 
    entitylist = set() # name of entity

    for row in db_res:
        userlist.add(row._user_table1.user_id)
        entitylist.add(row.name_of_entity)
        Scopelist.add(row.Scope)
        subscopeslist.add(row.From)
 
    if option == "userlist":
        
        finalfilter = userlist
        

    elif option == "entitylist":
        # Assuming db_res contains the result of your query
        for tl_obj in db_res:
            service_id = tl_obj.Service_ID
        # print(service_id,'Or do whatever you need with service_id')  # Or do whatever you need with service_id

        finalfilter = entitylist
    elif option == "scopelist":
        
        finalfilter = Scopelist
    elif option == "subscope":
        
        finalfilter = subscopeslist


    # print(userlist,"userlist")
    # print(entitylist, "entitylist")
    # print(Scopelist,"scopelist")
    # print(subscopeslist,"subscope")

    for usertof in finalfilter:

            

            combined_data = {
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
                'no_of_items' : set()
            }

            combined_datab = {
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
                'no_of_items' : set()
            }

            finalre = {
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


            end_time = pendulum.duration()
            hold = pendulum.duration()
            breaks = pendulum.duration()
            time_diff_work = pendulum.duration()
            call = pendulum.duration()
            meeting = pendulum.duration()
            in_progress = pendulum.duration()
            completed = pendulum.duration() 
            no_itemsa = 0
            for entry in  common( db  , date  , usertof   , option  , 'Non-Charchable'):
                # print(entry,'newone non-chargable')
                for key in combined_datab.keys():
                    if key == 'date':
                            
                            combined_datab[key].add(entry[key])
                    elif key == 'end_time':
                        try : 
                            end_time = end_time + entry[key]
                            
                        except:
                            end_time = end_time + pendulum.duration(hours=0)
                    elif key == 'hold':
                        try : 
                            hold = hold + entry[key]
                           
                        except:
                            hold = hold +  pendulum.duration(hours=0)
                    elif key == 'break':
                        try : 
                            breaks = breaks + entry[key]
                            
                        except:
                            breaks = breaks + pendulum.duration(hours=0)
                    elif key == 'time_diff_work':
                        try : 
                            time_diff_work = time_diff_work + entry[key]

                        except:
                            time_diff_work = time_diff_work + pendulum.duration(hours=0)
                    elif key == 'call':
                        try : 
                            call = call + entry[key]
                           
                        except:
                            call = call + pendulum.duration(hours=0)
                    elif key == 'meeting':
                        try : 
                            meeting = meeting + entry[key]

                        except:
                            meeting = meeting + pendulum.duration(hours=0)
                    elif key == 'in_progress':
                        try : 

                            in_progress = in_progress + entry[key]
                            
                        except:
                            in_progress = in_progress + pendulum.duration(hours=0)
                    elif key == 'completed':
                        try : 
                            completed = completed + entry[key]
                          
                        except:
                            completed = completed +  pendulum.duration(hours=0)   
     
                    elif key == 'no_of_items':
                        
                        
                        try : 
                            no_itemsa = no_itemsa +  int(entry[key])
                         
                            
                            
                        except:
                            None                                                                                                                                                           
                    else:
                        combined_datab[key].add(entry[key])



            end_timea = pendulum.duration()
            holda = pendulum.duration()
            breaksa = pendulum.duration()
            time_diff_worka = pendulum.duration()
            calla = pendulum.duration()
            meetinga = pendulum.duration()
            in_progressa = pendulum.duration()
            completeda = pendulum.duration()
            no_items = 0
            for entry in common( db  , date  , usertof   , option  , 'CHARGABLE'):
                
                for key in combined_data.keys():
                    if key == 'date':
                            
                            combined_data[key].add(entry[key])
                    elif key == 'end_time':
                        try : 
                            end_timea = end_timea + entry[key]
                            
                        except:
                            end_timea = end_timea + pendulum.duration(hours=0)
                    elif key == 'hold':
                        try : 
                            holda = holda + entry[key]
                           
                        except:
                            holda = holda +  pendulum.duration(hours=0)
                    elif key == 'break':
                        try : 
                            breaksa = breaksa + entry[key]
                            
                        except:
                            breaksa = breaksa + pendulum.duration(hours=0)
                    elif key == 'time_diff_work':
                        try : 
                            time_diff_worka = time_diff_worka + entry[key]

                        except:
                            time_diff_worka = time_diff_worka + pendulum.duration(hours=0)
                    elif key == 'call':
                        try : 
                            calla = calla + entry[key]
                           
                        except:
                            calla = calla + pendulum.duration(hours=0)
                    elif key == 'meeting':
                        try : 
                            meetinga = meetinga + entry[key]

                        except:
                            meetinga = meetinga + pendulum.duration(hours=0)
                    elif key == 'in_progress':
                        try : 

                            in_progressa = in_progressa + entry[key]
                            
                        except:
                            in_progressa = in_progressa + pendulum.duration(hours=0)
                    elif key == 'completed':
                        try : 
                            completeda = completeda + entry[key]
                          
                        except:
                            completeda = completeda +  pendulum.duration(hours=0)   

                    elif key == 'no_of_items':
                        
                        try : 

                            no_items = no_items +  int(entry[key])
                         
                        except:
                            None                                                                                                                                                                                           
                    else:
                        combined_data[key].add(entry[key])

#-------------------------------------------------------------------------------------------------------

            combined_data['end_time'] = end_timea
            combined_data['hold'] = holda
            combined_data['break'] = breaksa
            combined_data['time_diff_work'] = time_diff_worka
            combined_data['call'] = calla
            combined_data['meeting'] = meetinga
            combined_data['in_progress'] = in_progressa
            combined_data['completed'] =  completeda
            combined_data['no_of_items'].add(no_items)
            # print(no_itemsa ,no_items,'rrrrrrrrrrrrrrrrrr')

            # # Print the values of these variables
            # print("end_timea:", end_timea.in_words())
            # print("holda:", holda.in_words())
            # print("breaksa:", breaksa.in_words())
            # print("time_diff_worka:", time_diff_worka.in_words())
            # print("calla:", calla.in_words())
            # print("meetinga:", meetinga.in_words())
            # print("in_progressa:", in_progressa.in_words())
            # print("completeda:", completeda.in_words())


            combined_datab['end_time'] = end_time 
            combined_datab['hold'] = hold 
            combined_datab['break'] = breaks 
            combined_datab['time_diff_work'] = time_diff_work 
            combined_datab['call'] = call 
            combined_datab['meeting'] = meeting 
            combined_datab['in_progress'] = in_progress 
            combined_datab['completed'] =  completed 
            combined_datab['no_of_items'].add(no_itemsa)

            # print(common( db  , date  , usertof   , option  , 'CHARGABLE'),usertof,'result--------------------------')

            # Print the values of these variables
            # print("end_time:", end_time.in_words())
            # print("hold:", hold.in_words())
            # print("breaks:", breaks.in_words())
            # print("time_diff_work:", time_diff_work.in_words())
            # print("call:", call.in_words())
            # print("meeting:", meeting.in_words())
            # print("in_progress:", in_progress.in_words())
            # print("completed:", completed.in_words())

            for key in finalre.keys():

                        if key == 'end_time':
                            finalre[key].add(convert_to_duration(combined_datab[key]+combined_data[key]))
                        elif key == 'hold':
                            finalre[key].add(convert_to_duration(combined_datab[key]+combined_data[key]))
                        elif key == 'break':
                            finalre[key].add(convert_to_duration(combined_datab[key]+combined_data[key]))
                        elif key == 'time_diff_work':
                            finalre[key].add(convert_to_duration(combined_datab[key]+combined_data[key]))
                        elif key == 'call':
                            finalre[key].add(convert_to_duration(combined_datab[key]+combined_data[key]))
                        elif key == 'meeting':
                            finalre[key].add(convert_to_duration(combined_datab[key]+combined_data[key]))
                        elif key == 'in_progress':
                            finalre[key].add(convert_to_duration(combined_datab[key]+combined_data[key]))
                        elif key == 'completed':
                            finalre[key].add(convert_to_duration(combined_datab[key]+combined_data[key]))
                        elif key == 'no_of_items':
                            finalre[key].add(no_items+no_itemsa)
                        elif key == 'chargable':
                            finalre[key].add(convert_to_duration(combined_data['in_progress']+combined_data['completed']))
                        elif key == 'non-chargable':
                            finalre[key].add(convert_to_duration(combined_datab['in_progress']+combined_datab['completed']))
                        elif key == 'total-time':
                            finalre[key].add(convert_to_duration(combined_data['in_progress']+combined_data['completed']+combined_datab['in_progress']+combined_datab['completed']))
                        else:
                            finalre[key] = combined_data[key].union(combined_datab[key])

# ------------------------------------------------------------------------------------            

            list_data.append(finalre)
           
    return list_data

