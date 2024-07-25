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

def calculate_hold_time_diff(db,row, date):

    time_diff_hold = None
    db_res2 = db.query(models.HOLD).filter(
        models.HOLD.Service_ID == row.Service_ID,
        or_(
        models.HOLD.hold_time_start.like(f"%{date}%"),
        models.HOLD.hold_time_end.like(f"%{date}%")
        )).all()
    for row2 in db_res2:
            date_time11 = datetime.strptime(row2.hold_time_end, '%Y-%m-%d %H:%M:%S')
            date_time22 = datetime.strptime(row2.hold_time_start, '%Y-%m-%d %H:%M:%S')
            time_diff_hold = time_diff_hold + (date_time11 - date_time22)

    return time_diff_hold


def calculate_break_time_diff(db,row, date):

    time_diff_break = None
    db_res2 = db.query(models.BREAK).filter(
        models.BREAK.Service_ID == row.Service_ID,
        or_(
        models.BREAK.break_time_start.like(f"%{date}%"),
        models.BREAK.break_time_end.like(f"%{date}%")
        )).all()
    for row2 in db_res2:
            date_time11 = datetime.strptime(row2.break_time_end, '%Y-%m-%d %H:%M:%S')
            date_time22 = datetime.strptime(row2.break_time_start, '%Y-%m-%d %H:%M:%S')
            time_diff_break = time_diff_break + (date_time11 - date_time22)

    return time_diff_break

def calculate_meeting_time_diff(db,row, date):

    time_diff_meeting = None
    db_res2 = db.query(models.MEETING).filter(
        models.MEETING.Service_ID == row.Service_ID,
        or_(
        models.MEETING.meeting_time_start.like(f"%{date}%"),
        models.MEETING.meeting_time_end.like(f"%{date}%")
        )).all()
    for row2 in db_res2:
            date_time11 = datetime.strptime(row2.meeting_time_end, '%Y-%m-%d %H:%M:%S')
            date_time22 = datetime.strptime(row2.meeting_time_start, '%Y-%m-%d %H:%M:%S')
            time_diff_meeting = time_diff_meeting + (date_time11 - date_time22)

    return time_diff_meeting

def calculate_call_time_diff(db,row, date):

    time_diff_call = None
    db_res2 = db.query(models.CALL).filter(
        models.CALL.Service_ID == row.Service_ID,
        or_(
        models.CALL.call_time_start.like(f"%{date}%"),
        models.CALL.call_time_end.like(f"%{date}%")
        )).all()
    for row2 in db_res2:
            date_time11 = datetime.strptime(row2.call_time_end, '%Y-%m-%d %H:%M:%S')
            date_time22 = datetime.strptime(row2.call_time_start, '%Y-%m-%d %H:%M:%S')
            time_diff_call = time_diff_call + (date_time11 - date_time22)

    return time_diff_call

def calculate_end_of_day_time_diff(db,row, date):

    time_diff_end_of_day = None
    db_res2 = db.query(models.END_OF_DAY).filter(
        models.END_OF_DAY.Service_ID == row.Service_ID,
        or_(
        models.END_OF_DAY.end_time_start.like(f"%{date}%"),
        models.END_OF_DAY.end_time_end.like(f"%{date}%")
        )).all()
    for row2 in db_res2:
            date_time11 = datetime.strptime(row2.end_time_end, '%Y-%m-%d %H:%M:%S')
            date_time22 = datetime.strptime(row2.end_time_start, '%Y-%m-%d %H:%M:%S')
            time_diff_call = time_diff_call + (date_time11 - date_time22)

    return time_diff_call

def calculate_time_diff(db, model, idval, date, start_column, end_column):


        return None  

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
                print(calculate_time_diff(db, models.HOLD, row.Service_ID, date,'hold_time_start','hold_time_end'),'1111111111111111111111111111111111111111111111111111111111111111111111111111111111111')
                statusval = db_res[0].work_status

                tl_comp = db_res[0].completed_time

                if statusval == 'Completed':
                    if  db_res[0].working_time == date:
                        one =  db.query(models.END_OF_DAY).filter(models.END_OF_DAY.Service_ID == row.Service_ID, models.END_OF_DAY.end_time_end ==None).first()
                        two = db.query(models.HOLD).filter(models.HOLD.Service_ID == row.Service_ID, models.HOLD.hold_time_end ==None).first()
                        three = db.query(models.CALL).filter(models.CALL.Service_ID == row.Service_ID, models.CALL.call_time_end ==None).first()
                        foure = db.query(models.MEETING).filter(models.MEETING.Service_ID == row.Service_ID, models.MEETING.meeting_time_end ==None).first()
                        five = db.query(models.BREAK).filter(models.BREAK.Service_ID == row.Service_ID, models.BREAK.break_time_end ==None).first()
                        if one:
                            comp_time = one.end_time_start
                        elif two:
                            comp_time = two.hold_time_start
                        elif three:
                            comp_time = three.call_time_start
                        elif foure:
                            comp_time = foure.meeting_time_start
                        elif five:
                            comp_time = five.break_time_start
                        elif tl_comp:
                            comp_time = tl_comp

                        if one or two or tl_comp:
                            timei1 = db_res[0].working_time
                            timei2 = comp_time
                            print(calculate_hold_time_diff(db,row,date),'##########################################')
                            timei3 = calculate_time_diff(db, models.HOLD, row.Service_ID, date,'hold_time_start','hold_time_end')
                            timei4 = calculate_time_diff(db, models.BREAK, row.Service_ID, date,'break_time_start','break_time_end')
                            timei5 = calculate_time_diff(db, models.MEETING, row.Service_ID, date,'meeting_time_start','meeting_time_end')
                            timei6 = calculate_time_diff(db, models.CALL, row.Service_ID, date,'call_time_start','call_time_end')
                            timei7 = calculate_time_diff(db, models.END_OF_DAY, row.Service_ID, date,'end_time_start','end_time_end')

                            Totaltime1 = (timei1 + timei2) - (timei3 + timei4 + timei5 + timei6+timei7)

                            # data["completed"] = Totaltime1
                            # finalist.append(Totaltime1)
                            
                    else :
                        one =  db.query(models.END_OF_DAY).filter(models.END_OF_DAY.Service_ID == row.Service_ID, models.END_OF_DAY.end_time_end == date).first()
                        two = db.query(models.HOLD).filter(models.HOLD.Service_ID == row.Service_ID, models.HOLD.hold_time_end == date).first()
                        three = db.query(models.CALL).filter(models.CALL.Service_ID == row.Service_ID, models.CALL.call_time_end == date).first()
                        foure = db.query(models.MEETING).filter(models.MEETING.Service_ID == row.Service_ID, models.MEETING.meeting_time_end == date).first()
                        five = db.query(models.BREAK).filter(models.BREAK.Service_ID == row.Service_ID, models.BREAK.break_time_end == date).first()
                        if one:
                            work_time = one.end_time_start
                        elif two:
                            work_time = two.hold_time_start
                        elif three:
                            work_time = three.call_time_start
                        elif foure:
                            work_time = foure.meeting_time_start
                        elif five:
                            work_time = five.break_time_start


                        elone =  db.query(models.END_OF_DAY).filter(models.END_OF_DAY.Service_ID == row.Service_ID, models.END_OF_DAY.end_time_end ==None).first()
                        eltwo = db.query(models.HOLD).filter(models.HOLD.Service_ID == row.Service_ID, models.HOLD.hold_time_end ==None).first()
                        elthree = db.query(models.CALL).filter(models.CALL.Service_ID == row.Service_ID, models.CALL.call_time_end ==None).first()
                        elfoure = db.query(models.MEETING).filter(models.MEETING.Service_ID == row.Service_ID, models.MEETING.meeting_time_end ==None).first()
                        elfive = db.query(models.BREAK).filter(models.BREAK.Service_ID == row.Service_ID, models.BREAK.break_time_end ==None).first()
                        if one:
                            comp_time = elone.end_time_start
                        elif two:
                            comp_time = eltwo.hold_time_start
                        elif three:
                            comp_time = elthree.call_time_start
                        elif foure:
                            comp_time = elfoure.meeting_time_start
                        elif five:
                            comp_time = elfive.break_time_start
                        elif tl_comp:
                            comp_time = tl_comp

                        
                        if elone or eltwo:
                            timei7 = comp_time
                            timei8 = work_time
                            
                            time9 = calculate_time_diff(timei7, timei8)
                            print(calculate_hold_time_diff(db,row,date),'##########################################')
                            timei10 = calculate_time_diff(db, models.HOLD, row.Service_ID, date,'hold_time_start','hold_time_end')
                            timei11 = calculate_time_diff(db, models.BREAK, row.Service_ID, date,'break_time_start','break_time_end')
                            timei12 = calculate_time_diff(db, models.MEETING, row.Service_ID, date,'meeting_time_start','meeting_time_end')
                            timei13 = calculate_time_diff(db, models.CALL, row.Service_ID, date,'call_time_start','call_time_end')
                            timei13e = calculate_time_diff(db, models.END_OF_DAY, row.Service_ID, date,'end_time_start','end_time_end')
                            time14 = time9 - (timei10 + timei11 + timei12 + timei13+timei13e)

                           
                    # data["completed"] = time14
                    # data["break"] = timei11
                    # data["meeting"] = timei12
                    # data["call"] = timei13
                    # data["end_of_day"] = timei13e
                    # data["hold"] = timei10


                elif statusval == 'End Of Day':

                    if  db_res[0].working_time == date:
                        one =  db.query(models.END_OF_DAY).filter(models.END_OF_DAY.Service_ID == row.Service_ID, models.END_OF_DAY.end_time_end ==None).first()
                        if one:
                            comp_time = one.end_time_start

                        if one:
                            timei1 = db_res[0].working_time
                            timei2 = comp_time

                            print(calculate_hold_time_diff(db,row,date),'##########################################')
                            timei3 = calculate_time_diff(db, models.HOLD, row.Service_ID, date,'hold_time_start','hold_time_end')
                            timei4 = calculate_time_diff(db, models.BREAK, row.Service_ID, date,'break_time_start','break_time_end')
                            timei5 = calculate_time_diff(db, models.MEETING, row.Service_ID, date,'meeting_time_start','meeting_time_end')
                            timei6 = calculate_time_diff(db, models.CALL, row.Service_ID, date,'call_time_start','call_time_end')
                            timei13e = calculate_time_diff(db, models.END_OF_DAY, row.Service_ID, date,'end_time_start','end_time_end')
                            Totaltime1 = (timei1 + timei2) - (timei3 + timei4 + timei5 + timei6+timei13e)

                            # finalist.append(Totaltime1)
                    else :
                        one =  db.query(models.END_OF_DAY).filter(models.END_OF_DAY.Service_ID == row.Service_ID, models.END_OF_DAY.end_time_end ==date).first()
                        if one:
                            work_time = one.end_time_end

                        elone =  db.query(models.END_OF_DAY).filter(models.END_OF_DAY.Service_ID == row.Service_ID, models.END_OF_DAY.end_time_end ==None).first()
                        if elone:
                            comp_time = elone.end_time_start
                        
                        if elone or eltwo:
                            timei7 = comp_time
                            timei8 = work_time
                            
                            time9 = calculate_time_diff(timei7, timei8)
                            print(calculate_hold_time_diff(db,row,date),'##########################################')
                            timei10 = calculate_time_diff(db, models.HOLD, row.Service_ID, date,'hold_time_start','hold_time_end')
                            timei11 = calculate_time_diff(db, models.BREAK, row.Service_ID, date,'break_time_start','break_time_end')
                            timei12 = calculate_time_diff(db, models.MEETING, row.Service_ID, date,'meeting_time_start','meeting_time_end')
                            timei13 = calculate_time_diff(db, models.CALL, row.Service_ID, date,'call_time_start','call_time_end')
                            timei13e = calculate_time_diff(db, models.END_OF_DAY, row.Service_ID, date,'end_time_start','end_time_end')

                            time14 = time9 - (timei10 + timei11 + timei12 + timei13+timei13e)

                         
                    # data["in_progress"] = time14
                    # data["break"] = timei11
                    # data["meeting"] = timei12
                    # data["call"] = timei13
                    # data["end_of_day"] = timei13e
                    # data["hold"] = timei10


                elif statusval == 'HOLD':
                    
                    if  db_res[0].working_time == date:
                        one =  db.query(models.HOLD).filter(models.HOLD.Service_ID == row.Service_ID, models.HOLD.hold_time_end ==None).first()
                        if one:
                            comp_time = one.hold_time_start

                        if one:
                            timei1 = db_res[0].working_time
                            timei2 = comp_time

                            timei3 = calculate_time_diff(db, models.HOLD, row.Service_ID, date,'hold_time_start','hold_time_end')
                            timei4 = calculate_time_diff(db, models.BREAK, row.Service_ID, date,'break_time_start','break_time_end')
                            timei5 = calculate_time_diff(db, models.MEETING, row.Service_ID, date,'meeting_time_start','meeting_time_end')
                            timei6 = calculate_time_diff(db, models.CALL, row.Service_ID, date,'call_time_start','call_time_end')

                            Totaltime1 = (timei1 + timei2) - (timei3 + timei4 + timei5 + timei6)

                            # finalist.append(Totaltime1)
                    else :
                        one =  db.query(models.HOLD).filter(models.HOLD.Service_ID == row.Service_ID, models.HOLD.hold_time_end ==date).first()
                        if one:
                            work_time = one.hold_time_end

                        elone =  db.query(models.HOLD).filter(models.HOLD.Service_ID == row.Service_ID, models.HOLD.hold_time_end ==None).first()
                        if elone:
                            comp_time = elone.hold_time_start
                        
                        if elone or eltwo:
                            timei7 = comp_time
                            timei8 = work_time
                            
                            time9 = calculate_time_diff(timei7, timei8)
                            
                            timei10 = calculate_time_diff(db, models.HOLD, row.Service_ID, date,'hold_time_start','hold_time_end')
                            timei11 = calculate_time_diff(db, models.BREAK, row.Service_ID, date,'break_time_start','break_time_end')
                            timei12 = calculate_time_diff(db, models.MEETING, row.Service_ID, date,'meeting_time_start','meeting_time_end')
                            timei13 = calculate_time_diff(db, models.CALL, row.Service_ID, date,'call_time_start','call_time_end')

                            time14 = time9 - (timei10 + timei11 + timei12 + timei13)

                    # data["in_progress"] = time14
                    # data["break"] = timei11
                    # data["meeting"] = timei12
                    # data["call"] = timei13
                    # data["end_of_day"] = timei13e
                    # data["hold"] = timei10



                elif statusval in ['Work in Progress','Clarification Call','Reallocated','Meeting','Break']:
                    if  db_res[0].working_time == date:
                        one =  db.query(models.END_OF_DAY).filter(models.END_OF_DAY.Service_ID == row.Service_ID, models.END_OF_DAY.end_time_end ==None).first()
                        two = db.query(models.HOLD).filter(models.HOLD.Service_ID == row.Service_ID, models.HOLD.hold_time_end ==None).first()
                        three = db.query(models.CALL).filter(models.CALL.Service_ID == row.Service_ID, models.CALL.call_time_end ==None).first()
                        foure = db.query(models.MEETING).filter(models.MEETING.Service_ID == row.Service_ID, models.MEETING.meeting_time_end ==None).first()
                        five = db.query(models.BREAK).filter(models.BREAK.Service_ID == row.Service_ID, models.BREAK.break_time_end ==None).first()
                        if one:
                            comp_time = one.end_time_start
                        elif two:
                            comp_time = two.hold_time_start
                        elif three:
                            comp_time = three.call_time_start
                        elif foure:
                            comp_time = foure.meeting_time_start
                        elif five:
                            comp_time = five.break_time_start
                        elif tl_comp:
                            comp_time = tl_comp

                        if one or two or tl_comp:
                            timei1 = db_res[0].working_time
                            timei2 = comp_time

                            timei3 = calculate_time_diff(db, models.HOLD, row.Service_ID, date,'hold_time_start','hold_time_end')
                            timei4 = calculate_time_diff(db, models.BREAK, row.Service_ID, date,'break_time_start','break_time_end')
                            timei5 = calculate_time_diff(db, models.MEETING, row.Service_ID, date,'meeting_time_start','meeting_time_end')
                            timei6 = calculate_time_diff(db, models.CALL, row.Service_ID, date,'call_time_start','call_time_end')
                            timei7 = calculate_time_diff(db, models.END_OF_DAY, row.Service_ID, date,'end_time_start','end_time_end')

                            Totaltime1 = (timei1 + timei2) - (timei3 + timei4 + timei5 + timei6+timei7)

                            # finalist.append(Totaltime1)
                    else :
                        one =  db.query(models.END_OF_DAY).filter(models.END_OF_DAY.Service_ID == row.Service_ID, models.END_OF_DAY.end_time_end == date).first()
                        two = db.query(models.HOLD).filter(models.HOLD.Service_ID == row.Service_ID, models.HOLD.hold_time_end == date).first()
                        three = db.query(models.CALL).filter(models.CALL.Service_ID == row.Service_ID, models.CALL.call_time_end == date).first()
                        foure = db.query(models.MEETING).filter(models.MEETING.Service_ID == row.Service_ID, models.MEETING.meeting_time_end == date).first()
                        five = db.query(models.BREAK).filter(models.BREAK.Service_ID == row.Service_ID, models.BREAK.break_time_end == date).first()
                        if one:
                            work_time = one.end_time_start
                        elif two:
                            work_time = two.hold_time_start
                        elif three:
                            work_time = three.call_time_start
                        elif foure:
                            work_time = foure.meeting_time_start
                        elif five:
                            work_time = five.break_time_start


                        elone =  db.query(models.END_OF_DAY).filter(models.END_OF_DAY.Service_ID == row.Service_ID, models.END_OF_DAY.end_time_end ==None).first()
                        eltwo = db.query(models.HOLD).filter(models.HOLD.Service_ID == row.Service_ID, models.HOLD.hold_time_end ==None).first()
                        elthree = db.query(models.CALL).filter(models.CALL.Service_ID == row.Service_ID, models.CALL.call_time_end ==None).first()
                        elfoure = db.query(models.MEETING).filter(models.MEETING.Service_ID == row.Service_ID, models.MEETING.meeting_time_end ==None).first()
                        elfive = db.query(models.BREAK).filter(models.BREAK.Service_ID == row.Service_ID, models.BREAK.break_time_end ==None).first()
                        if one:
                            comp_time = elone.end_time_start
                        elif two:
                            comp_time = eltwo.hold_time_start
                        elif three:
                            comp_time = elthree.call_time_start
                        elif foure:
                            comp_time = elfoure.meeting_time_start
                        elif five:
                            comp_time = elfive.break_time_start
                        elif tl_comp:
                            comp_time = tl_comp

                        
                        if elone or eltwo:
                            timei7 = comp_time
                            timei8 = work_time
                            
                            time9 = calculate_time_diff(timei7, timei8)
                            
                            timei10 = calculate_time_diff(db, models.HOLD, row.Service_ID, date,'hold_time_start','hold_time_end')
                            timei11 = calculate_time_diff(db, models.BREAK, row.Service_ID, date,'break_time_start','break_time_end')
                            timei12 = calculate_time_diff(db, models.MEETING, row.Service_ID, date,'meeting_time_start','meeting_time_end')
                            timei13 = calculate_time_diff(db, models.CALL, row.Service_ID, date,'call_time_start','call_time_end')
                            timei13e = calculate_time_diff(db, models.END_OF_DAY, row.Service_ID, date,'end_time_start','end_time_end')
                            time14 = time9 - (timei10 + timei11 + timei12 + timei13+timei13e)

                    # data["in_progress"] = time14
                    # data["break"] = timei11
                    # data["meeting"] = timei12
                    # data["call"] = timei13
                    # data["end_of_day"] = timei13e
                    # data["hold"] = timei10




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
            db_res = db.query(models.TL).filter(
                models.TL.type_of_activity == 'CHARGABLE',
                models.TL.name_of_entity == dataoptiopns,
                or_(
                    models.TL.working_time.like(f"%{date}%"),
                    models.TL.reallocated_time.like(f"%{date}%"),
                )
                ).all()
            return commoncalculation(db,db_res, date)

        elif option == "entitylist"  and statusdata == 'Non-Charchable':
            
            db_res = db.query(models.TL).filter(
                models.TL.type_of_activity == 'Non-Charchable',
                models.TL.name_of_entity == dataoptiopns,
                or_(
                    models.TL.working_time.like(f"%{date}%"),
                    models.TL.reallocated_time.like(f"%{date}%"),
                )
                ).all()
            return commoncalculation(db,db_res, date)


        elif option == "scopelist" and statusdata == 'CHARGABLE':
            
            db_res = db.query(models.TL).filter(
                models.TL.type_of_activity == 'CHARGABLE',
                models.TL.Scope == dataoptiopns,
                or_(
                    models.TL.working_time.like(f"%{date}%"),
                    models.TL.reallocated_time.like(f"%{date}%"),
                  
                )
                ).all()
            return commoncalculation(db,db_res, date)

        elif option == "scopelist" and statusdata == 'Non-Charchable':
            
            db_res = db.query(models.TL).filter(
                models.TL.type_of_activity == 'Non-Charchable',
                models.TL.Scope == dataoptiopns,
                or_(
                    models.TL.working_time.like(f"%{date}%"),
                    models.TL.reallocated_time.like(f"%{date}%"),
                
                )
                ).all()
            return commoncalculation(db,db_res, date)

        elif option == "subscope"  and statusdata == 'CHARGABLE':
            
            db_res = db.query(models.TL).filter(
                models.TL.type_of_activity == 'CHARGABLE',
                models.TL.From == dataoptiopns,
                or_(
                    models.TL.working_time.like(f"%{date}%"),
                    models.TL.reallocated_time.like(f"%{date}%"),
                   
                )
                ).all()
            return commoncalculation(db,db_res, date)

        elif option == "subscope"  and statusdata == 'Non-Charchable':
            
            db_res = db.query(models.TL).filter(
                models.TL.type_of_activity == 'Non-Charchable',
                models.TL.From == dataoptiopns,
                or_(
                    models.TL.working_time.like(f"%{date}%"),
                    models.TL.reallocated_time.like(f"%{date}%"),
                    
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

#------------------------------------------------------------ total.py

from sqlalchemy import or_
from datetime import datetime

def calculate_time(db, dataoptiopns, date):
    serviceidval = []

   
    db_res = db.query(models.TL).filter(
            models.TL.type_of_activity == 'CHARGABLE',
        or_(
            models.TL.working_time.like(f"%{date}%"),
            models.TL.HOLD.like(f"%{date}%"),
            models.TL.BREAK.like(f"%{date}%"),
            models.TL.MEETING.like(f"%{date}%"),
            models.TL.CALL.like(f"%{date}%"),
            models.TL.reallocated_time.like(f"%{date}%"),
        )
    ).all()

    for tl in db_res:
        serviceidval.append(tl.Service_ID)

    finalist = []
    for idval in serviceidval:
        db_res = db.query(models.TL).filter(
            models.TL.Service_ID == idval,
        ).all()

        
        statusval = db_res[0].work_status

        tl_comp = db_res[0].completed_time

        if statusval == 'Completed':
            if  db_res[0].working_time.like(f"%{date}%"):
                one =  db.query(models.END_OF_DAY).filter(models.END_OF_DAY.Service_ID == idval, models.END_OF_DAY.end_time_end ==None).first()
                two = db.query(models.HOLD).filter(models.HOLD.Service_ID == idval, models.HOLD.hold_time_end ==None).first()
                three = db.query(models.CALL).filter(models.CALL.Service_ID == idval, models.CALL.call_time_end ==None).first()
                foure = db.query(models.MEETING).filter(models.MEETING.Service_ID == idval, models.MEETING.meeting_time_end ==None).first()
                five = db.query(models.BREAK).filter(models.BREAK.Service_ID == idval, models.BREAK.break_time_end ==None).first()
                if one:
                    comp_time = one.end_time_start
                elif two:
                    comp_time = two.hold_time_start
                elif three:
                    comp_time = three.call_time_start
                elif foure:
                    comp_time = foure.meeting_time_start
                elif five:
                    comp_time = five.break_time_start
                elif tl_comp:
                    comp_time = tl_comp

                if one or two or tl_comp:
                    timei1 = db_res[0].working_time
                    timei2 = comp_time

                    timei3 = calculate_time_diff(db, models.HOLD, idval, date,'hold_time_start','hold_time_end')
                    timei4 = calculate_time_diff(db, models.BREAK, idval, date,'break_time_start','break_time_end')
                    timei5 = calculate_time_diff(db, models.MEETING, idval, date,'meeting_time_start','meeting_time_end')
                    timei6 = calculate_time_diff(db, models.CALL, idval, date,'call_time_start','call_time_end')
                    timei7 = calculate_time_diff(db, models.END_OF_DAY, idval, date,'end_time_start','end_time_end')

                    Totaltime1 = (timei1 + timei2) - (timei3 + timei4 + timei5 + timei6+timei7)

                    finalist.append(Totaltime1)
            else :
                one =  db.query(models.END_OF_DAY).filter(models.END_OF_DAY.Service_ID == idval, models.END_OF_DAY.end_time_end.like(f"%{date}%")).first()
                two = db.query(models.HOLD).filter(models.HOLD.Service_ID == idval, models.HOLD.hold_time_end.like(f"%{date}%")).first()
                three = db.query(models.CALL).filter(models.CALL.Service_ID == idval, models.CALL.call_time_end.like(f"%{date}%")).first()
                foure = db.query(models.MEETING).filter(models.MEETING.Service_ID == idval, models.MEETING.meeting_time_end.like(f"%{date}%")).first()
                five = db.query(models.BREAK).filter(models.BREAK.Service_ID == idval, models.BREAK.break_time_end.like(f"%{date}%")).first()
                if one:
                    work_time = one.end_time_start
                elif two:
                    work_time = two.hold_time_start
                elif three:
                    work_time = three.call_time_start
                elif foure:
                    work_time = foure.meeting_time_start
                elif five:
                    work_time = five.break_time_start


                elone =  db.query(models.END_OF_DAY).filter(models.END_OF_DAY.Service_ID == idval, models.END_OF_DAY.end_time_end ==None).first()
                eltwo = db.query(models.HOLD).filter(models.HOLD.Service_ID == idval, models.HOLD.hold_time_end ==None).first()
                elthree = db.query(models.CALL).filter(models.CALL.Service_ID == idval, models.CALL.call_time_end ==None).first()
                elfoure = db.query(models.MEETING).filter(models.MEETING.Service_ID == idval, models.MEETING.meeting_time_end ==None).first()
                elfive = db.query(models.BREAK).filter(models.BREAK.Service_ID == idval, models.BREAK.break_time_end ==None).first()
                if one:
                    comp_time = elone.end_time_start
                elif two:
                    comp_time = eltwo.hold_time_start
                elif three:
                    comp_time = elthree.call_time_start
                elif foure:
                    comp_time = elfoure.meeting_time_start
                elif five:
                    comp_time = elfive.break_time_start
                elif tl_comp:
                    comp_time = tl_comp

                
                if elone or eltwo:
                    timei7 = comp_time
                    timei8 = work_time
                    
                    time9 = calculate_time_diff(timei7, timei8)
                    
                    timei10 = calculate_time_diff(db, models.HOLD, idval, date,'hold_time_start','hold_time_end')
                    timei11 = calculate_time_diff(db, models.BREAK, idval, date,'break_time_start','break_time_end')
                    timei12 = calculate_time_diff(db, models.MEETING, idval, date,'meeting_time_start','meeting_time_end')
                    timei13 = calculate_time_diff(db, models.CALL, idval, date,'call_time_start','call_time_end')
                    timei13e = calculate_time_diff(db, models.END_OF_DAY, idval, date,'end_time_start','end_time_end')
                    time14 = time9 - (timei10 + timei11 + timei12 + timei13+timei13e)

                    # finalist.append(time14)

            datas = {
                "date": date,
                "Completed": time14,
                "HOLD": timei10,
                "BREAK": timei11,
                "MEETING": timei12,
                "CALL": timei13,
                "END_OF_DAY": timei13e,
            }
            finalist.append(datas)

        elif statusval == 'End Of Day':

            if  db_res[0].working_time.like(f"%{date}%"):
                one =  db.query(models.END_OF_DAY).filter(models.END_OF_DAY.Service_ID == idval, models.END_OF_DAY.end_time_end ==None).first()
                if one:
                    comp_time = one.end_time_start

                if one:
                    timei1 = db_res[0].working_time
                    timei2 = comp_time

                    timei3 = calculate_time_diff(db, models.HOLD, idval, date,'hold_time_start','hold_time_end')
                    timei4 = calculate_time_diff(db, models.BREAK, idval, date,'break_time_start','break_time_end')
                    timei5 = calculate_time_diff(db, models.MEETING, idval, date,'meeting_time_start','meeting_time_end')
                    timei6 = calculate_time_diff(db, models.CALL, idval, date,'call_time_start','call_time_end')
                    timei13e = calculate_time_diff(db, models.END_OF_DAY, idval, date,'end_time_start','end_time_end')
                    Totaltime1 = (timei1 + timei2) - (timei3 + timei4 + timei5 + timei6+)

                    finalist.append(Totaltime1)
            else :
                one =  db.query(models.END_OF_DAY).filter(models.END_OF_DAY.Service_ID == idval, models.END_OF_DAY.end_time_end ==date).first()
                if one:
                    work_time = one.end_time_end

                elone =  db.query(models.END_OF_DAY).filter(models.END_OF_DAY.Service_ID == idval, models.END_OF_DAY.end_time_end ==None).first()
                if elone:
                    comp_time = elone.end_time_start
                
                if elone or eltwo:
                    timei7 = comp_time
                    timei8 = work_time
                    
                    time9 = calculate_time_diff(timei7, timei8)
                    
                    timei10 = calculate_time_diff(db, models.HOLD, idval, date,'hold_time_start','hold_time_end')
                    timei11 = calculate_time_diff(db, models.BREAK, idval, date,'break_time_start','break_time_end')
                    timei12 = calculate_time_diff(db, models.MEETING, idval, date,'meeting_time_start','meeting_time_end')
                    timei13 = calculate_time_diff(db, models.CALL, idval, date,'call_time_start','call_time_end')
                    timei13e = calculate_time_diff(db, models.END_OF_DAY, idval, date,'end_time_start','end_time_end')

                    time14 = time9 - (timei10 + timei11 + timei12 + timei13+timei13e)

                    # finalist.append(time14)
            datas = {
                "date": date,
                "inprogerss_time": time14,
                "HOLD": timei10,
                "BREAK": timei11,
                "MEETING": timei12,
                "CALL": timei13,
                "END_OF_DAY": timei13e,
            }
            finalist.append(datas)

        elif statusval == 'HOLD':
            
            if  db_res[0].working_time.like(f"%{date}%"):
                one =  db.query(models.HOLD).filter(models.HOLD.Service_ID == idval, models.HOLD.hold_time_end ==None).first()
                if one:
                    comp_time = one.hold_time_start

                if one:
                    timei1 = db_res[0].working_time
                    timei2 = comp_time

                    timei3 = calculate_time_diff(db, models.HOLD, idval, date,'hold_time_start','hold_time_end')
                    timei4 = calculate_time_diff(db, models.BREAK, idval, date,'break_time_start','break_time_end')
                    timei5 = calculate_time_diff(db, models.MEETING, idval, date,'meeting_time_start','meeting_time_end')
                    timei6 = calculate_time_diff(db, models.CALL, idval, date,'call_time_start','call_time_end')

                    Totaltime1 = (timei1 + timei2) - (timei3 + timei4 + timei5 + timei6)

                    finalist.append(Totaltime1)
            else :
                one =  db.query(models.HOLD).filter(models.HOLD.Service_ID == idval, models.HOLD.hold_time_end ==date).first()
                if one:
                    work_time = one.hold_time_end

                elone =  db.query(models.HOLD).filter(models.HOLD.Service_ID == idval, models.HOLD.hold_time_end ==None).first()
                if elone:
                    comp_time = elone.hold_time_start
                
                if elone or eltwo:
                    timei7 = comp_time
                    timei8 = work_time
                    
                    time9 = calculate_time_diff(timei7, timei8)
                    
                    timei10 = calculate_time_diff(db, models.HOLD, idval, date,'hold_time_start','hold_time_end')
                    timei11 = calculate_time_diff(db, models.BREAK, idval, date,'break_time_start','break_time_end')
                    timei12 = calculate_time_diff(db, models.MEETING, idval, date,'meeting_time_start','meeting_time_end')
                    timei13 = calculate_time_diff(db, models.CALL, idval, date,'call_time_start','call_time_end')

                    time14 = time9 - (timei10 + timei11 + timei12 + timei13)

            datas = {
                "date": date,
                "inprogerss_time": time14,
                "HOLD": timei10,
                "BREAK": timei11,
                "MEETING": timei12,
                "CALL": timei13,
                "END_OF_DAY": timei13e,
            }
            finalist.append(datas)



        elif statusval in ['Work in Progress','Clarification Call','Reallocated','Meeting','Break']:
            if  db_res[0].working_time.like(f"%{date}%"):
                one =  db.query(models.END_OF_DAY).filter(models.END_OF_DAY.Service_ID == idval, models.END_OF_DAY.end_time_end ==None).first()
                two = db.query(models.HOLD).filter(models.HOLD.Service_ID == idval, models.HOLD.hold_time_end ==None).first()
                three = db.query(models.CALL).filter(models.CALL.Service_ID == idval, models.CALL.call_time_end ==None).first()
                foure = db.query(models.MEETING).filter(models.MEETING.Service_ID == idval, models.MEETING.meeting_time_end ==None).first()
                five = db.query(models.BREAK).filter(models.BREAK.Service_ID == idval, models.BREAK.break_time_end ==None).first()
                if one:
                    comp_time = one.end_time_start
                elif two:
                    comp_time = two.hold_time_start
                elif three:
                    comp_time = three.call_time_start
                elif foure:
                    comp_time = foure.meeting_time_start
                elif five:
                    comp_time = five.break_time_start
                elif tl_comp:
                    comp_time = tl_comp

                if one or two or tl_comp:
                    timei1 = db_res[0].working_time
                    timei2 = comp_time

                    timei3 = calculate_time_diff(db, models.HOLD, idval, date,'hold_time_start','hold_time_end')
                    timei4 = calculate_time_diff(db, models.BREAK, idval, date,'break_time_start','break_time_end')
                    timei5 = calculate_time_diff(db, models.MEETING, idval, date,'meeting_time_start','meeting_time_end')
                    timei6 = calculate_time_diff(db, models.CALL, idval, date,'call_time_start','call_time_end')
                    timei7 = calculate_time_diff(db, models.END_OF_DAY, idval, date,'end_time_start','end_time_end')

                    Totaltime1 = (timei1 + timei2) - (timei3 + timei4 + timei5 + timei6+timei7)

                    finalist.append(Totaltime1)
            else :
                one =  db.query(models.END_OF_DAY).filter(models.END_OF_DAY.Service_ID == idval, models.END_OF_DAY.end_time_end.like(f"%{date}%")).first()
                two = db.query(models.HOLD).filter(models.HOLD.Service_ID == idval, models.HOLD.hold_time_end.like(f"%{date}%")).first()
                three = db.query(models.CALL).filter(models.CALL.Service_ID == idval, models.CALL.call_time_end.like(f"%{date}%")).first()
                foure = db.query(models.MEETING).filter(models.MEETING.Service_ID == idval, models.MEETING.meeting_time_end.like(f"%{date}%")).first()
                five = db.query(models.BREAK).filter(models.BREAK.Service_ID == idval, models.BREAK.break_time_end.like(f"%{date}%")).first()
                if one:
                    work_time = one.end_time_start
                elif two:
                    work_time = two.hold_time_start
                elif three:
                    work_time = three.call_time_start
                elif foure:
                    work_time = foure.meeting_time_start
                elif five:
                    work_time = five.break_time_start


                elone =  db.query(models.END_OF_DAY).filter(models.END_OF_DAY.Service_ID == idval, models.END_OF_DAY.end_time_end ==None).first()
                eltwo = db.query(models.HOLD).filter(models.HOLD.Service_ID == idval, models.HOLD.hold_time_end ==None).first()
                elthree = db.query(models.CALL).filter(models.CALL.Service_ID == idval, models.CALL.call_time_end ==None).first()
                elfoure = db.query(models.MEETING).filter(models.MEETING.Service_ID == idval, models.MEETING.meeting_time_end ==None).first()
                elfive = db.query(models.BREAK).filter(models.BREAK.Service_ID == idval, models.BREAK.break_time_end ==None).first()
                if one:
                    comp_time = elone.end_time_start
                elif two:
                    comp_time = eltwo.hold_time_start
                elif three:
                    comp_time = elthree.call_time_start
                elif foure:
                    comp_time = elfoure.meeting_time_start
                elif five:
                    comp_time = elfive.break_time_start
                elif tl_comp:
                    comp_time = tl_comp

                
                if elone or eltwo:
                    timei7 = comp_time
                    timei8 = work_time
                    
                    time9 = calculate_time_diff(timei7, timei8)
                    
                    timei10 = calculate_time_diff(db, models.HOLD, idval, date,'hold_time_start','hold_time_end')
                    timei11 = calculate_time_diff(db, models.BREAK, idval, date,'break_time_start','break_time_end')
                    timei12 = calculate_time_diff(db, models.MEETING, idval, date,'meeting_time_start','meeting_time_end')
                    timei13 = calculate_time_diff(db, models.CALL, idval, date,'call_time_start','call_time_end')
                    timei13e = calculate_time_diff(db, models.END_OF_DAY, idval, date,'end_time_start','end_time_end')
                    time14 = time9 - (timei10 + timei11 + timei12 + timei13+timei13e)

            datas = {
                "date": date,
                "inprogerss_time": time14,
                "HOLD": timei10,
                "BREAK": timei11,
                "MEETING": timei12,
                "CALL": timei13,
                "END_OF_DAY": timei13e,
            }
            finalist.append(datas)

    return finalist


def calculate_time_diff(db, model, idval, date, start_column, end_column):
    result = db.query(model).filter(
        model.Service_ID == idval,
        getattr(model, end_column).like(f"%{date}%")
    ).first()

    if result:
        start_time = getattr(result, start_column)
        end_time = getattr(result, end_column)

        
        start_time_dt = datetime.strptime(start_time, "%H:%M:%S")
        end_time_dt = datetime.strptime(end_time, "%H:%M:%S")

        return (end_time_dt - start_time_dt).total_seconds()
    else:
        return None  
