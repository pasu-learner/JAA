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
#-------------------------------------------------------------------------------------------

def insert_nature_of_work(db:Session,work_name_str:str):
   db_nature_of_work = models.Nature_Of_Work(work_name = work_name_str)
   db.add(db_nature_of_work)
   try:
        db.commit()
        return "Success"
   except :
       db.rollback()
       return "Failure"

def get_nature_of_work(db:Session):
    return db.query(models.Nature_Of_Work).filter(models.Nature_Of_Work.work_status==1).all()

def delete_nature_of_work(db:Session,work_id:int):
    db_res = db.query(models.Nature_Of_Work).filter(models.Nature_Of_Work.work_id==work_id).first()
    db_res.work_status = 0
    try:
        db.commit()
        return "Success"
    except:
        return "Failure"

def update_nature_of_work(db:Session,work_name:str,work_id:int):
    db_res = db.query(models.Nature_Of_Work).filter(models.Nature_Of_Work.work_id==work_id).first()
    db_res.work_name = work_name
    try:
        db.commit()
        return "Success"
    except:
        return "Failure"
    
#-------------------------------------------------------------------------------------------

def insert_user(db:Session,username:str,role:str,firstname:str,lastname:str,location:str):
   db_insert_user = models.User_table(username = username,role=role,firstname = firstname,lastname = lastname,location=location)
   db.add(db_insert_user)
   print(db)
   try:
        db.commit()
        return "Success"
   except :
       db.rollback()
       return "Failure"
   
def get_user(db:Session):
    return db.query(models.User_table).filter(models.User_table.user_status==1).all()

def delete_user(db:Session,user_id:int):
    db_res = db.query(models.User_table).filter(models.User_table.user_id==user_id).first()
    db_res.user_status = 0
    try:
        db.commit()
        return "Success"
    except:
        return "Failure"
    
def update_user(db:Session,user_id:int,username:str,user_role:str):
    db_res = db.query(models.User_table).filter(models.User_table.user_id==user_id).first()
    db_res.username = username
    db_res.role = user_role
    try:
        db.commit()
        return "Success"
    except:
        return "Failure"

#-------------------------------------------------------------------------------------------

def login_check(db:Session,username:str,password:str):
    db_count = db.query(models.User_table).filter(models.User_table.username==username,models.User_table.password==password,models.User_table.user_status==1).count()
    if db_count > 0:
        return db.query(models.User_table).filter(models.User_table.username==username,models.User_table.password==password,models.User_table.user_status==1).all()
    else:
        return []
    
#-------------------------------------------------------------------------------------------

def tl_insert(db:Session,name_of_entity:str,gst_or_tan:str,gst_tan:str,client_grade:str,Priority:str,Assigned_By:int,estimated_d_o_d:str,estimated_time:str,Assigned_To:int,Scope:str,nature_of_work:int,From:str,Actual_d_o_d:str):
    db_insert_tl = models.TL(name_of_entity=name_of_entity,gst_or_tan=gst_or_tan,gst_tan=gst_tan,client_grade=client_grade,Priority=Priority,Assigned_By=Assigned_By,estimated_d_o_d=estimated_d_o_d,estimated_time=estimated_time,Assigned_To=Assigned_To,Scope=Scope,nature_of_work=nature_of_work,From=From,Actual_d_o_d=Actual_d_o_d)
    db.add(db_insert_tl)
    try:
        db.commit()
        return "Success"
    except :
       db.rollback()
       return "Failure"
    
#-------------------------------------------------------------------------------------------

def tl_insert_bulk(db:Session,file1:UploadFile):
    tracemalloc.start()
    if file1.filename.endswith('.csv'):
        df1 = pd.read_csv(file1.file)
        print(df1.to_string())
    else:
        raise HTTPException(status_code=400, detail="File format not supported. Please upload CSV (.csv) files.")
    
    for index,row1 in df1.iterrows():

        nature_of_work = row1['nature_of_work']
        assigned_by = row1['Assigned_By']
        assigned_to = row1['Assigned_To']
        print(row1['nature_of_work'],"pradeep")
        db_res_count = db.query(models.Nature_Of_Work).filter(models.Nature_Of_Work.work_name==nature_of_work,models.Nature_Of_Work.work_status==1).count()
        
        print(db.query(models.Nature_Of_Work).filter(models.Nature_Of_Work.work_name==nature_of_work,models.Nature_Of_Work.work_status==1).count())
        print(nature_of_work)
        print(assigned_by,"one")
        print(assigned_to,"two")

        if db_res_count>0:
            db_res = db.query(models.Nature_Of_Work).filter(models.Nature_Of_Work.work_name==nature_of_work,models.Nature_Of_Work.work_status==1).first()
            nature_of_work_id = db_res.work_id
            print(nature_of_work_id)
            db_res_count1 = db.query(models.User_table).filter(models.User_table.username==assigned_by,models.User_table.user_status==1).count()
            if db_res_count1>0:
                db_res = db.query(models.User_table).filter(models.User_table.username==assigned_by,models.User_table.user_status==1).first()
                assigned_by_id = db_res.user_id
                db_res_count2 = db.query(models.User_table).filter(models.User_table.username==assigned_to,models.User_table.user_status==1).count()
                if db_res_count2>0:
                    db_res = db.query(models.User_table).filter(models.User_table.username==assigned_to,models.User_table.user_status==1).first()
                    assigned_to_id = db_res.user_id
                    db_insert_tl = models.TL(name_of_entity=row1['name_of_entity'],gst_or_tan=row1['gst_or_tan'],gst_tan=row1['gst_tan'],client_grade=row1['client_grade'],Priority=row1['Priority'],Assigned_By=int(assigned_by_id),estimated_d_o_d=row1['estimated_d_o_d'],estimated_time=row1['estimated_time'],Assigned_To=int(assigned_to_id),Scope=row1['Scope'],nature_of_work=int(nature_of_work_id),From=row1['From'],Actual_d_o_d=row1['Actual_d_o_d'])
                    db.add(db_insert_tl)
                    try:
                        db.commit()
                    except :
                        db.rollback()
                else:
                    return "Failure"
            else:
                return "Failure"
        else:
            return "Failure"
    return "Success"
        
#-------------------------------------------------------------------------------------------

def get_work(db:Session,user_id:int):
    task_list = []
    db_res = db.query(models.TL).filter(models.TL.Assigned_To==user_id,models.TL.status==1).all()
    
    for row in db_res:
        data = {}
        data['service_id'] = row.Service_ID
        data['name_of_entity'] = row.name_of_entity
        data['gst_or_tan'] = row.gst_or_tan
        data['gst_tan'] = row.gst_tan
        data['client_grade'] = row.client_grade   
        data['Priority'] = row.Priority
        data['Scope'] = row.Scope
       
        # Fetch Assigned_By details
        db_user = db.query(models.User_table).filter(models.User_table.user_id==row.Assigned_By).first()
        if db_user:
            data['Assigned_By'] = db_user.username
            data['Assigned_By_Id'] = db_user.user_id
        else:
            data['Assigned_By'] = '-'
            data['Assigned_By_Id'] = None

        data['Assigned_Date'] = row.Assigned_Date.strftime("%d-%m-%Y %H:%M:%S")
        data['estimated_d_o_d'] = row.estimated_d_o_d
        data['estimated_time'] = row.estimated_time

        # Fetch Assigned_To details
        db_user = db.query(models.User_table).filter(models.User_table.user_id==row.Assigned_To).first()
        if db_user:
            data['Assigned_To'] = db_user.username
            data['Assigned_To_Id'] = db_user.user_id
        else:
            data['Assigned_To'] = '-'
            data['Assigned_To_Id'] = None

        data['nature_of_work'] = row._nature_of_work.work_name
        data['nature_of_work_id'] = row.nature_of_work
        data['From'] = row.From
        data['Actual_d_o_d'] = row.Actual_d_o_d
        data['created_on'] = row.created_on.strftime("%d-%m-%Y ")
        data['type_of_activity'] = row.type_of_activity
        data['work_status'] = row.work_status
        data['no_of_items'] = row.no_of_items
        data['remarks'] = row.remarks
        data['working_time'] = row.working_time
        data['completed_time'] = row.completed_time
        data['reallocated_time'] = row.reallocated_time
        task_list.append(data)
    json_data = json.dumps(task_list)
    return json.loads(json_data)

def commonfunction_get_work_tl(db, db_res):
    task_list = []
    for row in db_res:
        data = {}
        data['service_id'] = row.Service_ID
        data['name_of_entity'] = row.name_of_entity
        data['gst_or_tan'] = row.gst_or_tan
        data['gst_tan'] = row.gst_tan
        data['client_grade'] = row.client_grade
        data['Priority'] = row.Priority
        data['Scope'] = row.Scope    
        # data['created_on'] = row.created_on
        # print(data['created_on'] , "5555555555555555555555555555555555555555555555555")
       
        # data['created_on'] = row.created_on.date()
       
        # Fetch Assigned_By details
        db_user = db.query(models.User_table).filter(models.User_table.user_id==row.Assigned_By).first()
        if db_user:
            data['Assigned_By'] = db_user.username
            data['Assigned_By_Id'] = db_user.user_id
        else:
            data['Assigned_By'] = '-'
            data['Assigned_By_Id'] = None

        data['Assigned_Date'] = row.Assigned_Date.strftime("%d-%m-%Y %H:%M:%S")
        data['estimated_d_o_d'] = row.estimated_d_o_d
        data['estimated_time'] = row.estimated_time

        # Fetch Assigned_To details
        db_user = db.query(models.User_table).filter(models.User_table.user_id==row.Assigned_To).first()
        if db_user:
            data['Assigned_To'] = db_user.username
            data['Assigned_To_Id'] = db_user.user_id
        else:
            data['Assigned_To'] = '-'
            data['Assigned_To_Id'] = None

        data['nature_of_work'] = row._nature_of_work.work_name
        data['nature_of_work_id'] = row.nature_of_work
        data['From'] = row.From
        data['Actual_d_o_d'] = row.Actual_d_o_d
        data['created_on'] = row.created_on.strftime("%d-%m-%Y ")
        data['type_of_activity'] = row.type_of_activity
        data['work_status'] = row.work_status
        data['no_of_items'] = row.no_of_items
        data['remarks'] = row.remarks
        data['working_time'] = row.working_time
        data['completed_time'] = row.completed_time
        data['reallocated_time'] = row.reallocated_time

        task_list.append(data)

    return json.dumps(task_list)

def get_work_tl(db:Session,user_id:int):
    user_roles = db.query(models.User_table).filter(models.User_table.user_id==user_id,models.User_table.user_status==1).all()
    role = [role.role for role in user_roles]
    if 'TL' in role:
        db_res = db.query(models.TL).filter(models.TL.Assigned_By==user_id,models.TL.status==1).all()
        json_data = commonfunction_get_work_tl(db, db_res)
        return json.loads(json_data)
    else:
        db_res = db.query(models.TL).filter(models.TL.status==1).all()
        json_data = commonfunction_get_work_tl(db, db_res)
        return json.loads(json_data)


#-------------------------------------------------------------------------------------------

def start(db:Session,service_id:int,type_of_activity:str,no_of_items:str):
    db_res = db.query(models.TL).filter(models.TL.Service_ID == service_id).first()
    db_res.type_of_activity = type_of_activity
    db_res.no_of_items = no_of_items
    db_res.work_status = "Work in Progress"
    if db_res.working_time == '':
        current_datetime = datetime.now()
        current_datetime_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        db_res.working_time = current_datetime_str
    db.commit()
    return "Success"

#-------------------------------------------------------------------------------------------

def reallocated(db:Session,service_id:int,remarks:str,user_id:int):
    # db_res = db.query(models.TL).filter(models.TL.Service_ID == service_id).first()
    # db_res.work_status = "Reallocated"
    # db_res.remarks = remarks
    # current_datetime = datetime.now()
    # current_datetime_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    # db_res.reallocated_time = current_datetime_str
    # db.commit()


    db_res = db.query(models.TL).filter(models.TL.Service_ID == service_id).first()
    if db_res:
        # Update the record's fields
        db_res.work_status = "Reallocated"
        db_res.Assigned_To = None
    

        db.commit()
        current_datetime = datetime.now()
        current_datetime_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        db_insert = models.REALLOCATED(Service_ID = service_id,user_id=user_id,re_time_start = current_datetime_str,remarks=remarks)
        db.add(db_insert)
        db.commit()

        return "Success"


def reallocated_end(db:Session,service_id:int,user_id:int):
    db_res = db.query(models.TL).filter(models.TL.Service_ID == service_id).first()
    db_res.work_status = "Not Picked"
    db_res.Assigned_To = user_id

    db.commit()
    # current_datetime = datetime.now()
    # current_datetime_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    # db_res2 = db.query(models.REALLOCATED).filter(
    #     models.REALLOCATED.Service_ID == service_id,
    #     models.REALLOCATED.user_id == user_id
    # ).order_by(
    #     models.REALLOCATED.id.desc()
    # ).first()
    # db_res2.re_time_end = current_datetime_str
    # db.commit()
    return "Success"

#-------------------------------------------------------------------------------------------

def get_count(db:Session,user_id:int):
    count_list = []
    data = {}
    print(user_id,"********************************************************************")
    db_completed_count = db.query(models.TL).filter(models.TL.Assigned_To==user_id,models.TL.work_status=="Completed",models.TL.status==1).count()
    data['completed_count'] = db_completed_count

    db_reallocated_count = db.query(models.TL).filter(models.TL.Assigned_To==user_id,models.TL.work_status=="Reallocated",models.TL.status==1).count()
    data['reallocated_count'] = db_reallocated_count

    db_not_picked_count = db.query(models.TL).filter(models.TL.Assigned_To==user_id,models.TL.work_status=="Not Picked",models.TL.status==1).count()
    data['not_picked_count'] = db_not_picked_count

    db_wip_count = db.query(models.TL).filter(models.TL.Assigned_To==user_id,models.TL.work_status=="Work in Progress",models.TL.status==1).count()
    data['wip_count'] = db_wip_count

    db_chargable_count = db.query(models.TL).filter(models.TL.Assigned_To==user_id,models.TL.type_of_activity=="CHARGABLE",models.TL.status==1).count()
    data['chargable_count'] = db_chargable_count

    db_non_chargable_count = db.query(models.TL).filter(models.TL.Assigned_To==user_id,models.TL.type_of_activity=="Non-Charchable",models.TL.status==1).count()
    data['non_chargable_count'] = db_non_chargable_count

    db_hold_count = db.query(models.TL).filter(models.TL.Assigned_To==user_id,models.TL.work_status=="Hold",models.TL.status==1).count()
    data['hold'] = db_hold_count

    db_training = db.query(models.TL).filter(models.TL.Assigned_To==user_id,models.TL.work_status=="Training",models.TL.status==1).count()
    data['Training'] = db_training

    count_list.append(data)
    return count_list

def get_count_tl(db:Session,user_id:int):
    count_list = []
    data = {}
    print(user_id,"********************************************************************")
    get_role = db.query(models.User_table).filter(models.User_table.user_id==user_id).all()
    user_role = ''
    if get_role:
        user_role = get_role[0].role
        print(f"Role for user_id {user_id}: {user_role}")
    else:
        print(f"No user found with user_id {user_id}")
    print(user_role,"-----------------------------")
    if (user_role == "TL"):
        db_completed_count = db.query(models.TL).filter(models.TL.Assigned_By==user_id,models.TL.work_status=="Completed",models.TL.status==1).count()
        data['completed_count'] = db_completed_count

        db_reallocated_count = db.query(models.TL).filter(models.TL.Assigned_By==user_id,models.TL.work_status=="Reallocated",models.TL.status==1).count()
        data['reallocated_count'] = db_reallocated_count

        db_not_picked_count = db.query(models.TL).filter(models.TL.Assigned_By==user_id,models.TL.work_status=="Not Picked",models.TL.status==1).count()
        data['not_picked_count'] = db_not_picked_count

        db_wip_count = db.query(models.TL).filter(models.TL.Assigned_By==user_id,models.TL.work_status=="Work in Progress",models.TL.status==1).count()
        data['wip_count'] = db_wip_count

        db_chargable_count = db.query(models.TL).filter(models.TL.Assigned_By==user_id,models.TL.type_of_activity=="CHARGABLE",models.TL.status==1).count()
        data['chargable_count'] = db_chargable_count

        db_non_chargable_count = db.query(models.TL).filter(models.TL.Assigned_By==user_id,models.TL.type_of_activity=="Non-Charchable",models.TL.status==1).count()
        data['non_chargable_count'] = db_non_chargable_count

        db_hold_count = db.query(models.TL).filter(models.TL.Assigned_By==user_id,models.TL.work_status=="Hold",models.TL.status==1).count()
        data['hold'] = db_hold_count

        db_training = db.query(models.TL).filter(models.TL.Assigned_By==user_id,models.TL.work_status=="Training",models.TL.status==1).count()
        data['Training'] = db_training

        count_list.append(data)
        return count_list
    elif (user_role == "Admin"):
        db_completed_count = db.query(models.TL).filter(models.TL.work_status=="Completed",models.TL.status==1).count()
        data['completed_count'] = db_completed_count

        db_reallocated_count = db.query(models.TL).filter(models.TL.work_status=="Reallocated",models.TL.status==1).count()
        data['reallocated_count'] = db_reallocated_count

        db_not_picked_count = db.query(models.TL).filter(models.TL.work_status=="Not Picked",models.TL.status==1).count()
        data['not_picked_count'] = db_not_picked_count

        db_wip_count = db.query(models.TL).filter(models.TL.work_status=="Work in Progress",models.TL.status==1).count()
        data['wip_count'] = db_wip_count

        db_chargable_count = db.query(models.TL).filter(models.TL.type_of_activity=="CHARGABLE",models.TL.status==1).count()
        data['chargable_count'] = db_chargable_count

        db_non_chargable_count = db.query(models.TL).filter(models.TL.type_of_activity=="Non-Charchable",models.TL.status==1).count()
        data['non_chargable_count'] = db_non_chargable_count

        db_hold_count = db.query(models.TL).filter(models.TL.work_status=="Hold",models.TL.status==1).count()
        data['hold'] = db_hold_count

        db_training = db.query(models.TL).filter(models.TL.work_status=="Training",models.TL.status==1).count()
        data['Training'] = db_training

        count_list.append(data)
        return count_list

#-------------------------------------------------------------------------------------------

def get_break_time_info(db:Session):
    db_res = db.query(models.TL).all()
    user_list = []
    for row in db_res:
        data = {}
        time_format = "%Y-%m-%d %H:%M:%S"
        time = datetime.strptime(row.break_time_str, time_format) 
        if time.hour > 1:
            data['user_name'] = row._user_table1.username
            data['user_id']=row.Assigned_To
            data['break_time'] = row.break_time_str
            user_list.append(data)
            return user_list
        elif time.hour ==1:
            if time.minute>0:
                data['user_name'] = row._user_table1.username
                data['user_id']=row.Assigned_To
                data['break_time'] = row.break_time_str
                user_list.append(data)
                return user_list
            else:
                return []         
        else:
            return user_list
#-------------------------------------------------------------------------------------------

async def get_reports(db:Session,fields:str):
    column_set = fields.split(",")
    db_res = db.query(models.TL).all()
    df = pd.DataFrame([r.__dict__ for r in db_res])
    new_df = df[column_set]
    return new_df

#-------------------------------------------------------------------------------------------

def break_start(db:Session,service_id:int,remarks:str,user_id:int):
    db_res = db.query(models.TL).filter(models.TL.Service_ID == service_id).first()
    db_res.work_status = "Break"
    db.commit()
    current_datetime = datetime.now()
    current_datetime_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    db_insert = models.BREAK(Service_ID = service_id,user_id=user_id,break_time_start = current_datetime_str,remarks=remarks)
    db.add(db_insert)
    db.commit()
    return "Success"

def break_end(db:Session,service_id:int,user_id:int):
    db_res = db.query(models.TL).filter(models.TL.Service_ID == service_id).first()
    db_res.work_status = "Work in Progress"
    db.commit()
    current_datetime = datetime.now()
    current_datetime_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    db_res2 = db.query(models.BREAK).filter(
        models.BREAK.Service_ID == service_id,
        models.BREAK.user_id == user_id
    ).order_by(
        models.BREAK.id.desc()
    ).first()
    db_res2.break_time_end = current_datetime_str
    db.commit()
    return "Success"


def call_start(db:Session,service_id:int,remarks:str,user_id:int):
    db_res = db.query(models.TL).filter(models.TL.Service_ID == service_id).first()
    db_res.work_status = "Clarification Call"
    db.commit()
    current_datetime = datetime.now()
    current_datetime_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    db_insert = models.CALL(Service_ID = service_id,user_id=user_id,call_time_start = current_datetime_str,remarks=remarks)
    db.add(db_insert)
    db.commit()
    return "Success"

def call_end(db:Session,service_id:int,user_id:int):
    db_res = db.query(models.TL).filter(models.TL.Service_ID == service_id).first()
    db_res.work_status = "Work in Progress"
    db.commit()
    current_datetime = datetime.now()
    current_datetime_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    db_res2 = db.query(models.CALL).filter(
        models.CALL.Service_ID == service_id,
        models.CALL.user_id == user_id
    ).order_by(
        models.CALL.id.desc()
    ).first()
    db_res2.call_time_end = current_datetime_str
    db.commit()
    return "Success"


def end_of_day_start(db:Session,service_id:int,remarks:str,user_id:int):
    
    db_res = db.query(models.TL).filter(models.TL.Service_ID == service_id).first()
    db_res.work_status = "End Of Day"
    db.commit()

    current_datetime = datetime.now()
    current_datetime_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    db_insert = models.END_OF_DAY(Service_ID = service_id,user_id=user_id,end_time_start = current_datetime_str,remarks=remarks)
    db.add(db_insert)
    db.commit()
    
    return "Success"

def end_of_day_end(db:Session,service_id:int,user_id:int):
    
    db_res = db.query(models.TL).filter(models.TL.Service_ID == service_id).first()
    db_res.work_status = "Work in Progress"
    current_datetime = datetime.now()
    current_datetime_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    db_res.reallocated_time = current_datetime_str
    db.commit()
    
    db_res2 = db.query(models.END_OF_DAY).filter(
        models.END_OF_DAY.Service_ID == service_id,
        models.END_OF_DAY.user_id == user_id
    ).order_by(
        models.END_OF_DAY.id.desc()
    ).first()
    db_res2.end_time_end = current_datetime_str
    db.commit()
    return "Success"


def hold_start(db:Session,service_id:int,remarks:str,user_id:int):
    
    db_res = db.query(models.TL).filter(models.TL.Service_ID == service_id).first()
    db_res.work_status = "Hold"
    db.commit()

    current_datetime = datetime.now()
    current_datetime_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    db_insert = models.HOLD(Service_ID = service_id,user_id=user_id,hold_time_start=current_datetime_str,remarks=remarks)
    db.add(db_insert)
    db.commit()
    
    return "Success"

def hold_end(db:Session,service_id:int,user_id:int):
    
    db_res = db.query(models.TL).filter(models.TL.Service_ID == service_id).first()
    db_res.work_status = "Work in Progress"
    db.commit()
    
    current_datetime = datetime.now()
    current_datetime_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    db_res2 = db.query(models.HOLD).filter(
        models.HOLD.Service_ID == service_id,
        models.HOLD.user_id == user_id
    ).order_by(
        models.HOLD.id.desc()
    ).first()
    db_res2.hold_time_end = current_datetime_str
    db.commit()
    return "Success"


def meeting_start(db:Session,service_id:int,remarks:str,user_id:int):
    
    db_res = db.query(models.TL).filter(models.TL.Service_ID == service_id).first()
    db_res.work_status = "Meeting"
    db.commit()

    current_datetime = datetime.now()
    current_datetime_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    db_insert = models.MEETING(Service_ID = service_id,user_id=user_id,meeting_time_start=current_datetime_str,remarks=remarks)
    db.add(db_insert)
    db.commit()
    
    return "Success"

def meeting_end(db:Session,service_id:int,user_id:int):
    
    db_res = db.query(models.TL).filter(models.TL.Service_ID == service_id).first()
    db_res.work_status = "Work in Progress"
    db.commit()
    
    current_datetime = datetime.now()
    current_datetime_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    db_res2 = db.query(models.MEETING).filter(
        models.MEETING.Service_ID == service_id,
        models.MEETING.user_id == user_id
    ).order_by(
        models.MEETING.id.desc()
    ).first()
    db_res2.meeting_time_end = current_datetime_str
    db.commit()
    return "Success"

def Completed(db:Session,service_id:int,remarks:str,count:str):
    
    current_datetime = datetime.now()
    current_datetime_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

    db_res = db.query(models.TL).filter(models.TL.Service_ID == service_id).first()
    db_res.work_status = "Completed"
    db_res.no_of_items = count
    db_res.completed_time = current_datetime_str
    db_res.remarks = remarks
    db.commit()
    return "Success"
#----------------------------------------------------------------------------
print
def cvt(b):
    time_str = str(b)
    # print("Vanakam ",b)
    days = 0
    try:
        days = time_str.split(',')[0]
        days = days.split(" ")[0]
        days = int(days)
        try:
            time_str = time_str.split(',')[1]
            hours, minutes, seconds_micro = time_str.split(':')
            seconds, microseconds = seconds_micro.split('.')
        except:
            hours, minutes, seconds = time_str.split(':')
        
        days = days
        hours = int(hours)
        minutes = int(minutes)
        seconds = int(seconds)
        total_seconds = (int(days)*24*60*60) + hours * 3600 + minutes * 60 + seconds
    
    except:
        print("Mudiyala"," ",time_str)
        try:
            hours, minutes, seconds_micro = time_str.split(':')
            seconds, microseconds = seconds_micro.split('.')
        except:
            hours, minutes, seconds = map(int, time_str.split(':'))
        hours = int(hours)
        minutes = int(minutes)
        seconds = int(seconds)
        total_seconds = hours * 3600 + minutes * 60 + seconds
    
    return total_seconds
#----------------------------------------------------------------------------
def cvt2(total_seconds):
    if isinstance(total_seconds, pendulum.Duration):
        total_seconds = total_seconds.total_seconds()

    days = total_seconds // (24 * 3600)
    remaining_seconds = total_seconds % (24 * 3600)
    hours = remaining_seconds // 3600
    remaining_seconds %= 3600
    minutes = remaining_seconds // 60
    seconds = remaining_seconds % 60
    time_string = f"{int(days)} days, {int(hours)} hours, {int(minutes)} minutes, {seconds:.6f} seconds"
    return time_string


def user_wise_report(db: Session,date: str):

    db_res = db.query(models.TL).filter(
        models.TL.status == 1,
#        models.TL.Assigned_To == 6,
        or_(
            models.TL.working_time.like(f"%{date}%"),
            models.TL.reallocated_time.like(f"%{date}%") ,
        )
    ).all()

    Scopelist = []
    subscopeslist = []
    userlist = []
    entitylist = []

    for row in db_res:
        userlist.append(row._user_table1.user_id)
        entitylist.append(row.name_of_entity)
        Scopelist.append(row.Scope)
        subscopeslist.append(row.From)

    print(userlist,"user list")
    print(entitylist, "entitylist")
    print(Scopelist,"scopelist")
    print(subscopeslist,"subscope")

    if 1==1:
        userwisechargable(db, date, 6)
        userwisenonchargable(db, date, 6)

    return ''


def userwisenonchargable(db: Session, date: str, user_id:int):

            list_data = []

            date_time1 = datetime.now()

            db_res = db.query(models.TL).filter(
            models.TL.type_of_activity == 'Non-Charchable',
            or_(
                models.TL.working_time.like(f"%{date}%"),
                models.TL.reallocated_time.like(f"%{date}%")
            )
            ).all()
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
                data["estimated_time"] =  row.estimated_time
                data["member_name"] = row._user_table1.firstname +' '+ row._user_table1.lastname

                date_time2 = datetime.strptime(row.working_time, '%Y-%m-%d %H:%M:%S')
                time_diff_work = date_time1 - date_time2
                if time_diff_work!=0:
                    time_diff_work = cvt(time_diff_work)
                    print(time_diff_work)

                
                #-------EOD------------#

                time_diff_end = timedelta(hours=0)
                if row.work_status == "Work in Progress":
                    
                    counter = db_res2 = db.query(models.END_OF_DAY).filter(
                        models.END_OF_DAY.Service_ID == row.Service_ID,
                        or_(
                        models.END_OF_DAY.end_time_start.like(f"%{date}%"),
                        models.END_OF_DAY.end_time_end .like(f"%{date}%")
                        )
                    ).count()
                    if counter > 0:
                        db_res2 = db.query(models.END_OF_DAY).filter(
                            models.END_OF_DAY.Service_ID == row.Service_ID,
                            or_(
                            models.END_OF_DAY.end_time_start.like(f"%{date}%"),
                            models.END_OF_DAY.end_time_end .like(f"%{date}%")
                            )
                        ).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.end_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.end_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_end = time_diff_end + (date_time11 - date_time22)

                        if time_diff_end!=0:
                            time_diff_end = cvt(time_diff_end)
                else:
                    total_rows = db_res2 = db.query(models.END_OF_DAY).filter(
                        models.END_OF_DAY.Service_ID == row.Service_ID,
                        or_(
                        models.END_OF_DAY.end_time_start.like(f"%{date}%"),
                        models.END_OF_DAY.end_time_end.like(f"%{date}%")
                        )
                    ).count()
                    if total_rows>0:
                        db_res2 = db.query(models.END_OF_DAY).filter(
                        models.END_OF_DAY.Service_ID == row.Service_ID,
                        or_(
                        models.END_OF_DAY.end_time_start.like(f"%{date}%"),
                        models.END_OF_DAY.end_time_end.like(f"%{date}%")
                        )).limit(total_rows - 1).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.end_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.end_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_end = time_diff_end + (date_time11 - date_time22)
                        
                        print("First time"," ",time_diff_end)

                        db_res2 = db.query(models.END_OF_DAY).filter(
                            models.END_OF_DAY.Service_ID == row.Service_ID,
                            models.END_OF_DAY.end_time_start.like(f"%{date}%"),
                        ).order_by(models.END_OF_DAY.id.desc()).first()
                        date_time11 = date_time1
                        date_time22 = datetime.strptime(db_res2.end_time_start, '%Y-%m-%d %H:%M:%S')
                        
                        print("Second time"," ",date_time22)
                        
                        time_diff_end = time_diff_end + (date_time11 - date_time22)
                        
                        print("final time"," ",time_diff_end)
                        
                        if time_diff_end!=0:

                            print("BI"," ",time_diff_end)
                            
                            time_diff_end = cvt(time_diff_end)

                        data["end_time"] = time_diff_end
 
                #-------HOLD------------#

                time_diff_hold = timedelta(hours=0)
                if row.work_status == "Work in Progress":
                    
                    counter = db_res2 = db.query(models.HOLD).filter(
                        models.HOLD.Service_ID == row.Service_ID,
                        or_(
                        models.HOLD.hold_time_start.like(f"%{date}%"),
                        models.HOLD.hold_time_end .like(f"%{date}%")
                        )
                    ).count()
                    if counter > 0:
                        db_res2 = db.query(models.HOLD).filter(
                            models.HOLD.Service_ID == row.Service_ID,
                            or_(
                            models.HOLD.hold_time_start.like(f"%{date}%"),
                            models.HOLD.hold_time_end .like(f"%{date}%")
                            )
                        ).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.hold_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.hold_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_hold = time_diff_hold + (date_time11 - date_time22)

                        if time_diff_hold!=0:
                            time_diff_hold = cvt(time_diff_hold)
                else:
                    total_rows = db_res2 = db.query(models.HOLD).filter(
                        models.HOLD.Service_ID == row.Service_ID,
                        or_(
                        models.HOLD.hold_time_start.like(f"%{date}%"),
                        models.HOLD.hold_time_end.like(f"%{date}%")
                        )
                    ).count()
                    if total_rows>0:
                        db_res2 = db.query(models.HOLD).filter(
                        models.HOLD.Service_ID == row.Service_ID,
                        or_(
                        models.HOLD.hold_time_start.like(f"%{date}%"),
                        models.HOLD.hold_time_end.like(f"%{date}%")
                        )).limit(total_rows - 1).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.hold_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.hold_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_hold = time_diff_hold + (date_time11 - date_time22)
                        
                        print("First time"," ",time_diff_hold)

                        db_res2 = db.query(models.HOLD).filter(
                            models.HOLD.Service_ID == row.Service_ID,
                            models.HOLD.hold_time_start.like(f"%{date}%"),
                        ).order_by(models.HOLD.id.desc()).first()
                        date_time11 = date_time1
                        date_time22 = datetime.strptime(db_res2.hold_time_start, '%Y-%m-%d %H:%M:%S')
                        
                        print("Second time"," ",date_time22)
                        
                        time_diff_hold = time_diff_hold + (date_time11 - date_time22)
                        
                        print("final time"," ",time_diff_hold)
                        
                        if time_diff_hold!=0:

                            print("BI"," ",time_diff_hold)
                            
                            time_diff_hold = cvt(time_diff_hold)

                        data["hold"] = time_diff_hold

                #------- meeting ------------#

                time_diff_meet = timedelta(hours=0)
                if row.work_status == "Work in Progress":
                    
                    counter = db_res2 = db.query(models.MEETING).filter(
                        models.MEETING.Service_ID == row.Service_ID,
                        or_(
                        models.MEETING.meeting_time_start.like(f"%{date}%"),
                        models.MEETING.meeting_time_end .like(f"%{date}%")
                        )
                    ).count()
                    if counter > 0:
                        db_res2 = db.query(models.MEETING).filter(
                            models.MEETING.Service_ID == row.Service_ID,
                            or_(
                            models.MEETING.meeting_time_start.like(f"%{date}%"),
                            models.MEETING.meeting_time_end .like(f"%{date}%")
                            )
                        ).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.meeting_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.meeting_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_meet = time_diff_meet + (date_time11 - date_time22)

                        if time_diff_meet!=0:
                            time_diff_meet = cvt(time_diff_meet)
                else:
                    total_rows = db_res2 = db.query(models.MEETING).filter(
                        models.MEETING.Service_ID == row.Service_ID,
                        or_(
                        models.MEETING.meeting_time_start.like(f"%{date}%"),
                        models.MEETING.meeting_time_end.like(f"%{date}%")
                        )
                    ).count()
                    if total_rows>0:
                        db_res2 = db.query(models.MEETING).filter(
                        models.MEETING.Service_ID == row.Service_ID,
                        or_(
                        models.MEETING.meeting_time_start.like(f"%{date}%"),
                        models.MEETING.meeting_time_end.like(f"%{date}%")
                        )).limit(total_rows - 1).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.meeting_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.meeting_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_meet = time_diff_meet + (date_time11 - date_time22)
                        
                        print("First time"," ",time_diff_meet)

                        db_res2 = db.query(models.MEETING).filter(
                            models.MEETING.Service_ID == row.Service_ID,
                            models.MEETING.meeting_time_start.like(f"%{date}%"),
                        ).order_by(models.MEETING.id.desc()).first()
                        date_time11 = date_time1
                        date_time22 = datetime.strptime(db_res2.meeting_time_start, '%Y-%m-%d %H:%M:%S')
                        
                        print("Second time"," ",date_time22)
                        
                        time_diff_meet = time_diff_meet + (date_time11 - date_time22)
                        
                        print("final time"," ",time_diff_meet)
                        
                        if time_diff_meet!=0:

                            print("BI"," ",time_diff_meet)
                            
                            time_diff_meet = cvt(time_diff_meet)

                        data["meeting"] = time_diff_meet

                #-------Break Hour------------#

                time_diff_break = timedelta(hours=0)
                if row.work_status == "Work in Progress":
                    
                    counter = db_res2 = db.query(models.BREAK).filter(
                        models.BREAK.Service_ID == row.Service_ID,
                        or_(
                        models.BREAK.break_time_start.like(f"%{date}%"),
                        models.BREAK.break_time_end .like(f"%{date}%")
                        )
                    ).count()
                    if counter > 0:
                        db_res2 = db.query(models.BREAK).filter(
                            models.BREAK.Service_ID == row.Service_ID,
                            or_(
                            models.BREAK.break_time_start.like(f"%{date}%"),
                            models.BREAK.break_time_end .like(f"%{date}%")
                            )
                        ).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.break_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.break_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_break = time_diff_break + (date_time11 - date_time22)

                        if time_diff_break!=0:
                            time_diff_break = cvt(time_diff_break)
                else:
                    total_rows = db_res2 = db.query(models.BREAK).filter(
                        models.BREAK.Service_ID == row.Service_ID,
                        or_(
                        models.BREAK.break_time_start.like(f"%{date}%"),
                        models.BREAK.break_time_end.like(f"%{date}%")
                        )
                    ).count()
                    if total_rows>0:
                        db_res2 = db.query(models.BREAK).filter(
                        models.BREAK.Service_ID == row.Service_ID,
                        or_(
                        models.BREAK.break_time_start.like(f"%{date}%"),
                        models.BREAK.break_time_end.like(f"%{date}%")
                        )).limit(total_rows - 1).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.break_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.break_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_break = time_diff_break + (date_time11 - date_time22)
                        
                        print("First time"," ",time_diff_break)

                        db_res2 = db.query(models.BREAK).filter(
                            models.BREAK.Service_ID == row.Service_ID,
                            models.BREAK.break_time_start.like(f"%{date}%"),
                        ).order_by(models.BREAK.id.desc()).first()
                        date_time11 = date_time1
                        date_time22 = datetime.strptime(db_res2.break_time_start, '%Y-%m-%d %H:%M:%S')
                        
                        print("Second time"," ",date_time22)
                        
                        time_diff_break = time_diff_break + (date_time11 - date_time22)
                        
                        print("final time"," ",time_diff_break)
                        
                        if time_diff_break!=0:

                            print("BI"," ",time_diff_break)
                            
                            time_diff_break = cvt(time_diff_break)

                        data["break"] = time_diff_break


                #-------call Hour------------#

                time_diff_call = timedelta(hours=0)
                if row.work_status == "Work in Progress":
                    
                    counter = db_res2 = db.query(models.CALL).filter(
                        models.CALL.Service_ID == row.Service_ID,
                        or_(
                        models.CALL.call_time_start.like(f"%{date}%"),
                        models.CALL.call_time_end .like(f"%{date}%")
                        )
                    ).count()
                    if counter > 0:
                        db_res2 = db.query(models.CALL).filter(
                            models.CALL.Service_ID == row.Service_ID,
                            or_(
                            models.CALL.call_time_start.like(f"%{date}%"),
                            models.CALL.call_time_end .like(f"%{date}%")
                            )
                        ).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.call_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.call_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_call = time_diff_call + (date_time11 - date_time22)

                        if time_diff_call!=0:
                            time_diff_call = cvt(time_diff_call)
                else:
                    total_rows = db_res2 = db.query(models.CALL).filter(
                        models.CALL.Service_ID == row.Service_ID,
                        or_(
                        models.CALL.call_time_start.like(f"%{date}%"),
                        models.CALL.call_time_end.like(f"%{date}%")
                        )
                    ).count()
                    if total_rows>0:
                        db_res2 = db.query(models.CALL).filter(
                        models.CALL.Service_ID == row.Service_ID,
                        or_(
                        models.CALL.call_time_start.like(f"%{date}%"),
                        models.CALL.call_time_end.like(f"%{date}%")
                        )).limit(total_rows - 1).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.call_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.call_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_call = time_diff_call + (date_time11 - date_time22)
                        
                        print("First time"," ",time_diff_call)

                        db_res2 = db.query(models.CALL).filter(
                            models.CALL.Service_ID == row.Service_ID,
                            models.CALL.call_time_start.like(f"%{date}%"),
                        ).order_by(models.CALL.id.desc()).first()
                        date_time11 = date_time1
                        date_time22 = datetime.strptime(db_res2.call_time_start, '%Y-%m-%d %H:%M:%S')
                        
                        print("Second time"," ",date_time22)
                        
                        time_diff_call = time_diff_call + (date_time11 - date_time22)
                        
                        print("final time"," ",time_diff_call)
                        
                        if time_diff_call!=0:

                            print("BI"," ",time_diff_call)
                            
                            time_diff_call = cvt(time_diff_call)

                        data["call"] = time_diff_call

                
                try:
                    
                    time_diff_worktime = pendulum.duration(seconds=time_diff_work) 
                    time_diff_work = time_diff_worktime

                except:

                    time_diff_work = pendulum.duration(hours=0)

                try:
                    time_diff_calltime = pendulum.duration(seconds=time_diff_call) 
                    time_diff_call = time_diff_calltime 

                except:

                    time_diff_call = pendulum.duration(hours=0)

                try:

                    time_diff_breaktime = pendulum.duration(seconds=time_diff_break) 
                    time_diff_break = time_diff_breaktime 

                except:

                    time_diff_break = pendulum.duration(hours=0)

                try:

                    time_diff_meettime = pendulum.duration(seconds=time_diff_meet) 
                    time_diff_meet = time_diff_meettime 

                except:

                    time_diff_meet = pendulum.duration(hours=0)

                try:

                    time_diff_holdtime = pendulum.duration(seconds=time_diff_hold) 
                    time_diff_hold = time_diff_holdtime 

                except:

                    time_diff_hold = pendulum.duration(hours=0)

                try:
                    time_diff_endtime = pendulum.duration(seconds=time_diff_end) 
                    time_diff_end = time_diff_endtime 

                except:

                    time_diff_end = pendulum.duration(hours=0)

                print(time_diff_work,time_diff_call,time_diff_break,time_diff_meet,time_diff_hold,time_diff_end)
                
                print(data)
                if(row.work_status=="Completed"):
                    data["in_progress"] = 0
                    data["completed"] = time_diff_work-(time_diff_call+time_diff_break+time_diff_meet+time_diff_hold+time_diff_end)
                    data["completed"] = cvt2(data["completed"])
                    print(data["completed"],"22222222222222222222222222222222222222222222")
                else:
                    data["completed"] = 0
                    data["in_progress"] = time_diff_work-(time_diff_call+time_diff_break+time_diff_meet+time_diff_hold+time_diff_end)
                    data["in_progress"] = cvt2(data["in_progress"])
                    print(data["in_progress"],"22222222222222222222222222222222222222222222")
                    

            return []

def userwisechargable(db: Session, date: str, user_id:int):

            list_data = []

            date_time1 = datetime.now()

            db_res = db.query(models.TL).filter(
            models.TL.type_of_activity == 'CHARGABLE',
            or_(
                models.TL.working_time.like(f"%{date}%"),
                models.TL.reallocated_time.like(f"%{date}%")
            )
            ).all()
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
                data["estimated_time"] =  row.estimated_time
                data["member_name"] = row._user_table1.firstname +' '+ row._user_table1.lastname

                date_time2 = datetime.strptime(row.working_time, '%Y-%m-%d %H:%M:%S')
                time_diff_work = date_time1 - date_time2
                if time_diff_work!=0:
                    time_diff_work = cvt(time_diff_work)
                    print(time_diff_work)

                
                #-------EOD------------#

                time_diff_end = timedelta(hours=0)
                if row.work_status == "Work in Progress":
                    
                    counter = db_res2 = db.query(models.END_OF_DAY).filter(
                        models.END_OF_DAY.Service_ID == row.Service_ID,
                        or_(
                        models.END_OF_DAY.end_time_start.like(f"%{date}%"),
                        models.END_OF_DAY.end_time_end .like(f"%{date}%")
                        )
                    ).count()
                    if counter > 0:
                        db_res2 = db.query(models.END_OF_DAY).filter(
                            models.END_OF_DAY.Service_ID == row.Service_ID,
                            or_(
                            models.END_OF_DAY.end_time_start.like(f"%{date}%"),
                            models.END_OF_DAY.end_time_end .like(f"%{date}%")
                            )
                        ).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.end_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.end_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_end = time_diff_end + (date_time11 - date_time22)

                        if time_diff_end!=0:
                            time_diff_end = cvt(time_diff_end)
                else:
                    total_rows = db_res2 = db.query(models.END_OF_DAY).filter(
                        models.END_OF_DAY.Service_ID == row.Service_ID,
                        or_(
                        models.END_OF_DAY.end_time_start.like(f"%{date}%"),
                        models.END_OF_DAY.end_time_end.like(f"%{date}%")
                        )
                    ).count()
                    if total_rows>0:
                        db_res2 = db.query(models.END_OF_DAY).filter(
                        models.END_OF_DAY.Service_ID == row.Service_ID,
                        or_(
                        models.END_OF_DAY.end_time_start.like(f"%{date}%"),
                        models.END_OF_DAY.end_time_end.like(f"%{date}%")
                        )).limit(total_rows - 1).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.end_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.end_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_end = time_diff_end + (date_time11 - date_time22)
                        
                        print("First time"," ",time_diff_end)

                        db_res2 = db.query(models.END_OF_DAY).filter(
                            models.END_OF_DAY.Service_ID == row.Service_ID,
                            models.END_OF_DAY.end_time_start.like(f"%{date}%"),
                        ).order_by(models.END_OF_DAY.id.desc()).first()
                        date_time11 = date_time1
                        date_time22 = datetime.strptime(db_res2.end_time_start, '%Y-%m-%d %H:%M:%S')
                        
                        print("Second time"," ",date_time22)
                        
                        time_diff_end = time_diff_end + (date_time11 - date_time22)
                        
                        print("final time"," ",time_diff_end)
                        
                        if time_diff_end!=0:

                            print("BI"," ",time_diff_end)
                            
                            time_diff_end = cvt(time_diff_end)

                        data["end_time"] = time_diff_end
 
                #-------HOLD------------#

                time_diff_hold = timedelta(hours=0)
                if row.work_status == "Work in Progress":
                    
                    counter = db_res2 = db.query(models.HOLD).filter(
                        models.HOLD.Service_ID == row.Service_ID,
                        or_(
                        models.HOLD.hold_time_start.like(f"%{date}%"),
                        models.HOLD.hold_time_end .like(f"%{date}%")
                        )
                    ).count()
                    if counter > 0:
                        db_res2 = db.query(models.HOLD).filter(
                            models.HOLD.Service_ID == row.Service_ID,
                            or_(
                            models.HOLD.hold_time_start.like(f"%{date}%"),
                            models.HOLD.hold_time_end .like(f"%{date}%")
                            )
                        ).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.hold_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.hold_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_hold = time_diff_hold + (date_time11 - date_time22)

                        if time_diff_hold!=0:
                            time_diff_hold = cvt(time_diff_hold)
                else:
                    total_rows = db_res2 = db.query(models.HOLD).filter(
                        models.HOLD.Service_ID == row.Service_ID,
                        or_(
                        models.HOLD.hold_time_start.like(f"%{date}%"),
                        models.HOLD.hold_time_end.like(f"%{date}%")
                        )
                    ).count()
                    if total_rows>0:
                        db_res2 = db.query(models.HOLD).filter(
                        models.HOLD.Service_ID == row.Service_ID,
                        or_(
                        models.HOLD.hold_time_start.like(f"%{date}%"),
                        models.HOLD.hold_time_end.like(f"%{date}%")
                        )).limit(total_rows - 1).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.hold_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.hold_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_hold = time_diff_hold + (date_time11 - date_time22)
                        
                        print("First time"," ",time_diff_hold)

                        db_res2 = db.query(models.HOLD).filter(
                            models.HOLD.Service_ID == row.Service_ID,
                            models.HOLD.hold_time_start.like(f"%{date}%"),
                        ).order_by(models.HOLD.id.desc()).first()
                        date_time11 = date_time1
                        date_time22 = datetime.strptime(db_res2.hold_time_start, '%Y-%m-%d %H:%M:%S')
                        
                        print("Second time"," ",date_time22)
                        
                        time_diff_hold = time_diff_hold + (date_time11 - date_time22)
                        
                        print("final time"," ",time_diff_hold)
                        
                        if time_diff_hold!=0:

                            print("BI"," ",time_diff_hold)
                            
                            time_diff_hold = cvt(time_diff_hold)

                        data["hold"] = time_diff_hold

                #------- meeting ------------#

                time_diff_meet = timedelta(hours=0)
                if row.work_status == "Work in Progress":
                    
                    counter = db_res2 = db.query(models.MEETING).filter(
                        models.MEETING.Service_ID == row.Service_ID,
                        or_(
                        models.MEETING.meeting_time_start.like(f"%{date}%"),
                        models.MEETING.meeting_time_end .like(f"%{date}%")
                        )
                    ).count()
                    if counter > 0:
                        db_res2 = db.query(models.MEETING).filter(
                            models.MEETING.Service_ID == row.Service_ID,
                            or_(
                            models.MEETING.meeting_time_start.like(f"%{date}%"),
                            models.MEETING.meeting_time_end .like(f"%{date}%")
                            )
                        ).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.meeting_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.meeting_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_meet = time_diff_meet + (date_time11 - date_time22)

                        if time_diff_meet!=0:
                            time_diff_meet = cvt(time_diff_meet)
                else:
                    total_rows = db_res2 = db.query(models.MEETING).filter(
                        models.MEETING.Service_ID == row.Service_ID,
                        or_(
                        models.MEETING.meeting_time_start.like(f"%{date}%"),
                        models.MEETING.meeting_time_end.like(f"%{date}%")
                        )
                    ).count()
                    if total_rows>0:
                        db_res2 = db.query(models.MEETING).filter(
                        models.MEETING.Service_ID == row.Service_ID,
                        or_(
                        models.MEETING.meeting_time_start.like(f"%{date}%"),
                        models.MEETING.meeting_time_end.like(f"%{date}%")
                        )).limit(total_rows - 1).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.meeting_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.meeting_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_meet = time_diff_meet + (date_time11 - date_time22)
                        
                        print("First time"," ",time_diff_meet)

                        db_res2 = db.query(models.MEETING).filter(
                            models.MEETING.Service_ID == row.Service_ID,
                            models.MEETING.meeting_time_start.like(f"%{date}%"),
                        ).order_by(models.MEETING.id.desc()).first()
                        date_time11 = date_time1
                        date_time22 = datetime.strptime(db_res2.meeting_time_start, '%Y-%m-%d %H:%M:%S')
                        
                        print("Second time"," ",date_time22)
                        
                        time_diff_meet = time_diff_meet + (date_time11 - date_time22)
                        
                        print("final time"," ",time_diff_meet)
                        
                        if time_diff_meet!=0:

                            print("BI"," ",time_diff_meet)
                            
                            time_diff_meet = cvt(time_diff_meet)

                        data["meeting"] = time_diff_meet

                #-------Break Hour------------#

                time_diff_break = timedelta(hours=0)
                if row.work_status == "Work in Progress":
                    
                    counter = db_res2 = db.query(models.BREAK).filter(
                        models.BREAK.Service_ID == row.Service_ID,
                        or_(
                        models.BREAK.break_time_start.like(f"%{date}%"),
                        models.BREAK.break_time_end .like(f"%{date}%")
                        )
                    ).count()
                    if counter > 0:
                        db_res2 = db.query(models.BREAK).filter(
                            models.BREAK.Service_ID == row.Service_ID,
                            or_(
                            models.BREAK.break_time_start.like(f"%{date}%"),
                            models.BREAK.break_time_end .like(f"%{date}%")
                            )
                        ).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.break_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.break_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_break = time_diff_break + (date_time11 - date_time22)

                        if time_diff_break!=0:
                            time_diff_break = cvt(time_diff_break)
                else:
                    total_rows = db_res2 = db.query(models.BREAK).filter(
                        models.BREAK.Service_ID == row.Service_ID,
                        or_(
                        models.BREAK.break_time_start.like(f"%{date}%"),
                        models.BREAK.break_time_end.like(f"%{date}%")
                        )
                    ).count()
                    if total_rows>0:
                        db_res2 = db.query(models.BREAK).filter(
                        models.BREAK.Service_ID == row.Service_ID,
                        or_(
                        models.BREAK.break_time_start.like(f"%{date}%"),
                        models.BREAK.break_time_end.like(f"%{date}%")
                        )).limit(total_rows - 1).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.break_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.break_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_break = time_diff_break + (date_time11 - date_time22)
                        
                        print("First time"," ",time_diff_break)

                        db_res2 = db.query(models.BREAK).filter(
                            models.BREAK.Service_ID == row.Service_ID,
                            models.BREAK.break_time_start.like(f"%{date}%"),
                        ).order_by(models.BREAK.id.desc()).first()
                        date_time11 = date_time1
                        date_time22 = datetime.strptime(db_res2.break_time_start, '%Y-%m-%d %H:%M:%S')
                        
                        print("Second time"," ",date_time22)
                        
                        time_diff_break = time_diff_break + (date_time11 - date_time22)
                        
                        print("final time"," ",time_diff_break)
                        
                        if time_diff_break!=0:

                            print("BI"," ",time_diff_break)
                            
                            time_diff_break = cvt(time_diff_break)

                        data["break"] = time_diff_break


                #-------call Hour------------#

                time_diff_call = timedelta(hours=0)
                if row.work_status == "Work in Progress":
                    
                    counter = db_res2 = db.query(models.CALL).filter(
                        models.CALL.Service_ID == row.Service_ID,
                        or_(
                        models.CALL.call_time_start.like(f"%{date}%"),
                        models.CALL.call_time_end .like(f"%{date}%")
                        )
                    ).count()
                    if counter > 0:
                        db_res2 = db.query(models.CALL).filter(
                            models.CALL.Service_ID == row.Service_ID,
                            or_(
                            models.CALL.call_time_start.like(f"%{date}%"),
                            models.CALL.call_time_end .like(f"%{date}%")
                            )
                        ).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.call_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.call_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_call = time_diff_call + (date_time11 - date_time22)

                        if time_diff_call!=0:
                            time_diff_call = cvt(time_diff_call)
                else:
                    total_rows = db_res2 = db.query(models.CALL).filter(
                        models.CALL.Service_ID == row.Service_ID,
                        or_(
                        models.CALL.call_time_start.like(f"%{date}%"),
                        models.CALL.call_time_end.like(f"%{date}%")
                        )
                    ).count()
                    if total_rows>0:
                        db_res2 = db.query(models.CALL).filter(
                        models.CALL.Service_ID == row.Service_ID,
                        or_(
                        models.CALL.call_time_start.like(f"%{date}%"),
                        models.CALL.call_time_end.like(f"%{date}%")
                        )).limit(total_rows - 1).all()

                        for row2 in db_res2:
                            date_time11 = datetime.strptime(row2.call_time_end, '%Y-%m-%d %H:%M:%S')
                            date_time22 = datetime.strptime(row2.call_time_start, '%Y-%m-%d %H:%M:%S')
                            time_diff_call = time_diff_call + (date_time11 - date_time22)
                        
                        print("First time"," ",time_diff_call)

                        db_res2 = db.query(models.CALL).filter(
                            models.CALL.Service_ID == row.Service_ID,
                            models.CALL.call_time_start.like(f"%{date}%"),
                        ).order_by(models.CALL.id.desc()).first()
                        date_time11 = date_time1
                        date_time22 = datetime.strptime(db_res2.call_time_start, '%Y-%m-%d %H:%M:%S')
                        
                        print("Second time"," ",date_time22)
                        
                        time_diff_call = time_diff_call + (date_time11 - date_time22)
                        
                        print("final time"," ",time_diff_call)
                        
                        if time_diff_call!=0:

                            print("BI"," ",time_diff_call)
                            
                            time_diff_call = cvt(time_diff_call)

                        data["call"] = time_diff_call

                
                try:
                    
                    time_diff_worktime = pendulum.duration(seconds=time_diff_work) 
                    time_diff_work = time_diff_worktime

                except:

                    time_diff_work = pendulum.duration(hours=0)

                try:
                    time_diff_calltime = pendulum.duration(seconds=time_diff_call) 
                    time_diff_call = time_diff_calltime 

                except:

                    time_diff_call = pendulum.duration(hours=0)

                try:

                    time_diff_breaktime = pendulum.duration(seconds=time_diff_break) 
                    time_diff_break = time_diff_breaktime 

                except:

                    time_diff_break = pendulum.duration(hours=0)

                try:

                    time_diff_meettime = pendulum.duration(seconds=time_diff_meet) 
                    time_diff_meet = time_diff_meettime 

                except:

                    time_diff_meet = pendulum.duration(hours=0)

                try:

                    time_diff_holdtime = pendulum.duration(seconds=time_diff_hold) 
                    time_diff_hold = time_diff_holdtime 

                except:

                    time_diff_hold = pendulum.duration(hours=0)

                try:
                    time_diff_endtime = pendulum.duration(seconds=time_diff_end) 
                    time_diff_end = time_diff_endtime 

                except:

                    time_diff_end = pendulum.duration(hours=0)

                print(time_diff_work,time_diff_call,time_diff_break,time_diff_meet,time_diff_hold,time_diff_end)
                
                
                if(row.work_status=="Completed"):
                    data["in_progress"] = 0
                    data["completed"] = time_diff_work-(time_diff_call+time_diff_break+time_diff_meet+time_diff_hold+time_diff_end)
                    data["completed"] = cvt2(data["completed"])
                    print(data["completed"],"22222222222222222222222222222222222222222222")
                else:
                    data["completed"] = 0
                    data["in_progress"] = time_diff_work-(time_diff_call+time_diff_break+time_diff_meet+time_diff_hold+time_diff_end)
                    data["in_progress"] = cvt2(data["in_progress"])
                    print(data["in_progress"],"22222222222222222222222222222222222222222222")
                    

            return []
#----------------------------------------------------------------------------
def User_Wise_Day_Wise_Part_1(db: Session, picked_date: str, to_date: str):
    date_time_formate_string = '%Y-%m-%d %H:%M:%S'
    list_data = []



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
    print(dates_list)

    for item in dates_list:
        print(item)
        user_wise_report(db,item)

    

    converted_list_of_dicts = []
    return converted_list_of_dicts

#-------------------------------------------------------------------------------------------


#-------------------------------------------------------------------------------------------

def insert_tds(db:Session,tds_str:str):
   db_tds = models.tds(tds = tds_str)
   db.add(db_tds)
   try:
        db.commit()
        return "Success"
   except :
       db.rollback()
       return "Failure"

def get_tds(db:Session):
    return db.query(models.tds).filter(models.tds.tds_status==1).all()

def delete_tds(db:Session,tds_id:int):
    db_res = db.query(models.tds).filter(models.tds.tds_id==tds_id).first()
    db_res.tds_status = 0
    try:
        db.commit()
        return "Success"
    except:
        return "Failure"

def update_tds(db:Session,tds_name:str,tds_id:int):
    db_res = db.query(models.tds).filter(models.tds.tds_id==tds_id).first()
    db_res.tds = tds_name
    try:
        db.commit()
        return "Success"
    except:
        return "Failure"
    
#-------------------------------------------------------------------------------------------

def insert_gst(db:Session,gst_str:str):
   db_gst = models.gst(gst = gst_str)
   db.add(db_gst)
   try:
        db.commit()
        return "Success"
   except :
       db.rollback()
       return "Failure"

def get_gst(db:Session):
    return db.query(models.gst).filter(models.gst.gst_status==1).all()

def delete_gst(db:Session,gst_id:int):
    db_res = db.query(models.gst).filter(models.gst.gst_id==gst_id).first()
    db_res.gst_status = 0
    try:
        db.commit()
        return "Success"
    except:
        return "Failure"

def update_gst(db:Session,gst:str,gst_id:int):
    db_res = db.query(models.gst).filter(models.gst.gst_id==gst_id).first()
    db_res.gst = gst
    try:
        db.commit()
        return "Success"
    except:
        return "Failure"
    
#-------------------------------------------------------------------------------------------


def delete_entity(db:Session,record_service_id:int):
    db_res = db.query(models.TL).filter(models.TL.Service_ID==record_service_id).first()
    db_res.status = 0
    try:
        db.commit()
        return "Success"
    except:
        return "Failure"
    
#-------------------------------------------------------------------------------------------
