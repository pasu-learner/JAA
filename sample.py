    date_time1 = datetime.now()

    db_res = db.query(models.TL).filter(
    or_(
        models.TL.working_time.between(picked_date, to_date),
        models.TL.reallocated_time.between(picked_date, to_date)
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
        print(time_diff_work)
        if time_diff_work!=0:
            time_diff_work = cvt(time_diff_work)

        #-------EOD------------#

        time_diff_end = timedelta(hours=0)
        if row.work_status == "Work in Progress":
            db_res2 = db.query(models.END_OF_DAY).filter(
                models.END_OF_DAY.Service_ID == row.Service_ID,
                models.END_OF_DAY.end_time_start >= picked_date,
                models.END_OF_DAY.end_time_end <= to_date
            ).all()

            for row2 in db_res2:
                date_time11 = datetime.strptime(row2.end_time_end, '%Y-%m-%d %H:%M:%S')
                date_time22 = datetime.strptime(row2.end_time_start, '%Y-%m-%d %H:%M:%S')
                time_diff_end = time_diff_end + (date_time11 - date_time22)

            if time_diff_end!=0:
                print("HI", " ",time_diff_end)
                time_diff_end = cvt(time_diff_end)
        else:
            total_rows = db.query(func.count(models.END_OF_DAY.id)).scalar()
            print(total_rows)
            db_res2 = db.query(models.END_OF_DAY).filter(
                models.END_OF_DAY.Service_ID == row.Service_ID,
                models.END_OF_DAY.end_time_start >= picked_date,
                models.END_OF_DAY.end_time_end <= to_date
            ).limit(total_rows - 1).all()

            for row2 in db_res2:
                date_time11 = datetime.strptime(row2.end_time_end, '%Y-%m-%d %H:%M:%S')
                date_time22 = datetime.strptime(row2.end_time_start, '%Y-%m-%d %H:%M:%S')
                time_diff_end = time_diff_end + (date_time11 - date_time22)
            print("First time"," ",time_diff_end)

            db_res2 = db.query(models.END_OF_DAY).filter(
                models.END_OF_DAY.Service_ID == row.Service_ID,
                models.END_OF_DAY.end_time_start >= picked_date,
            ).order_by(models.END_OF_DAY.id.desc()).first()
            date_time11 = date_time1
            date_time22 = datetime.strptime(db_res2.end_time_start, '%Y-%m-%d %H:%M:%S')
            print("Second time"," ",date_time22)
            time_diff_end = time_diff_end + (date_time11 - date_time22)
            print("final time"," ",time_diff_end)
            if time_diff_end!=0:
                print("BI"," ",time_diff_end)
                time_diff_end = cvt(time_diff_end)
          
# data

            data["end_time"] = time_diff_end    

        #-----Hold Hour------#

        time_diff_hold = timedelta(hours=0)
        if row.work_status == "Work in Progress":
            db_res2 = db.query(models.HOLD).filter(
                models.HOLD.Service_ID == row.Service_ID,
                models.HOLD.hold_time_start >= picked_date,
                models.HOLD.hold_time_end <= to_date
            ).all()

            for row2 in db_res2:
                date_time11 = datetime.strptime(row2.hold_time_end, '%Y-%m-%d %H:%M:%S')
                date_time22 = datetime.strptime(row2.hold_time_start, '%Y-%m-%d %H:%M:%S')
                time_diff_hold = time_diff_hold + (date_time11 - date_time22)

            if time_diff_hold!=0:
                print("HI", " ",time_diff_hold)
                time_diff_hold = cvt(time_diff_hold)
        else:
            total_rows = db.query(func.count(models.HOLD.id)).scalar()
            print(total_rows)
            db_res2 = db.query(models.HOLD).filter(
                models.HOLD.Service_ID == row.Service_ID,
                models.HOLD.hold_time_start >= picked_date,
                models.HOLD.hold_time_end <= to_date
            ).limit(total_rows - 1).all()

            for row2 in db_res2:
                date_time11 = datetime.strptime(row2.hold_time_end, '%Y-%m-%d %H:%M:%S')
                date_time22 = datetime.strptime(row2.hold_time_start, '%Y-%m-%d %H:%M:%S')
                time_diff_hold = time_diff_hold + (date_time11 - date_time22)
            print("First time"," ",time_diff_hold)

            db_res2 = db.query(models.HOLD).filter(
                models.HOLD.Service_ID == row.Service_ID,
                models.HOLD.hold_time_start >= picked_date,
            ).order_by(models.HOLD.id.desc()).first()
            date_time11 = date_time1
            date_time22 = datetime.strptime(db_res2.hold_time_start, '%Y-%m-%d %H:%M:%S')
            print("Second time"," ",date_time22)
            time_diff_hold = time_diff_hold + (date_time11 - date_time22)
            print("final time"," ",time_diff_hold)
            if time_diff_hold!=0:
                print("BI"," ",time_diff_hold)
                time_diff_hold = cvt(time_diff_hold)

# data 

            data["hold"] = time_diff_hold

        # #-----Meeting Hour------#

        time_diff_meet = timedelta(hours=0)
        if row.work_status == "Work in Progress":
            db_res2 = db.query(models.MEETING).filter(
                models.MEETING.Service_ID == row.Service_ID,
                models.MEETING.meeting_time_start >= picked_date,
                models.MEETING.meeting_time_end <= to_date
            ).all()

            for row2 in db_res2:
                date_time11 = datetime.strptime(row2.meeting_time_end, '%Y-%m-%d %H:%M:%S')
                date_time22 = datetime.strptime(row2.meeting_time_start, '%Y-%m-%d %H:%M:%S')
                time_diff_meet = time_diff_meet + (date_time11 - date_time22)

            if time_diff_meet!=0:
                print("HI", " ",time_diff_meet)
                time_diff_meet = cvt(time_diff_meet)
        else:
            total_rows = db.query(func.count(models.MEETING.id)).scalar()
            print(total_rows)
            db_res2 = db.query(models.MEETING).filter(
                models.MEETING.Service_ID == row.Service_ID,
                models.MEETING.meeting_time_start >= picked_date,
                models.MEETING.meeting_time_end <= to_date
            ).limit(total_rows - 1).all()

            for row2 in db_res2:
                date_time11 = datetime.strptime(row2.meeting_time_end, '%Y-%m-%d %H:%M:%S')
                date_time22 = datetime.strptime(row2.meeting_time_start, '%Y-%m-%d %H:%M:%S')
                time_diff_meet = time_diff_meet + (date_time11 - date_time22)
            print("First time"," ",time_diff_meet)

            db_res2 = db.query(models.MEETING).filter(
                models.MEETING.Service_ID == row.Service_ID,
                models.MEETING.meeting_time_start >= picked_date,
            ).order_by(models.MEETING.id.desc()).first()
            date_time11 = date_time1
            date_time22 = datetime.strptime(db_res2.meeting_time_start, '%Y-%m-%d %H:%M:%S')
            print("Second time"," ",date_time22)
            time_diff_meet = time_diff_meet + (date_time11 - date_time22)
            print("final time"," ",time_diff_meet)
            if time_diff_meet!=0:
                print("BI"," ",time_diff_meet)
                time_diff_meet = cvt(time_diff_meet)

# data 
            data["meeting"] = time_diff_meet

        #-----Break Hour------#
        
        time_diff_break = timedelta(hours=0)
        if row.work_status == "Work in Progress":
            db_res2 = db.query(models.BREAK).filter(
                models.BREAK.Service_ID == row.Service_ID,
                models.BREAK.break_time_start >= picked_date,
                models.BREAK.break_time_end <= to_date
            ).all()

            for row2 in db_res2:
                date_time11 = datetime.strptime(row2.break_time_end, '%Y-%m-%d %H:%M:%S')
                date_time22 = datetime.strptime(row2.break_time_start, '%Y-%m-%d %H:%M:%S')
                time_diff_break = time_diff_break + (date_time11 - date_time22)

            if time_diff_break!=0:
                print("HI", " ",time_diff_break)
                time_diff_break = cvt(time_diff_break)
        else:
            total_rows = db.query(func.count(models.BREAK.id)).scalar()
            print(total_rows)
            db_res2 = db.query(models.BREAK).filter(
                models.BREAK.Service_ID == row.Service_ID,
                models.BREAK.break_time_start >= picked_date,
                models.BREAK.break_time_end <= to_date
            ).limit(total_rows - 1).all()

            for row2 in db_res2:
                date_time11 = datetime.strptime(row2.break_time_end, '%Y-%m-%d %H:%M:%S')
                date_time22 = datetime.strptime(row2.break_time_start, '%Y-%m-%d %H:%M:%S')
                time_diff_break = time_diff_break + (date_time11 - date_time22)
            print("First time"," ",time_diff_break)

            db_res2 = db.query(models.BREAK).filter(
                models.BREAK.Service_ID == row.Service_ID,
                models.BREAK.break_time_start >= picked_date,
            ).order_by(models.BREAK.id.desc()).first()
            date_time11 = date_time1
            date_time22 = datetime.strptime(db_res2.break_time_start, '%Y-%m-%d %H:%M:%S')
            print("Second time"," ",date_time22)
            time_diff_break = time_diff_break + (date_time11 - date_time22)
            print("final time"," ",time_diff_break)
            if time_diff_break!=0:
                print("BI"," ",time_diff_break)
                time_diff_break = cvt(time_diff_break)

# data 

            data["break"] = time_diff_break


        # #-----Call Hour------#

        time_diff_call = timedelta(hours=0)
        if row.work_status == "Work in Progress":
            db_res2 = db.query(models.CALL).filter(
                models.CALL.Service_ID == row.Service_ID,
                models.CALL.call_time_start >= picked_date,
                models.CALL.call_time_end <= to_date
            ).all()

            for row2 in db_res2:
                date_time11 = datetime.strptime(row2.call_time_end, '%Y-%m-%d %H:%M:%S')
                date_time22 = datetime.strptime(row2.call_time_start, '%Y-%m-%d %H:%M:%S')
                time_diff_call = time_diff_call + (date_time11 - date_time22)

            if time_diff_call!=0:
                print("HI", " ",time_diff_call)
                time_diff_call = cvt(time_diff_call)
        else:
            total_rows = db.query(func.count(models.CALL.id)).scalar()
            print(total_rows)
            db_res2 = db.query(models.CALL).filter(
                models.CALL.Service_ID == row.Service_ID,
                models.CALL.call_time_start >= picked_date,
                models.CALL.call_time_end <= to_date
            ).limit(total_rows - 1).all()

            for row2 in db_res2:
                date_time11 = datetime.strptime(row2.call_time_end, '%Y-%m-%d %H:%M:%S')
                date_time22 = datetime.strptime(row2.call_time_start, '%Y-%m-%d %H:%M:%S')
                time_diff_call = time_diff_call + (date_time11 - date_time22)
            print("First time"," ",time_diff_call)

            db_res2 = db.query(models.CALL).filter(
                models.CALL.Service_ID == row.Service_ID,
                models.CALL.call_time_start >= picked_date,
            ).order_by(models.CALL.id.desc()).first()
            date_time11 = date_time1
            date_time22 = datetime.strptime(db_res2.call_time_start, '%Y-%m-%d %H:%M:%S')
            print("Second time"," ",date_time22)
            time_diff_call = time_diff_call + (date_time11 - date_time22)
            print("final time"," ",time_diff_call)
            if time_diff_call!=0:
                print("BI"," ",time_diff_call)
                time_diff_call = cvt(time_diff_call)

# data

            data["call"] = time_diff_call

        #--------------------------------------------------------------------------

        if(row.work_status=="Completed"):
            data["in_progress"] = 0
            data["completed"] = time_diff_work-(time_diff_call+time_diff_break+time_diff_meet+time_diff_hold+time_diff_end)
            data["completed"] = cvt2(data["completed"])
            print(data["completed"])
        else:
            data["completed"] = 0
            data["in_progress"] = time_diff_work-(time_diff_call+time_diff_break+time_diff_meet+time_diff_hold+time_diff_end)
            data["in_progress"] = cvt2(data["in_progress"])
            print(data["in_progress"])


# old part of code


        data["total_time_taken"] =  (data["in_progress"] + data["completed"] )
        # data["second_report_data"] = call_hour_diff + hold_hour_diff + data["completed"] + data["in_progress"]
        data["second_report_data"] =  data["completed"] + data["in_progress"]


# chargable code ---------------------------------------------------------------


        #-----Hold Hour------#

        time_diff_hold = timedelta(hours=0)
        if row.work_status == "Work in Progress":
            db_res2 = db.query(models.HOLD).join(
            models.TL, models.HOLD.Service_ID == models.TL.Service_ID
            ).filter(
                models.HOLD.Service_ID == row.Service_ID,
                models.HOLD.hold_time_start >= picked_date,
                models.HOLD.hold_time_end <= to_date,
                models.TL.type_of_activity == 'CHARGABLE'
            ).all()

            for row2 in db_res2:
                date_time11 = datetime.strptime(row2.hold_time_end, '%Y-%m-%d %H:%M:%S')
                date_time22 = datetime.strptime(row2.hold_time_start, '%Y-%m-%d %H:%M:%S')
                time_diff_end = time_diff_end + (date_time11 - date_time22)

            if time_diff_end!=0:
                print("HI", " ",time_diff_end)
                time_diff_end = cvt(time_diff_end)
        else:
            total_rows = db.query(func.count(models.HOLD.id)).scalar()
            print(total_rows)
            db_res2 = db.query(models.HOLD).filter(
                models.HOLD.Service_ID == row.Service_ID,
                models.HOLD.hold_time_start >= picked_date,
                models.HOLD.hold_time_end <= to_date
            ).limit(total_rows - 1).all()

            for row2 in db_res2:
                date_time11 = datetime.strptime(row2.hold_time_end, '%Y-%m-%d %H:%M:%S')
                date_time22 = datetime.strptime(row2.hold_time_start, '%Y-%m-%d %H:%M:%S')
                time_diff_end = time_diff_end + (date_time11 - date_time22)
            print("First time"," ",time_diff_end)

            db_res2 = db.query(models.HOLD).filter(
                models.HOLD.Service_ID == row.Service_ID,
                models.HOLD.hold_time_start >= picked_date,
            ).order_by(models.HOLD.id.desc()).first()
            date_time11 = date_time1
            date_time22 = datetime.strptime(db_res2.hold_time_start, '%Y-%m-%d %H:%M:%S')
            print("Second time"," ",date_time22)
            time_diff_end = time_diff_end + (date_time11 - date_time22)
            print("final time"," ",time_diff_end)
            if time_diff_end!=0:
                print("BI"," ",time_diff_end)
                time_diff_end = cvt(time_diff_end)

# data 

            data["hold"] = time_diff_hold

        # #-----Meeting Hour------#
