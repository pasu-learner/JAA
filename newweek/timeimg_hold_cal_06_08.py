
                if str(current_time_report.date()) == date:
                
                    


                        db_res2h = db.query(models.HOLD).filter(
                            models.HOLD.Service_ID == row.Service_ID,
                            models.HOLD.hold_time_start.like(f"%{date}%"),
                            or_(
                                models.HOLD.hold_time_end.like(f"%{date}%"),
                                models.HOLD.hold_time_end == None
                            )
                        ).all()

                        for row2 in db_res2h:
                            if hold_time_report != None:
                                if row2.hold_time_end and row2.hold_time_start:
                                    date_time11 = datetime.strptime(row2.hold_time_end, date_time_formate_string)
                                    date_time22 = datetime.strptime(row2.hold_time_start, date_time_formate_string)
                                    time_diff = date_time11 - date_time22
                                    hold_time_report += time_diff
                                else :
                                    if not(row.work_status == "Completed" or row.work_status == "in_progress"):
                                        time1 =  datetime.now() - datetime.strptime(row2.hold_time_start, date_time_formate_string)
                                        hold_time_report +=  time1
                            else:
                                if row2.hold_time_end and row2.hold_time_start:
                                    date_time11 = datetime.strptime(row2.hold_time_end, date_time_formate_string)
                                    date_time22 = datetime.strptime(row2.hold_time_start, date_time_formate_string)
                                    time_diff = date_time11 - date_time22
                                    hold_time_report = time_diff
                                else :
                                    if not(row.work_status == "Completed" or row.work_status == "in_progress"):
                                        time1 =  datetime.now() - datetime.strptime(row2.hold_time_start, date_time_formate_string)
                                        hold_time_report =  time1

                        data["hold"] = hold_time_report
                        
        #----------------------- hold code completed



                        # ----- Break Hour ------
                        db_res2 = db.query(models.BREAK).filter(
                            models.BREAK.Service_ID == row.Service_ID,
                            models.BREAK.break_time_start.like(f"%{date}%"),
                            or_(
                            models.BREAK.break_time_end.like(f"%{date}%"),
                            models.BREAK.break_time_end==None
                            )
                        ).all()



                        for row2 in db_res2:
                            if break_time_report != None:
                                if row2.break_time_end and row2.break_time_start:
                                    date_time11 = datetime.strptime(row2.break_time_end, date_time_formate_string)
                                    date_time22 = datetime.strptime(row2.break_time_start, date_time_formate_string)
                                    time_diff = date_time11 - date_time22
                                    break_time_report += time_diff
                                else :
                                    if not(row.work_status == "Completed" or row.work_status == "in_progress"):
                                        time1 =  datetime.now() - datetime.strptime(row2.break_time_start, date_time_formate_string)
                                    
                                        break_time_report += time1
                            else:
                                if row2.break_time_end and row2.break_time_start:
                                    date_time11 = datetime.strptime(row2.break_time_end, date_time_formate_string)
                                    date_time22 = datetime.strptime(row2.break_time_start, date_time_formate_string)
                                    time_diff = date_time11 - date_time22
                                    # print(type(time_diff),type(break_time_report),'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffff')
                                    break_time_report = time_diff
                                else :
                                    if not(row.work_status == "Completed" or row.work_status == "in_progress"):
                                        time1 =  datetime.now() - datetime.strptime(row2.break_time_start, date_time_formate_string)
                                    
                                        break_time_report = time1

                        data["break"] = break_time_report

        #------------------------- break code completed


                        # ----- Meeting Hour ------
                        db_res2 = db.query(models.MEETING).filter(
                            models.MEETING.Service_ID == row.Service_ID,
                            models.MEETING.meeting_time_start.like(f"%{date}%"),
                            or_(
                            models.MEETING.meeting_time_end.like(f"%{date}%"),
                            models.MEETING.meeting_time_end==None
                            )
                        ).all()

                        meeting_time_report = timedelta(hours=0)

                        for row2 in db_res2:
                            if meeting_time_report != None:
                                if row2.meeting_time_end and row2.meeting_time_start:
                                    date_time11 = datetime.strptime(row2.meeting_time_end, date_time_formate_string)
                                    date_time22 = datetime.strptime(row2.meeting_time_start, date_time_formate_string)
                                    time_diff = date_time11 - date_time22
                                    meeting_time_report += time_diff
                                else :
                                    if not(row.work_status == "Completed" or row.work_status == "in_progress"):
                                        time1 =  datetime.now() - datetime.strptime(row2.meeting_time_start, date_time_formate_string)
                                        meeting_time_report +=   time1
                            else:
                                if row2.meeting_time_end and row2.meeting_time_start:
                                    date_time11 = datetime.strptime(row2.meeting_time_end, date_time_formate_string)
                                    date_time22 = datetime.strptime(row2.meeting_time_start, date_time_formate_string)
                                    time_diff = date_time11 - date_time22
                                    meeting_time_report = time_diff
                                else :
                                    if not(row.work_status == "Completed" or row.work_status == "in_progress"):
                                        time1 =  datetime.now() - datetime.strptime(row2.meeting_time_start, date_time_formate_string)
                                        meeting_time_report =   time1                        


                        data["meeting"] = meeting_time_report

        #--------------------- meeting code end

                        db_res2 = db.query(models.CALL).filter(
                            models.CALL.Service_ID == row.Service_ID,
                            models.CALL.call_time_start.like(f"%{date}%"),
                            or_(
                            models.CALL.call_time_end.like(f"%{date}%"),
                            models.CALL.call_time_end==None
                            )
                        ).all()

                        call_hour_diff = timedelta(hours=0)

                        for row2 in db_res2:
                            if meeting_time_report != None:
                                if row2.call_time_end and row2.call_time_start:
                                    date_time11 = datetime.strptime(row2.call_time_end, date_time_formate_string)
                                    date_time22 = datetime.strptime(row2.call_time_start, date_time_formate_string)
                                    time_diff = date_time11 - date_time22
                                    call_hour_diff += time_diff
                                else :
                                    if not(row.work_status == "Completed" or row.work_status == "in_progress"):
                                        time1 =  datetime.now() - datetime.strptime(row2.call_time_start, date_time_formate_string)
                                    
                                        call_hour_diff += time1
                            else:
                                if row2.call_time_end and row2.call_time_start:
                                    date_time11 = datetime.strptime(row2.call_time_end, date_time_formate_string)
                                    date_time22 = datetime.strptime(row2.call_time_start, date_time_formate_string)
                                    time_diff = date_time11 - date_time22
                                    call_hour_diff = time_diff
                                else :
                                    if not(row.work_status == "Completed" or row.work_status == "in_progress"):
                                        time1 =  datetime.now() - datetime.strptime(row2.call_time_start, date_time_formate_string)
                                    
                                        call_hour_diff = time1
                        data["call"] = call_hour_diff

        #------------------------------ call code end

