# main.py

#--------------------------------------------------------------------------------------------------

@app.post("/reportsnew")
def lastfivereports(picked_date:Annotated[str,Form()],to_date:Annotated[str,Form()],report_name:Annotated[str,Form()],db:Session=Depends(get_db)):
     return crud.lastfivereports(db,picked_date,to_date,report_name)


#---------------------------------------------------------------------------------------------------
# curd.py

def lastfivereports(db: Session, picked_date: str, to_date: str, reportoptions: str ):
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
    # print(dates_list)

    for item in dates_list:
        # print(item)
        list_data.append(report.user_wise_report(db,item,reportoptions))
        

    


    return list_data

#---------------------------------------------------------------------------------------------------
