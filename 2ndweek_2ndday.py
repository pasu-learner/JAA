    date = '2024-07-29'
   
    list_data = []
    list_ddata1 = set()


    db_res2 = db.query(models.TL).filter(
        models.TL.status == 1,
        models.TL.working_time.like(f"%{date}%")    
    )

    for row2 in db_res2:
        list_ddata1.add(row2.Service_ID)

    db_res2 = db.query(models.TL).filter(
        models.TL.status == 1,
        models.TL.Service_ID == models.HOLD.Service_ID,
        or_(
            models.HOLD.hold_time_end.like(f"%{date}%"),
            models.TL.working_time.like(f"%{date}%")
        )
        
    )

    for row2 in db_res2:
        list_ddata1.add(row2.Service_ID)
    
    db_res2 = db.query(models.TL).filter(
        models.TL.status == 1,
        models.TL.Service_ID == models.END_OF_DAY.Service_ID,
        or_(
            models.END_OF_DAY.end_time_end.like(f"%{date}%"),
            models.TL.working_time.like(f"%{date}%")
        )
        
    )

    for row2 in db_res2:
        list_ddata1.add(row2.Service_ID)
        

# final query
    newquery =  db.query(models.TL).filter(
        models.TL.status == 1,
        models.TL.Service_ID.in_(list_ddata1)
    ).all()

    
#----------------------------
    for row2 in newquery:
        
        print("new one ",row2.name_of_entity)
