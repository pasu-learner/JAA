    date = '2024-07-30'

    query = db.query(models.HOLD).filter(
        models.HOLD.Service_ID == 4,
        models.HOLD.hold_time_start.like(f"%{date}%"),
        models.HOLD.hold_time_end.like(f"%{date}%")
    )
    print(query)
    if query:
        nu = query.count()
        re = query.all()
        print(nu,'dddddddddddddddd')
        print(re[nu-1].hold_time_end)
    else:
        print("No hold record found for Service_ID 14")

#--------------------------------------------------------------

    query = db.query(models.HOLD).filter(
        models.HOLD.Service_ID == 4,
        models.HOLD.hold_time_start < date
    ).order_by(func.abs(func.date(models.HOLD.hold_time_start) - date))

    if query:
        nu = query.count()
        re = query.all()
        print(nu,'hhhhhhhhhhhhhhhhhhhh')
        print(re[nu-1].hold_time_end)
    else:
        print("No hold record found for Service_ID 14")
