import mysql.connector as mysql
import json

db_username = input("Enter mysql username: ")
db_password = input("Enter mysql password: ")
db_host = input("Enter mysql host (e.g. localhost): ")
db_database = input("Enter database (e.g. Yelp): ")

db = mysql.connect(user=db_username, password=db_password, host = db_host, database=db_database)

try:
    with open("review.json", 'r', encoding="utf-8") as f:
        count=0;
        for line in f:
            data = json.loads(line)
            cur=db.cursor()
            qry = "insert into review (review_id, user_id,business_id,stars,date,useful,funny,cool) values(%s,%s,%s,%s,%s,%s,%s,%s)"
            count=count+1
            cur.execute(qry,(data['review_id'],data['user_id'],data['business_id'],data['stars'],data['date'],data['useful'],data['funny'],data['cool']))

            print(count)

    db.commit()
    print ("records added successfully to review table")
except Exception as ex:
    print (ex)
    db.rollback()


try:
    with open("business.json", 'r', encoding="utf-8") as f:
        count=0;
        for line in f:
            data = json.loads(line)

            cur=db.cursor()
            qry = "insert into business (business_id, name, address,city,state,postal_code,latitude,longitude,stars,review_count,is_open,Categories,attributes,hours) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            count=count+1
            cur.execute(qry,(data['business_id'],data['name'],data['address'],data['city'],data['state'],data['postal_code'],data['latitude'],data['longitude'],data['stars'],data['review_count'],data['is_open'],data['categories'],json.dumps(data['attributes']),json.dumps(data['hours'])))

            print(count)

    db.commit()
    print ("records added successfully to business table")
except Exception as ex:
    print (ex)
    db.rollback()


try:
    with open("user.json", 'r', encoding="utf-8") as f:
        count=0;
        for line in f:
            data = json.loads(line)

            cur=db.cursor()
            qry = "insert into user (user_id, name, review_count,yelping_since,useful,funny,cool,elite,\
            fans,average_stars,compliment_hot,compliment_more,compliment_profile,compliment_cute,\
            compliment_list,compliment_note,compliment_plain,compliment_cool,compliment_funny,\
            compliment_writer,compliment_photos)\
             values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            count=count+1
            cur.execute(qry,(data['user_id'],data['name'],data['review_count'],data['yelping_since'],data['useful'],data['funny'],data['cool'],data['elite'],data['fans'],data['average_stars'],data['compliment_hot'],data['compliment_more'],data['compliment_profile'],data['compliment_cute'],data['compliment_list'],data['compliment_note'],data['compliment_plain'],data['compliment_cool'],data['compliment_funny'],data['compliment_writer'],data['compliment_photos']))

            print(count)

    db.commit()
    print ("records added successfully to user table")
except Exception as ex:
    print (ex)
    db.rollback()


db.close()