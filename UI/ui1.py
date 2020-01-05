from tkinter import *
from tkinter.ttk import *
from matplotlib import pyplot
from mpl_toolkits.mplot3d import Axes3D
import numpy
import math
import mysql.connector as mysql
import json

db=mysql.connect()
def dbconnect(usr,paswrd,hst,datab):

    try:

        global db
        db= mysql.connect(user=usr, password=paswrd, host=hst, database=datab)
        if(db!=''):
            L5['text']='Connection DB successfull'
            B8['state']='disabled'
            B9['state']='normal'
            B1['state'] = 'normal'
            B2['state'] = 'normal'


    except Exception as ex:
        print(ex);
        L5['text'] = ex



def dbdisconnect():

    try:

        if(db!=''):
            db.close()
            L5['text'] = 'Disconnected from DB successfully'
            B8['state'] = 'normal'
            B9['state'] = 'disabled'
            B1['state'] = 'disabled'
            B2['state'] = 'disabled'
            B3['state'] = 'disabled'
            B4['state'] = 'disabled'
            B5['state'] = 'disabled'
            B6['state'] = 'disabled'
            B7['state'] = 'disabled'
            combo2['values']=[];
            B10['state']='disabled'

    except Exception as ex:
        print(ex)

def clean():
    print(db,'hello')
    try:
        L5['text'] = 'Started data cleaning'

        cursor = db.cursor()
        cursor.callproc('clean_data')
        cursor.close()
        L5['text'] = 'Completed data cleaning'
        B2['state'] = 'normal'
        B1['state'] = 'disabled'
    except Exception as ex:
        print(ex)
        db.rollback()

def analyse(k):
    try:
        k_val = int(k)
    except Exception as ex:
        print("k not an int!")
        return
    try:
        L5['text'] = 'Started data analysis'
        incity=combo.get();
        print(incity)
        k_val = int(k)
        if k_val<1 or k_val>50:
            k_val = 5
        cursor = db.cursor()
        cursor.callproc('analysis_proc',[incity,k_val,])


        cursor.close()
        L5['text'] = 'Completed data analysis'
        B3['state'] = 'normal'
        B4['state'] = 'normal'
    except Exception as ex:
        print(ex)
        db.rollback()
def validate(k):
    try:
        k_val = int(k)
    except Exception as ex:
        print("k not an int!")
        return
    try:
        L5['text'] = 'Started data validation'
        k_val = int(k)
        if k_val<1 or k_val>50:
            k_val = 5
        cursor = db.cursor()
        cursor.callproc('validation_proc',[k_val,])
        cursor.close()
        L5['text'] = 'Completed data validation'
        B5['state'] = 'normal'
        B6['state']='normal'
        B7['state']='normal'
        sql_select_Query = "select user_id from kmeans_final_validation_data limit 5";
        cursor = db.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        user_list = [];
        for row in records:
            cluster_id = row[0];
            user_list.append(cluster_id)
        combo2['values'] = user_list
        combo2.current(0)
        cursor.close()
        B10['state']='normal'
    except Exception as ex:
        print(ex)
        db.rollback()
def retrieveanalysisdata(k_val):
    try:
        k = int(k_val)
    except Exception as ex:
        print("k not an int!")
        return
    try:
        sql_select_Query = "select * from kmeans_final_analysis_data";
        cursor = db.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        #print(records[0])
        k = int(k_val)
        x_list = [[] for i in range(k)]
        y_list = [[] for i in range(k)]
        z_list = [[] for i in range(k)]
        for row in records:
            cluster_id = row[4];
            x_list[cluster_id - 1].append(row[1])
            y_list[cluster_id - 1].append(row[2])
            z_list[cluster_id - 1].append(row[3])

        fig = pyplot.figure()
        ax = Axes3D(fig)
        for i in range(k):
            c = numpy.random.rand(3, )
            c = c.tolist()
            ax.scatter(x_list[i], y_list[i], z_list[i], color=c)

        # for i in range(20):
        #    c = numpy.random.rand(3,)
        #     c=c.tolist()

        #        pyplot.scatter(x_list[i],y_list[i],color=c)

        ax.set_xlabel("Latitude")
        ax.set_ylabel("Longitude")
        ax.set_zlabel("Stars")

        pyplot.show()

        cursor.close()
    except Exception as ex:
        print(ex)
        db.rollback()
def retrievevalidationdata(k_val):
    try:
        k = int(k_val)
    except Exception as ex:
        print("k not an int!")
        return
    try:
        sql_select_Query = "select * from kmeans_final_validation_data";
        cursor = db.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        #print('validation data is ',records)
        #print(records[0])
        k = int(k_val)
        x_list = [[] for i in range(k)]
        y_list = [[] for i in range(k)]
        z_list = [[] for i in range(k)]
        for row in records:
            cluster_id = row[4];
            x_list[cluster_id - 1].append(row[1])
            y_list[cluster_id - 1].append(row[2])
            z_list[cluster_id - 1].append(row[3])

        fig = pyplot.figure()
        ax = Axes3D(fig)
        for i in range(k):
            c = numpy.random.rand(3, )
            c = c.tolist()
            ax.scatter(x_list[i], y_list[i], z_list[i], color=c)

        # for i in range(20):
        #    c = numpy.random.rand(3,)
        #     c=c.tolist()

        #        pyplot.scatter(x_list[i],y_list[i],color=c)

        ax.set_xlabel("Latitude")
        ax.set_ylabel("Longitude")
        ax.set_zlabel("Stars")

        pyplot.show()

        cursor.close()
    except Exception as ex:
        print(ex)
        db.rollback()
def seechangeiscentroid(k_val):
    try:
        k = int(k_val)
    except Exception as ex:
        print("k not an int!")
        return
    sql_select_Query = "select * from km_centroid_train_set order by id";
    cursor = db.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    k = int(k_val)
    x_list1 = [[] for i in range(k)]
    y_list1 = [[] for i in range(k)]
    z_list1 = [[] for i in range(k)]
    for row in records:
        cluster_id = row[0];
        x_list1[cluster_id - 1].append(row[1])
        y_list1[cluster_id - 1].append(row[2])
        z_list1[cluster_id - 1].append(row[3])
    cursor.close()

    sql_select_Query = "select * from km_centroid_test_set order by id";
    cursor = db.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()

    x_list2 = [[] for i in range(k)]
    y_list2 = [[] for i in range(k)]
    z_list2 = [[] for i in range(k)]
    for row in records:
        cluster_id = row[0];
        x_list2[cluster_id - 1].append(row[1])
        y_list2[cluster_id - 1].append(row[2])
        z_list2[cluster_id - 1].append(row[3])

    x_list3 = [[] for i in range(k)]
    y_list3 = [[] for i in range(k)]
    z_list3 = [[] for i in range(k)]
    for i in range(k):
        x_list3[i].append(x_list1[i][0])
        x_list3[i].append(x_list2[i][0])
        y_list3[i].append(y_list1[i][0])
        y_list3[i].append(y_list2[i][0])
        z_list3[i].append(z_list1[i][0])
        z_list3[i].append(z_list2[i][0])
    print(x_list3[0])
    print(y_list3[0])
    print(z_list3[0])
    fig = pyplot.figure()
    #ax = Axes3D(fig)
    #for i in range(k):
    #    c = numpy.random.rand(3, )
    #    c = c.tolist()
    #    ax.plot(x_list3[i], y_list3[i], z_list3[i], color=c)

    #ax.set_xlabel("Latitude")
    #ax.set_ylabel("Longitude")
    #ax.set_zlabel("Stars")
    #pyplot.show()
    rmse = 0
    centroid_shift = [0]*k
    for i in range(k):
    	centroid_shift[i] = math.sqrt((abs(x_list3[i][0]-x_list3[i][1])**2)+(abs(y_list3[i][0]-y_list3[i][1])**2)+(abs(z_list3[i][0]-z_list3[i][1])**2)) #/ \
    	#math.sqrt((abs(x_list3[i][0])**2)+(abs(y_list3[i][0])**2)+(abs(z_list3[i][0])**2));
    	rmse = rmse+((centroid_shift[i])**2)

    rmse = rmse/k
    L32['text'] = str(rmse)
    cluster_num = range(1,k+1)

    pyplot.stem(cluster_num, centroid_shift)

    pyplot.xlabel("Cluster ID")
    pyplot.ylabel("Centroid Shift euclidian L1 distance")
    #pyplot.ylim(0, 10)
    #pyplot.xlim(0, 20)
    pyplot.show()
    
def showpredictionerror(k_val):
    try:
        k = int(k_val)
    except Exception as ex:
        print("k not an int!")
        return
    sql_select_Query = "select * from km_centroid_test_set order by id";
    cursor = db.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    k = int(k_val)
    x_list2 = []
    y_list2 = []
    for row in records:
        x_list2.append(row[0])
        y_list2.append(row[4])

    print(x_list2)
    print(y_list2)
    fig = pyplot.figure()
    pyplot.stem(x_list2, y_list2)

    pyplot.xlabel("Cluster ID")
    pyplot.ylabel("Star Rating Prediction Error")
    pyplot.ylim(0, 5)
    #pyplot.xlim(0, 20)

    pyplot.show()

def recommend():
    try:

        L13['text'] =''
        L14['text'] = ''
        L15['text'] = ''
        L16['text'] = ''
        L17['text'] = ''
        u_id=combo2.get();
        #db = mysql.connect(user='mouli', password='root', host='localhost', database='mydb')
        cursor = db.cursor()
        cursor.callproc('sample_recommend', [u_id, ])
        records = cursor.stored_results()
        for row in records:
            temp = row.fetchall()
        cursor.close()
        for i in range(len(temp)):
            li=temp[i]
            res=li[0]+'---'+li[1]
            if(i==0):
                 L13['text']=res
            if(i==1):
                L14['text']=res
            if(i==2):
                L15['text']=res
            if(i==3):
                L16['text']=res
            if(i==4):
                L17['text']=res
    except Exception as ex:
        print(ex)
        db.rollback()
top = Tk()
canvas = Canvas(top, height=700, width=800)
canvas.pack()
top.title('ECE656')
L1 = Label(top, text = "1. Clean data for analysis")
L1.place(x = 10,y = 10)
B1 = Button(top, text = "Clean",command=lambda: clean(),state="disabled")
B1.place(x =230, y = 10)
L2 = Label(top, text = "2. Data analysis (12min)")
L2.place(x =10,y = 50)
L21 = Label(top, text = "Select value of k in range 1 to 50")
L21.place(x=20, y=70)
E21 = Entry(top)
E21.place(x=230, y=70)
B2 = Button(top, text = "Analyse",command=lambda: analyse(E21.get()),state="disabled")
B2.place(x =230, y = 95)
B4 = Button(top, text = "View Clusters Training Data",command=lambda: retrieveanalysisdata(E21.get()),state="disabled")
B4.place(x =230, y = 125)
combo = Combobox(top)

combo['values'] = ('Las vegas', 'Phoenix', 'Toronto')
combo.current(0)
combo.place(x=20,y=95)
L3 = Label(top, text = "3. Validation")
L3.place(x = 10,y = 180	)
B3 = Button(top, text = "Validate",command=lambda: validate(E21.get()),state="disabled")
B3.place(x =230, y = 180)

B5 = Button(top, text = "View Clusters Full Data (validation)",command=lambda: retrievevalidationdata(E21.get()),state="disabled")
B5.place(x =100, y = 210)
B6 = Button(top, text = "View Centroid Shift",command=lambda: seechangeiscentroid(E21.get()),state="disabled")
B6.place(x =100, y = 270)
L31 = Label(top, text = "Root mean square error")
L31.place(x = 100, y=245)
L32 = Label(top, text = "")
L32.place(x = 250, y=245)
B7 = Button(top, text = "View Star Rating Prediction error per cluster",command=lambda: showpredictionerror(E21.get()),state="disabled")
B7.place(x =100, y = 310)

L10 = Label(top, text = "4. Recommendation")
L10.place(x =10,y = 350)
B10 = Button(top, text = "Recommend",command=lambda: recommend(),state='disabled')
B10.place(x =230, y =350)
combo2 = Combobox(top)
combo2.place(x=20,y=380)
L11 = Label(top, text = "Select any Sample User ID from above to view their recommendation ")
L11.place(x =20,y = 410)
L12 = Label(top, text = "Recommendations: ")
L12.place(x =20,y = 430)
L13= Label(top, text = "")
L13.place(x =20,y = 450)
L14 = Label(top, text = "")
L14.place(x =20,y = 470)
L15 = Label(top, text = "")
L15.place(x =20,y = 490)
L16 = Label(top, text = "")
L16.place(x =20,y = 510)
L17 = Label(top, text = "")
L17.place(x =20,y = 530)


L4= Label(top, text = "System Status:")
L4.place(x = 10,y = 570)
L5= Label(top, text = "No running process")
L5.place(x = 10,y = 610)
L6= Label(top, text = "------------------------------------------------------------")
L6.place(x = 10,y = 630)
L7= Label(top, text = "User Name for DB")
L7.place(x = 500,y = 10)
E1 = Entry(top)
E1.place(x=670, y=10)
L8= Label(top, text = "Password for DB")
L8.place(x = 500,y = 50)
E2= Entry(top)
E2.place(x=670, y=50)
L9= Label(top, text = "Host")
L9.place(x = 500,y = 90)
E3 = Entry(top)
E3.place(x=670, y=90)
L9= Label(top, text = "Database")
L9.place(x = 500,y = 130)
E4 = Entry(top)
E4.place(x=670, y=130)
B8 = Button(top, text = "Connect to DB",command=lambda: dbconnect(E1.get(),E2.get(),E3.get(),E4.get()))
B8.place(x =500, y = 170)
B9 = Button(top, text = "Disconnect ",command=lambda: dbdisconnect(),state='disabled')
B9.place(x =670, y = 170)

top.mainloop();
