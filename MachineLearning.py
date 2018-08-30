import numpy as np                                # For matrix operations and numerical processing
import pandas as pd                               # For munging tabular data
import sklearn as sk                              # For access to a variety of machine learning models
import matplotlib.pyplot as plt                   # For charts and visualizations
from IPython.display import Image                 # For displaying images in the notebook
from IPython.display import display               # For displaying outputs in the notebook
from sklearn.datasets import dump_svmlight_file   # For outputting data to libsvm format for xgboost
from time import gmtime, strftime                 # For labeling SageMaker models, endpoints, etc.



from sklearn.model_selection import train_test_split
from sklearn import datasets
from sklearn import svm
from datetime import datetime
import datetime as dt
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error
from sklearn.metrics import accuracy_score
from sklearn.metrics import confusion_matrix
from sklearn.metrics import classification_report
from time import gmtime, strftime
from sklearn.externals.six import StringIO  
from IPython.display import Image  
from sklearn.tree import export_graphviz
import time
from Preprocessing import get_category
import seaborn as sn

from TestMQTT import setup
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import logging
import time
import argparse
import json
from math import sqrt

topic = 'project/lutron'

#import anaconda_project 
#!pip install pydotplus

df = pd.read_csv('/home/ec2-user/SageMaker/IoTAnalytics/Clean_Data.csv')
Output_lookup = pd.read_csv('/home/ec2-user/SageMaker/lookup Tables - Output.csv')



# Function for random forest to predict room category
    
def random_forest_cat(X_train,X_test,y_train,y_test):
    random_forest = RandomForestClassifier(n_estimators=50, oob_score=True)
    random_forest.fit(X_train, y_train)

    Y_train_pred = random_forest.predict(X_train)
    Y_test_pred = random_forest.predict(X_test)


    random_forest.score(X_train, y_train)
    acc_random_forest = round(random_forest.score(X_train, y_train) * 100, 2)

    predicted = random_forest.predict(X_test)
    accuracy = accuracy_score(y_test, predicted)
    #print(f'Out-of-bag score estimate: {random_forest.oob_score_:.3}')
    #print(f'Mean accuracy score: {accuracy:.3}')
    #print(accuracy)
    
    report = classification_report(y_test, predicted)
    #print(report)
    matrix = confusion_matrix(y_test, predicted)
    #print(matrix)
    #sn.heatmap(matrix)
    return random_forest

def lutron_input(room,brightness):
    
    brightness_prediction = str(round(brightness))
    room_prediction = int(room)
    Output_Room = Output_lookup[(Output_lookup.RoomLabel ==room_prediction)]
    brightness_prediction= brightness_prediction.replace('.0', '')
    Output_Room['Output_w_Brightness'] = Output_Room['DL Zone Input'].astype(str)+ brightness_prediction
    string_list = Output_Room['Output_w_Brightness'].values.tolist()
    Output_string = '\r'.join(string_list)
    print(Output_string)
    return Output_string
    

# Predict Room 

#predict Room top features
X1 = df.iloc[:,[9,10,13,15,17,18,20]]
Y1 = df.iloc[:,19]

#Run simulation
X1_train, X1_test, y1_train, y1_test= train_test_split(X1, Y1, test_size=0.3, shuffle=True)
room_forest = random_forest_cat(X1_train,X1_test,y1_train,y1_test)


#Predict Brightness

X = df.iloc[:,[9,10,13,15,17,18,19,20]]
Y = df.iloc[:,5]

# based on feature extraction tidy up data
X_train, X_test, y_train, y_test= train_test_split(X, Y, test_size=0.3, shuffle=True)
brightness_forest = random_forest_cat(X_train,X_test,y_train,y_test)



# Real Time DataFrame

sunrise = pd.read_csv('/home/ec2-user/SageMaker/Sunrise - Sheet1.csv')
sunrise['time0'] = pd.to_datetime(sunrise['time0'],format ='%m/%d/%Y')

# Defining Day _Label
Day_Label = {'Category': 'Day_Label','Morning': 0,'Day': 1,'Evening': 2, 'Night':3}
Day_Label = {'Category' : pd.Series(['Morning', 'Day', 'Evening','Night'], index=[0, 1, 2, 3]),'Day_Label' : pd.Series([0, 1, 2,3], index=[0, 1, 2, 3])}
Day_Label_df = pd.DataFrame(Day_Label)

MyMQTTClient = setup()
MyMQTTClient
room_forest = random_forest_cat(X1_train,X1_test,y1_train,y1_test)
brightness_forest = random_forest_cat(X_train,X_test,y_train,y_test)

Floor = 0
loopcount =0 
while loopcount <10:
    # Define now
    now_time= datetime.now()

    # Get Num_Week and Weekday
    Num_Week =now_time.isocalendar()[1]
    weekday = now_time.weekday()

    # Get string time
    string_times = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    date, times =string_times.split(' ')
    hour,minute,second = times.split(':')

    # Get Total minutes since Midnight
    minutes = (int(hour))*60 + int(minute)

    # Get Temp & Cloud Amount !!!!!!!!!

    # Define input data in DataFrame
    data = {'DateTime':string_times,'Num_Week' : Num_Week, 'weekday' : weekday,'Minute': minutes,'Floor': Floor,'temp':20,'cloud_amt':5 }
    predict_df = pd.DataFrame(data,index=[1])

    # Format DateTime to get sunrise & sunset times
    predict_df = predict_df.join(predict_df['DateTime'].str.split(' ', expand=True).add_prefix('time'))
    predict_df['time0'] = pd.to_datetime(predict_df['time0'],format ='%Y-%m-%d')

    predict_df = predict_df.merge(sunrise,on='time0',how="left")

    # Get Category of Day/Night/Morning/Evning
    predict_df = predict_df.join(predict_df['Sunrise'].str.split(':', expand=True).add_prefix('Sunrise'))
    predict_df = predict_df.join(predict_df['Sunset'].str.split(':', expand=True).add_prefix('Sunset'))


    predict_df['Sunrise0'] = predict_df['Sunrise0'].apply(pd.to_numeric, errors='coerce')
    predict_df['Sunrise1'] = predict_df['Sunrise1'].apply(pd.to_numeric, errors='coerce')
    predict_df['Sunset0'] = predict_df['Sunset0'].apply(pd.to_numeric, errors='coerce')
    predict_df['Sunset1'] = predict_df['Sunset0'].apply(pd.to_numeric, errors='coerce')

    predict_df['Sunrise_Sec'] = predict_df['Sunrise0']*60 + predict_df['Sunrise1']
    predict_df['Sunset_Sec'] = predict_df['Sunset0']*60 + predict_df['Sunset1']

    predict_df = predict_df.drop(['Sunrise','Sunset','Sunrise0','Sunrise1','Sunset0','Sunset1','Sunrise2','Sunset2'],axis = 1)

    predict_df['Category'] = predict_df.apply(get_category, axis = 1)
    predict_df = predict_df.merge(Day_Label_df,on='Category',how="left")
    predict_df = predict_df.drop(['Category','Sunrise_Sec','Sunset_Sec','time0','time1','DateTime'], axis=1)
    print(predict_df)
    

    real_time_room = room_forest.predict(predict_df)
    print (real_time_room)
    predict_df['RoomLabel'] = real_time_room
    real_time_brightness = brightness_forest.predict(predict_df)
    print (real_time_brightness)
    
    
    DL_string = lutron_input(real_time_room[0],real_time_brightness[0])
    my_list = DL_string.split('\r')
    message = {}
    

    for x in my_list:
        if (len(x)>0): 
            message['message'] = x
            message['sequence'] = loopcount
            message['timestamp'] = string_times
            message['unique_key'] = string_times + str(loopcount)
            message['Source'] = "Model"
  
            messageJson = json.dumps(message)
            MyMQTTClient.publish(topic, messageJson, 1)
            print('Published topic %s: %s\n' % (topic, messageJson))
            loopcount=loopcount+1
    if Floor == 0:
        print(1)
        Floor =1
    elif Floor ==1:
        Floor = 0
    time.sleep(60)









