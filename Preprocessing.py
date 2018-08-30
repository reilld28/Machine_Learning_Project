import numpy as np                                
import pandas as pd                               
import sklearn as sk                              
from time import gmtime, strftime                                                       
from sklearn import datasets
from datetime import datetime
import datetime as dt

#Pull in various CSV files
df = pd.read_csv('/home/ec2-user/SageMaker/41e74c0c-f961-4299-80d6-eb312313a176.csv')
KP_lookup = pd.read_csv('/home/ec2-user/SageMaker/lookup Tables - Both.csv')
KP_lookup.columns = ['Location','Floor','Room','RoomLabel']
sunrise = pd.read_csv('/home/ec2-user/SageMaker/Sunrise - Sheet1.csv')
weather = pd.read_csv('/home/ec2-user/SageMaker/weather.csv')


#Split message by "," to get action, location, attribute
df1 = df.join(df['message'].str.split(',', expand=True).add_prefix('message'))
#Remove unique key as not needed and any KBR or KBP messsages
df1 = df1.drop('unique_key', 1)
df1 = df1[df1['message0'] != "KBR"]
df1 = df1[df1['message0'] != "KBP"]
df1 = df1[df1['message0'] != "KBH"]

# Define collumns
df1.columns = ['Output','DateTime','Action','Location', 'Attribute']
#Only keep if Action says DL
df1 = df1[df1['Action'].isin(['DL'])]
df1 = df1.dropna(how='any',axis=0) 
df1.Location = df1.Location.str.replace('\s+', '')
# Set Attribute as integer
df1['Attribute'] = df1['Attribute'].apply(pd.to_numeric, errors='coerce')

# Format DateTime column so that one collumn contains date, time
df1 = df1.join(df1['DateTime'].str.split(',', expand=True).add_prefix('time'))
df1 = df1.join(df1['time0'].str.split('/', expand=True).add_prefix('date'))
df1 = df1[['Output', 'DateTime', 'Action', 'Location', 'Attribute','time0','time1','date1','date0','date2']]

# Flip the date format to month/day/year, hour:min:sec
df1['date1'] = df1['date1'].astype(str) + '/' +  df1['date0'].astype(str) + '/' + df1['date2'].astype(str)
df1['date1'] = df1['date1'].astype(str) + ', ' + df1['time1'].astype(str)
df1 = df1.drop('date0', 1)
df1 = df1.drop('date2', 1)

# Change to datetime format
df1['DateTime'] = pd.to_datetime(df1['DateTime'],format ='%d/%m/%Y, %H:%M:%S')
df1['date1'] = pd.to_datetime(df1['date1'],format ='%m/%d/%Y, %H:%M:%S')
# Sort DateTime
df1 = df1.sort_values('DateTime')
# Round to nearest hour as needed to merge for weather data
df1['date1'] = df1['date1'].dt.floor('h')
#Pull out week and day of week for new columns
df1['Num_Week'] =df1['date1'].dt.week
df1['time0'] = pd.to_datetime(df1['time0'],format ='%d/%m/%Y')
df1['weekday'] = df1['DateTime'].dt.dayofweek


# Create column for Total Seconds since midnight to the nearest minute

df1 = df1.join(df1['time1'].str.split(':', expand=True).add_prefix('seconds'))
df1['seconds0'] = df1['seconds0'].apply(pd.to_numeric, errors='coerce')
df1['seconds1'] = df1['seconds1'].apply(pd.to_numeric, errors='coerce')
df1['Minute'] = df1['seconds0']*60 + df1['seconds1']
df1 = df1.drop('seconds2', 1)

#Pull in CSV weather data from Met Eirean 
weather = weather.join(weather['date,ind,rain,ind,temp,ind,wetb,dewpt,vappr,rhum,msl,ind,wdsp,ind,wddir,ww,w,sun,vis,clht,clamt'].str.split(',', expand =True).add_prefix('message'))
weather.columns = ['total','date','ind','rain','ind','temp','ind','wetb','dewpt','vappr','rhum','msl','ind','wdsp','ind','wddir','ww','w','sun','vis','clht','clamt']
weather = weather.drop('total', 1)
weather = weather.drop('ind', 1)
# Define columns by names
weather.columns = ['DateTime','rain','temp','wetbulb','dew point','vappr pressure','relative hum','mean sea level','wind speed','wind dir','ww','w','sun duration','visability','cloud height','cloud_amt']
# Delete columns that aren't needed
weather.drop(weather.columns[[3,4,5,6,7,8,9,10,11,13,14]], axis=1, inplace=True)
# define columns as integers
weather['rain'] = weather['rain'].apply(pd.to_numeric, errors='coerce')
weather['temp'] = weather['temp'].apply(pd.to_numeric, errors='coerce')
weather['sun duration'] = weather['sun duration'].apply(pd.to_numeric, errors='coerce')
weather['cloud_amt'] = weather['cloud_amt'].apply(pd.to_numeric, errors='coerce')
weather['DateTime'] = pd.to_datetime(weather['DateTime'],format ='%d-%b-%Y %H:%M')
weather.columns = ['date1','rain','temp','sun duration','cloud_amt']

# Merge into 1 dataframe for weather, lookup location, 
df1 = df1.merge(weather,on='date1',how="left")
df1 = df1.merge(KP_lookup,on='Location',how="left")
floor_mapping = {"Ground": 0, "First Floor": 1}
df1['Floor'] = df1['Floor'].map(floor_mapping)
sunrise['time0'] = pd.to_datetime(sunrise['time0'],format ='%m/%d/%Y')
df1 = df1.merge(sunrise,on='time0',how="left")

# Formatting Sunrise Time

df1 = df1.join(df1['Sunrise'].str.split(':', expand=True).add_prefix('Sunrise'))
df1 = df1.join(df1['Sunset'].str.split(':', expand=True).add_prefix('Sunset'))
df1['Sunrise0'] = df1['Sunrise0'].apply(pd.to_numeric, errors='coerce')
df1['Sunrise1'] = df1['Sunrise1'].apply(pd.to_numeric, errors='coerce')
df1['Sunset0'] = df1['Sunset0'].apply(pd.to_numeric, errors='coerce')
df1['Sunset1'] = df1['Sunset1'].apply(pd.to_numeric, errors='coerce')
df1['Sunrise_Sec'] = df1['Sunrise0']*60 + df1['Sunrise1']
df1['Sunset_Sec'] = df1['Sunset0']*60 + df1['Sunset1']

df1 = df1.drop(['Sunrise', 'Sunset','Sunrise0','Sunrise1','Sunset0','Sunset1','Sunrise2','Sunset2'], axis=1)

# function to define day category
def get_category(df1):
    if ((df1['Minute'] >= (df1['Sunrise_Sec'] + 180)) & (df1['Minute'] <= (df1['Sunset_Sec']- 90))):
        return 'Day'
    elif ((df1['Minute'] < (df1['Sunrise_Sec'] + 180)) & (df1['Minute'] > df1['Sunrise_Sec'])):
        return 'Morning'
    elif ((df1['Minute'] > (df1['Sunset_Sec'] - 90)) & (df1['Minute'] < (df1['Sunset_Sec'] + 90))):
        return 'Evening'
    else:
        return 'Night'
df1['Category'] = df1.apply(get_category, axis = 1)

            
# Set Label Day Category 
Day_Label = {'Category': 'Day_Label','Morning': 0,'Day': 1,'Evening': 2, 'Night':3}
Day_Label = {'Category' : pd.Series(['Morning', 'Day', 'Evening','Night'], index=[0, 1, 2, 3]),'Day_Label' : pd.Series([0, 1, 2, 3], index=[0, 1, 2, 3])}
Day_Label_df = pd.DataFrame(Day_Label)
df1 = df1.merge(Day_Label_df,on='Category',how="left")
df1 = df1.drop(['Room', 'Category','Sunrise_Sec','Sunset_Sec'], axis=1)

# function to put brightness into cateogry
def brightness_category(df1):
    if ((df1['Attribute'] <= 100) & (df1['Attribute'] > 90)):
        return 100
    elif ((df1['Attribute'] <= 90) & (df1['Attribute'] > 70)):
        return 70
    elif ((df1['Attribute'] <= 70) & (df1['Attribute'] > 50)):
        return 50
    elif ((df1['Attribute'] <= 50) & (df1['Attribute'] > 30)):
        return 30
    elif ((df1['Attribute'] <= 30) & (df1['Attribute'] >= 0)):
        return 0
df1['Attribute'] = df1.apply(brightness_category, axis = 1)
               
# Remove all null values and output to Clean_data.csv
non_null_df = df1.dropna()
non_null_df.to_csv('Clean_Data.csv')
