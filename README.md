# Machine_Learning_Project
This is the final code used in my Master Level project

BasicPubSub2.py should be run on microcontroller collecting lighting information. Uses MQTT

Preprocessing.py reads in csv of data collected from lighting system. Preprocessing uses 41e74c0c-f961-4299-80d6-eb312313a176 (1).csv, Sunrise - Sheet1 (5).csv, lookup Tables - Both (4).csv, weather.csv to gather information

MachineLearning.py is the real time prediction program. Uses Sunrise - Sheet1 (5).csv, lookup Tables - Output.csv, TestMQTT.py preprocessing.py, RealTimeTemp.py

