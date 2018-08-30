'''
/*
 * Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
 '''

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
from datetime import datetime
import logging
import time
import argparse
import json
import serial

AllowedActions = ['both', 'publish', 'subscribe']

#Read RS232 parameters
ser = serial.Serial("/dev/ttyUSB0",9600)
ser.bytesize = serial.EIGHTBITS
ser.party = serial.PARITY_NONE
ser.stopbits = serial.STOPBITS_ONE
ser.xonxoff = False
ser.rtscts = False
ser.dsrdtr = False




# Read in Serial Port
def readLutron():
    lutron_list = []
    lutron_final = ""

    while True:
        bytesToRead = ser.inWaiting()
        read_data = ser.read(bytesToRead)
        #print "In Loop reading...", read_data

        if read_data!="":
            #print "Have receieved data\n" , read_data
            return read_data
            #print "Size of bytes is.." , bytesToRead
            #print type(read_data)

            #if(read_data != "\r"):
            
            #    lutron_list.append(read_data)
            #    print "Data is..." , lutron_list
            #    return lutron_list
                    
            #else:
                #print "Character Return"
             #   lutron_final= ''.join(lutron_list)
             #   timestamp = datetime.now().strftime(" ,%d/%m/%Y, %H:%M:%S")
             #   lutron_final += timestamp
             #   print "Final Answer..", lutron_final
             #   return lutron_final
             #   lutron_list = []
             #   lutron_final =""
        else:
            return ""


# Custom MQTT message callback
def customCallback(client, userdata, message):
    print("Received a new message: ")
    print(message.payload)
    print("from topic: ")
    print(message.topic)
    print("--------------\n\n")


# Read in command-line parameters
parser = argparse.ArgumentParser()
parser.add_argument("-e", "--endpoint", action="store", required=True, dest="hos
t", help="Your AWS IoT custom endpoint")
parser.add_argument("-r", "--rootCA", action="store", required=True, dest="rootC
APath", help="Root CA file path")
parser.add_argument("-c", "--cert", action="store", dest="certificatePath", help
="Certificate file path")
parser.add_argument("-k", "--key", action="store", dest="privateKeyPath", help="
Private key file path")
parser.add_argument("-w", "--websocket", action="store_true", dest="useWebsocket
", default=False,
                    help="Use MQTT over WebSocket")
parser.add_argument("-id", "--clientId", action="store", dest="clientId", defaul
t="RasbperryPi",
                    help="Targeted client id")
parser.add_argument("-t", "--topic", action="store", dest="topic", default="proj
ect/lutron", help="Targeted topic")
parser.add_argument("-m", "--mode", action="store", dest="mode", default="both",
                    help="Operation modes: %s"%str(AllowedActions))
parser.add_argument("-M", "--message", action="store", dest="message", default="
Hello World!",
                    help="Message to publish")

args = parser.parse_args()
host = args.host
rootCAPath = args.rootCAPath
certificatePath = args.certificatePath
privateKeyPath = args.privateKeyPath
useWebsocket = args.useWebsocket
clientId = args.clientId
topic = args.topic

if args.mode not in AllowedActions:
    parser.error("Unknown --mode option %s. Must be one of %s" % (args.mode, str
(AllowedActions)))
    exit(2)

if args.useWebsocket and args.certificatePath and args.privateKeyPath:
    parser.error("X.509 cert authentication and WebSocket are mutual exclusive. 
Please pick one.")
    exit(2)

if not args.useWebsocket and (not args.certificatePath or not args.privateKeyPat
h):
    parser.error("Missing credentials for authentication.")
    exit(2)

# Configure logging
logger = logging.getLogger("AWSIoTPythonSDK.core")
#logger.setLevel(logging.DEBUG)
streamHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(messag
e)s')
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

# Init AWSIoTMQTTClient
myAWSIoTMQTTClient = None
if useWebsocket:
    myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId, useWebsocket=True)
    myAWSIoTMQTTClient.configureEndpoint(host, 443)
    myAWSIoTMQTTClient.configureCredentials(rootCAPath)
else:
    myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId)
    myAWSIoTMQTTClient.configureEndpoint(host, 8883)
    myAWSIoTMQTTClient.configureCredentials(rootCAPath, privateKeyPath, certific
atePath)

# AWSIoTMQTTClient connection configuration
myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publi
sh queueing
myAWSIoTMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
myAWSIoTMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec

# Connect and subscribe to AWS IoT
myAWSIoTMQTTClient.connect()
if args.mode == 'both' or args.mode == 'subscribe':
    myAWSIoTMQTTClient.subscribe(topic, 1, customCallback)
time.sleep(2)

# Publish to the same topic in a loop forever
loopCount = 0
while True:
    if args.mode == 'both' or args.mode == 'publish':
        message = {}
        s = readLutron()
        my_list = s.split('\r')
        
        for x in my_list:
            if (len(x)>0):
                date = datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
                message['message'] = x
                message['sequence'] = loopCount
                message['timestamp'] = date
                message['unique_key'] = date + str(loopCount)
		message['Source'] = "Lutron"
                messageJson = json.dumps(message)
                myAWSIoTMQTTClient.publish(topic, messageJson, 1)
    
                if args.mode == 'both':
                    print('Published topic %s: %s\n' % (topic, messageJson))
                loopCount += 1
            time.sleep(0.5)
