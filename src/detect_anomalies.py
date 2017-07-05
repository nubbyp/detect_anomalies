#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jul  5 06:24:48 2017

@author: nubby
"""



from __future__ import division
import json
from datetime import datetime
from sets import Set
import numpy as np
import sys

#cwd = os.getcwd()
#batch_input_file = cwd + '/../log_input/batch_log.json'
#stream_log_file = cwd + '/../log_input/stream_log.json'
#flagged_purchases_file = cwd + '/../log_output/flagged_purchases.json'


batch_input_file = sys.argv[1]
stream_log_file = sys.argv[2]
flagged_purchases_file = sys.argv[3]

degrees = None
tracked_purchases = None

    
timestamp_counter = 0
timestamp_previous = None
users = {}
epoch = datetime(1970, 1, 1)

flagged_purchases = []

class User():

    def __init__(self, user_id):
        self.user_id = user_id
        self.friends = []
        self.purchases = []

    # A purchase_tuple contains three values: the timestamp as an epoch, a timestamp counter which is the order 
    # value for occurences of this second in the input file (there are more than one record for each second) and the amount
    def add_purchase(self,purchase_tuple):
        self.purchases.append(purchase_tuple)
        
    def befriend(self,friend_id):
        self.friends.append(friend_id)
        
    def unfriend(self,friend_id):
        try:
            self.friends.remove(friend_id)
        except ValueError:
            print ("Could not find user " + str(friend_id) + " to unfriend in user " + str(self.user_id) + " list of friends.")

            
# This function processes a single line from either the batch_input or the stream_log file.
# If the check_anomaly boolean value is true then any purchases added are checked for pricing anomalies.
# This flag is true when a line from the stream_log is processed but false when a line from the initial batch_input is processed. 
def process_line(check_anomaly, dict_in):

    global timestamp_previous, timestamp_counter
    
    try:
        #Convert timestamp to epoch long and have a counter to keep track
        # of the order in which events came in that share the same timestamp.
        # this counter resets to 0 when the timestamp moves on to a new second. 
        timestamp_str = dict_in['timestamp']
        timestamp_format = '%Y-%m-%d %H:%M:%S'

        timestamp = (datetime.strptime(timestamp_str, timestamp_format) - epoch).total_seconds()
        if (timestamp == timestamp_previous):
            timestamp_counter += 1
        else:
            timestamp_counter = 0
        timestamp_previous = timestamp
        
            
        #print ("timestamp: " + str(timestamp))
        #print ("timestamp_counter: " + str(timestamp_counter))
        
        event_type = dict_in['event_type']

        #print ("event_type: " + event_type)

        if (event_type == "purchase"):
            
            user_id = dict_in['id']

            user = users.get(user_id) 
            if (user == None):
                user = User(user_id)
                users[user_id] = user
                
            amount = float(dict_in['amount'])
            user.add_purchase((timestamp, timestamp_counter, amount ))
            
            if (check_anomaly == True):
                temp_network = create_network(user)
                check_for_price_anomaly(user_id, temp_network, dict_in)
            
        elif (event_type == "befriend" or event_type == "unfriend"):
            
                                    
            user_id1 = dict_in['id1']
            user1 = users.get(user_id1)
            if (user1 == None):
                user1 = User(user_id1)
                users[user_id1] = user1

            user_id2 = dict_in['id2']
            user2 = users.get(user_id2)
            if (user2 == None):
                user2 = User(user_id2)
                users[user_id2] = user2
            
            if (event_type == "befriend"):

                user1.befriend(user_id2)
                user2.befriend(user_id1)
                
            elif (event_type == "unfriend"):
 
                user1.unfriend(user_id2)
                user2.unfriend(user_id1)
            
            
        else:
            raise Exception("Unexpected event type")
            
    except KeyError:
        raise Exception("Unexpected input line")           

# This function loads in the initial batch_log file to set up the initial system state. 
def build_initial_state():
    
    print ("Loading initial data")
    
    params_found = False

    global degrees, tracked_purchases
    
    with open(batch_input_file) as f:
        for line in f:
            dict_in = json.loads(line)

            if (not params_found):
                try:
                    degrees = dict_in['D']
                    tracked_purchases = dict_in['T']
                    params_found = True
                    continue
                except KeyError:
                    pass
            
            try:
                process_line(False, dict_in)
            except Exception as e:
                print (str(e) + " in batch_input line: " + line)
            

    print ("Initial Users count: " + str(len(users)))
    
   

# This function processes new lines and looks for price anomalies in them
def process_streamed_data():
    

    with open(stream_log_file) as f:
        for line in f:
            dict_in = json.loads(line)

            try:
                process_line(True, dict_in)
            except Exception as e:
                print (str(e) + " in stream_log line: " + line)
            

def create_network(user):
    
    tempSet = Set(user.friends)
    
    #print("SET: " + str(tempSet))
    for i in range(degrees - 1):
        for user_id in tempSet:
            if (user_id != user.user_id):
                tempUser = users.get(user_id)
                tempFriends = tempUser.friends
                tempSet = tempSet.union(Set(tempFriends))
                
    #print ("TEMPSET: " + str(tempSet))
    
    return tempSet
          
def check_for_price_anomaly(user_id, network, dict_in):
    
    network_purchases = []
    
    for id in network:
        # We don't add this actual user's purchase history to their network's purchase history. Discount them. 
        if (id == user_id):
            continue
        user = users.get(id)
        if (user != None):
            # Get the last N purchases for each user in the network where N is the parameter tracked_purchases we read in.
            # This gets more than we will need but is better than getting all purchases for each user in the network.
            # We then order by timestamp and timestamp_counter within each timestamp and get the last N overall purchases
            # across all users in the network (except for the actual user who made the purchase)
            network_purchases.extend(user.purchases[-tracked_purchases:])

    # Now sort this list of tuples of all purchases for the network and get the last N of these (where N is tracked_purchases parameter)
    # The default sort order is by first element (timestamp) and then by second element (timestamp counter). This is what we want. 
    network_purchases.sort()  
    
    #print ("network purchases: " + str(network_purchases))
    latest_purchases = network_purchases[-tracked_purchases:]

    #print ("latest purchases: " + str(latest_purchases) )   
    
    mean = np.mean([latest_purchase[2] for latest_purchase in latest_purchases])
    std_dev = np.std([latest_purchase[2] for latest_purchase in latest_purchases])
    
    amount = float(dict_in.get('amount'))
    if (amount > (mean + (3 * std_dev))):
        # Truncate mean and sd to 2 places
        dict_in['mean'] = "%.2f" % mean
        dict_in['sd'] = "%.2f" % std_dev
        flagged_purchases.append(dict_in)
            
            
build_initial_state()



if (degrees == None or tracked_purchases == None):
    print ("Required parameter D or T not found.")
    
else:
    print ("Degrees: " + degrees)
    print ("Tracked Purchases: " + tracked_purchases)

    degrees = int(degrees)
    tracked_purchases = int(tracked_purchases)
    process_streamed_data()
    
    
    with open(flagged_purchases_file, 'w') as outfile:
        for line in flagged_purchases:
            outfile.write('{"event_type":"purchase", "timestamp":"'+line['timestamp']+'", "id": "'+line['id']+
            '", "amount": "'+line['amount']+'", "mean": "'+line['mean']+'", "sd": "'+line['sd']+'"}\n')
        
 

    
    
    
    