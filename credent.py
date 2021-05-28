from collections import Counter
from twython import TwythonStreamer
import json, csv, sys
import pandas as pd
from geopy.geocoders import Nominatim
import gmplot

'''This loads my Twitter API keys from a json file and store in cred'''
with open(r'C:\Users\richa\OneDrive\Desktop\Scrape\Twi_API.json') as jsonFile:
    cred = json.load(jsonFile)

lst = [] #I couldn't get Twython to work with a if statement unless I append to a global var.

def process_tweet(tweet):
    s = {} #instantiate an empty dictionary to collect tweet data
    s['hashtags'] = [hashtag['text'] for hashtag in tweet['entities']['hashtags']]
    s['text'] = tweet['text']
    s['user'] = tweet['user']['screen_name']
    s['user_loc'] = tweet['user']['location']
    return s

'''I use a normal file writer to write header to a new csv file to store the data. If I do this under the 
MyStreamer class below, the header (tweet.keys()) will be written multiple times'''
with open(r'saved_tweets2.csv', 'w', newline='', encoding='utf-8') as file:
    header = 'hashtags, text, user, user_loc\n'
    file.write(header)

class MyStreamer(TwythonStreamer):
    '''Overidding the MYSTreamer class of the TwythonStreamer to decide what happens when our code
    is working and when we encounter an error'''

    def on_success(self, data): 
        if data['lang'] == 'en': #Collect only tweets that are in English
            tweet_data = process_tweet(data)
            tweet_length =  tweet_data['user'] #Get the usernames
            lst.append(tweet_length) #Append to a globa var.
            self.save_as_csv(tweet_data) #Collect data from the dictionary and send to save_as_csv function
            if len(lst) >= 9500: #It is unlikely I get up to 9500 tweets in a single run, but in case.
                self.disconnect()

    def on_error(self, status_code, data):
        print(status_code, data) #If any error, print the error and disconnect streamer
        self.disconnect()

    def save_as_csv(self, tweet):
        '''The data collected is appended to the saved_tweets.csv file I opened above.
        Only the dictionary values need to be appended since I already wrote the keys as headers.'''

        with open(r'saved_tweets2.csv', 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file,  delimiter = ',')
            writer.writerow(tweet.values())

stream = MyStreamer(cred["consumer_key"], cred["app_secret"], cred["oauth_token"], cred["oauth_token_secret"])

'''A utf-8 encoded CSV file may not still handle all tweet encodings.
Adding an engine argument to streamer helps Python to do the fine conversion.'''
stream.statuses.filter(track='python', engine='python')

'''The next part extracts the location from collected data and renders on Google map.'''

tweets = pd.read_csv("saved_tweets2.csv", names = ['hashtags', 'text', 'user', 'user_loc'])

geolocator = Nominatim(user_agent="Riklinks") #Any name as user agent should do. Please, refer to Neomatim doc

#Add tweets locations to coordinates dictionary.
coordinates = {'lat': [], 'long': []}
for user_loc in tweets.user_loc:
    try:
        location = geolocator.geocode(user_loc)
        
        if location: #If coordinates are found for location
            coordinates['lat'].append(location.latitude)
            coordinates['long'].append(location.longitude)
            #I could save the coordiate in a txt/csv file for further use.
            
    #If too many connection requests, don't do anything
    except:
        pass
    
#Instantiate and center a GoogleMapPlotter object to show map
gmap = gmplot.GoogleMapPlotter(30, 0, 3)

gmap.heatmap(coordinates['lat'], coordinates['long'], radius=20)

#Save the map to html file
gmap.draw("Tweet_python.html")