#import
from googleapiclient.discovery import build
from pymongo import MongoClient
import mysql.connector
import pandas as pd
import streamlit as st
from datetime import datetime as dt
from dateutil.parser import isoparse
from isodate import parse_duration

#connecting to youtube
youtube = build ('youtube', 'v3', developerKey="YOUR_API_KEY")

#function to get channel details
def get_channel_details(channel_id):
    # request youtube channels
    request = youtube.channels().list(part="snippet,contentDetails,statistics",id=channel_id)
    response = request.execute()
    
    for i in response['items']:
        data = dict(
            Channel_ID = i["id"],
            Channel_Name = i["snippet"]["title"],
            Channel_Description = i["snippet"]['description'],
            Channel_Subscribers = i['statistics']['subscriberCount'],
            Channel_Video_Count = i['statistics']['videoCount'],
            Channel_View_Count = i['statistics']['viewCount'],
            Channel_Published_At = i["snippet"]['publishedAt'],
            Channel_Playlist_ID = i['contentDetails']['relatedPlaylists']['uploads']
        )
        
    return data

#function to get video ids
def get_video_ids(channel_id):
    
    video_ids = []
    # request youtube channels
    request = youtube.channels().list(part="contentDetails",id=channel_id)
    response = request.execute()

    Channel_Playlist_ID = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']


    next_page_token = None

    while True:
        # request youtube playlistitems
        request = youtube.playlistItems().list(part="contentDetails",
                                                 playlistId=Channel_Playlist_ID,
                                                 maxResults=50,
                                                 pageToken=next_page_token)
        response = request.execute()

        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['contentDetails']['videoId'])
        
        # next page token to fetch all the ids from the following pages
        next_page_token = response.get('nextPageToken')

        if next_page_token is None:
            break
            
    return video_ids


#function to get video details
def get_video_details(video_ids):
    
    video_details = []

    for video_id in video_ids:
        # request youtube videos
        request = youtube.videos().list(part="snippet,contentDetails,statistics",id=video_id)
        response = request.execute()

        for i in response["items"]:
            data = dict(Channel_Name = i['snippet']['channelTitle'],
                        Channel_ID = i['snippet']['channelId'],
                        Video_Title = i['snippet']['title'],
                        Video_ID = i['id'] ,
                        Video_Description = i['snippet'].get('description'),
                        Video_Tag = i['snippet'].get('tags'),
                        Video_Published_At = i['snippet']['publishedAt'],
                        Video_Duration = i['contentDetails']['duration'],
                        Video_View_Count = i['statistics'].get('viewCount'),
                        Video_Like_Count = i['statistics'].get('likeCount'),
                        Video_Favorite_Count = i['statistics'].get('favoriteCount'),
                        Video_Comment_Count = i['statistics'].get('commentCount'),
                        Video_Thumbnail = i['snippet']['thumbnails']['default']['url'],
                        Video_Caption_Status = i['contentDetails']['caption'])

            video_details.append(data)
    
    return video_details


#function to get comment details
def get_comment_details(video_ids):
    comment_details= []
    try:
        for video_id in video_ids:
            # request youtube commentthreads
            request = youtube.commentThreads().list(part="snippet",
                                                    videoId=video_id,
                                                    maxResults=100)
            response = request.execute()

            for i in response["items"]:
                data = dict(Channel_ID = i['snippet']['channelId'],
                            Comment_ID = i['id'],
                            Video_ID = i['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text = i['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author = i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published_At = i['snippet']['topLevelComment']['snippet']['publishedAt'])

                comment_details.append(data)
    except:
        pass
    
    return comment_details


#function to scrap channel into mongodb
def scrape_channel(channel_id):
    
    # Connect to MongoDB
    client = MongoClient("localhost",27017)
    db = client.Youtube_Data_Harvesting
    
    # Fetch channel details, video IDs, video details, and comment details
    channel_details = get_channel_details(channel_id)
    video_ids = get_video_ids(channel_id)
    video_details = get_video_details(video_ids)
    comment_details = get_comment_details(video_ids)
    
    # Create a collection
    Channel_Details = db["channel_details"]
    Video_Details = db["video_details"]
    Comment_Details = db["comment_details"]
    
    #check for duplication
    existing_channel = Channel_Details.find_one({"Channel_ID": channel_details["Channel_ID"]})
    if existing_channel:
        st.warning(f"Channel ID '{Channel_ID}' has already been processed.")         
    
    # Insert data into respective collections 
    Channel_Details.insert_one(channel_details)
    Video_Details.insert_many(video_details)
    Comment_Details.insert_many(comment_details)
    
    return "data added successfully"


#function to migrate channel details from mongodb to sql
def channel_details_table():
    #establishing MySQL connection
    connection  = mysql.connector.connect(user='root', 
                                              password='YOUR_PASSWORD', 
                                              host='localhost', 
                                              database="youtube_scraping")

    cursor = connection.cursor()

    # drop channel_details table
    drop_query = "drop table if exists channel_details"
    cursor.execute(drop_query)
    connection.commit()


    #create channel_details table    
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS channel_details(Channel_ID VARCHAR(80) PRIMARY KEY,
                                                                    Channel_Name VARCHAR(100), 
                                                                    Channel_Description TEXT,
                                                                    Channel_Subscribers BIGINT, 
                                                                    Channel_Video_Count INT,
                                                                    Channel_View_Count BIGINT,
                                                                    Channel_Published_At DATETIME, #timestamp
                                                                    Channel_Playlist_ID VARCHAR(80))'''

        cursor.execute(create_query)
        connection.commit()

    except Exception as e:
        print(f"Error: {e}")


    #fetch channels details from MongoBD
    ch_details = []
    
    client = MongoClient("localhost",27017)
    db = client.Youtube_Data_Harvesting
    Channel_Details = db["channel_details"]

    for ch_data in Channel_Details.find({},{"_id":0}):
        ch_details.append(ch_data)

    df = pd.DataFrame(ch_details)


    #insert channels details into MySQL
    for index, row in df.iterrows():
        insert_query = '''INSERT into channel_details(Channel_ID,
                                                        Channel_Name,
                                                        Channel_Description,
                                                        Channel_Subscribers,
                                                        Channel_Video_Count,
                                                        Channel_View_Count,
                                                        Channel_Published_At,
                                                        Channel_Playlist_ID)

                                                        values(%s,%s,%s,%s,%s,%s,%s,%s)'''


        values =(row['Channel_ID'],
                row['Channel_Name'],
                row['Channel_Description'],
                row['Channel_Subscribers'],
                row['Channel_Video_Count'],
                row['Channel_View_Count'],
                isoparse(row['Channel_Published_At']),
                row['Channel_Playlist_ID'])

        try:                     
            cursor.execute(insert_query, values)
            connection.commit()

        except Exception as e:
            print(f"Error inserting data for Channel_ID {row['Channel_ID']}: {e}")
          
    
#function to migrate video details from mongodb to sql
def video_details_table():
    #establishing MySQL connection
    connection  = mysql.connector.connect(user='root', 
                                              password='YOUR_PASSWORD', 
                                              host='localhost', 
                                              database="youtube_scraping")

    cursor = connection.cursor()

    # drop video_details table
    drop_query = "drop table if exists video_details"
    cursor.execute(drop_query)
    connection.commit()


    #create video_details table     
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS video_details(Channel_Name VARCHAR(100),
                                                                    Channel_ID VARCHAR(100),
                                                                    Video_Title VARCHAR(150), 
                                                                    Video_ID VARCHAR(30) PRIMARY KEY,
                                                                    Video_Description TEXT,
                                                                    Video_Tag VARCHAR(500),
                                                                    Video_Published_At DATETIME,
                                                                    Video_Duration DECIMAL(10, 2),
                                                                    Video_View_Count BIGINT,
                                                                    Video_Like_Count BIGINT,
                                                                    Video_Favorite_Count BIGINT,
                                                                    Video_Comment_Count BIGINT,
                                                                    Video_Thumbnail VARCHAR(150),
                                                                    Video_Caption_Status VARCHAR(30))'''

        cursor.execute(create_query)
        connection.commit()

    except Exception as e:
        print(f"Error: {e}")


    #fetch video details from MongoBD
    ch_videos = []

    client = MongoClient("localhost",27017)
    db = client.Youtube_Data_Harvesting
    Video_Details = db["video_details"]

    for ch_data in Video_Details.find({},{"_id":0}):
        ch_videos.append(ch_data)

    df = pd.DataFrame(ch_videos)


    #insert video details into MySQL
    for index, row in df.iterrows():


        insert_query = '''INSERT into video_details(Channel_Name,
                                                Channel_ID,
                                                Video_Title, 
                                                Video_ID,
                                                Video_Description,
                                                Video_Tag,
                                                Video_Published_At,
                                                Video_Duration,
                                                Video_View_Count,
                                                Video_Like_Count,
                                                Video_Favorite_Count,
                                                Video_Comment_Count,
                                                Video_Thumbnail,
                                                Video_Caption_Status)

                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''


        video_tags = row['Video_Tag'] if isinstance(row['Video_Tag'], list) else []
        tags_string = ','.join(video_tags)
        
        duration_seconds = parse_duration(row['Video_Duration']).total_seconds()
        duration_minutes = duration_seconds / 60

        values =(row['Channel_Name'],
                row['Channel_ID'],
                row['Video_Title'],
                row['Video_ID'],
                row['Video_Description'],
                tags_string,
                isoparse(row['Video_Published_At']),
                duration_minutes,
                row['Video_View_Count'],
                row['Video_Like_Count'],
                row['Video_Favorite_Count'],
                row['Video_Comment_Count'],
                row['Video_Thumbnail'],
                row['Video_Caption_Status'])

        try:                     
            cursor.execute(insert_query, values)
            connection.commit()

        except Exception as e:
            print(f"Error inserting data for Channel_ID {row['Channel_ID']}: {e}")
            
                         
#function to migrate comment details from mongodb to sql           
def comment_details_table():
    #establishing MySQL connection
    connection  = mysql.connector.connect(user='root', 
                                              password='YOUR_PASSWORD', 
                                              host='localhost', 
                                              database="youtube_scraping")

    cursor = connection.cursor()

    # drop comment_details table
    drop_query = "drop table if exists comment_details"
    cursor.execute(drop_query)
    connection.commit()


    #create comment_details table    
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS comment_details(Channel_ID VARCHAR(100),
                                                                    Comment_ID VARCHAR(100) PRIMARY KEY,
                                                                    Video_ID VARCHAR(100),
                                                                    Comment_Text TEXT, 
                                                                    Comment_Author VARCHAR(100),
                                                                    Comment_Published_At DATETIME)'''

        cursor.execute(create_query)
        connection.commit()

    except Exception as e:
        print(f"Error: {e}")


    #fetch comments details from MongoBD
    ch_comments = []

    client = MongoClient("localhost",27017)
    db = client.Youtube_Data_Harvesting
    Comment_Details = db["comment_details"]

    for ch_data in Comment_Details.find({},{"_id":0}):
        ch_comments.append(ch_data)

    df = pd.DataFrame(ch_comments)


    #insert comments details into MySQL
    for index, row in df.iterrows():

        insert_query = '''INSERT into Comment_Details(Channel_ID,
                                                        Comment_ID,
                                                        Video_ID,
                                                        Comment_Text, 
                                                        Comment_Author,
                                                        Comment_Published_At)

                                                        values(%s,%s,%s,%s,%s,%s)'''


        values =(row['Channel_ID'],
                 row['Comment_ID'],
                 row['Video_ID'],
                 row['Comment_Text'],
                 row['Comment_Author'],   
                 isoparse(row['Comment_Published_At']))

        try:                     
            cursor.execute(insert_query, values)
            connection.commit()

        except Exception as e:
            print(f"Error inserting data for Channel_ID {row['Channel_ID']}: {e}")
    
    
# function to call all the tables to execute           
def tables():
    channel_details_table()
    video_details_table()
    comment_details_table()
    
    return "Data transferred successfully to MySQL"        


# function to channel table from MySQL
def show_channels_table():
    connection  = mysql.connector.connect(user='root', 
                                          password='YOUR_PASSWORD', 
                                          host='localhost', 
                                          database="youtube_scraping")
    cursor = connection.cursor()
    
    try:
        query = "SELECT * FROM channel_details;"
        cursor.execute(query)
        result = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        result.index = result.index + 1
        st.dataframe(result)
        cursor.close()
        connection.close()
        
    except mysql.connector.Error as e:
        st.error(f"Error connecting to MySQL: {e}")
        
        
# function to video table from MySQL        
def show_videos_table():
    connection  = mysql.connector.connect(user='root', 
                                          password='YOUR_PASSWORD', 
                                          host='localhost', 
                                          database="youtube_scraping")
    cursor = connection.cursor()
    
    try:
        query = "SELECT * FROM video_details;"
        cursor.execute(query)
        result = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        result.index = result.index + 1
        st.dataframe(result)
        cursor.close()
        connection.close()
        
    except mysql.connector.Error as e:
        st.error(f"Error connecting to MySQL: {e}")        
        

# function to comment table from MySQL        
def show_comments_table():
    connection  = mysql.connector.connect(user='root', 
                                          password='YOUR_PASSWORD', 
                                          host='localhost', 
                                          database="youtube_scraping")
    cursor = connection.cursor()
    
    try:
        query = "SELECT * FROM comment_details;"
        cursor.execute(query)
        result = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        result.index = result.index + 1
        st.dataframe(result)
        cursor.close()
        connection.close()
        
    except mysql.connector.Error as e:
        st.error(f"Error connecting to MySQL: {e}")
               
        

# streamlit UI

# setting page configuration
st.set_page_config(page_title="YouTube Data Harvesting",
                   page_icon="📊",
                   layout="wide")

# main Content Area
st.title("YouTube Data Harvesting and Warehousing 🔍")

# navigation
nav_options = ["Home", "Scrape Channel", "Migrate to SQL", "Show Table", "Analysis"]
selected_nav = st.sidebar.radio("Navigation", nav_options)

# page content based on navigation selection
if selected_nav == "Home":
    st.write("Welcome to the Home Page!")
    
# button to trigger scraping
elif selected_nav == "Scrape Channel":
    st.write("This is the Page to Scrape Channels.")
    channel_id = st.text_input('Enter YouTube Channel ID:')
    if st.button('Scrape Channel'):
        # Check if the channel ID has already been processed
        client = MongoClient("localhost",27017)
        db = client["Youtube_Data_Harvesting"]
        Channel_Details = db["channel_details"]
        processed_channels = set(db["channel_details"].distinct("Channel_ID"))
        if channel_id in processed_channels:
            st.warning(f"Channel ID '{channel_id}' has already been processed.")
        else:
            try:
                # Call the scraping function
                info_message = st.info(f"Scraping data for Channel ID: {channel_id}...")
                scrape_channel(channel_id)
                info_message.success(f"Scraping successful for Channel ID '{channel_id}'.")
                info_message = st.empty()
                processed_channels.add(channel_id)  # Add the channel ID to the set of processed channels
            except Exception as e:
                st.error(f"Error while scraping Channel ID '{channel_id}': {e}")
            finally:
                client.close()  # Close the MongoDB connection
        
                    
        
# button to transfer data from MongoDB to MySQL 
elif selected_nav == "Migrate to SQL":
    st.write("This is the Migrate to SQL Page.")
    if st.button("Migrate to SQL"):
        info_message = st.info(f"Migrating data from MongoDB to MySQL")
        display = tables()
        st.success(display)
        info_message.empty()

        
# dorpdown to show table    
elif selected_nav == "Show Table":
    st.write("This is the Show Table Page.")
    
    show_table_options = ["Channel Details", "Video Details", "Comment Details"]
    show_table = st.selectbox("Select the table to view", show_table_options)
    
    st.markdown("---")
            
    if st.button("Show Table"):
        if show_table == "Channel Details":
            show_channels_table()
        elif show_table == "Video Details":
            show_videos_table()
        elif show_table == "Comment Details":
            show_comments_table()

# analysis page with 10 question in selectbox
elif selected_nav == "Analysis":
    st.write("This is the Analysis Page.")
    question = st.selectbox('Please Select Your Question',('1. All the videos and their corresponding channels',
                                                         '2. Channel with most number of videos',
                                                         '3. Top 10 most viewed videos',
                                                         '4. Number of comments in each video',
                                                         '5. Videos with highest number of likes',
                                                         '6. Total number of likes in each videos',
                                                         '7. Total number of views in each channel',
                                                         '8. Videos published in the year 2022',
                                                         '9. Average duration of all the videos in each channel',
                                                         '10. Video with highest number of comments'))  
    st.markdown("---")
    
    #connecting mysql
    connection  = mysql.connector.connect(user='root', 
                                      password='YOUR_PASSWORD', 
                                      host='localhost', 
                                      database="youtube_scraping")

    cursor = connection.cursor()
    
    if question == "1. All the videos and their corresponding channels":
        
        query = '''SELECT Channel_Name, Video_Title 
                    FROM video_details'''
        cursor.execute(query)

        ans=cursor.fetchall()
        df=pd.DataFrame(ans,columns=["Channel Name", "Video Title"])
        df.index = df.index + 1
        st.write(df)
        
    elif question == "2. Channel with most number of videos":
        
        query = '''SELECT Channel_Name, Channel_Video_Count 
                    FROM channel_details
                    ORDER BY Channel_Video_Count DESC
                    LIMIT 10'''
        cursor.execute(query)

        ans=cursor.fetchall()
        df=pd.DataFrame(ans,columns=["Channel Name", "Number of Videos"])
        df.index = df.index + 1
        st.write(df)

    elif question == "3. Top 10 most viewed videos":

        query = '''SELECT Channel_Name, Video_Title, Video_View_Count 
                    FROM video_details
                    ORDER BY Video_View_Count DESC
                    LIMIT 10'''
        cursor.execute(query)

        ans=cursor.fetchall()
        df=pd.DataFrame(ans,columns=["Channel Name", "Video Title", "View Count"])
        df.index = df.index + 1
        st.write(df)
            
    elif question == "4. Number of comments in each video":

        query = '''SELECT Video_Title, Video_Comment_Count
                     FROM video_details
                     WHERE Video_Comment_Count IS NOT NULL
                     ORDER BY Video_Comment_Count DESC'''

        cursor.execute(query)

        ans=cursor.fetchall()
        df=pd.DataFrame(ans,columns=["Video Title", "Comments Count"])
        df.index = df.index + 1
        st.write(df)
        
            
    elif question == "5. Videos with highest number of likes":

            query = '''SELECT Channel_Name, Video_Title, Video_Like_Count
                         FROM video_details
                         WHERE Video_Like_Count IS NOT NULL
                         ORDER BY Video_Like_Count DESC
                         LIMIT 3'''

            cursor.execute(query)

            ans=cursor.fetchall()
            df=pd.DataFrame(ans,columns=["Channel Name", "Video Title", "Like Count"])
            df.index = df.index + 1
            st.write(df)  
            
            
    elif question == "6. Total number of likes in each videos":

            query = '''SELECT Channel_Name, Video_Title, Video_Like_Count
                         FROM video_details
                         ORDER BY Video_Like_Count DESC'''

            cursor.execute(query)

            ans=cursor.fetchall()
            df=pd.DataFrame(ans,columns=["Channel Name", "Video Title", "Like Count"])
            df.index = df.index + 1
            st.write(df)            
                    
            
            
    elif question == "7. Total number of views in each channel":

            query = '''SELECT Channel_Name, Channel_View_Count
                         FROM channel_details
                         ORDER BY Channel_View_Count DESC'''

            cursor.execute(query)

            ans=cursor.fetchall()
            df=pd.DataFrame(ans,columns=["Channel Name","View Count"])
            df.index = df.index + 1
            st.write(df)
            
            
            
    elif question == "8. Videos published in the year 2022":

            query = '''SELECT Channel_Name, Video_Title, Video_Published_At
                         FROM video_details
                         WHERE EXTRACT(YEAR FROM Video_Published_At) = 2022
                         ORDER BY Video_Published_At ASC'''

            cursor.execute(query)

            ans=cursor.fetchall()
            df=pd.DataFrame(ans,columns=["Channel Name", "Video Title", "Published At"])
            df.index = df.index + 1
            st.write(df)
            
            
    elif question == "9. Average duration of all the videos in each channel":

            query = '''SELECT Channel_Name, AVG (Video_Duration) AS Average_Duration 
                        FROM video_details
                        GROUP BY Channel_Name
                        ORDER BY Average_Duration DESC'''

            cursor.execute(query)

            ans=cursor.fetchall()
            df=pd.DataFrame(ans,columns=["Channel Name", "Average Duration in Minutes"])
            df.index = df.index + 1
            st.write(df)
            
            
    elif question == "10. Video with highest number of comments":

            query = '''SELECT Channel_Name, Video_Title, Video_Comment_Count
                        FROM video_details
                        ORDER BY Video_Comment_Count DESC
                        LIMIT 3'''

            cursor.execute(query)

            ans=cursor.fetchall()
            df=pd.DataFrame(ans,columns=["Channel Name", "Video Title", "Comment Count"])
            df.index = df.index + 1
            st.write(df)
            
# end
