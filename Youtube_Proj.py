import googleapiclient.discovery
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import sqlalchemy
import pymysql
import mysql.connector
from sqlalchemy import create_engine

# API connection
api_service_name = "youtube"
api_version = "v3"

api_key = "API"
youtube = googleapiclient.discovery.build(
api_service_name, api_version, developerKey = api_key)

# Streamlit code 
st.header(":red[Youtube Data harvesting and warehousing.]")
channel_id = st.text_input("Enter Channel Id")
go =st.button(":red[Scrape and migrate to MySQL]")
with st.sidebar:
    st.caption("Workflow")
    st.caption("-Retrieve channel data")
    st.caption("-Store and migrate to database")
    st.caption("-Analyze data")

# Get channel data
def channel_data(channel_id):

        request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id= channel_id)
        response = request.execute()


        
        data= {'channel_name':response['items'][0]['snippet']['title'],
               'channel_id':response['items'][0]['id'],
               'channel_ds':response['items'][0]['snippet']['description'],
               'channel_pat':response['items'][0]['snippet']['publishedAt'],
               'channel_pid':response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
               'video_count':response['items'][0]['statistics']['videoCount'],
               'view_count':response['items'][0]['statistics']['viewCount'],
               'sub_count':response['items'][0]['statistics']['subscriberCount']}
        return data 


# Get video ids 
def get_video_id(channel_id):
    video_ids = []
    response = youtube.channels().list( part='contentDetails',
    id=channel_id).execute()
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True :
        response1 = youtube.playlistItems().list(part= 'snippet',playlistId =Playlist_Id,maxResults=50,pageToken = next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids     

# Get video details
def get_video_details(video_IDS):
    video_data = []
    for videoId in video_IDS:
            request = youtube.videos().list(
                part="snippet,ContentDetails,statistics",id = videoId
            )
            response = request.execute()
            for item in response.get("items", []):
                data = dict(Video_id = item['id'],
                Channel_id = item['snippet']['channelId'],
                Video_name=item['snippet']['title'],
                Video_description = item['snippet']['description'],
                Tags = item['snippet'].get('tags', []),
                PublishedAt = item['snippet']['publishedAt'],
                View_count = item['statistics'].get('viewCount', 0),
                Comment_count = item['statistics'].get('commentCount', 0),
                Favourite_count = item['statistics'].get('favoriteCount', 0),
                Like_count =  item['statistics'].get('likeCount', 0),
                Duration = item['contentDetails'].get('duration', ''),
                Thumbnail = item['snippet']['thumbnails'],
                Caption_status = item['contentDetails'].get('caption', ''))
                video_data.append(data)
    return video_data



# get comment details
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for videoid in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=videoid,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
                
    except:
        pass
    return Comment_data

#get Playlist details
def get_channel_playlists(channel_id):
    playlists = []
    next_page_token = None
    request = youtube.playlists().list(
        part='snippet',
        channelId=channel_id,
        maxResults=50 
    )
    
    response = request.execute()
    page_token = next_page_token
    
    for item in response['items']:
        playlist_info = {
            'Playlist_id': item['id'],
            'Channel_id': item['snippet']['channelId'],
            'Playlist_name': item['snippet']['title']
        }
        playlists.append(playlist_info)
    
    return playlists



# Function to retrieve entire channel details
def channel_info(channel_id):
    try:
        ch = channel_data(channel_id)
        Video_IDS = get_video_id(channel_id) 
        video_det = get_video_details(Video_IDS)
        commentinfo = get_comment_info(Video_IDS)
        play = get_channel_playlists(channel_id)
        
      

        if ch and Video_IDS and video_det and commentinfo and play:
            channel = pd.DataFrame([ch])
            df = pd.DataFrame(video_det)
            commentsdata = pd.DataFrame(commentinfo)
            play1 = pd.DataFrame(play)
            


            return {
                "channel_details": channel,
                "video_details": df,
                "comment_details": commentsdata,
                "playlist_details":play1
            }
        else:
            return None
    except Exception as e:
        print("Error:", e)
        return None

cha = channel_info(channel_id)

# Connect to SQL 
connect = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root") 
      
mycursor = connect.cursor()
mycursor.execute("CREATE DATABASE IF NOT EXISTS yt")

#Close connection
mycursor.close()
connect.close()

#Connect to SQL to migrate details to database
engine = create_engine('mysql+mysqlconnector://root:root@localhost/yt', echo=False)
if cha:
    cha["channel_details"].to_sql('channel', engine, if_exists='append', index=False,
                    dtype={"Channel_id":sqlalchemy.types.VARCHAR(225),
                            "channel_name": sqlalchemy.types.VARCHAR(225),
                            "channel_ds": sqlalchemy.types.TEXT,
                            "channel_pid": sqlalchemy.types.VARCHAR(225),
                            "video_count": sqlalchemy.types.BigInteger,
                            "view_count": sqlalchemy.types.BigInteger,
                            "sub_count": sqlalchemy.types.BigInteger})
    cha["video_details"].to_sql('video', engine, if_exists='append', index=False,
                  dtype={"Video_id": sqlalchemy.types.VARCHAR(225),
                         "Channel_id": sqlalchemy.types.VARCHAR(225),
                         "Video_name": sqlalchemy.types.VARCHAR(225),
                         "Video_description": sqlalchemy.types.TEXT,
                         "Tags": sqlalchemy.types.TEXT,
                         "PublishedAt":sqlalchemy.types.VARCHAR(50),
                         "View_count":sqlalchemy.types.INT,
                         "Comment_count":sqlalchemy.types.INT,
                         "Favourite_count":sqlalchemy.types.INT,
                         "Like_count":sqlalchemy.types.INT,
                         "Duration":sqlalchemy.types.VARCHAR(225),
                         "Thumbnail":sqlalchemy.types.TEXT,
                         "Caption_status":sqlalchemy.types. VARCHAR(225),})
    
    cha["comment_details"].to_sql('comments', engine, if_exists='append', index=False,
                        dtype = {'Comment_Id': sqlalchemy.types.VARCHAR(225),
                                 'Video_Id': sqlalchemy.types.VARCHAR(225),
                                 'Comment_Text': sqlalchemy.types.TEXT,
                                'Comment_Author': sqlalchemy.types.VARCHAR(225),
                                'Comment_Published': sqlalchemy.types.VARCHAR(50)})
    chan["playlist_details"].to_sql('playlist', engine, if_exists='append', index=False,
                            dtype = {"Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                                    "Playlist_Id": sqlalchemy.types.VARCHAR(length=225),
                                    "Playlist_name":sqlalchemy.types.TEXT})


# streamlit code to write details based on user selection
if go:
 st.success("Retrieved data and migrated successfully!")  

selected = option_menu(menu_title = None,options=["Channel detail","Video detail","Comment details"],orientation="horizontal")
try:
        
    if selected =="Channel detail":
                        st.write(cha["channel_details"])
    elif selected =="Video detail":
                        st.write(cha["video_details"])
    elif selected =="Comment details":
                        st.write(cha["comment_details"])
except:
        pass

st.subheader(":red[Channel Analysis]")


# Query selection
question = st.selectbox("Query zone",
                        ["Select a question",
            '1. What are the names of all the videos and their corresponding channels?',
            '2. Which channels have the most number of videos, and how many videos do they have?',
            '3. What are the top 10 most viewed videos and their respective channels?',
            '4. How many comments were made on each video, and what are their corresponding video names?',
            '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
            '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
            '7. What is the total number of views for each channel, and what are their corresponding channel names?',
            '8. What are the names of all the channels that have published videos in the year 2022?',
            '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
            '10. Which videos have the highest number of comments, and what are their corresponding channel names?'], index = 0)

# SQL connection 
query = pymysql.connect(host='localhost', user='root', password='root', db='yt')
cursor = query.cursor()



# Q1
if question == '1. What are the names of all the videos and their corresponding channels?':
            cursor.execute("SELECT channel.channel_name, video.Video_name FROM channel JOIN video ON channel.Channel_id = video.Channel_id group by channel.channel_name,video.Video_name;")
            ans_1 = cursor.fetchall()
            q1 = pd.DataFrame(ans_1, columns=['Channel Name', 'Video Name']).reset_index(drop=True)
            q1.index += 1
            st.dataframe(q1)

# Q2
elif question == '2. Which channels have the most number of videos, and how many videos do they have?':
                cursor.execute("SELECT channel_name, video_count FROM channel join video on channel.Channel_id = video.Channel_id Group by channel_name,video_count ORDER BY video_count DESC;")
                ans_2 = cursor.fetchall()
                q2 = pd.DataFrame(ans_2,columns=['Channel Name','Video Count']).reset_index(drop=True)
                q2.index += 1
                st.dataframe(q2)
                
                

# Q3
elif question == '3. What are the top 10 most viewed videos and their respective channels?':
                cursor.execute("SELECT distinct video.Video_name,channel.channel_name,video.View_count FROM channel JOIN video ON channel.Channel_id = video.Channel_id ORDER BY video.View_count DESC LIMIT 10;")
                ans_3 = cursor.fetchall()
                q3 = pd.DataFrame(ans_3,columns=['Video Name','Channel Name', 'View count']).reset_index(drop=True)
                q3.index += 1
                st.dataframe(q3)


# Q4 
elif question== '4. How many comments were made on each video, and what are their corresponding video names?':
            cursor.execute("SELECT  video.Video_name, video.Comment_Count FROM video; ")
            ans_4 = cursor.fetchall()
            q4 = pd.DataFrame(ans_4,columns=[ 'Video Name', 'Comment count']).reset_index(drop=True)
            q4.index += 1
            st.dataframe(q4)

# Q5
elif question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            cursor.execute("select distinct video.Video_name,channel.channel_name, Like_count from channel join video On channel.Channel_id = video.Channel_id order by Like_count DESC;")
            ans_5= cursor.fetchall()
            q5 = pd.DataFrame(ans_5,columns=['Video Name', 'Channel Name', 'Like count']).reset_index(drop=True)
            q5.index += 1
            st.dataframe(q5)

# Q6
elif question == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            st.write('**YouTube removed the public dislike count from all of its videos.**')
            cursor.execute("SELECT channel.Channel_Name, video.Video_Name, video.Like_Count FROM  video ORDER BY video.Like_Count DESC;")
            ans_6= cursor.fetchall()
            q6 = pd.DataFrame(ans_6,columns=['Channel Name', 'Video Name', 'Like count']).reset_index(drop=True)
            q6.index += 1
            st.dataframe(q6)

# Q7
elif question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
            cursor.execute("SELECT channel_name, view_count FROM channel group by channel_name,view_count ORDER BY view_count DESC;")
            ans_7= cursor.fetchall()
            q7 = pd.DataFrame(ans_7,columns=['Channel Name', 'Total number of views']).reset_index(drop=True)
            q7.index += 1
            st.dataframe(q7)
            
# Q8
elif question == '8. What are the names of all the channels that have published videos in the year 2022?':
            cursor.execute("SELECT channel.channel_Name, video.Video_name, video.PublishedAt FROM channel JOIN video ON channel.Channel_id = video.Channel_id  WHERE EXTRACT(YEAR FROM PublishedAt) = 2022 group by channel_name,video_name,PublishedAt;")
            ans_8= cursor.fetchall()
            q8 = pd.DataFrame(ans_8,columns=['Channel Name','Video Name', 'Year 2022 only']).reset_index(drop=True)
            q8.index += 1
            st.dataframe(q8)

        # Q9
elif question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            cursor.execute("SELECT channel_name, AVG(duration_seconds) AS average_duration_seconds FROM (SELECT c.channel_name,SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, 'T', -1), 'M', 1) * 60 + SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, 'M', -1), 'S', 1) AS duration_seconds FROM video v JOIN channel c ON v.Channel_id = c.Channel_id) AS channel_durations GROUP BY channel_name;")
            ans_9= cursor.fetchall()
            q9 = pd.DataFrame(ans_9,columns=['Channel Name','Average duration of videos']).reset_index(drop=True)
            q9.index += 1
            st.dataframe(q9)
        

# Q10
elif question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            cursor.execute("SELECT channel.channel_name, video.Video_name, video.Comment_count FROM channel JOIN video ON channel.Channel_id = video.Channel_id group by channel_name,Video_name,Comment_count ORDER BY video.Comment_count DESC;")
            ans_10= cursor.fetchall()
            q10 = pd.DataFrame(ans_10,columns=['Channel Name','Video Name', 'Number of comments']).reset_index(drop=True)
            q10.index += 1
            st.dataframe(q10)
            

# close SQL connection
query.close()
