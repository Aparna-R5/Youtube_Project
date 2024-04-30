# Youtube Data harvesting and Warehousing Project

**1.Overview**

This project aims to retrieve data of different channels that are given by the user using channel ID that is unique to the channel. The data involves an outline of the channel, its purpose along with videos and comment details .The data are then stored in a database and queried to extract relevant information.

**2. Key Skills**
**2.1 Python**

Python Programming language was used in retrieval ,manipulation and analysis of channel data along with some libraries .

**2.2 SQL**

Structured Query Language was used to analyze information obtained from different  channels after the data gets migrated to MySQL database.

**2.3 Streamlit**

Streamlit framework works as a user interface to accept  a channel Id and showcase different details as requested by the user.

**3. Libraries Installed**
The following libraries were used:


**4. Workflow**
**4.1 Data Retrieval**

API key helps  in accessing the channel information and the information is separated in the form of channel, video, comment and playlist details. 

**4.2 Migration to MySQL**

 Data is loaded to pandas following which a database is created in MySQL  and  the channel details are migrated to that database .
 
**4.3 Analysis**

Connection to SQL database is established  to answer the queries as chosen by the user and display the result in the streamlit application.
