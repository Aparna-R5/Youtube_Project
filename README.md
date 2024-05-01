# Youtube Data harvesting and Warehousing Project

**1.Overview**

This project aims to retrieve data from different channels that are given by the user using a Channel ID that is unique to the channel. The data involves an outline of the channel, along with videos and comment details .The data is then  migrated to a database for further analysis.

**2. Key Skills**

**2.1 Python**

Python Programming language was used in retrieval ,manipulation and analysis of channel data along with some libraries sucha as pandas.

**2.2 SQL**

Structured Query Language was used to analyze information obtained from different  channels after the data gets migrated to MySQL database.

**2.3 Streamlit**

Streamlit framework works as a user interface to accept  a channel Id and showcase different details as requested by the user.

**3. Libraries Installed**
The following libraries were used:


![image112](https://github.com/Aparna-R5/Youtube_Project/assets/167562414/d37b28f7-31a3-4289-9aec-a686060605ad)


**4. Workflow**

**4.1 Data Retrieval**

Youtube API is used to access the channel information and googleapiclient library in python is used to connect to the API.The channel details are loaded to pandas dataframe before warehousing.

**4.2 Migration to MySQL**

A database is created in MySQL and the channel details are migrated to that database with separate tables for videos , comments and playlist .
 
**4.3 Analysis**

Connection to SQL database is established  to answer the queries as chosen by the user and display the result in the streamlit application.
