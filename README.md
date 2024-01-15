# YouTube Data Harvesting and Warehousing

This project focuses on collecting data from YouTube using Python, storing it in MongoDB, and subsequently transferring the data to SQL. The frontend is powered by Streamlit to provide an interactive interface for data exploration.

## Table of Content

* Introduction
* Usage
* Tools Used
* Installation
* Documentation
* Contact Details
## Introduction:

This project aims to collect data from YouTube using Python, storing the data initially in MongoDB, and later transferring it to an SQL database. The Streamlit dashboard provides an intuitive interface for users to explore and see the analyzed data.

## Usage:

The data harvesting module involves making API calls to the YouTube Data API to fetch relevant information such as channel detaisl, video details, comment details. 

Ensure you have API keys configured for YouTube Data API.

#### Database Structure:

The project uses both SQL and MongoDB to warehouse the harvested data. The MongoDB provides flexibility for unstructured data, while SQL database stores structured information.
 
#### Streamlit Dashboard:

The Streamlit dashboard offers an interactive interface to explore and visualize YouTube data.


## Tools Used

### Python:
  Python is a high-level, general-purpose programming language known for its simplicity and readability. With a vast ecosystem of libraries and frameworks, Python is widely used for web development, data analysis, artificial intelligence, and more. Its versatility and ease of learning make it a popular choice among developers.

### MongoDB:
  MongoDB is a NoSQL database that stores data in a flexible, JSON-like format called BSON. It is known for its scalability, flexibility, and ability to handle unstructured data. MongoDB is particularly suitable for projects where data structures may evolve over time, and it's commonly used in web applications and big data solutions.

### MySQL:
  MySQL is a popular open-source relational database management system (RDBMS) that uses SQL for querying and managing data. It is widely used for web applications and is known for its reliability, performance, and ease of integration. MySQL is a part of the LAMP (Linux, Apache, MySQL, PHP/Python/Perl) stack, a common choice for web development.

### Streamlit:
  Streamlit is a Python library that simplifies the process of creating web applications for data science and machine learning. With just a few lines of code, developers can turn data scripts into interactive web apps. Streamlit is known for its user-friendly syntax and its ability to rapidly prototype and share data-driven insights, making it a valuable tool for data scientists and developers.

  
## Project Installation Instructions:

### Prerequisites:

Make sure you have the following dependencies installed on your system:

* IDE to run Python [e.g., Jupyer or Visual Studio]
* NoSQL [e.g., MongoDB, Cassandra, Amazon DynamoDB]
* SQL [e.g., MySQL, PostgreSQL or Microsoft SQL Server]

### Install Dependencies:

Install the project dependencies using the package manager [e.g., pip]

* Install Google API Python Client ```pip install google-api-python-client```

* Install MongoDB driver for Python  ```pip install pymongo```

* Install MySQL Connector for Python  ```pip install mysql-connector-python```

* Install Pandas library  ```pip install pandas```

* Install Streamlit library  ```pip install streamlit```

* Install isodate library  ```pip install isodate```

* Install python-dateutil library  ```pip install python-dateutil```

### Configuration:

Users need to set up these configurations according to the project's requirements.

* Use your developer key
          
      developerKey="YOUR_API_KEY"

* Use your MongoDB credentials

      connection  = mysql.connector.connect(user='root', 
                                              password='YOUR_PASSWORD', 
                                              host='localhost', 
                                              database="YOUR_DATABASE_NAME")

* Use your SQL credentials

      client = MongoClient("localhost",27017)
      db = client.YOUR_DATABASE_NAME

### Run the Project:
Once everything is set up, users can run the project with the  below command in the terminal.

    streamlit run YOUR_FILE_NAME.py

## Documentation

### API Key

```https://developers.google.com/youtube/v3/getting-started```

### MongoDB

```https://www.mongodb.com/docs/manual/reference/method/connect/```

### MySQL

```https://dev.mysql.com/doc/connector-python/en/connector-python-example-connecting.html```
  
  
### Streamlit

```https://docs.streamlit.io/library/api-reference```


## Contact Information

Desilva A S
  
  Email: asdesilva3@gmail.com

Feel free to reach out if you have any questions, suggestions, or if you just want to say hello! I appreciate your interest and engagement with my project.👍
