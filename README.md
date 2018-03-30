# Zomato Mart


A program to fetch restaurant related data from [zomato.com](https://www.zomato.com) via its public APIs and populate a set of Oracle tables. 

Objective: Build and maintain history for restaurant ratings, restaurant collections, other restaurant attributes and location indices. Additionally, new restaurant alerts are sent out to subscribers. Data fetch is restricted via parameters to a predefined set of cities/localities. Tables are designed to maintain history at a monthly grain. 

[![Zomato](https://b.zmtcdn.com/images/developers/zomato-developers-logo.png)](https://developers.zomato.com/)
* [Zomato API](https://developers.zomato.com/api) - Zomato API Documentation


Installation
------------
This process is currently manual and involves the following steps:

    1. Create Zomato Mart tables (execute zomato_ddls.sql)
    2. Setup Parameters to filter data by City/Locality (edit and execute zomato_parameters.sql)
    3. Copy over the .py files to the appropriate folders
    4. Modify API key file location (base_dir) in apikey.py, db_oracle.py and db_mysql.py
    5. Schedule program to run monthly

Pre-Requisites
------------
1. Zomato API key is required to connect to the APIs and can be generated [here](https://developers.zomato.com/api).
2. For alert functionality (optional), Mailgun API key is required and can be generated [here](https://www.mailgun.com/).

In the Backlog
------------
1. Refactor Code
2. Refine New Restaurant Notification
2. Visualization
