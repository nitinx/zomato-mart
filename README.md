# Zomato Mart

A program to fetch restaurant related data from zomato.com via its public APIs and populate a set of Oracle tables. Purpose is to build and maintain history for restaurant ratings, restaurant collections and location indices. Data fetch is restricted to a predefined set of cities/localities via parameters. 

* [Zomato API](https://developers.zomato.com/api) - Zomato API Documentation

Installation
------------
This process is currently manual and involves the following steps:

    1. Create the Zomato Mart tables (execute zomato_ddls.sql)
    2. Setup Parameters to filter data by City/Locality (edit and execute zomato_parameters.sql)
    3. Copy over the .py files to the appropriate folders
    4. Schedule the program to run monthly

