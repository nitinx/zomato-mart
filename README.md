# [(https://b.zmtcdn.com/images/logo/zomato_logo.svg)] Zomato Mart

A program to fetch restaurant related data from [zomato.com](https://www.zomato.com) via its public APIs and populate a set of Oracle tables. Purpose is to build and maintain history for restaurant ratings, restaurant collections, other restaurant attributes and location indices. Data fetch is restricted via parameters to a predefined set of cities/localities. Tables are designed to maintain history at a monthly grain. 

* [Zomato API](https://developers.zomato.com/api) - Zomato API Documentation

Installation
------------
This process is currently manual and involves the following steps:

    1. Create Zomato Mart tables (execute zomato_ddls.sql)
    2. Setup Parameters to filter data by City/Locality (edit and execute zomato_parameters.sql)
    3. Copy over the .py files to the appropriate folders
    4. Modify API key file location in nxcommon.py
    5. Schedule program to run monthly

Pre-Requisites
------------
Zomato API key is required to connect to the APIs. This can be generated [here](https://developers.zomato.com/api).
