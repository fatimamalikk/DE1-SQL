## Airbnb Austin, Texas Dataset
This folder contains all the work files used for the [ Data Engineering term project 1](https://github.com/fatimamalikk/DE1-SQL/tree/main/Term_Project_1). I set out to build a MySQL schema using the **Airbnb Austin, Texas dataset** available on the Inside Airbnb website - [link]( http://insideairbnb.com/get-the-data.html).

### Task Interpretation ###
Airbnb plans to conduct beta testing for a new business plan. They aim at increasing their revenue by incentivizing the hosts to maintain a positive score while reducing the costs. The top five hosts in a city will be rewarded according to their review scores. Airbnb is hoping that this will persuade the host to exhibit more hospitality, improve services, and customer relationships.

Airbnb hopes that satisfied customers will leave a positive review that will attract the masses towards the hosts ultimately translating into revenue. The main competitors are the mainstream hotels. The beta test is planned to be conducted in Austin, a city in Texas, United States.

Airbnb has a huge bulk of data to support their analytics team, but they are missing a central database to store this data. They gave me the task to design a system and provide high quality, easily interpretable data to the following three analytical groups:

- The first one is the price unit which looks at the price statistics of different property types e.g. min, max, avg.
- The second one is the host unit which looks at hosts with host rating scores less than 65 and the number of reviews greater than equal to 3. It will issue warning triggers so they can improve their services.
- The third one is the super-host unit which identifies the top 10 super hosts every month, who will gain reward points.

I decided to implement the solution in MYSQL RDBMS engine.

### Airbnb Relational Database Schema ###
The Airbnb schema comprises property listing data from [listings_austin.csv](https://github.com/fatimamalikk/DE1-SQL/tree/main/Term_Project_1/data/listings_austin.csv) file, linked to the hosts’ table from [hosts_austin.csv](https://github.com/fatimamalikk/DE1-SQL/tree/main/Term_Project_1/data/hosts_austin.csv) file on host_id, and calendar data from [calender_austin.csv](https://github.com/fatimamalikk/DE1-SQL/tree/main/Term_Project_1/data/calender_austin.csv) file on listing_id.
### OPERATIONAL LAYER ###
My operational layer consists of [3 tables stored in csv](https://github.com/fatimamalikk/DE1-SQL/tree/main/Term_Project_1/data) . The below EER diagram represents this schema. The **listings** table includes the properties listed by all hosts and the respective property attributes like property_type, bedrooms, bathrooms, price, reviews, etc. The **listings** table is linked to the **hosts** table with **host_id**. The **hosts** table includes the host particulars like name, the date on joining Airbnb, number of property listings, etc. Lastly, the **calendar** table shows the price and availability of the property on a particular date, which is linked to the **listings** table through **listing_id**. 

![Schema](https://github.com/fatimamalikk/DE1-SQL/blob/main/Term_Project_1/schema.PNG)

The operational layer was created using the following [queries - Line 1 to 103](https://github.com/fatimamalikk/DE1-SQL/tree/main/Term_Project_1/DE1_fatima_arshad.sql)

### Analytics Plan

My analytics plan is the following:
1. Loading up the acquired data
2. Create an ETL pipeline to create two data warehouses 
3. Create an ETL pipeline to create data marts for the following analytics.

This is illustrated in the below figure: 

![Analytics plan diagram](https://github.com/fatimamalikk/DE1-SQL/blob/main/Term_Project_1/analytics_plan.png)

I created two data warehouses through stored procedures:
1. **Available Listings Data Warehouse** focusing on property listings available by the host 
2. **Hosts Rating Data Warehouse** focusing on host reviews. 

I have also put a trigger in place which saves current information and stores it in a new table called **'listings_audit'** before any update is made.
I have considered the following questions for analysis while creating the data warehouses:

~~~~
1. What are the price statistics 'overall' and 'per person' by property type in Austin, Texas, United States?
~~~~
I sliced this subset of the data from the **available_listings** data warehouse to compute the rounded MIN, MAX, and AVG prices grouped by property_type, for both overall and in_person scenarios. I sorted the table by property types in ascending alphabetical order.
~~~~
2. Which hosts have an overall rating of less than 65 with the number of reviews greater or equal to 3 to single out badly performing hosts?
~~~~
I used the **host_ratings** data warehouse to extract information and created a view through a scheduled event. This event creates a monthly listing i.e. every 30 days for concerning5 months to identify the hosts who need to be sent a warning. Moreover, a trigger saves a trigger-issued statement into a separate table **messages** to help us make sure that the trigger was executed.

~~~~
3. Who are the top 5 best performing hosts in terms of ratings and number of reviews?
~~~~
I used the **host_ratings** data warehouse to extract this information and created a view through a scheduled event. This event creates a monthly listing i.e. every 30 days for 5 months to identify hosts eligible for the reward points.



### ANALYTICAL LAYER and ETL PIPELINE ###
I created a denormalized snapshot of combined listings and hosts tables for the available_listings subject. I created in a stored procedure that contains commands to extract, transform and load the data into a new table. This combination of important variables from different tables into a single table i.e. a data warehouse will help us in further analysis for our new business plan. An analytical layer was created using the following queries:

**Available Listings**
![availability_listings](https://github.com/fatimamalikk/DE1-SQL/blob/main/Term_Project_1/available_listings.PNG)

**Host Ratings** 
Next, I created a data warehouse through a stored procedure that encloses host-related information such as the number of reviews and ratings.

![host_ratings](https://github.com/fatimamalikk/DE1-SQL/blob/main/Term_Project_1/host_ratings.PNG)

#### TRIGGERS ####
Furthermore, I have put a trigger in place to save the current information for a listing, before the user updates it. This helps to keep track of all the host and listings activity in case of any technical or legal issue.
A trigger is a stored program that is invoked automatically in response to an ‘action’ such as an insert, update or delete that occurs in the associated table which is useful for tracking changes to the data in the database. 
For this, I created a new **listings_audit** table to save the old information before its updated. 

The Before Host Info Update Trigger was created using the following [Queries - LINE 183 to 218](https://github.com/fatimamalikk/DE1-SQL/tree/main/Term_Project_1/DE1_fatima_arshad.sql)
Once the trigger had been created, we tested the trigger to ensure that it works using the following [test code - LINE 219 to 229]()
~~~~
UPDATE listings
SET 
	number_of_reviews = 29,
	review_scores_rating = 78
WHERE
    listing_id = 2265;
~~~~


The trigger runs successfully and the listings_audit table is updated with the old information.

![warning_trigger](https://github.com/fatimamalikk/DE1-SQL/blob/main/Term_Project_1/trigger_warning.PNG)


### DATAMARTS with VIEWS ###
I created the following three data marts for BI operations and analytics:
1. Price View: 
This view shows the price statistics with respect to the property type. It includes the minimum, maximum, and average price ‘overall’ and ‘per person.

2. Low performing hosts: 
This view enlists hosts to whom a warning should be issued due to their host rating scores being less than 65 and the number of reviews being greater than equal to 3. Such metrics will indicate that the hosts need to improve the experience that they provide to the users. This warning will incentivize the hosts to earn more reward points by working on the loopholes that exist and improving their rating.

3. Top performing hosts: 
This view enlists the top 5 hosts with the highest rating and customer satisfaction.


##### Low Perfroming Hosts

This view highlights the hosts that are consistently exhibiting poor performance. Hosts having reviews below a certain threshold will be warned by the system. The warning will intimate them about their unsatisfied clientage and their risk of losing reward points. It is expected that such a measure will persuade the host to provide better and more professional service leading to more revenue.

##### Top Five Hosts #####

The goal of this view is to display the top five hosts from the data. Depending upon the specified metrics, the top five hosts will be displayed. They shall be awarded reward points. The purpose of the points is to incentivize the hosts to provide better service to the users. Users will be attracted to hosts with higher points generating more revenue. This creates a win-win situation for the entire system.

##### Price View #####
This view will display the hosts providing the best service at the lowest costs. This view will help users on a budget who are looking for good service. Users looking for affordable accommodation without having to compromise on hospitality will be the prime targets. The overall objective is to make the service as inclusive as possible for all the strata of the societies.
