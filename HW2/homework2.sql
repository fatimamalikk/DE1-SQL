use firstdb;

CREATE TABLE birdstrikes 
(id INTEGER NOT NULL,
aircraft VARCHAR(32),
flight_date DATE NOT NULL,
damage VARCHAR(16) NOT NULL,
airline VARCHAR(255) NOT NULL,
state VARCHAR(255),
phase_of_flight VARCHAR(32),
reported_date DATE,
bird_size VARCHAR(16),
cost INTEGER NOT NULL,
speed INTEGER,PRIMARY KEY(id));

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/birdstrikes_small.csv' 
INTO TABLE birdstrikes 
FIELDS TERMINATED BY ';' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES 
(id, aircraft, flight_date, damage, airline, state, phase_of_flight, @v_reported_date, bird_size, cost, @v_speed)
SET
reported_date = nullif(@v_reported_date, ''),
speed = nullif(@v_speed, '');

-- *** ANSWERING QUESTIONS FROM CLASS2 ***

--------------------------  Question 1) 
-- What state figures in the 145th line of our database?
select state from birdstrikes limit 144,1;
-- Answer is Tennessee

-------------------------- Question 2)
-- What is flight_date of the latest birstrike in this database?
select flight_date 
from birdstrikes 
order by flight_date desc 
limit 1; 
-- Answer is 2000-04-18

-------------------------- Question 3)
-- What was the cost of the 50th most expensive damage?
select distinct cost from birdstrikes order by cost desc limit 49,1; 
-- Answer is: 5345

-------------------------- Question 4)
-- What state figures in the 2nd record, if you filter out all records which have no state and no bird_size specified?
-- 		Solution A -	In this solution I show all records where state and bird size was an empty string as the question  
-- 						was about fields where the two variables are not specified. Since those two fields are never shown as NULL
-- 						(I checked this with query select * form db where bird_size is null or state is NULL) it made more sense to check for empty strings
select state from birdstrikes where bird_size <> '' and state <> '' limit 1,1 ; 
-- The answer is Colorado
-- 		Solution B -	This is solution used in class.  
select state from birdstrikes where bird_size is not NUll and state is not null limit 1,1;
-- Answer is: empty string

-------------------------- Question 5)
-- How many days elapsed between the current date and the flights happening in week 52, for incidents from Colorado? (Hint: use NOW, DATEDIFF, WEEKOFYEAR)
select DATEDIFF(NOW(),flight_date) as date_diff 
from birdstrikes b 
where b.state = 'Colorado' 
and weekofyear(b.flight_date) = '52';  
-- The answer is: 7582

