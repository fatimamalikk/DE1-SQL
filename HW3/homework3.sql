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
-- If speed is NULL or speed < 100 create a "LOW SPEED" category, otherwise, mark as "HIGH SPEED". Use IF instead of CASE!?
select if(speed is null, "LOW SPEED", if(speed<100, "LOW SPEED","HIGH SPEED")) as SPEED_CATEGORY from birdstrikes;	
select if(speed is null or speed <100, "LOW SPEED","HIGH SPEED") as SPEED_CATEGORY from birdstrikes;	

-- Answer is the above column

-------------------------- Question 2)
-- How many distinct 'aircraft' we have in the database?
select count(distinct aircraft) from birdstrikes;

-- Answer is 3

-------------------------- Question 3)
-- Exercise3: What was the lowest speed of aircrafts starting with 'H'
select min(speed) from birdstrikes where left(aircraft,1) = 'H'; 
-- Answer is: 9

-------------------------- Question 4)
-- Which phase_of_flight has the least of incidents?
select phase_of_flight,count(*) as no_incident from birdstrikes group by phase_of_flight order by 2 asc limit 1;
-- The answer is Taxi

-------------------------- Question 5)
-- What is the rounded highest average cost by phase_of_flight?
select phase_of_flight, round(avg(cost)) as avg_cost from birdstrikes group by phase_of_flight order by 2 desc limit 1;
-- The answer is: 54673 and the phase is Climb

-------------------------- Question 6)
-- What the highest AVG speed of the states with names less than 5 characters?
select state, avg(speed) as avg_speed from birdstrikes group by state having length(state)<5 order by 2 desc limit 1; 
-- The answer is: 2862.5, state is Iowa 
