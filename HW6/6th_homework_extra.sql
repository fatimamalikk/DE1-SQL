use classicmodels;

drop table if exists phone_prefix;

CREATE TABLE phone_prefix 
(
city varchar (255),
area_code INTEGER,
primary key(city) );


LOAD DATA INFILE 'c:/ProgramData/MySQL/MySQL Server 8.0/Uploads/phone_prefix.csv' 
INTO TABLE phone_prefix
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n' 
ignore 1 rows
;

select * from phone_prefix order by 2;
-- Brickhaven needs to be enriched with prefix 706
update phone_prefix set area_code = 706 where city = 'Brickhaven';


DROP PROCEDURE IF EXISTS FixUSPhones; 

DELIMITER $$

CREATE PROCEDURE FixUSPhones ()
BEGIN
	DECLARE finished INTEGER DEFAULT 0;
	DECLARE phone varchar(50) DEFAULT "x";
	DECLARE customerNumber INT DEFAULT 0;
	DECLARE country varchar(50) DEFAULT "";
    DECLARE area_code int default 0;
    DECLARE city varchar(50) default 'x';

	-- declare cursor for customer
	DECLARE curPhone
		CURSOR FOR 
            		SELECT customers.customerNumber, customers.phone, customers.country, customers.city 
				FROM classicmodels.customers;

	-- declare NOT FOUND handler
	DECLARE CONTINUE HANDLER 
        FOR NOT FOUND SET finished = 1;

	OPEN curPhone;
    
    	-- create a copy of the customer table 
	DROP TABLE IF EXISTS classicmodels.fixed_customers;
	CREATE TABLE classicmodels.fixed_customers LIKE classicmodels.customers;
	INSERT fixed_customers SELECT * FROM classicmodels.customers;

	fixPhone: LOOP
		FETCH curPhone INTO customerNumber,phone, country, city;
		IF finished = 1 THEN 
			LEAVE fixPhone;
		END IF;
		 
		-- insert into messages select concat('country is: ', country, ' and phone is: ', phone);
         
		IF country = 'USA'  THEN
			IF phone NOT LIKE '+%' THEN
				IF LENGTH(phone) = 10 THEN 
					set area_code = (select phone_prefix.area_code from phone_prefix where phone_prefix.city = city);
					SET phone = CONCAT('+1',area_code,right(phone,7));
					UPDATE classicmodels.fixed_customers 
						SET fixed_customers.phone=phone 
							WHERE fixed_customers.customerNumber = customerNumber;
                		    
				elseif length(phone) = 7 then
					set area_code = (select phone_prefix.area_code from phone_prefix where phone_prefix.city = city);
					SET phone = CONCAT('+1',area_code,phone); 
                    UPDATE classicmodels.fixed_customers 
						SET fixed_customers.phone=phone 
							WHERE fixed_customers.customerNumber = customerNumber;
                            
				END IF;
			END IF;
		END IF;

	END LOOP fixPhone;
	CLOSE curPhone;

END$$
DELIMITER ;

call FixUSPhones();


select * from fixed_customers where country = 'USA';