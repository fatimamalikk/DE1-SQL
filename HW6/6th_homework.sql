use classicmodels;
-------------
-- Exercise1: Create a stored procedure which displays the first X entries of payment table. X is IN parameter for the procedure.
-------------
DELIMITER //
DROP PROCEDURE IF EXISTS first_x_payments;

create procedure first_x_payments (
	IN top_x INT
)
BEGIN 
	select * from payments limit top_x;
END //
DELIMITER ;

call first_x_payments(13);


-------------
-- Exercise2: Create a stored procedure which returns the amount for Xth entry of payment table. X is IN parameter for the procedure. Display the returned amount.
-------------

drop procedure if exists xth_payments;

DELIMITER //

create procedure xth_payments (
	in x int,
    out amountout decimal (10,2)
    )

begin 
	set x=x-1;	
	select amount into amountout from payments limit x ,1 ;
end //

delimiter ;

call xth_payments(10,@amount);
select @amount;
-- INOUT


-------------
-- Exercise3:  Create a stored procedure which returns category of a given row. Row number is IN parameter, while category is OUT parameter.
--  Display the returned category. CAT1 - amount > 100.000, CAT2 - amount > 10.000, CAT3 - amount <= 10.000
-------------
drop procedure if exists getCategory;

delimiter // 

create procedure getCategory(
	in rownum INT,
    out category varChar(255)
)
begin 
set rownum = rownum-1;
select case when amount > 100000 then 'CAT1' when amount >10000 then 'CAT2' else 'CAT3' end as category
	into category 
		from payments 
        limit rownum,1;
end //
 
delimiter ;

call getCategory(3,@category);
select @category;

-------------
-- Exercise 4: create a loop which counts to 5 and displays the actualy count in each step as select 
-------------
drop procedure if exists looping; 
delimiter //
create procedure looping()

begin 
	declare x int;
	set x = 0;
    
    myloop:LOOP
        set x=x+1; 
        select x;
        
        if (x = 5) then 
			leave myloop;
		end if ;
	end loop myloop;
end //
delimiter ;
call looping;

-------------
-- Exercise 5: Loop through orders table. Fetch orderNumber + shippedDate. Write in both fields into messages as one line.alter
-------------

select * from orders;

drop table if exists messages;

CREATE TABLE messages (message varchar(100));

DROP PROCEDURE IF EXISTS order_shipped;

DELIMITER //

CREATE PROCEDURE order_shipped()
BEGIN

DECLARE ordernum INTEGER DEFAULT 0; 
DECLARE shipped DATE DEFAULT 0;  
DECLARE finished integer DEFAULT 0;
      
DECLARE ornum CURSOR FOR SELECT orders.orderNumber FROM classicmodels.orders;
DECLARE ship CURSOR FOR SELECT orders.shippedDate FROM classicmodels.orders;
DECLARE CONTINUE HANDLER  FOR NOT FOUND SET finished = 1; 

OPEN ornum;
OPEN ship;

TRUNCATE messages;
	myloop: LOOP 
	           
		FETCH ornum INTO ordernum;
        FETCH ship INTO shipped;
        
        INSERT INTO messages SELECT CONCAT(ordernum,shipped);
        
		IF finished = 1 THEN LEAVE myloop;
		END IF;
         
	END LOOP myloop;
END //

DELIMITER ;

call order_shipped();
select * from messages;


-------------
-- Exercise6: Copy the birdstrikes structure into a new tabe called birdstrikes2. Insert into birdstrikes2 the line where id is 10.
-- Hints:
-- Use the samples from Chapter2 for copy
-- For insert user the format like: INSERT INTO bla SELECT blabla
------------

CREATE TABLE birdstrikes2 LIKE birdstrikes;
INSERT INTO birdstrikes2 select * from birdstrikes where id = 10;

-------------
-- Exercise7: Create a view, which contains product_sales rows of 2003 and 2005. How many row has the resulting view?
-------------
drop view if exists product_sales_2003_2005;

CREATE view product_sales_2003_2005 as 
	select * from product_sales where year(date) in (2003,2005);

select count(*) from product_sales_2003_2005;