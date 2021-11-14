-- Exercise1: Create a stored procedure which displays the first X entries of payment table. X is IN parameter for the procedure.
use classicmodels; 
drop procedure if exists TOP_ENTRIES; 

DELIMITER //

create procedure TOP_ENTRIES (
	in top_limit int
    )
    
begin
select * from payments limit top_limit;
end //

DELIMITER ;

call TOP_ENTRIES(3);

-- Exercise2: Create a stored procedure which returns the amount for Xth entry of payment table. X is IN parameter for the procedure. Display the returned amount.
drop procedure if exists xth_entry;

DELIMITER //

create procedure xth_entry (
	in nth_row int,
    out nth_amount decimal(10,2)
    )
    
begin 
	set nth_row = nth_row-1;
	select amount into nth_amount from payments limit nth_row,1;
end //

DELIMITER ;

call xth_entry(3,@nth_amount);
select @nth_amount;

-- Exercise3:  Create a stored procedure which returns category of a given row. Row number is IN parameter, while category is OUT parameter.
--  Display the returned category. CAT1 - amount > 100.000, CAT2 - amount > 10.000, CAT3 - amount <= 10.000
drop procedure if exists assign_category;

DELIMITER //

create procedure assign_category (
	in row_num INT,
    out category VARCHAR(255)
) 

begin 
set row_num = row_num-1;
select case when amount > 100000 then 'CAT1' when amount > 10000 then 'CAT2' else 'CAT3' end as category into category from payments limit row_num,1;
end // 

DELIMITER ;

call assign_category(3,@category);
select @category;