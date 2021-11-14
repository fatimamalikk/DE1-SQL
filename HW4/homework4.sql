use classicmodels;

-- INNER join orders,orderdetails,products and customers. Return back:
-- orderNumber, priceEach, quantityOrdered, productName, productLine, city, country, orderDate

select orderNumber, priceEach, quantityOrdered, productName, productLine, city, country, orderdate from
orders o1 inner join orderdetails o2 using(ordernumber)
inner join products p using (productcode) inner join 
customers c using(customernumber)