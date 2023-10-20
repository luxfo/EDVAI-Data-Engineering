-- 1- Obtener una lista de todas las categorías distintas
select distinct category_name
from categories c 

-- 2- Obtener una lista de todas las regiones distintas para los clientes
select distinct region
from customers c 

-- 3- Obtener una lista de todos los títulos de contacto distintos
select distinct contact_title
from customers c 

-- 4- Obtener una lista de todos los clientes, ordenados por país
select *
from customers c 
order by country 

-- 5- Obtener una lista de todos los pedidos, ordenados por id del empleado y fecha del pedido
select * from orders o 
order by employee_id, order_date

-- 6- Insertar un nuevo cliente en la tabla Customers
insert into customers 
(customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax)
values
('EDVAI', 'Escuela de Datos Vivos', 'Pablo Casas', 'CEO', 'edvai 1234', 'Buenos Aires', null, '1234', 'Argentina', '(54) 11-2458-8949', null)

select * from customers c 
where customer_id = 'EDVAI'

-- 7- Insertar una nueva región en la tabla Región
insert into region 
(region_id, region_description)
values 
(5, 'Region Pampeana')

select * from region r 

-- 8- Obtener todos los clientes de la tabla Customers donde el campo Región es NULL
select * from customers c 
where region is null

-- 9- Obtener Product_Name y Unit_Price de la tabla Products, y si Unit_Price es NULL,
--		use el precio estándar de $10 en su lugar
select product_name, coalesce(unit_price, 10) as price 
from products p 

-- 10- Obtener el nombre de la empresa, el nombre del contacto y la fecha del pedido de todos los pedidos
select c.company_name, c.contact_name, o.order_date
from customers c
	 inner join orders o ON (o.customer_id = c.customer_id)

-- 11- Obtener la identificación del pedido, el nombre del producto y
--		el descuento de todos los detalles del pedido y productos
select o.order_id, p.product_name, od.discount
from orders o
	 inner join order_details od on (od.order_id = o.order_id)
	 inner join products p on (p.product_id = od.product_id)

-- 12- Obtener el identificador del cliente, el nombre de la compañía, el identificador y la fecha
--		de la orden de todas las órdenes y aquellos clientes que hagan match
SELECT c.customer_id, c.company_name, o.order_id, o.order_date 
FROM customers c
	 left join orders o on (o.customer_id = c.customer_id)

-- 13- Obtener el identificador del empleados, apellido, identificador de territorio y descripción
--		del territorio de todos los empleados y aquellos que hagan match en territorios
select e.employee_id, e.last_name, et.territory_id, t.territory_description 
from employees e
	 left join employee_territories et on (et.employee_id = e.employee_id)
	 left join territories t on (t.territory_id = et.territory_id)

-- 14- Obtener el identificador de la orden y el nombre de la empresa de todos las órdenes y
--		aquellos clientes que hagan match
select o.order_id, c.company_name 
from orders o
	 left join customers c on (c.customer_id = o.customer_id)

-- 15- Obtener el identificador de la orden, y el nombre de la compañía de todas las órdenes y
--		aquellos clientes que hagan match
select o.order_id, c.company_name 
from orders o
	 right join customers c on (c.customer_id = o.customer_id)
	
-- 16- Obtener el nombre de la compañía, y la fecha de la orden de todas las órdenes y
--		aquellos transportistas que hagan match. Solamente para aquellas ordenes del año 1996
select s.company_name, o.order_date 
from orders o
	 right join shippers s on (s.shipper_id = o.ship_via)
where extract(year from o.order_date) = 1996

-- 17- Obtener nombre y apellido del empleados y el identificador de territorio, de todos los
--		empleados y aquellos que hagan match o no de employee_territories
select e.first_name, e.last_name, et.territory_id 
from employees e 
	 full outer join employee_territories et on (et.employee_id = e.employee_id)

-- 18- Obtener el identificador de la orden, precio unitario, cantidad y total de todas las
--		órdenes y aquellas órdenes detalles que hagan match o no
select o.order_id, od.unit_price, od.quantity, (od.unit_price * od.quantity) as total
from orders o
	 full outer join order_details od on (od.order_id = o.order_id)

-- 19- Obtener la lista de todos los nombres de los clientes y los nombres de los proveedores
select c.company_name  
from customers c
union
select s.company_name 
from suppliers s 

-- 20- Obtener la lista de los nombres de todos los empleados y los nombres de los gerentes de departamento
select e.first_name
from employees e
union
select e2.first_name 
from employees e2 
where e2.title = 'Sales Manager'

-- 21- Obtener los productos del stock que han sido vendidos
select p.product_name, p.product_id
from products p
where p.product_id in (select od.product_id from order_details od)

-- 22- Obtener los clientes que han realizado un pedido con destino a Argentina
select c.company_name 
from customers c
where c.customer_id in (select o.customer_id 
						from orders o 
						where o.ship_country = 'Argentina')

-- 23- Obtener el nombre de los productos que nunca han sido pedidos por clientes de Francia
select p.product_name 
from products p
where p.product_id not in (select od.product_id 
						   from customers c
						   		inner join orders o on (o.customer_id = c.customer_id)
						   		inner join order_details od on (od.order_id = o.order_id)
						   where c.country = 'France')

-- 24- Obtener la cantidad de productos vendidos por identificador de orden
select od.order_id, sum(od.quantity)
from order_details od
group by od.order_id 

-- 25- Obtener el promedio de productos en stock por producto
select p.product_name, avg(units_in_stock)
from products p 
group by p.product_name 

-- 26- Cantidad de productos en stock por producto, donde haya más de 100 productos en stock
select p.product_name, sum(units_in_stock)
from products p 
group by p.product_name 
having sum(units_in_stock) > 100

-- 27- Obtener el promedio de pedidos por cada compañía y solo mostrar aquellas con un
--		promedio de pedidos superior a 10
select c.company_name, avg(o.order_id) as averageorders
from orders o 
	 inner join customers c on (c.customer_id = o.customer_id)
group by c.company_name
having avg(o.order_id) > 10

-- 28- Obtener el nombre del producto y su categoría, pero muestre "Discontinued" en lugar
--		del nombre de la categoría si el producto ha sido descontinuado
select p.product_name, case when p.discontinued = 1 then 'Discontinued' else c.category_name end as product_category
from products p
	 left join categories c on (c.category_id = p.category_id)

-- 29- Obtener el nombre del empleado y su título, pero muestre "Gerente de Ventas" en lugar
--		del título si el empleado es un gerente de ventas (Sales Manager)
select e.first_name, e.last_name, case when e.title = 'Sales Manager' then 'Gerente de Ventas' else e.title end as jobtitle
from employees e 
