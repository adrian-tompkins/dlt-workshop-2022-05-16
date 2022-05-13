------- Setup Table ------
set xact_abort off

begin tran

--cleanup existing
begin try
exec sys.sp_cdc_disable_table  
@source_schema = N'dbo',  
@source_name   = N'dlt_example',  
@capture_instance = N'dbo_dlt_example'  
end try
begin catch
end catch
go

set xact_abort on

drop table if exists [dbo].[dlt_example]

create table [dbo].[dlt_example] (
	Id int primary key not null,
	FirstName nvarchar(255) not null,
	LastName nvarchar(255) not null,
	City nvarchar(50) not null,
	Balance decimal not null
)

exec sys.sp_cdc_enable_table  
@source_schema = N'dbo',  
@source_name   = N'dlt_example', 
@role_name     = NULL,   
@supports_net_changes = 1  

commit

------- Insert some sample data across multiple transactions and commands ------
begin tran
insert into dlt_example (Id, FirstName, LastName, City, Balance)
values
(1, 'Kianna', 'Veum', 'Perth', 100.00),
(2, 'Kevin', 'Lesch', 'Sydney', 50.50),
(3, 'Zack', 'Runofsdottir', 'Darwin', -29.23),
(4, 'Darryl', 'Morisette', 'Perth', 3022.89)
go
insert into dlt_example (Id, FirstName, LastName, City, Balance)
values
(5, 'Clay', 'Mertz', 'Melbourne', 33.33),
(6, 'Leila', 'Wisozk', 'Brisbane', -1.00),
(7, 'Isabel', 'Toy', 'Sydney', 0.00),
(8, 'Deshawn', 'Stracke', 'Canberra ', 299.99)
commit

------- Make some updates ------
begin tran
update dlt_example set City = 'Melbourne', Balance = 20 where id = 1
update dlt_example set Balance = 55.5 where id = 2
update dlt_example set Balance = -89.39 where  id = 3
update dlt_example set Balance = 30 where id = 1
go
update dlt_example set Balance = 65.5 where id = 2
commit
begin tran
update dlt_example set City = 'Perth', Balance = -99.39 where  id = 3
commit

------- Delete some records, insert some new records ------
begin tran
delete from dlt_example where City = 'Sydney'
insert into dlt_example (Id, FirstName, LastName, City, Balance)
values
(9, 'Melissa', 'Wuckert', 'Darwin', 1.00),
(10, 'Jarrod', 'Windler', 'Hobart', 8.00)
commit