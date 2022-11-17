drop database if exists project;
create database project;
use project;

-- store metadata of files
drop table if exists file_info; 
create table file_info (
	id int unsigned primary key auto_increment,
    file_name varchar(200) not null,
    file_type enum('directory', 'file') not null
);
    
-- store directory structure
-- use precedure to get file structure
drop table if exists file_structure;
create table file_structure (
    parent int unsigned not null,
    child int unsigned not null,
    primary key (parent, child),
	foreign key (parent) references file_info(id) on delete cascade,
	foreign key (child) references file_info(id) on delete cascade
);

drop procedure if exists mkdir;
delimiter //
create procedure mkdir(
    in new_directory_name varchar(200),
    in parent int)
begin
    declare latest_id int;
	insert into file_info(file_name, file_type) values (new_directory_name, 'directory');

    select max(id) into latest_id from file_info where file_info.file_name = new_directory_name;
    insert into file_structure values (parent, latest_id);

end; //
delimiter ;

drop procedure if exists rm;
delimiter //
create procedure rm(
    in file_id int
#     in max_partition int,
#     in table_name varchar(200)
    )
begin
#     declare counter int default 0;
#     declare partition_table_name varchar(200);
    delete from file_info where id = file_id;
#     drop table if exists table_name;
#     while counter <= max_partition do
#         select concat(table_name, "_", counter) into partition_table_name;
#         drop table if exists partition_table_name;
#         set counter = counter + 1;
#     end while;
end; //
delimiter ;

drop procedure if exists put;
delimiter //
create procedure put(
    in new_file_name varchar(200),
    in parent int)
begin
    declare latest_id int;
	insert into file_info(file_name, file_type) values (new_file_name, 'file');

    select max(id) into latest_id from file_info where file_info.file_name = new_file_name;
    insert into file_structure values (parent, latest_id);

end; //
delimiter ;


insert into file_info values (1, 'root', 'directory');







