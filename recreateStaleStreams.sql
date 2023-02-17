-- DDL query for stale STREAMS to Recreate them 

 show streams in database dbName;
 -- use result scan Id from above query
 select 'create or replace stream '|| "schema_name"|| '.'|| "name" || ' on ' || "source_type" || ' ' ||"table_name" ||';' from table(RESULT_SCAN('01aaaacv-7676-b91b-7676-67f507fc247a')) where "name" in 
    (<comma separated streams Name > );
 
