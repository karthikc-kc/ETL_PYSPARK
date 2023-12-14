Some Observation and missed out features as future enhancements. 
# code refactor
1. Modularize the source sync components from the base code. Example Ingestion, Transformation and Loading.
2. Introduce global configs for DB connections and introduce utilities - Example Date formats standardization. 
3. Introduce data validation as a separate module, since Date formats can vary from supplier to supplier.

# Data validation and ETL Handling. 
1. Introduce the incremental load and handle upsert mechanism for returning transaction. 
2. Application returns the record which is having some issues, example data Anamolies like DOB greater 
than the club joining date, this can be extended to other validation checks. 
3. Data count checks can be introduced as part of ETL - load. 
4. Efficient handling of DB connections, ideally built for batch loads. Need to be cautions of number of 
connection created while using along with distributed framework like spark. 
5. Introduce intermediate staging (temp tables) layer in the DBMS to handle the incremental loads (large batches), the Merge 
operations can be leveraged instead of updating row by row. 
Example from Distributed app pipeline -->create_temp_tables --> merge operation into target tables. 
6. Handle duplication from the incoming dataset.  
7. Data lineage needs to be included - currently file name has to be introduced for every transaction. 
8. Insert to sqlite3 tables ingestion needed refinement. 

# Schema Improvement.

1. Introduce the helper columns like transaction_created_at, updated_at, updated_by for the DBMS tables. 
2. Introduce the primary key - Generated key based on clustered columns (unique keys)
3. Introduce the Join of the company.csv as it is inside the DBMS as catalog table, this can act like a 
global referential table. 
4. Introduce Two letter state code as global reference tables.
5. Effective handling of Error records - this can be pushed into the DBMS for business verification.
6. Post this, can introduce OLAP for business reporting and metrics.
