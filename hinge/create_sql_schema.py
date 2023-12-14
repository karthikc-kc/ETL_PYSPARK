import sqlite3
from sql_queries import create_table_queries


def create_database():
    """
    - Creates and connects to the default db
    - Returns the connection and cursor info
    """

    # connect to default database
    conn = sqlite3.connect('hinge.db')

    # Create a cursor object to interact with the database
    cur = conn.cursor()
    return cur, conn


# def drop_tables(cur, conn):
#     """
#     Drops each table using the queries in `drop_table_queries` list.
#     """
#     for query in drop_table_queries:
#         cur.execute(query)
#         conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()
    
    # drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
