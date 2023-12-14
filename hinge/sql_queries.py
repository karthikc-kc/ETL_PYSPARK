
# CREATE TABLES
members_transaction = ("CREATE TABLE IF NOT EXISTS members_transaction (first_name TEXT,last_name TEXT, dob TEXT,company_id INTEGER , last_active TEXT,score INTEGER, member_since	TEXT, state TEXT)")

companies_catalog = ("CREATE TABLE IF NOT EXISTS companies_catalog (id integer primary key, name TEXT)")


create_table_queries = [members_transaction, companies_catalog]
