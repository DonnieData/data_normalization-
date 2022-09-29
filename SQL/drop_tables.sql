--queries to clear out schema for testing 
DROP TABLE IF EXISTS sf_ticket_trans.fact_transactions CASCADE;
DROP TABLE IF EXISTS sf_ticket_trans.dim_payment CASCADE;
DROP TABLE IF EXISTS sf_ticket_trans.dim_street CASCADE;
DROP TABLE IF EXISTS sf_ticket_trans.dim_meterPost CASCADE;
DROP TABLE IF EXISTS sf_ticket_trans.dim_grossPayAmmount CASCADE;
DROP TABLE IF EXISTS sf_ticket_trans.dim_timeGroup CASCADE;
