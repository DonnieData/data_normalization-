-- schema 
CREATE SCHEMA IF NOT EXISTS sf_ticket_trans;

CREATE TABLE sf_ticket_trans.fact_transactions ( 
transmission_datetime TEXT,
payment_type_id INTEGER,
street_block_id INTEGER,
post_id TEXT,
meter_event_type TEXT,
gross_paid_amt DECIMAL,
session_start_dt TIMESTAMP,
session_end_date TIMESTAMP


CREATE TABLE sf_ticket_trans.dim_payment ( 
payment_type_id INTEGER,
payment_type TEXT 
    
    
CREATE TABLE sf_ticket_trans.dim_street( 
street_block_id INTEGER,
street_block TEXT




-- constriants 

ALTER TABLE sf_ticket_trans.dim_payment
ALTER COLUMN payment_type_id ADD GENERATED ALWAYS AS IDENTITY;
  
ALTER TABLE sf_ticket_trans.dim_street
ALTER COLUMN street_block_id ADD GENERATED ALWAYS AS IDENTITY;
    
ALTER TABLE sf_ticket_trans.fact_transactions
ADD PRIMARY KEY (transmission_datetime),

ADD CONSTRAINT fk_streetblockid FOREIGN KEY (street_block_id) 
    REFERENCES sf_ticket_trans.dim_street (street_block_id);
    
ALTER TABLE sf_ticket_trans.dim_street
ADD PRIMARY KEY (street_block_id),
ADD CONSTRAINT unique_block UNIQUE (street_block)
    
ALTER TABLE sf_ticket_trans.dim_payment
ADD PRIMARY KEY (payment_type_id),
ADD CONSTRAINT unique_payment_type UNIQUE (payment_type)
    

-- trucate table and all forieng referenced tables with casacade and restart identity sequence if any 
TRUNCATE sf_ticket_trans.fact_transactions RESTART IDENTITY CASCADE
TRUNCATE sf_ticket_trans.dim_payment RESTART IDENTITY CASCADE
TRUNCATE sf_ticket_trans.dim_street RESTART IDENTITY CASCADE