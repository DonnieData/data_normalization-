CREATE TABLE sf_ticket_trans.dim_payment ( 
payment_type_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
payment_type TEXT NOT NULL CONSTRAINT unique_payment UNIQUE
);

CREATE TABLE sf_ticket_trans.dim_street( 
street_block_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
street_block TEXT NOT NULL CONSTRAINT unique_street UNIQUE
);

CREATE TABLE sf_ticket_trans.dim_meterPost( 
meter_post_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
post_id TEXTTEXT NOT NULL CONSTRAINT unique_post UNIQUE
);

CREATE TABLE sf_ticket_trans.dim_grossPayAmmount( 
pay_ammount_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
pay_ammount DECIMAL NOT NULL CONSTRAINT unique_paygroup UNIQUE
);
    
CREATE TABLE sf_ticket_trans.dim_timeGroup( 
time_group_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
time_group TIME NOT NULL CONSTRAINT unique_time UNIQUE
);

CREATE TABLE sf_ticket_trans.fact_transactions ( 
transmission_datetime TEXT PRIMARY KEY,
payment_type_id INTEGER REFERENCES sf_ticket_trans.dim_payment(payment_type_id),
street_block_id INTEGER REFERENCES sf_ticket_trans.dim_street(street_block_id),
meter_post_id INTEGER REFERENCES sf_ticket_trans.dim_meterPost(meter_post_id),
meter_event_type TEXT,
pay_ammount_id INTEGER REFERENCES sf_ticket_trans.dim_grossPayAmmount(pay_ammount_id),
session_start_date DATE,
session_end_date DATE,
session_start_time_id INTEGER REFERENCES sf_ticket_trans.dim_timeGroup(time_group_id),
session_end_time_id INTEGER REFERENCES sf_ticket_trans.dim_timeGroup(time_group_id) 
);