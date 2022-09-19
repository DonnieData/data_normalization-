#insrt quert for fact table
fact_tran_insert ='''
DO $$
BEGIN 
IF NOT EXISTS 
    (select 1 from sf_ticket_trans.fact_transactions 
        where transmission_datetime = %(transmission_dt)s) 
THEN INSERT INTO sf_ticket_trans.fact_transactions (
    transmission_datetime ,
    payment_type_id,
    street_block_id ,
    post_id ,
    meter_event_type ,
    gross_paid_amt ,
    session_start_dt ,
    session_end_date 
    )
VALUES (
    %(transmission_dt)s ,
    (SELECT payment_type_id 
        FROM sf_ticket_trans.dim_payment WHERE payment_type = %(payment)s ) ,
    (SELECT street_block_id 
        FROM sf_ticket_trans.dim_street WHERE street_block = %(street)s ) ,
    %(post)s ,
    %(meter_event)s ,
    %(paid_amt)s ,
    %(sessionstart)s ,
    %(sessionend)s 
    );
END IF;
END;
$$ 
    '''
#insert query for dim_payment
dim_paymeny_insert = '''
DO $$
BEGIN 
IF NOT EXISTS (select 1 from sf_ticket_trans.dim_payment where payment_type = %(payment)s) 
THEN INSERT INTO sf_ticket_trans.dim_payment (payment_type) VALUES (%(payment)s);
END IF;
END;
$$ 
'''

#insert query for dim_street
dim_street_insert = '''
DO $$
BEGIN 
IF NOT EXISTS (select 1 from sf_ticket_trans.dim_street where street_block = %(street)s) 
THEN INSERT INTO sf_ticket_trans.dim_street (street_block) VALUES ( %(street)s  );
END IF;
END;
$$ 
'''

#insert query for dim_meterPost
dim_meter_insert =  '''
DO $$
BEGIN 
IF NOT EXISTS (select 1 from sf_ticket_trans.dim_meterPost where post_id = %(postid)s) 
THEN INSERT INTO sf_ticket_trans.dim_meterPost (street_block) VALUES ( %(postid)s  );
END IF;
END;
$$ 
'''

