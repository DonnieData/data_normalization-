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
    meter_post_id ,
    meter_event_type ,
    pay_ammount_id,
    session_start_date ,
    session_end_date,
    session_start_time_id,
    session_end_time_id
    )
VALUES (
    %(transmission_dt)s ,
    (SELECT payment_type_id 
        FROM sf_ticket_trans.dim_payment WHERE payment_type = %(payment)s ) ,
    (SELECT street_block_id 
        FROM sf_ticket_trans.dim_street WHERE street_block = %(street)s ) ,
    (SELECT meter_post_id 
        FROM sf_ticket_trans.dim_meterPost WHERE post_id = %(post)s ) ,
    %(meter_event)s , 
        SELECT pay_ammount_id 
        FROM sf_ticket_trans.dim_grossPayAmmount WHERE pay_ammount = %(paid_amt)s ),
    %(start_date)s,
    %(end_date)s,
    SELECT time_group_id  
        FROM sf_ticket_trans.dim_timeGroup WHERE time_group = %(start_time)s ) ,
     SELECT time_group_id  
        FROM sf_ticket_trans.dim_timeGroup WHERE time_group = %(end_time)s 

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
THEN INSERT INTO sf_ticket_trans.dim_meterPost (post_id) VALUES ( %(postid)s  );
END IF;
END;
$$ 
'''

#insert query for dim_grossPayAmmount
dim_payammount_insert = '''
DO $$
BEGIN 
IF NOT EXISTS (select 1 from sf_ticket_trans.dim_grossPayAmmount where pay_ammount = %(p_ammount)s) 
THEN INSERT INTO sf_ticket_trans.dim_grossPayAmmount (pay_ammount) VALUES ( %(p_ammount)s  );
END IF;
END;
$$ 
'''

#insert query for dim_grossPayAmmount
dim_timeGroup_insert = '''
DO $$
BEGIN 
IF NOT EXISTS (select 1 from sf_ticket_trans.dim_timeGroup where time_group = %(time_g)s) 
THEN INSERT INTO sf_ticket_trans.dim_timeGroup (time_group) VALUES ( %(time_g)s  );
END IF;
END;
$$ 
'''

