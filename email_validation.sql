
SELECT 'a@acom' as email, cntl.fn_is_valid_email(email) as is_email_valid ;

create database ajp;
create schema cntl;
create schema landing;
create schema nice;

create or replace function ajp.cntl.fn_is_valid_email(email string)
    returns boolean
    language python
    runtime_version = 3.8
    packages =('email_validator')
    handler = 'isValid' 
    comment = 'Basic email checker whenever valid and/or deliverable'
as $$
from email_validator import validate_email

def isValid(email):
    try:
        validate_email(email, check_deliverability=False)
        return True
    except:
        return False
$$;

create or replace stage landing_stage
file_format = (type = 'CSV' field_delimiter = ',' SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');

LIST @LANDING.landing_stage


        

;
CREATE OR REPLACE TABLE NICE.EMAIL_TARGET
AS
SELECT t.$1 as email, t.$2 as user_name, t.$3 as user_address, t.$4 as is_active, cntl.fn_is_valid_email(email) as is_valid_email
FROM @LANDING.landing_stage/email_sample_list.csv t
;



SELECT *
FROM nice.email_target
WHERE is_valid_email
;


;
SELECT *
FROM ADMIN.ACCOUNT_USAGE.ACCESS_HISTORY
LIMIT 100
;

-- DROP TABLE EMAIL_TARGET;

create or replace TABLE NICE.EMAIL_TARGET (
	EMAIL VARCHAR(16777216),
	USER_NAME VARCHAR(16777216),
	USER_ADDRESS VARCHAR(16777216),
	IS_ACTIVE VARCHAR(16777216)
);

copy into NICE.EMAIL_TARGET from @LANDING.landing_stage/email_sample_list.csv;



    