
-- validation script for email
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

-- functional test for the validation email
SELECT 'a@acom' as email, cntl.fn_is_valid_email(email) as is_email_valid ;


-- we need stage to upload test file, vscode can create one without sql but it would be temporary (dropped once session ends)
create or replace stage landing_stage
file_format = (type = 'CSV' field_delimiter = ',' SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- do we have file uploaded?
LIST @LANDING.landing_stage;

-- one way to load for demonstration purposes only (not recommended for prod)
CREATE OR REPLACE TABLE NICE.EMAIL_TARGET
AS
SELECT t.$1 as email, t.$2 as user_name, t.$3 as user_address, t.$4 as is_active, cntl.fn_is_valid_email(email) as is_valid_email
FROM @LANDING.landing_stage/email_sample_list.csv t
;


-- see if it worked and start emailing
SELECT *
FROM nice.email_target
WHERE is_valid_email
;


;
SELECT *
FROM ADMIN.ACCOUNT_USAGE.ACCESS_HISTORY
LIMIT 100
;

-- standard way to load
-- DROP TABLE EMAIL_TARGET;

create or replace TABLE NICE.EMAIL_TARGET (
	EMAIL VARCHAR(16777216),
	USER_NAME VARCHAR(16777216),
	USER_ADDRESS VARCHAR(16777216),
	IS_ACTIVE VARCHAR(16777216)
);

copy into NICE.EMAIL_TARGET from @LANDING.landing_stage/email_sample_list.csv;



    