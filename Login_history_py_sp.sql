create or replace procedure gh.cntl.login_history(sf_user string)
    returns Table()
    language python
    runtime_version = 3.8
    packages =('snowflake-snowpark-python')
    handler = 'main' 
    comment = 'Check Login History of a Snowflake User'
    EXECUTE AS CALLER
    as 
    $$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session, sf_user): 
    # Your code goes here, inside the "main" handler.
    # sf_user = 'ghillam'
    sql2run = f"""select EVENT_TIMESTAMP,
    EVENT_TYPE||' as '||USER_NAME||' from IP '||CLIENT_IP as User_IP,
    REPORTED_CLIENT_TYPE||' - '||REPORTED_CLIENT_VERSION||' - '||FIRST_AUTHENTICATION_FACTOR as Client_ver_auth,
    to_varchar(IS_SUCCESS)||' - '||ifnull(to_varchar(ERROR_CODE),'')||' - '||ifnull(ERROR_MESSAGE,'') as login_result
    from table(information_schema.login_history_by_user('{sf_user}', result_limit=>100))
    order by event_timestamp desc"""
    res = run_sync_sql(session, sql2run, return_type=2)

    # Return value will appear in the Results tab.
    return res

def run_sync_sql(session, sql2run, return_type=1):
    """
    Executes a Snowflake SQL query synchronously and returns the results.
    
    Parameters:
    session (snowpark.Session): The Snowflake session to use for the query.
    sql2run (str): The SQL query to run.
    return_type (int): Determines the format of the returned data. 
                       1: List, 2: Snowflake Dataframe, 3: Pandas Dataframe.
                       
    Returns:
    Depends on return_type: a list, a Snowflake Dataframe, or a Pandas Dataframe.
    """
    try:
        if return_type == 1: # Output a List
            return session.sql(sql2run).collect()
        elif return_type == 2: # Output Snowflake Dataframe
            return session.sql(sql2run)
        elif return_type == 3: # Output a Pandas Dataframe
            return session.sql(sql2run).to_pandas()
        else: return None
    except ex.SnowparkSQLException as e:
        print(f'Snowflake query error was raised:\nQuery with issues:\n\n{sql2run.lstrip()}\n')
        print(f'The query failed with an error {e.error_code} {e.message}\n\tSee Query_ID: {e.sfqid} for more details\n')
$$;

call gh.cntl.login_history('jpranckevicius');
