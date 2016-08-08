CREATE OR REPLACE  PACKAGE DATA_COMPARE_PKG AS
   PROCEDURE COMPARE_TABLE(STARTING_ROWID IN  ROWID,ENDING_ROWID IN  ROWID);
   PROCEDURE START_DATA_COMP(STARTING_ROWID IN  ROWID,ENDING_ROWID IN  ROWID);     
   PROCEDURE CLEAN_JOBS;
   PROCEDURE START_JOBS;
END DATA_COMPARE_PKG;
/

CREATE OR REPLACE  PACKAGE BODY DATA_COMPARE_PKG AS
   --- TABLES INFO
  
    TAB1_NAME VARCHAR2(30) :='A_USERS'; 
    TAB2_NAME VARCHAR2(30) :='B_USERS'; 
    PRIMARY_KEY VARCHAR2(30) :='USERID';
    
   
   
   
   
   --- JOBS RELATED VARS
   TASK_NAME VARCHAR2(40) :='A_B_DATA_COMPARE';
   TOTAL_ROW_NUMBER NUMBER :=0;  
  
------------------------------------------------------------------------------------------------------------------
--    COMPARE_TABLE COMPARE PROCEDURE 
------------------------------------------------------------------------------------------------------------------                                               
   
  PROCEDURE  COMPARE_TABLE(STARTING_ROWID IN  ROWID,
                                                ENDING_ROWID IN  ROWID)
    IS 
       TYPE TAB_CUR_TYPE IS REF CURSOR;
       TAB_CUR   TAB_CUR_TYPE;
       P_REC  P_IAM_PROFILE%ROWTYPE;
       T_REC  T_IAM_PROFILE%ROWTYPE;
       PRIMARY_KEY_VALUE VARCHAR2(250) :=''; 
       SQL_STMT VARCHAR2(1200);   
       V_STATUS VARCHAR2(1000);  
       USERID VARCHAR2(200);  
       ROWNUMB   NUMBER  := 2;
       MISS_MATCH NUMBER  := 0;
       TYPE COL_TYPE IS TABLE OF VARCHAR2(40);
       COL_NAME COL_TYPE;
       COL_DATA_TYPE COL_TYPE;
       NO_DATA_IN_T  BOOLEAN :=FALSE;
       EXCLUDE_COLS VARCHAR2(400) :='''ADDRESS'', ''EMAIL''';
       --EXCLUDE_COLS VARCHAR2(400) :=NULL;
     
    BEGIN
      
        SQL_STMT := 'SELECT COLUMN_NAME,DATA_TYPE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '''|| TAB2_NAME ||'''';
        IF EXCLUDE_COLS IS NOT NULL THEN          
            SQL_STMT :=SQL_STMT || ' AND COLUMN_NAME IN ('|| EXCLUDE_COLS || ')';
        END IF;
        --DBMS_OUTPUT.PUT_LINE ('CMD: ' || SQL_STMT);
        EXECUTE IMMEDIATE SQL_STMT   BULK COLLECT INTO COL_NAME ,COL_DATA_TYPE;
        SQL_STMT := 'SELECT /*+ROWID('||TAB1_NAME||')*/  * FROM '|| TAB1_NAME ||'    WHERE ROWID  BETWEEN :STARTING_ROWID AND :ENDING_ROWID';
        OPEN TAB_CUR FOR SQL_STMT USING STARTING_ROWID,ENDING_ROWID; 
      
       LOOP
         FETCH TAB_CUR INTO P_REC;
         EXIT WHEN TAB_CUR%NOTFOUND;         
         
         SQL_STMT :=  'DECLARE 
                    T_REC  '|| TAB2_NAME ||'%ROWTYPE; 	
                    P_REC  '|| TAB1_NAME ||'%ROWTYPE; 
                  
                 BEGIN     
                      P_REC :=:P_REC;                  
                     SELECT  *   INTO T_REC FROM '|| TAB2_NAME ||' TP WHERE '||PRIMARY_KEY||'=P_REC.'||PRIMARY_KEY||'; 
                      :PRIMARY_KEY_VALUE :=P_REC.'||PRIMARY_KEY||';
                     :T_REC :=T_REC;
               END;'; 
        --DBMS_OUTPUT.PUT_LINE ('CMD: ' || SQL_STMT);
        
         NO_DATA_IN_T :=FALSE;
         BEGIN           
               EXECUTE IMMEDIATE SQL_STMT  USING IN  P_REC,OUT PRIMARY_KEY_VALUE, OUT T_REC; 
               NO_DATA_IN_T :=FALSE;
         EXCEPTION WHEN NO_DATA_FOUND THEN  
           NO_DATA_IN_T :=TRUE;
          
         END;
         
         --EXECUTE IMMEDIATE 'SELECT  *   FROM '|| TAB2_NAME ||' TP WHERE USERID=''' || P_REC.USERID || '''' INTO T_REC;
         IF NO_DATA_IN_T=FALSE THEN
          --DBMS_OUTPUT.PUT_LINE ('COL_NAME.COUNT ' ||COL_NAME.COUNT );
         
             FOR INDX IN 1 .. COL_NAME.COUNT      
             LOOP   
               IF  COL_NAME(INDX) !='LASTUPDATEDTIMESTAMP' THEN
                   SQL_STMT :=  'DECLARE 
                          P_REC  '|| TAB1_NAME ||'%ROWTYPE; 
                          T_REC  '|| TAB2_NAME ||'%ROWTYPE;  
                       BEGIN    P_REC := :P_REC;  T_REC := :T_REC;   
                       SELECT DECODE(P_REC.' || COL_NAME(INDX) || ',T_REC.' || COL_NAME(INDX) || ',0,1) INTO :MISS_MATCH FROM DUAL;                       
                     END;'; 
                    /* this might speed up instead of using dual  above*/
					
					
                   /* SQL_STMT :=  'DECLARE 
                          P_REC  '|| TAB1_NAME ||'%ROWTYPE; 
                          T_REC  '|| TAB2_NAME ||'%ROWTYPE;  
                          MISS_MATCH NUMBER  := 1;
                       BEGIN  
                            P_REC := :P_REC;  T_REC := :T_REC;  
                           IF  '''||COL_DATA_TYPE(INDX) ||''' =''TIMESTAMP(6)'' OR  '''||COL_DATA_TYPE(INDX) ||''' =''DATE'' THEN
                              IF NVL(P_REC.' || COL_NAME(INDX) || ',TO_DATE(''20020101'',''YYYYMMDD''))= NVL(T_REC.' || COL_NAME(INDX) || ',TO_DATE(''20020101'',''YYYYMMDD'')) THEN
                                MISS_MATCH :=0;
                              END IF;
                       END IF;
                       IF  '''||COL_DATA_TYPE(INDX) ||''' =''NUMBER'' OR  '''||COL_DATA_TYPE(INDX) ||''' =''VARCHAR2'' THEN
                           IF NVL(P_REC.' || COL_NAME(INDX) || ',''0'')= NVL(T_REC.' || COL_NAME(INDX) || ',''0'') THEN
                                MISS_MATCH :=0;
                            END IF;
                       END IF;   
                       :MISS_MATCH :=MISS_MATCH;
                      
                     END;';  */
                   --  DBMS_OUTPUT.PUT_LINE ('CMD: ' || SQL_STMT);
                   EXECUTE IMMEDIATE SQL_STMT USING IN  P_REC , IN T_REC , OUT MISS_MATCH;
                      
                    
                    IF MISS_MATCH =1 THEN    
                      IF V_STATUS IS  NULL THEN
                          V_STATUS := COL_NAME(INDX) ;
                        ELSE
                          V_STATUS :=NVL(V_STATUS,' ') || ',' ||COL_NAME(INDX) ;
                       END IF; 
                    END IF; 
                 END IF;     
             END LOOP;       
         
           --DBMS_OUTPUT.PUT_LINE ( ' TAB2_NAME' || TAB1_NAME || 'PRIMARY_KEY_VALUE'  || PRIMARY_KEY_VALUE || 'V_STATUS' || V_STATUS );
            IF V_STATUS IS NOT NULL THEN
                 --DBMS_OUTPUT.PUT_LINE ( 'ERROR FOUND' );
               INSERT INTO REPORT_COMPARE_DATA(TABLE_NAME,PRIMARY_KEY,STATUS )
                VALUES(TAB1_NAME, PRIMARY_KEY_VALUE ,V_STATUS);
            END IF;
         ELSE
             --DBMS_OUTPUT.PUT_LINE ( ' TAB2_NAME' || TAB1_NAME || 'PRIMARY_KEY_VALUE'  || PRIMARY_KEY_VALUE || 'PRIMARY_KEY :' || PRIMARY_KEY );
              IF PRIMARY_KEY_VALUE IS NOT NULL THEN    
                 --DBMS_OUTPUT.PUT_LINE ( 'NO DATA FOUND' );
                 INSERT INTO REPORT_COMPARE_DATA(TABLE_NAME,PRIMARY_KEY,STATUS )
                     VALUES(TAB1_NAME, PRIMARY_KEY_VALUE ,PRIMARY_KEY);
              END IF;
               NO_DATA_IN_T :=FALSE;
         END IF;    
        V_STATUS :='';
       END LOOP;
       CLOSE TAB_CUR;
    END;

------------------------------------------------------------------------------------------------------------------
--    START COMPARE PROCEDURE 
------------------------------------------------------------------------------------------------------------------                                               
   PROCEDURE START_DATA_COMP(STARTING_ROWID IN  ROWID,ENDING_ROWID IN  ROWID)
    IS  
    ERRORS VARCHAR2(100);
    BEGIN     
    --INSERT INTO   REPORT_COMPARE_DATA     ( STATUS )     VALUES   (   ROWIDTOCHAR(starting_rowid) || ' '||  ROWIDTOCHAR(ending_rowid));   
    COMPARE_TABLE(STARTING_ROWID,ENDING_ROWID); 
    EXCEPTION
       WHEN OTHERS  THEN
            ERRORS := 'NO JOBS RUNNING' ||SQLCODE ||  ' - ' || SQLERRM;         
            INSERT INTO   REPORT_COMPARE_DATA VALUES   ( '' ,'', ERRORS); COMMIT;     
             --DBMS_OUTPUT.PUT_LINE(ERRORS);
    END;
------------------------------------------------------------------------------------------------------------------
--    CLEAN PROCEDURE START
------------------------------------------------------------------------------------------------------------------    
    PROCEDURE CLEAN_JOBS 
       IS
     BEGIN       
          DBMS_OUTPUT.PUT_LINE('CHECKING EXITING JOBS');
          BEGIN
            DBMS_OUTPUT.PUT_LINE('STATUS: '|| DBMS_PARALLEL_EXECUTE.TASK_STATUS (TASK_NAME));   
            EXCEPTION
              WHEN OTHERS  THEN 
                  DBMS_OUTPUT.PUT_LINE('NO JOBS RUNNING' ||SQLCODE);
           END;
          BEGIN 
            DBMS_PARALLEL_EXECUTE.STOP_TASK (TASK_NAME); 
            EXCEPTION
              WHEN OTHERS  THEN 
                  DBMS_OUTPUT.PUT_LINE('NO JOBS RUNNING' ||SQLCODE);
           END;
          BEGIN  
            DBMS_PARALLEL_EXECUTE.PURGE_PROCESSED_CHUNKS(TASK_NAME);   
            EXCEPTION
              WHEN OTHERS  THEN 
                  DBMS_OUTPUT.PUT_LINE('NO JOBS RUNNING' ||SQLCODE);
           END;
          BEGIN
            DBMS_PARALLEL_EXECUTE.DROP_TASK (TASK_NAME);  
            EXCEPTION
              WHEN OTHERS  THEN 
                  DBMS_OUTPUT.PUT_LINE('NO JOBS RUNNING' ||SQLCODE);
           END;
    END;
------------------------------------------------------------------------------------------------------------------
--    CLEAN PROCEDURE DONE
------------------------------------------------------------------------------------------------------------------
    PROCEDURE START_JOBS 
       IS
    TIMESTART NUMBER;
    C_UPDATE_STATEMENT CONSTANT VARCHAR2 (200)
      :=  'BEGIN DATA_COMPARE_PKG.START_DATA_COMP(:start_id, :end_id); END;';
    L_ATTEMPTS    PLS_INTEGER := 1; 
    RETRIES_IN  PLS_INTEGER :=1;
    L_TRY      NUMBER;
    L_STATUS   NUMBER;    
      BEGIN
       DBMS_OUTPUT.ENABLE;
       CLEAN_JOBS;
       TIMESTART := DBMS_UTILITY.GET_TIME();  
     
       BEGIN
         DBMS_PARALLEL_EXECUTE.CREATE_TASK (TASK_NAME);       
         DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID (TASK_NAME => TASK_NAME, TABLE_OWNER => USER , TABLE_NAME => TAB1_NAME , BY_ROW => TRUE , CHUNK_SIZE => 800000  );
         DBMS_PARALLEL_EXECUTE.RUN_TASK (TASK_NAME => TASK_NAME , SQL_STMT => C_UPDATE_STATEMENT  , LANGUAGE_FLAG => DBMS_SQL.NATIVE  , PARALLEL_LEVEL =>64  );
       EXCEPTION
              WHEN OTHERS  THEN 
                  DBMS_OUTPUT.PUT_LINE('FAIL TO RUN  JOBS, ERROR CODE:' ||SQLCODE);
                  RETURN;
       END;
    
      L_TRY := 0;
      L_STATUS := DBMS_PARALLEL_EXECUTE.TASK_STATUS(TASK_NAME);
      WHILE(L_TRY < 2 AND L_STATUS != DBMS_PARALLEL_EXECUTE.FINISHED) 
      LOOP
        L_TRY := L_TRY + 1;
        DBMS_PARALLEL_EXECUTE.RESUME_TASK(TASK_NAME);
        L_STATUS := DBMS_PARALLEL_EXECUTE.TASK_STATUS(TASK_NAME);
      END LOOP;
    
      --  DBMS_PARALLEL_EXECUTE.DROP_TASK (TASK_NAME);
        DBMS_OUTPUT.PUT_LINE( 'TRY: ' || L_TRY );
      
        DBMS_OUTPUT.PUT_LINE('TOTAL TIME ' ||   ROUND( (DBMS_UTILITY.GET_TIME-TIMESTART)/100, 2 )  ||' SECONDS' );
        
    END;
   -- INITIALIZATION 
  BEGIN 
      DBMS_OUTPUT.PUT_LINE('DATA_COMPARE_PKG INITIALIZE');
      EXECUTE IMMEDIATE  'SELECT  COUNT(1)  FROM   '|| TAB1_NAME INTO TOTAL_ROW_NUMBER;
      DBMS_OUTPUT.PUT_LINE('TOTAL RECORD : ' || TOTAL_ROW_NUMBER);
  END DATA_COMPARE_PKG;
  /




SET SERVEROUTPUT ON 
BEGIN 
       EXECUTE IMMEDIATE 'GRANT CREATE ANY JOB TO CA';
      
      BEGIN
        DATA_COMPARE_PKG.START_JOBS;                
      END;  
END;
/ 




