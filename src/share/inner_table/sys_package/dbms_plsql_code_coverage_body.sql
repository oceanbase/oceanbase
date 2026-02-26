#package_name: dbms_plsql_code_coverage
#author: wangbai.wx

CREATE OR REPLACE PACKAGE BODY dbms_plsql_code_coverage IS
   RUN_STATUS VARCHAR2(64) := NULL;
   USER_SCHEMA VARCHAR2(128) := NULL;
   RUN_COMMENT VARCHAR2(4000) := NULL;
   TYPE BLOCKNUM_ARRAY     IS TABLE OF NUMBER;
   TYPE LINE_ARRAY         IS TABLE OF NUMBER;
   TYPE COLUMN_ARRAY       IS TABLE OF NUMBER;
   TYPE NOT_FEASIBLE_ARRAY IS TABLE OF NUMBER;
   TYPE SUBPROGRAM_ARRAY   IS TABLE OF VARCHAR2(32767);
   SUCCESS BINARY_INTEGER := 0;

   FUNCTION START_COVERAGE_INNER(RUN_COMMENT IN VARCHAR2, RUNID OUT POSITIVE)
   RETURN BINARY_INTEGER;
   PRAGMA INTERFACE(c, DBMS_PLSQL_CODE_COVERAGE_START_CODE_COVERAGE);

   FUNCTION STOP_COVERAGE_INNER RETURN BINARY_INTEGER;
   PRAGMA INTERFACE(c, DBMS_PLSQL_CODE_COVERAGE_STOP_CODE_COVERAGE);

   PROCEDURE CREATE_COVERAGE_TABLES(FORCE_IT IN BOOLEAN := FALSE) IS
     PRAGMA AUTONOMOUS_TRANSACTION;
     TABLE_COUNT NUMBER := 0;
     V_SQL VARCHAR2(4000);
     V_TABLE_NAMES VARCHAR2(200);
     PROCEDURE DROP_TABLE(T IN VARCHAR2) IS
       PRAGMA AUTONOMOUS_TRANSACTION;
       TABLE_DOESNT_EXIST EXCEPTION;
       PRAGMA EXCEPTION_INIT(TABLE_DOESNT_EXIST, -5019);
     BEGIN
       EXECUTE IMMEDIATE 'drop table '|| T;
       EXCEPTION
         WHEN TABLE_DOESNT_EXIST THEN
           NULL;
     END DROP_TABLE;
   BEGIN
     IF (FORCE_IT) THEN
       DROP_TABLE('DBMSPCC_Blocks');
       DROP_TABLE('DBMSPCC_Units');
       DROP_TABLE('DBMSPCC_Runs');
     ELSE
       V_TABLE_NAMES := '''DBMSPCC_RUNS'', ''DBMSPCC_UNITS'', ''DBMSPCC_BLOCKS''';
       V_SQL := 'SELECT COUNT(*) FROM SYS.USER_OBJECTS WHERE ' ||
                'OBJECT_NAME IN (' || V_TABLE_NAMES || ') AND OBJECT_TYPE = ''TABLE''';
       EXECUTE IMMEDIATE V_SQL INTO TABLE_COUNT;
       IF TABLE_COUNT > 0 THEN
         RAISE COVERAGE_ERROR;
       END IF;
     END IF;
     EXECUTE IMMEDIATE
       'Create Table DBMSPCC_Runs '                   ||
       '('                                            ||
       '  Run_ID         integer,  '                  ||
       '  Run_Comment    varchar2(4000), '            ||
       '  Run_Owner      varchar2(128) constraint'    ||
       '    DBMSPCC_Runs_Run_Owner_NN      not null,' ||
       '  Run_Timestamp  date constraint '            ||
       '    DBMSPCC_Runs_Run_Timestamp_NN  not null,' ||
       '  constraint DBMSPCC_Runs_PK primary '        ||
       '      key(Run_ID)'                            ||
       ')';
     EXECUTE IMMEDIATE
       'Create Table DBMSPCC_Units'                     ||
       '('                                              ||
       '  Run_ID         integer,'                      ||
       '  Object_ID      integer,'                      ||
       '  Owner          varchar2(128)  constraint '    ||
       '      DBMSPCC_Units_Owner_NN         not null,' ||
       '  Name           varchar2(128)  constraint '    ||
       '      DBMSPCC_Units_Name_NN          not null,' ||
       '  Type           varchar2(12)   constraint '    ||
       '      DBMSPCC_Units_Type_NN          not null,' ||
       '  Last_DDL_Time  date           constraint '    ||
       '      DBMSPCC_Units_Last_DDL_Time_NN not null,' ||
       '  constraint DBMSPCC_Units_PK primary '         ||
       '      key(Run_ID, Object_ID), '                 ||
       '  constraint DBMSPCC_Units_FK foreign '         ||
       '      key(Run_ID) references          '         ||
       '      DBMSPCC_Runs(Run_ID) on delete cascade '  ||
       ')';
     EXECUTE IMMEDIATE
       'Create Table DBMSPCC_Blocks'                  ||
       '('                                            ||
       '  Run_ID         integer,'                    ||
       '  Object_ID      integer,'                    ||
       '  Block          integer,'                    ||
       '  Line           integer  constraint '        ||
       '    DBMSPCC_Blocks_Line_NN not null, '        ||
       '  Col            integer  constraint '        ||
       '    DBMSPCC_Blocks_Col_NN not null,  '        ||
       '  Covered    number(1,0)  constraint '        ||
       '    DBMSPCC_Blocks_Covered_NN not null, '     ||
       '  Not_Feasible   number(1,0) constraint '     ||
       '    DBMSPCC_Blocks_Not_Feasible_NN not null,' ||
       '  constraint DBMSPCC_Blocks_PK primary '      ||
       '    key(Run_ID, Object_ID, Block), '          ||
       '  constraint DBMSPCC_Blocks_FK foreign '      ||
       '    key(Run_ID, Object_ID) references '       ||
       '    DBMSPCC_Units(Run_ID, Object_ID) on '     ||
       '    delete cascade, '                         ||
       '  constraint DBMSPCC_Blocks_Block_Ck check '  ||
       '    (Block >= 0), '                           ||
       '  constraint DBMSPCC_Blocks_Line_Ck '         ||
       '    check (Line >= 0), '                      ||
       '  constraint DBMSPCC_Blocks_Col_Ck  '         ||
       '    check (Col >= 0),            '            ||
       '  constraint DBMSPCC_Blocks_Covered_Ck '      ||
       '    check (Covered in (0, 1)),          '     ||
       '  constraint DBMSPCC_Blocks_Not_Feasible_Ck ' ||
       '    check (Not_Feasible in (0, 1))          ' ||
       ')';
   END;

   FUNCTION START_COVERAGE(RUN_COMMENT IN VARCHAR2) RETURN NUMBER
   IS
     FUNC_RET BINARY_INTEGER;
     RUNID    NUMBER;
   BEGIN
     IF (RUN_STATUS IS NULL) THEN
       FUNC_RET := START_COVERAGE_INNER(RUN_COMMENT, RUNID);
       IF (FUNC_RET <> SUCCESS) THEN
         RAISE COVERAGE_ERROR;
       END IF;
       RUN_STATUS := RUNID;
       RETURN RUNID;
      ELSE
       RUNID := RUN_STATUS;
       RETURN RUNID;
     END IF;
   END START_COVERAGE;

   PROCEDURE STOP_COVERAGE IS
     FUNC_RET BINARY_INTEGER;
   BEGIN
    IF (RUN_STATUS IS NOT NULL) THEN
      FUNC_RET := STOP_COVERAGE_INNER;
      RUN_STATUS := NULL;
      IF (FUNC_RET <> SUCCESS) THEN
        RAISE COVERAGE_ERROR;
      END IF;
    END IF;
   END STOP_COVERAGE;

 END DBMS_PLSQL_CODE_COVERAGE;
 //