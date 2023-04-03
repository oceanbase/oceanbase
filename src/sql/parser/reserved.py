#!/usr/bin/env python3
# description:
class SqlKeywords:
    reserved_92 = '''
         ABSOLUTE  ACTION  ADD  ALL  ALLOCATE  ALTER  AND  ANY  ARE
          AS  ASC  ASSERTION  AT  AUTHORIZATION  AVG
          BEGIN  BETWEEN  BIT  BIT_LENGTH  BOTH  BY
          CASCADE  CASCADED  CASE  CAST  CATALOG  CHAR  CHARACTER  CHARACTER_LENGTH
          CHAR_LENGTH  CHECK  CLOSE  COALESCE  COLLATE  COLLATION  COLUMN  COMMIT
          CONNECT  CONNECTION  CONSTRAINT  CONSTRAINTS  CONTINUE  CONVERT  CORRESPONDING
          CREATE  CROSS  CURRENT  CURRENT_DATE  CURRENT_TIME  CURRENT_TIMESTAMP  CURRENT_USER  CURSOR
          DATABASE DATE  DAY  DEALLOCATE  DEC  DECIMAL  DECLARE  DEFAULT
          DEFERRABLE  DEFERRED  DELETE  DESC  DESCRIBE  DESCRIPTOR  DIAGNOSTICS
          DISCONNECT  DISTINCT  DOMAIN  DOUBLE  DROP
          ELSE  END  END-EXEC  ESCAPE  EXCEPT  EXCEPTION  EXEC  EXECUTE  EXISTS  EXTERNAL  EXTRACT
          FALSE  FETCH  FIRST  FLOAT  FOR  FOREIGN  FOUND  FROM  FULL
          GET  GLOBAL  GO  GOTO  GRANT  GROUP
          HAVING  HOUR
          IDENTITY  IMMEDIATE  IN  INDICATOR  INITIALLY  INNER  INPUT  INSENSITIVE
          INSERT  INT  INTEGER  INTERSECT  INTERVAL  INTO  IS  ISOLATION
          JOIN
          KEY
          LANGUAGE  LAST  LEADING  LEFT  LEVEL  LIKE  LOCAL  LOWER
          MATCH  MAX  MIN  MINUTE  MODULE  MONTH
          NAMES  NATIONAL  NATURAL  NCHAR  NEXT  NO  NOT  NULL  NULLIF  NUMERIC
          OCTET_LENGTH  OF  ON  ONLY  OPEN  OPTION  OR  ORDER  OUTER  OUTPUT  OVERLAPS
          PAD  PARTIAL  POSITION  PRECISION  PREPARE  PRESERVE  PRIMARY  PRIOR  PRIVILEGES  PROCEDURE  PUBLIC
          READ  REAL  REFERENCES  REGION RELATIVE  RESTRICT  REVOKE  RIGHT  ROLLBACK  ROWS
          SCHEMA  SCROLL  SECOND  SECTION  SELECT  SESSION  SESSION_USER  SET
          SIZE  SMALLINT  SOME  SPACE  SQL  SQLCODE  SQLERROR  SQLSTATE  SUBSTRING  SUM  SYSTEM_USER
          TABLE  TEMPORARY  THEN  TIME  TIMESTAMP  TIMEZONE_HOUR  TIMEZONE_MINUTE
          TO  TRAILING  TRANSACTION  TRANSLATE  TRANSLATION  TRIM  TRUE
          UNION  UNIQUE  UNKNOWN  UPDATE  UPPER  USAGE  USER  USING
          VALUE  VALUES  VARCHAR  VARYING  VIEW
          WHEN  WHENEVER  WHERE  WITH  WORK  WRITE
          YEAR
          ZONE'''
    non_reserved_92 = '''
         ADA
          C  CATALOG_NAME  CHARACTER_SET_CATALOG  CHARACTER_SET_NAME  CHARACTER_SET_SCHEMA
          CLASS_ORIGIN  COBOL  COLLATION_CATALOG  COLLATION_NAME  COLLATION_SCHEMA
          COLUMN_NAME  COMMAND_FUNCTION  COMMITTED  CONDITION_NUMBER  CONNECTION_NAME
          CONSTRAINT_CATALOG  CONSTRAINT_NAME  CONSTRAINT_SCHEMA  CURSOR_NAME
          DATA  DATETIME_INTERVAL_CODE  DATETIME_INTERVAL_PRECISION  DYNAMIC_FUNCTION
          FORTRAN
          LENGTH
          MESSAGE_LENGTH  MESSAGE_OCTET_LENGTH  MESSAGE_TEXT  MORE  MUMPS
          NAME  NULLABLE  NUMBER
          PASCAL  PLI
          REPEATABLE  RETURNED_LENGTH  RETURNED_OCTET_LENGTH  RETURNED_SQLSTATE  ROW_COUNT
          SCALE  SCHEMA_NAME  SERIALIZABLE  SERVER_NAME  SUBCLASS_ORIGIN
          TABLE_NAME  TYPE
          UNCOMMITTED  UNNAMED'''
    reserved_99 = '''
         ABSOLUTE  ACTION  ADD  AFTER  ALL  ALLOCATE  ALTER  AND  ANY  ARE
          ARRAY  AS  ASC  ASSERTION  AT  AUTHORIZATION
          BEFORE  BEGIN  BETWEEN  BINARY  BIT  BLOB  BOOLEAN  BOTH
          BREADTH  BY
          CALL  CASCADE  CASCADED  CASE  CAST  CATALOG  CHAR  CHARACTER
          CHECK  CLOB  CLOSE  COLLATE  COLLATION  COLUMN  COMMIT
          CONDITION  CONNECT  CONNECTION  CONSTRAINT  CONSTRAINTS
          CONSTRUCTOR  CONTINUE  CORRESPONDING  CREATE  CROSS  CUBE
          CURRENT  CURRENT_DATE  CURRENT_DEFAULT_TRANSFORM_GROUP
          CURRENT_TRANSFORM_GROUP_FOR_TYPE  CURRENT_PATH  CURRENT_ROLE
          CURRENT_TIME  CURRENT_TIMESTAMP  CURRENT_USER  CURSOR  CYCLE
          DATA  DATE  DAY  DEALLOCATE  DEC  DECIMAL  DECLARE  DEFAULT
          DEFERRABLE  DEFERRED  DELETE  DEPTH  DEREF  DESC
          DESCRIBE  DESCRIPTOR  DETERMINISTIC
          DIAGNOSTICS  DISCONNECT  DISTINCT  DO  DOMAIN  DOUBLE
          DROP  DYNAMIC
          EACH  ELSE  ELSEIF  END  END-EXEC  EQUALS  ESCAPE  EXCEPT
          EXCEPTION  EXEC  EXECUTE  EXISTS  EXIT  EXTERNAL
          FALSE  FETCH  FIRST  FLOAT  FOR  FOREIGN  FOUND  FROM  FREE
          FULL  FUNCTION
          GENERAL  GET  GLOBAL  GO  GOTO  GRANT  GROUP  GROUPING
          HANDLE  HAVING  HOLD  HOUR
          IDENTITY  IF  IMMEDIATE  IN  INDICATOR
          INITIALLY  INNER  INOUT  INPUT  INSERT  INT  INTEGER
          INTERSECT  INTERVAL  INTO  IS  ISOLATION
          JOIN
          KEY
          LANGUAGE  LARGE  LAST  LATERAL  LEADING  LEAVE  LEFT
          LEVEL  LIKE  LOCAL  LOCALTIME  LOCALTIMESTAMP  LOCATOR  LOOP
          MAP  MATCH  METHOD  MINUTE  MODIFIES  MODULE  MONTH
          NAMES  NATIONAL  NATURAL  NCHAR  NCLOB  NESTING  NEW  NEXT
          NO  NONE  NOT  NULL  NUMERIC
          OBJECT  OF  OLD  ON  ONLY  OPEN  OPTION
          OR  ORDER  ORDINALITY  OUT  OUTER  OUTPUT  OVERLAPS
          PAD  PARAMETER  PARTIAL  PATH  PRECISION
          PREPARE  PRESERVE  PRIMARY  PRIOR  PRIVILEGES  PROCEDURE  PUBLIC
          READ  READS  REAL  RECURSIVE  REDO  REF  REFERENCES  REFERENCING
          REGION  RELATIVE  RELEASE  REPEAT  RESIGNAL  RESTRICT  RESULT  RETURN
          RETURNS  REVOKE  RIGHT  ROLE  ROLLBACK  ROLLUP  ROUTINE
          ROW  ROWS
          SAVEPOINT  SCHEMA  SCROLL  SEARCH  SECOND  SECTION  SELECT
          SESSION  SESSION_USER  SET  SETS  SIGNAL  SIMILAR  SIZE
          SMALLINT  SOME  SPACE  SPECIFIC  SPECIFICTYPE  SQL  SQLEXCEPTION
          SQLSTATE  SQLWARNING  START  STATE  STATIC  SYSTEM_USER
          TABLE  TEMPORARY  THEN  TIME  TIMESTAMP
          TIMEZONE_HOUR  TIMEZONE_MINUTE  TO  TRAILING  TRANSACTION
          TRANSLATION  TREAT  TRIGGER  TRUE
          UNDER  UNDO  UNION  UNIQUE  UNKNOWN  UNNEST  UNTIL  UPDATE
          USAGE  USER  USING
          VALUE  VALUES  VARCHAR  VARYING  VIEW
          WHEN  WHENEVER  WHERE  WHILE  WITH  WITHOUT  WORK  WRITE
          YEAR
          ZONE'''
    non_reserved_99 = '''
         ABS  ADA  ADMIN  ASENSITIVE  ASSIGNMENT  ASYMMETRIC  ATOMIC
          ATTRIBUTE  AVG
          BIT_LENGTH
          C  CALLED  CARDINALITY  CATALOG_NAME  CHAIN  CHAR_LENGTH
          CHARACTERISTICS  CHARACTER_LENGTH  CHARACTER_SET_CATALOG
          CHARACTER_SET_NAME  CHARACTER_SET_SCHEMA  CHECKED  CLASS_ORIGIN
          COALESCE  COBOL  COLLATION_CATALOG  COLLATION_NAME  COLLATION_SCHEMA
          COLUMN_NAME  COMMAND_FUNCTION  COMMAND_FUNCTION_CODE  COMMITTED
          CONDITION_IDENTIFIER  CONDITION_NUMBER  CONNECTION_NAME
          CONSTRAINT_CATALOG  CONSTRAINT_NAME  CONSTRAINT_SCHEMA  CONTAINS
          CONVERT  COUNT  CURSOR_NAME
          DATETIME_INTERVAL_CODE  DATETIME_INTERVAL_PRECISION  DEFINED
          DEFINER  DEGREE  DERIVED  DISPATCH
          EVERY  EXTRACT
          FINAL  FORTRAN
          G  GENERATED  GRANTED
          HIERARCHY
          IMPLEMENTATION  INSENSITIVE  INSTANCE  INSTANTIABLE  INVOKER
          K  KEY_MEMBER  KEY_TYPE
          LENGTH  LOWER
          M  MAX  MIN  MESSAGE_LENGTH  MESSAGE_OCTET_LENGTH  MESSAGE_TEXT
          MOD  MORE  MUMPS
          NAME  NULLABLE  NUMBER  NULLIF
          OCTET_LENGTH  ORDERING  OPTIONS  OVERLAY  OVERRIDING
          PASCAL  PARAMETER_MODE  PARAMETER_NAME
          PARAMETER_ORDINAL_POSITION  PARAMETER_SPECIFIC_CATALOG
          PARAMETER_SPECIFIC_NAME  PARAMETER_SPECIFIC_SCHEMA  PLI  POSITION
          REPEATABLE  RETURNED_CARDINALITY  RETURNED_LENGTH
          RETURNED_OCTET_LENGTH  RETURNED_SQLSTATE  ROUTINE_CATALOG
          ROUTINE_NAME  ROUTINE_SCHEMA  ROW_COUNT
          SCALE  SCHEMA_NAME  SCOPE  SECURITY  SELF  SENSITIVE  SERIALIZABLE
          SERVER_NAME  SIMPLE  SOURCE  SPECIFIC_NAME  STATEMENT  STRUCTURE
          STYLE  SUBCLASS_ORIGIN  SUBSTRING  SUM  SYMMETRIC  SYSTEM
          TABLE_NAME  TOP_LEVEL_COUNT  TRANSACTIONS_COMMITTED
          TRANSACTIONS_ROLLED_BACK  TRANSACTION_ACTIVE  TRANSFORM
          TRANSFORMS  TRANSLATE  TRIGGER_CATALOG  TRIGGER_SCHEMA
          TRIGGER_NAME  TRIM  TYPE
          UNCOMMITTED  UNNAMED  UPPER '''
    non_reserved_03 = '''
         A
          ABS
          ABSOLUTE
          ACTION
          ADA
          ADMIN
          AFTER
          ALWAYS
          ASC
          ASSERTION
          ASSIGNMENT
          ATTRIBUTE
          ATTRIBUTES
          AVG
          BEFORE
          BERNOULLI
          BREADTH
          C
          CARDINALITY
          CASCADE
          CATALOG
          CATALOG_NAME
          CEIL
          CEILING
          CHAIN
          CHARACTERISTICS
          CHARACTERS
          CHARACTER_LENGTH
          CHARACTER_SET_CATALOG
          CHARACTER_SET_NAME
          CHARACTER_SET_SCHEMA
          CHAR_LENGTH
          CHECKED
          CLASS_ORIGIN
          COALESCE
          COBOL
          CODE_UNITS
          COLLATION
          COLLATION_CATALOG
          COLLATION_NAME
          COLLATION_SCHEMA
          COLLECT
          COLUMN_NAME
          COMMAND_FUNCTION
          COMMAND_FUNCTION_CODE
          COMMITTED
          CONDITION
          CONDITION_NUMBER
          CONNECTION_NAME
          CONSTRAINTS
          CONSTRAINT_CATALOG
          CONSTRAINT_NAME
          CONSTRAINT_SCHEMA
          CONSTRUCTORS
          CONTAINS
          CONVERT
          CORR
          COUNT
          COVAR_POP
          COVAR_SAMP
          CUME_DIST
          CURRENT_COLLATION
          CURSOR_NAME
          DATA
          DATETIME_INTERVAL_CODE
          DATETIME_INTERVAL_PRECISION
          DEFAULTS
          DEFERRABLE
          DEFERRED
          DEFINED
          DEFINER
          DEGREE
          DENSE_RANK
          DEPTH
          DERIVED
          DESC
          DESCRIPTOR
          DIAGNOSTICS
          DISPATCH
          DOMAIN
          DYNAMIC_FUNCTION
          DYNAMIC_FUNCTION_CODE
          EQUALS
          EVERY
          EXCEPTION
          EXCLUDE
          EXCLUDING
          EXP
          EXTRACT
          FINAL
          FIRST
          FIRST_VALUE
          FLOOR
          FOLLOWING
          FORTRAN
          FOUND
          FUSION
          G
          GENERAL
          GO
          GOTO
          GRANTED
          HIERARCHY
          IMPLEMENTATION
          INCLUDING
          INCREMENT
          INITIALLY
          INSTANCE
          INSTANTIABLE
          INTERSECTION
          INVOKER
          ISOLATION
          K
          KEY
          KEY_MEMBER
          KEY_TYPE
          LAST
          LAST_VALUE
          LENGTH
          LEVEL
          LN
          LOCATOR
          LOWER
          M
          MAP
          MATCHED
          MAX
          MAXVALUE
          MESSAGE_LENGTH
          MESSAGE_OCTET_LENGTH
          MESSAGE_TEXT
          MIN
          MINVALUE
          MOD
          MORE
          MUMPS
          NAME
          NAMES
          NESTING
          NEXT
          NORMALIZE
          NORMALIZED
          NULLABLE
          NULLIF
          NULLS
          NUMBER
          OBJECT
          OCTETS
          OCTET_LENGTH
          OPTION
          OPTIONS
          ORDERING
          ORDINALITY
          OTHERS
          OVERLAY
          OVERRIDING
          PAD
          PARAMETER_MODE
          PARAMETER_NAME
          PARAMETER_ORDINAL_POSITION
          PARAMETER_SPECIFIC_CATALOG
          PARAMETER_SPECIFIC_NAME
          PARAMETER_SPECIFIC_SCHEMA
          PARTIAL
          PASCAL
          PATH
          PERCENTILE_CONT
          PERCENTILE_DISC
          PERCENT_RANK
          PLACING
          PLI
          POSITION
          POWER
          PRECEDING
          PRESERVE
          PRIOR
          PRIVILEGES
          PUBLIC
          RANK
          READ
          REGION
          RELATIVE
          REPEATABLE
          RESTART
          RETURNED_CARDINALITY
          RETURNED_LENGTH
          RETURNED_OCTET_LENGTH
          RETURNED_SQLSTATE
          ROLE
          ROUTINE
          ROUTINE_CATALOG
          ROUTINE_NAME
          ROUTINE_SCHEMA
          ROW_COUNT
          ROW_NUMBER
          SCALE
          SCHEMA
          SCHEMA_NAME
          SCOPE_CATALOG
          SCOPE_NAME
          SCOPE_SCHEMA
          SECTION
          SECURITY
          SELF
          SEQUENCE
          SERIALIZABLE
          SERVER_NAME
          SESSION
          SETS
          SIMPLE
          SIZE
          SOURCE
          SPACE
          SPECIFIC_NAME
          SQRT
          STATE
          STATEMENT
          STDDEV_POP
          STDDEV_SAMP
          STRUCTURE
          STYLE
          SUBCLASS_ORIGIN
          SUBSTRING
          SUM
          TABLESAMPLE
          TABLE_NAME
          TEMPORARY
          TIES
          TOP_LEVEL_COUNT
          TRANSACTION
          TRANSACTIONS_COMMITTED
          TRANSACTIONS_ROLLED_BACK
          TRANSACTION_ACTIVE
          TRANSFORM
          TRANSFORMS
          TRANSLATE
          TRIGGER_CATALOG
          TRIGGER_NAME
          TRIGGER_SCHEMA
          TRIM
          TYPE
          UNBOUNDED
          UNCOMMITTED
          UNDER
          UNNAMED
          USAGE
          USER_DEFINED_TYPE_CATALOG
          USER_DEFINED_TYPE_CODE
          USER_DEFINED_TYPE_NAME
          USER_DEFINED_TYPE_SCHEMA
          VIEW
          WORK
          WRITE
          ZONE '''
    reserved_03 = '''
         ADD
          ALL
          ALLOCATE
          ALTER
          AND
          ANY
          ARE
          ARRAY
          AS
          ASENSITIVE
          ASYMMETRIC
          AT
          ATOMIC
          AUTHORIZATION
          BEGIN
          BETWEEN
          BIGINT
          BINARY
          BLOB
          BOOLEAN
          BOTH
          BY
          CALL
          CALLED
          CASCADED
          CASE
          CAST
          CHAR
          CHARACTER
          CHECK
          CLOB
          CLOSE
          COLLATE
          COLUMN
          COMMIT
          CONNECT
          CONSTRAINT
          CONTINUE
          CORRESPONDING
          CREATE
          CROSS
          CUBE
          CURRENT
          CURRENT_DATE
          CURRENT_DEFAULT_TRANSFORM_GROUP
          CURRENT_PATH
          CURRENT_ROLE
          CURRENT_TIME
          CURRENT_TIMESTAMP
          CURRENT_TRANSFORM_GROUP_FOR_TYPE
          CURRENT_USER
          CURSOR
          CYCLE
          DATE
          DAY
          DEALLOCATE
          DEC
          DECIMAL
          DECLARE
          DEFAULT
          DELETE
          DEREF
          DESCRIBE
          DETERMINISTIC
          DISCONNECT
          DISTINCT
          DOUBLE
          DROP
          DYNAMIC
          EACH
          ELEMENT
          ELSE
          END
          END-EXEC
          ESCAPE
          EXCEPT
          EXEC
          EXECUTE
          EXISTS
          EXTERNAL
          FALSE
          FETCH
          FILTER
          FLOAT
          FOR
          FOREIGN
          FREE
          FROM
          FULL
          FUNCTION
          GET
          GLOBAL
          GRANT
          GROUP
          GROUPING
          HAVING
          HOLD
          HOUR
          IDENTITY
          IMMEDIATE
          IN
          INDICATOR
          INNER
          INOUT
          INPUT
          INSENSITIVE
          INSERT
          INT
          INTEGER
          INTERSECT
          INTERVAL
          INTO
          IS
          ISOLATION
          JOIN
          LANGUAGE
          LARGE
          LATERAL
          LEADING
          LEFT
          LIKE
          LOCAL
          LOCALTIME
          LOCALTIMESTAMP
          MATCH
          MEMBER
          MERGE
          METHOD
          MINUTE
          MODIFIES
          MODULE
          MONTH
          MULTISET
          NATIONAL
          NATURAL
          NCHAR
          NCLOB
          NEW
          NO
          NONE
          NOT
          NULL
          NUMERIC
          OF
          OLD
          ON
          ONLY
          OPEN
          OR
          ORDER
          OUT
          OUTER
          OUTPUT
          OVER
          OVERLAPS
          PARAMETER
          PARTITION
          PRECISION
          PREPARE
          PRIMARY
          PROCEDURE
          RANGE
          READS
          REAL
          RECURSIVE
          REF
          REFERENCES
          REFERENCING
          REGR_AVGX
          REGR_AVGY
          REGR_COUNT
          REGR_INTERCEPT
          REGR_R2
          REGR_SLOPE
          REGR_SXX
          REGR_SXY
          REGR_SYY
          RELEASE
          RESULT
          RETURN
          RETURNS
          REVOKE
          RIGHT
          ROLLBACK
          ROLLUP
          ROW
          ROWS
          SAVEPOINT
          SCROLL
          SEARCH
          SECOND
          SELECT
          SENSITIVE
          SESSION_USER
          SET
          SIMILAR
          SMALLINT
          SOME
          SPECIFIC
          SPECIFICTYPE
          SQL
          SQLEXCEPTION
          SQLSTATE
          SQLWARNING
          START
          STATIC
          SUBMULTISET
          SYMMETRIC
          SYSTEM
          SYSTEM_USER
          TABLE
          THEN
          TIME
          TIMESTAMP
          TIMEZONE_HOUR
          TIMEZONE_MINUTE
          TO
          TRAILING
          TRANSLATION
          TREAT
          TRIGGER
          TRUE
          UESCAPE
          UNION
          UNIQUE
          UNKNOWN
          UNNEST
          UPDATE
          UPPER
          USER
          USING
          VALUE
          VALUES
          VAR_POP
          VAR_SAMP
          VARCHAR
          VARYING
          WHEN
          WHENEVER
          WHERE
          WIDTH_BUCKET
          WINDOW
          WITH
          WITHIN
          WITHOUT
          YEAR'''

    mysql_56_reserved_keywords = '''
         ACCESSIBLE
         ADD
         ALL
         ALTER
         ANALYZE
         AND
         AS
         ASC
         ASENSITIVE
         BEFORE
         BETWEEN
         BIGINT
         BINARY
         BLOB
         BOTH
         BY
         CALL
         CASCADE
         CASE
         CHANGE
         CHAR
         CHARACTER
         CHECK
         COLLATE
         COLUMN
         CONDITION
         CONSTRAINT
         CONTINUE
         CONVERT
         CREATE
         CROSS
         CURRENT_DATE
         CURRENT_TIME
         CURRENT_TIMESTAMP
         CURRENT_USER
         CURSOR
         DATABASE
         DATABASES
         DAY_HOUR
         DAY_MICROSECOND
         DAY_MINUTE
         DAY_SECOND
         DEC
         DECIMAL
         DECLARE
         DEFAULT
         DELAYED
         DELETE
         DESC
         DESCRIBE
         DETERMINISTIC
         DISTINCT
         DISTINCTROW
         DIV
         DOUBLE
         DROP
         DUAL
         EACH
         ELSE
         ELSEIF
         ENCLOSED
         ESCAPED
         EXISTS
         EXIT
         EXPLAIN
         FALSE
         FETCH
         FLOAT
         FLOAT4
         FLOAT8
         FOR
         FORCE
         FOREIGN
         FROM
         FULLTEXT
         GET
         GRANT
         GROUP
         HAVING
         HIGH_PRIORITY
         HOUR_MICROSECOND
         HOUR_MINUTE
         HOUR_SECOND
         IF
         IFIGNORE
         IN
         INDEX
         INFILE
         INNER
         INOUT
         INSENSITIVE
         INSERT
         INT
         INT1
         INT2
         INT3
         INT4
         INT8
         INTEGER
         INTERVAL
         INTO
         IO_AFTER_GTIDS
         IO_BEFORE_GTIDS
         IS
         ITERATE
         JOIN
         KEY
         KEYS
         KILL
         LEADING
         LEAVE
         LEFT
         LIKE
         LIMIT
         LINEAR
         LINES
         LOAD
         LOCALTIME
         LOCALTIMESTAMP
         LOCK
         LONG
         LONGBLOB
         LONGTEXT
         LOOP
         LOW_PRIORITY
         MASTER_BIND
         MASTER_SSL_VERIFY_SERVER_CERT
         MATCH
         MAXVALUE
         MEDIUMBLOB
         MEDIUMINT
         MEDIUMTEXT
         MIDDLEINT
         MINUTE_MICROSECOND
         MINUTE_SECOND
         MOD
         MODIFIES
         NATURAL
         NOT
         NO_WRITE_TO_BINLOG
         NULL
         NUMERIC
         ON
         OPTIMIZE
         OPTION
         OPTIONALLY
         OR
         ORDER
         OUT
         OUTER
         OUTFILE
         PARTITION
         PRECISION
         PRIMARY
         PROCEDURE
         PURGE
         RANGE
         READ
         READS
         READ_WRITE
         REAL
         REFERENCES
         REGEXP
         RELEASE
         RENAME
         REPEAT
         REPLACE
         REQUIRE
         RESIGNAL
         RESTRICT
         RETURN
         REVOKE
         RIGHT
         RLIKE
         SCHEMA
         SCHEMAS
         SECOND_MICROSECOND
         SELECT
         SENSITIVE
         SEPARATOR
         SET
         SHOW
         SIGNAL
         SMALLINT
         SPATIAL
         SPECIFIC
         SQL
         SQLEXCEPTION
         SQLSTATE
         SQLWARNING
         SQL_BIG_RESULT
         SQL_CALC_FOUND_ROWS
         SQL_SMALL_RESULT
         SSL
         STARTING
         STRAIGHT_JOIN
         TABLE
         TERMINATED
         THEN
         TINYBLOB
         TINYINTTINYTEXT
         TO
         TRAILING
         TRIGGER
         TRUE
         UNDO
         UNION
         UNIQUE
         UNLOCK
         UNSIGNED
         UPDATE
         USAGE
         USE
         USING
         UTC_DATE
         UTC_TIME
         UTC_TIMESTAMPVALUE
         VALUES
         VARBINARY
         VARCHAR
         VARCHARACTER
         VARYING
         WHEN
         WHERE
         WHILE
         WITH
         WRITE
         XOR
         YEAR_MONTH
         ZEROFILL
         '''
    mysql_56_non_reserved_keywords = '''
         ACTION
         AFTER
         AGAINST
         AGGREGATE
         ALGORITHM
         ANALYSE
         ANY
         ASCII
         AT
         AUTHORS
         AUTOEXTEND_SIZE
         AUTO_INCREMENT
         AVG
         AVG_ROW_LENGTH
         BACKUP
         BEGIN
         BINLOG
         BIT
         BLOCK
         BOOL
         BOOLEAN
         BTREE
         BYTE
         CACHE
         CASCADED
         CATALOG_NAME
         CHAIN
         CHANGED
         CHARSET
         CHECKSUM
         CIPHER
         CLASS_ORIGIN
         CLIENT
         CLOSE
         COALESCE
         CODE
         COLLATION
         COLUMNS
         COLUMN_FORMAT
         COLUMN_NAME
         COMMENT
         COMMIT
         COMMITTED
         COMPACT
         COMPLETION
         COMPRESSED
         CONCURRENT
         CONNECTION
         CONSISTENT
         CONSTRAINT_CATALOG
         CONSTRAINT_NAME
         CONSTRAINT_SCHEMA
         CONTAINS
         CONTEXT
         CONTRIBUTORS
         CPU
         CUBE
         CURRENT
         CURSOR_NAME
         DATA
         DATAFILE

         DATE
         DATETIME
         DAY
         DEALLOCATE
         DEFAULT_AUTH
         DEFINER
         DELAY_KEY_WRITE
         DES_KEY_FILE
         DIAGNOSTICS
         DIRECTORY
         DISABLE
         DISCARD
         DISK
         DO
         DUMPFILE
         DUPLICATE
         DYNAMIC
         ENABLE
         END
         ENDS
         ENGINE_
         ENGINES
         ENUM
         ERROR
         ERRORS
         ESCAPE
         EVENT
         EVENTS
         EVERY
         EXCHANGE
         EXECUTE
         EXPANSION
         EXPIRE
         EXPORT
         EXTENDED
         EXTENT_SIZE
         FAST
         FAULTS
         FIELDS
         FILE
         FIRST
         FIXED
         FLUSH
         FORMAT
         FOUND
         FRAGMENTATION
         FULL
         FUNCTION
         GENERAL
         GEOMETRY
         GEOMETRYCOLLECTION
         GET_FORMAT
         GLOBAL
         GRANTS
         HANDLER
         HASH
         HELP
         HOST
         HOSTS
         HOUR
         IDENTIFIED
         IGNORE_SERVER_IDS
         IMPORT
         INDEXES
         INITIAL_SIZE
         INSERT_METHOD
         INSTALL
         INVOKER
         IO
         IO_THREAD
         IPC
         ISOLATION
         ISSUER
         KEY_BLOCK_SIZE
         LANGUAGE
         LAST
         LEAVES
         LESS
         LEVEL
         LINESTRING
         LIST_
         LOCAL
         LOCKS
         LOGFILE
         LOGS
         MASTER
         MASTER_AUTO_POSITION
         MASTER_CONNECT_RETRY
         MASTER_DELAY
         MASTER_HEARTBEAT_PERIOD
         MASTER_HOST
         MASTER_LOG_FILE
         MASTER_LOG_POS
         MASTER_PASSWORD
         MASTER_PORT
         MASTER_RETRY_COUNT
         MASTER_SERVER_ID
         MASTER_SSL
         MASTER_SSL_CA
         MASTER_SSL_CAPATH
         MASTER_SSL_CERT
         MASTER_SSL_CIPHER
         MASTER_SSL_CRL
         MASTER_SSL_CRLPATH
         MASTER_SSL_KEY
         MASTER_USER
         MAX_CONNECTIONS_PER_HOUR
         MAX_QUERIES_PER_HOUR
         MAX_ROWS
         MAX_SIZE
         MAX_UPDATES_PER_HOUR
         MAX_USER_CONNECTIONS
         MEDIUM
         MEMORY
         MERGE
         MESSAGE_TEXT
         MICROSECOND
         MIGRATE
         MINUTE
         MIN_ROWS
         MODE
         MODIFY
         MONTH
         MULTILINESTRING
         MULTIPOINT
         MULTIPOLYGON
         MUTEX
         MYSQL_ERRNO
         NAME
         NAMES
         NATIONAL
         NCHAR
         NDB
         NDBCLUSTER
         NEW
         NEXT
         NO
         NODEGROUP
         NONE
         NO_WAIT
         NUMBER
         NVARCHAR
         OFFSET
         OLD_PASSWORD
         ONE
         ONE_SHOT
         ONLY
         OPEN
         OPTIONS
         OWNER
         PACK_KEYS
         PAGE
         PARSER
         PARTIAL
         PARTITIONING
         PARTITIONS
         PASSWORD
         PHASE
         PLUGIN
         PLUGINS
         PLUGIN_DIR
         POINT
         POLYGON
         PORT
         PREPARE
         PRESERVE
         PREV
         PRIVILEGES
         PROCESSLIST
         PROFILE
         PROFILES
         PROXY
         QUARTER
         QUERY
         QUICK
         READ_ONLY
         REBUILD
         RECOVER
         REDOFILE
         REDO_BUFFER_SIZE
         REDUNDANT
         RELAY
         RELAYLOG
         RELAY_LOG_FILE
         RELAY_LOG_POS
         RELAY_THREAD
         RELOAD
         REMOVE
         REORGANIZE
         REPAIR
         REPEATABLE
         REPLICATION
         RESET
         RESTORE
         RESUME
         RETURNED_SQLSTATE
         RETURNS
         REVERSE
         ROLLBACK
         ROLLUP
         ROUTINE
         ROW
         ROWS
         ROW_COUNT
         ROW_FORMAT
         RTREE
         SAVEPOINT
         SCHEDULE
         SCHEMA_NAME
         SECOND
         SECURITY
         SERIAL
         SERIALIZABLE
         SERVER
         SESSION
         SHARE
         SHUTDOWN
         SIGNED
         SIMPLE
         SLAVE
         SLOW
         SNAPSHOT
         SOCKET
         SOME
         SONAME
         SOUNDS
         SOURCE
         SQL_AFTER_GTIDS
         SQL_AFTER_MTS_GAPS
         SQL_BEFORE_GTIDS
         SQL_BUFFER_RESULT
         SQL_CACHE
         SQL_NO_CACHE
         SQL_THREAD
         SQL_TSI_DAY
         SQL_TSI_HOUR
         SQL_TSI_MINUTE
         SQL_TSI_MONTH
         SQL_TSI_QUARTER
         SQL_TSI_SECOND
         SQL_TSI_WEEK
         SQL_TSI_YEAR
         START
         STARTS
         STATS_AUTO_RECALC
         STATS_PERSISTENT
         STATS_SAMPLE_PAGES
         STATUS
         STOP
         STORAGE
         STRING
         SUBCLASS_ORIGIN
         SUBJECT
         SUBPARTITION
         SUBPARTITIONS
         SUPER
         SUSPEND
         SWAPS
         SWITCHES
         TABLES
         TABLESPACE
         TABLE_CHECKSUM
         TABLE_NAME
         TEMPORARY
         TEMPTABLE
         TEXT
         THAN
         TIME
         TIMESTAMP
         TIMESTAMPADD
         TIMESTAMPDIFF
         TRANSACTION
         TRIGGERS
         TRUNCATE
         TYPE
         TYPES
         UNCOMMITTED
         UNDEFINED
         UNDOFILE
         UNDO_BUFFER_SIZE
         UNICODE
         UNINSTALL
         UNKNOWN
         UNTIL
         UPGRADE
         USER
         USER_RESOURCES
         USE_FRM
         VARIABLES
         VIEW
         WAIT
         WARNINGS
         WASH
         WEEK
         WEIGHT_STRING
         WORK
         WRAPPER
         X509_
         XA
         XML
         YEAR
         '''

    ob_reserved_keywords = '''
         ACCESSIBLE
         ADD
         ALTER
         AND
         ANALYZE
         ALL
         AS
         ASENSITIVE
         ASC
         BETWEEN
         BEFORE
         BIGINT
         BINARY
         BLOB
         BOTH
         BY
         CALL
         CASCADE
         CASE
         CHANGE
         CHAR
         CHARACTER
         CHECK
         CONDITION
         CONSTRAINT
         CONTINUE
         CONVERT
         COLLATE
         COLUMN
         CREATE
         CROSS
         CURRENT_DATE
         CURRENT_TIME
         CURRENT_TIMESTAMP
         CURRENT_USER
         CURSOR
         DAY_HOUR
         DAY_MICROSECOND
         DAY_MINUTE
         DAY_SECOND
         DATABASE
         DATABASES
         DEC
         DECIMAL
         DECLARE
         DEFAULT
         DELAYED
         DELETE
         DESC
         DESCRIBE
         DETERMINISTIC
         DISTINCT
         DISTINCTROW
         DOUBLE
         DROP
         DUAL
         EACH
         ENCLOSED
         ELSE
         ELSEIF
         ESCAPED
         EXISTS
         EXIT
         EXPLAIN
         FETCH
         FOREIGN
         FLOAT
         FLOAT4
         FLOAT8
         FOR
         FORCE
         FROM
         FULLTEXT
         GET
         GRANT
         GROUP
         HAVING
         HIGH_PRIORITY
         HOUR_MICROSECOND
         HOUR_MINUTE
         HOUR_SECOND
         IF
         IFIGNORE
         IN
         INDEX
         INNER
         INFILE
         INOUT
         INSENSITIVE
         INT
         INT1
         INT2
         INT3
         INT4
         INT8
         INTEGER
         INTERVAL
         INSERT
         INTO
         IO_AFTER_GTIDS
         IO_BEFORE_GTIDS
         IS
         ITERATE
         JOIN
         KEY
         KEYS
         KILL
         LEADING
         LEAVE
         LEFT
         LIMIT
         LIKE
         LINEAR
         LINES
         LOAD
         LOCALTIME
         LOCALTIMESTAMP
         LOCK
         LONG
         LONGBLOB
         LONGTEXT
         LOOP
         LOW_PRIORITY
         MASTER_BIND
         MASTER_SSL_VERIFY_SERVER_CERT
         MATCH
         MAXVALUE
         MEDIUMBLOB
         MEDIUMINT
         MEDIUMTEXT
         MIDDLEINT
         MINUTE_MICROSECOND
         MINUTE_SECOND
         MOD
         MODIFIES
         NATURAL
         NO_WRITE_TO_BINLOG
         DIV
         NOT
         NUMERIC
         ON
         OPTION
         OPTIMIZE
         OPTIONALLY
         OR
         ORDER
         OUT
         OUTER
         OUTFILE
         PROCEDURE
         PURGE
         PARTITION
         PRECISION
         PRIMARY
         RANGE
         READ
         READ_WRITE
         READS
         REAL
         RELEASE
         REFERENCES
         REGEXP
         RENAME
         REPLACE
         REPEAT
         REQUIRE
         RESIGNAL
         RESTRICT
         RETURN
         REVOKE
         RIGHT
         RLIKE
         SECOND_MICROSECOND
         SELECT
         SCHEMA
         SCHEMAS
         SEPARATOR
         SET
         SENSITIVE
         SHOW
         SIGNAL
         SMALLINT
         SPATIAL
         SPECIFIC
         SQL
         SQLEXCEPTION
         SQLSTATE
         SQLWARNING
         SQL_BIG_RESULT
         SQL_CALC_FOUND_ROWS
         SQL_SMALL_RESULT
         SSL
         STARTING
         STRAIGHT_JOIN
         TERMINATED
         TINYBLOB
         TINYINTTINYTEXT
         TABLE
         THEN
         TO
         TRAILING
         TRIGGER
         UNDO
         UNION
         UNIQUE
         UNLOCK
         UNSIGNED
         UPDATE
         USAGE
         USE
         USING
         UTC_DATE
         UTC_TIME
         UTC_TIMESTAMPVALUE
         VALUES
         VARBINARY
         VARCHAR
         VARCHARACTER
         VARYING
         WHERE
         WHEN
         WHILE
         WITH
         WRITE
         XOR
         YEAR_MONTH
         ZEROFILL
         TRUE
         FALSE
         NULL
         @@GLOABL
         @@SESSION
         _UTF8
         _UTF8MB4
         _BINARY
         '''
    ob_non_reserved_keywords = '''
         ACCOUNT
         ACTION
         ACTIVE
         ADDDATE
         AFTER
         AGAINST
         AGGREGATE
         ALGORITHM
         ANALYSE
         ANY
         ASCII
         AT
         AUTHORS
         AUTOEXTEND_SIZE
         AUTO_INCREMENT
         AVG
         AVG_ROW_LENGTH
         BACKUP
         BEGIN
         BINLOG
         BIT
         BLOCK
         BLOCK_SIZE
         BOOL
         BOOLEAN
         BOOTSTRAP
         BTREE
         BYTE
         CACHE
         CANCEL
         CASCADED
         CAST
         CATALOG_NAME
         CHAIN
         CHANGED
         CHARSET
         CHECKSUM
         CIPHER
         CLASS_ORIGIN
         CLEAN
         CLEAR
         CLIENT
         CLOSE
         CLUSTER_ID
         COALESCE
         CODE
         COLLATION
         COLUMN_FORMAT
         COLUMN_NAME
         COLUMNS
         COMMENT
         COMMIT
         COMMITTED
         COMPACT
         COMPLETION
         COMPRESSED
         COMPRESSION
         CONCURRENT
         CONNECTION
         CONSISTENT
         CONSISTENT_MODE
         CONSTRAINT_CATALOG
         CONSTRAINT_NAME
         CONSTRAINT_SCHEMA
         CONTAINS
         CONTEXT
         CONTRIBUTORS
         COPY
         COUNT
         CPU
         CREATE_TIMESTAMP
         CUBE
         CURDATE
         CURRENT
         CURTIME
         CURSOR_NAME
         DATA
         DATAFILE
         DATE
         DATE_ADD
         DATE_SUB
         DATETIME
         DAY
         DEALLOCATE
         DEFAULT_AUTH
         DEFINER
         DELAY
         DELAY_KEY_WRITE
         DES_KEY_FILE
         DESTINATION
         DIAGNOSTICS
         DIRECTORY
         DISABLE
         DISCARD
         DISK
         DO
         DUMP
         DUMPFILE
         DUPLICATE
         DYNAMIC
         EFFECTIVE
         ENABLE
         END
         ENDS
         ENGINE_
         ENGINES
         ENUM
         ERROR
         ERROR_CODE
         ERROR_P
         ERRORS
         ESCAPE
         EVENT
         EVENTS
         EVERY
         EXCEPT
         EXCHANGE
         EXECUTE
         EXPANSION
         EXPIRE
         EXPIRE_INFO
         EXPORT
         EXTENDED
         EXTENT_SIZE
         EXTRACT
         FAILED_LOGIN_ATTEMPTS
         FAST
         FAULTS
         FIELDS
         FILE
         FILEX
         FINAL_COUNT
         FIRST
         FIXED
         FLUSH
         FOLLOWER
         FORMAT
         FOUND
         FRAGMENTATION
         FREEZE
         FUNCTION
         GENERAL
         GEOMETRY
         GEOMETRYCOLLECTION
         GET_FORMAT
         GLOBAL
         GLOBAL_NAME
         GRANTS
         HANDLER
         HASH
         HELP
         HOST
         HOSTS
         HOUR
         IDENTIFIED
         IGNORE
         IGNORE_SERVER_IDS
         IMPORT
         INDEXES
         INITIAL_SIZE
         INSERT_METHOD
         INSTALL
         INTERSECT
         INVOKER
         IO
         IO_THREAD
         IPC
         ISOLATION
         ISSUER
         JSON
         KEY_BLOCK_SIZE
         KEY_VERSION
         LANGUAGE
         LAST
         LAST_INSERT_ID
         LEADER
         LEAVES
         LESS
         LEVEL
         LINESTRING
         LIST_
         LOCAL
         LOCKED
         LOCKS
         LOGFILE
         LOGS
         MAJOR
         MASTER
         MASTER_AUTO_POSITION
         MASTER_CONNECT_RETRY
         MASTER_DELAY
         MASTER_HEARTBEAT_PERIOD
         MASTER_HOST
         MASTER_LOG_FILE
         MASTER_LOG_POS
         MASTER_PASSWORD
         MASTER_PORT
         MASTER_RETRY_COUNT
         MASTER_SERVER_ID
         MASTER_SSL
         MASTER_SSL_CA
         MASTER_SSL_CAPATH
         MASTER_SSL_CERT
         MASTER_SSL_CIPHER
         MASTER_SSL_CRL
         MASTER_SSL_CRLPATH
         MASTER_SSL_KEY
         MASTER_USER
         MAX
         MAX_CONNECTIONS_PER_HOUR
         MAX_CPU
         LOG_DISK_SIZE
         MAX_IOPS
         MEMORY_SIZE
         MAX_QUERIES_PER_HOUR
         MAX_ROWS
         MAX_SIZE
         MAX_UPDATES_PER_HOUR
         MAX_USER_CONNECTIONS
         MEDIUM
         MEMORY
         MEMTABLE
         MERGE
         MESSAGE_TEXT
         META
         MICROSECOND
         MIGRATE
         MIN
         MIN_CPU
         MIN_IOPS
         MINOR
         MIN_ROWS
         MINUTE
         MODE
         MODIFY
         MONTH
         MOVE
         MULTILINESTRING
         MULTIPOINT
         MULTIPOLYGON
         MUTEX
         MYSQL_ERRNO
         NAME
         NAMES
         NATIONAL
         NCHAR
         NDB
         NDBCLUSTER
         NEW
         NEXT
         NEXTVAL
         NO
         NODEGROUP
         NONE
         NORMAL
         NOW
         NO_WAIT
         NOWAIT
         NUMBER
         NVARCHAR
         OFF
         OFFSET
         OLD_PASSWORD
         ONE
         ONE_SHOT
         ONLY
         OPEN
         OPTIONS
         OWNER
         PACK_KEYS
         PAGE
         PARAMETERS
         PARSER
         PARTIAL
         PARTITION_ID
         PARTITIONING
         PARTITIONS
         PASSWORD
         PASSWORD_LOCK_TIME
         PAUSE
         PHASE
         PLUGIN
         PLUGIN_DIR
         PLUGINS
         POINT
         POLYGON
         POOL
         PORT
         PREPARE
         PRESERVE
         PREV
         PRIMARY_ZONE
         PRIVILEGES
         PROCESSLIST
         PROFILE
         PROFILES
         PROXY
         QUARTER
         QUERY
         QUICK
         READ_ONLY
         REBUILD
         RECOVER
         RECYCLE
         REDO_BUFFER_SIZE
         REDOFILE
         REDUNDANT
         REFRESH
         REGION
         RELAY
         RELAYLOG
         RELAY_LOG_FILE
         RELAY_LOG_POS
         RELAY_THREAD
         RELOAD
         REMOVE
         REORGANIZE
         REPAIR
         REPEATABLE
         REPLICA
         REPLICA_NUM
         REPLICATION
         REPORT
         RESET
         RESOURCE
         RESOURCE_POOL_LIST
         RESTART
         RESTORE
         RESUME
         RETURNED_SQLSTATE
         RETURNS
         REVERSE
         ROLLBACK
         ROLLUP
         ROOT
         ROOTTABLE
         ROUTINE
         ROW
         ROW_COUNT
         ROW_FORMAT
         ROWS
         RTREE
         SAVEPOINT
         SCHEDULE
         SCHEMA_NAME
         SCOPE
         SECOND
         SECURITY
         SERIAL
         SERIALIZABLE
         SERVER
         SERVER_IP
         SERVER_PORT
         SERVER_TYPE
         SESSION
         SESSION_USER
         SET_MASTER_CLUSTER
         SET_SLAVE_CLUSTER
         SHARE
         SHUTDOWN
         SIGNED
         SIMPLE
         SLAVE
         SLOW
         SNAPSHOT
         SOCKET
         SOME
         SONAME
         SOUNDS
         SOURCE
         SPLIT
         SPFILE
         SQL_AFTER_GTIDS
         SQL_AFTER_MTS_GAPS
         SQL_BEFORE_GTIDS
         SQL_BUFFER_RESULT
         SQL_CACHE
         SQL_NO_CACHE
         SQL_THREAD
         SQL_TSI_DAY
         SQL_TSI_HOUR
         SQL_TSI_MINUTE
         SQL_TSI_MONTH
         SQL_TSI_QUARTER
         SQL_TSI_SECOND
         SQL_TSI_WEEK
         SQL_TSI_YEAR
         START
         STARTS
         STATS_AUTO_RECALC
         STATS_PERSISTENT
         STATS_SAMPLE_PAGES
         STATUS
         STEP_MERGE_NUM
         STOP
         STORAGE
         STORING
         STRING
         SUBCLASS_ORIGIN
         SUBDATE
         SUBJECT
         SUBPARTITION
         SUBPARTITIONS
         SUBSTR
         SUBSTRING
         SUM
         SUPER
         SUSPEND
         SWAPS
         SWITCH
         SWITCHES
         SYSDATE
         SYSTEM
         SYSTEM_USER
         TABLE_CHECKSUM
         TABLEGROUP
         TABLE_ID
         TABLET_ID
         TABLE_NAME
         TABLES
         TABLESPACE
         TABLET
         TABLET_MAX_SIZE
         TEMPORARY
         TEMPTABLE
         TENANT
         TEXT
         THAN
         TIME
         TIMESTAMP
         TIMESTAMPADD
         TIMESTAMPDIFF
         TINYINT
         TP_NAME
         TRADITIONAL
         TRANSACTION
         TRIGGER
         TRIGGERS
         TRIM
         TRUNCATE
         TYPE
         TYPES
         UNCOMMITTED
         UNDEFINED
         UNDO_BUFFER_SIZE
         UNDOFILE
         UNICODE
         UNINSTALL
         UNIT
         UNIT_NUM
         UNLOCKED
         UNKNOWN
         UNTIL
         UNUSUAL
         UPGRADE
         USE_BLOOM_FILTER
         USE_FRM
         USER
         USER_RESOURCES
         VALUE
         VARIABLES
         VERBOSE
         VIEW
         WAIT
         WARNINGS
         WASH
         WEEK
         WEIGHT_STRING
         WORK
         WRAPPER
         X509_
         XA
         XML
         YEAR
         ZONE
         ZONE_LIST
         LOCATION
                  '''

    ob_keywords = '''
         ADD
         AND
         ANY
         ALL
         AS
         ASC
         AUTO_INCREMENT
         BETWEEN
         BIGINT
         BINARY
         BOOLEAN
         BY
         CASE
         CHARACTER
         CNNOP
         COLUMNS
         COMPRESSION
         CREATE
         CREATETIME
         DATE
         DATETIME
         DECIMAL
         DEFAULT
         DELETE
         DESC
         DESCRIBE
         DISTINCT
         DOUBLE
         DROP
         ELSE
         END
         END_P
         ERROR
         EXCEPT
         EXISTS
         JOIN_INFO
         EXPIRE_INFO
         EXPLAIN
         FLOAT
         FROM
         FULL
         GLOBAL
         GROUP
         HAVING
         IF
         IN
         INNER
         INTEGER
         INTERSECT
         INSERT
         INTO
         IS
         JOIN
         JOIN_INFO
         KEY
         LEFT
         LIMIT
         LIKE
         MEDIUMINT
         MOD
         MODIFYTIME
         NOT
         NUMERIC
         OFFSET
         ON
         OR
         ORDER
         OUTER
         PRECISION
         PRIMARY
         PRIMARY_ZONE
         REAL
         REPLACE
         REPLICA_NUM
         RIGHT
         SCHEMA
         SELECT
         SERVER
         SESSION
         SET
         SHOW
         SMALLINT
         STATUS
         TABLE
         TABLES
         TABLET_MAX_SIZE
         THEN
         TIME
         TIMESTAMP
         TINYINT
         UNION
         UPDATE
         USE_BLOOM_FILTER
         VALUES
         VARCHAR
         VARBINARY
         VARIABLES
         VERBOSE
         WHERE
         WHEN
         USER
         IDENTIFIED
         PASSWORD
         FOR
         ALTER
         RENAME
         TO
         LOCKED
         UNLOCKED
         GRANT
         PRIVILEGES
         OPTION
         REVOKE
         '''

def ob_r_info():
    a = SqlKeywords()
    for k in a.ob_reserved_keywords.split():
        for c in [a.mysql_56_reserved_keywords]:
            try:
                i = c.split().index(k)
                print("%s is r" %k)
            except ValueError:
                for d in [a.mysql_56_non_reserved_keywords]:
                    try:
                        i = d.split().index(k)
                        print("%s is u" %k)
                    except ValueError:
                        print("%s is y" %k)
def ob_non_r_info():
    a = SqlKeywords()
    for k in a.ob_non_reserved_keywords.split():
        for c in [a.mysql_56_reserved_keywords]:
            try:
                i = c.split().index(k)
                print("%s is r" %k)
            except ValueError:
                for d in [a.mysql_56_non_reserved_keywords]:
                    try:
                        i = d.split().index(k)
                        print("%s is u" %k)
                    except ValueError:
                        print("%s is y" %k)

def ob_mysql_r_info():
    a = SqlKeywords()
    for k in a.mysql_56_reserved_keywords.split():
        for c in [a.ob_reserved_keywords]:
            try:
                i = c.split().index(k)
                print("%s is r" %k)
            except ValueError:
                for d in [a.ob_non_reserved_keywords]:
                    try:
                        i = d.split().index(k)
                        print("%s is u" %k)
                    except ValueError:
                        print("%s is y" %k)

def ob_mysql_non_r_info():
    a = SqlKeywords()
    for k in a.mysql_56_non_reserved_keywords.split():
        for c in [a.ob_reserved_keywords]:
            try:
                i = c.split().index(k)
                print("%s is r" %k)
            except ValueError:
                for d in [a.ob_non_reserved_keywords]:
                    try:
                        i = d.split().index(k)
                        print("%s is u" %k)
                    except ValueError:
                        print("%s is y" %k)
def gen_non_reserved():
    a = SqlKeywords()
    for k in a.ob_non_reserved_keywords.split():
        print '  {\"%s\", %s},' %(k.lower(), k)
def gen_reserved():
    a = SqlKeywords()
    for k in a.ob_reserved_keywords.split():
        print '%-30s{ return %s; }' %(k, k)


if (__name__ == '__main__'):
    ##gen_non_reserved()
##    gen_reserved()
  ## ob_non_r_info()
     ob_r_info()
  ##   ob_mysql_r_info()
    ## ob_mysql_non_r_info()

##if (__name__ == '__ma}in__'):
##    a = SqlKeywords()
##    for k in ob_keywords.split():
##        print("%s " % k, end = "")
##        for c in [a.reserved_92, a.reserved_99, a.reserved_03]:
##            try:
##                i = c.split().index(k)
##                print("R ", end = "")
##            except ValueError:
##                print("U ", end = "")
##        for n in [a.non_reserved_92, a.non_reserved_99, a.non_reserved_03]:
##            try:
##                i = n.split().index(k)
##                print("N ", end = "")
##            except ValueError:
##                print("U ", end = "")
##        print("")
