#package_name: dbms_hybrid_search
#author: wenxiangjing.wxj

CREATE OR REPLACE PACKAGE BODY dbms_hybrid_search

    FUNCTION SEARCH (IN table_name VARCHAR(65535),
                    IN search_params LONGTEXT)
    RETURN JSON;
    PRAGMA INTERFACE(C, DBMS_HYBRID_VECTOR_MYSQL_SEARCH);

    FUNCTION GET_SQL (IN table_name VARCHAR(65535),
                    IN search_params LONGTEXT)
    RETURN LONGTEXT;
    PRAGMA INTERFACE(C, DBMS_HYBRID_VECTOR_MYSQL_GET_SQL);

END dbms_hybrid_search;