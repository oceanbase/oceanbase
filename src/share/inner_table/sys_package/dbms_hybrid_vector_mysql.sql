#package_name: dbms_hybrid_search
#author: wenxiangjing.wxj

CREATE OR REPLACE PACKAGE dbms_hybrid_search AUTHID CURRENT_USER

    FUNCTION SEARCH (IN table_name VARCHAR(65535),
                    IN search_params LONGTEXT)
    RETURN JSON;

    FUNCTION GET_SQL (IN table_name VARCHAR(65535),
                    IN search_params LONGTEXT)
    RETURN LONGTEXT;

END dbms_hybrid_search;