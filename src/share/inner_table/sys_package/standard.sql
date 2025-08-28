#package_name:standard
#author: yaojie.lrj

CREATE OR REPLACE PACKAGE DBMS_STANDARD IS 

    procedure raise_application_error(
        num binary_integer, 
        msg varchar2,
        keeperrorstack boolean default FALSE
    );

END;
//