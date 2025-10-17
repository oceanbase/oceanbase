#package_name:dbms_python
#author:webber.wb

CREATE OR REPLACE PACKAGE BODY dbms_python
  PROCEDURE loadpython(IN py_name VARCHAR(65535), IN content LONGBLOB, IN comment VARCHAR(65535) DEFAULT '');
    PRAGMA INTERFACE(C, DBMS_PYTHON_LOADPYTHON_MYSQL);

  PROCEDURE droppython(IN py_name VARCHAR(65535));
    PRAGMA INTERFACE(C, DBMS_PYTHON_DROPPYTHON_MYSQL);
END dbms_python;
