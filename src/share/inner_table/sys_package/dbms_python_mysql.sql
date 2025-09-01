#package_name:dbms_python
#author:webber.wb
CREATE OR REPLACE PACKAGE dbms_python AUTHID CURRENT_USER
  PROCEDURE loadpython(IN py_name VARCHAR(65535), IN content LONGBLOB, IN comment VARCHAR(65535) DEFAULT '');

  PROCEDURE droppython(IN py_name VARCHAR(65535));
END dbms_python;
