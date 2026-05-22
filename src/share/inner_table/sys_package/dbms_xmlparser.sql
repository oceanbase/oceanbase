#package_name:DBMS_XMLPARSER
#author:zongmei.zzm

CREATE OR REPLACE TYPE Parser FORCE OID '300035' AS OPAQUE
(
  CONSTRUCTOR FUNCTION Parser RETURN SELF AS RESULT
);
//

CREATE OR REPLACE PACKAGE dbms_xmlparser AUTHID CURRENT_USER AS
  SUBTYPE Parser IS SYS.Parser;
  FUNCTION newParser RETURN Parser;
  PROCEDURE parseClob(p IN Parser, doc IN CLOB);
  FUNCTION getDocument(p IN Parser) RETURN DBMS_XMLDOM.DOMDocument;
  PROCEDURE freeParser(p IN Parser);
END dbms_xmlparser;
//
