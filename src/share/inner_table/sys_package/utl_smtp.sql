#package_name:utl_smtp
#author: xinning.lf

CREATE OR REPLACE PACKAGE utl_smtp AUTHID CURRENT_USER AS

  TYPE connection IS RECORD (
    host             VARCHAR2(255),
    port             PLS_INTEGER,
    tx_timeout       PLS_INTEGER,
    private_tcp_con  utl_tcp.connection,
    private_state    PLS_INTEGER
  );

  TYPE reply IS RECORD (
    code     PLS_INTEGER,
    text     VARCHAR2(508)
  );

  TYPE replies IS TABLE OF reply INDEX BY BINARY_INTEGER;

  invalid_operation   EXCEPTION;
  transient_error     EXCEPTION;
  permanent_error     EXCEPTION;
  unsupported_scheme  EXCEPTION;
  no_supported_scheme EXCEPTION;
  invalid_operation_errcode   CONSTANT PLS_INTEGER:= -9794;
  transient_error_errcode     CONSTANT PLS_INTEGER:= -9795;
  permanent_error_errcode     CONSTANT PLS_INTEGER:= -9796;
  unsupported_scheme_errcode  CONSTANT PLS_INTEGER:= -9797;
  no_supported_scheme_errcode CONSTANT PLS_INTEGER:= -9798;
  PRAGMA EXCEPTION_INIT(invalid_operation,   -9794);
  PRAGMA EXCEPTION_INIT(transient_error,     -9795);
  PRAGMA EXCEPTION_INIT(permanent_error,     -9796);
  PRAGMA EXCEPTION_INIT(unsupported_scheme,  -9797);
  PRAGMA EXCEPTION_INIT(no_supported_scheme, -9798);

  ALL_SCHEMES CONSTANT VARCHAR2(80) := 'CRAM-MD5 PLAIN LOGIN';
  NON_CLEARTEXT_PASSWORD_SCHEMES CONSTANT VARCHAR2(80) := 'CRAM-MD5';

  FUNCTION open_connection(host            IN  VARCHAR2,
                           port            IN  PLS_INTEGER DEFAULT 25,
                           c               OUT connection,
                           tx_timeout      IN  PLS_INTEGER DEFAULT NULL,
                           wallet_path     IN  VARCHAR2 DEFAULT NULL,
                           wallet_password IN  VARCHAR2 DEFAULT NULL,
                           secure_connection_before_smtp IN BOOLEAN DEFAULT FALSE,
                           secure_host     IN  VARCHAR2 DEFAULT NULL)
                           RETURN reply;

  FUNCTION open_connection(host            IN  VARCHAR2,
                           port            IN  PLS_INTEGER DEFAULT 25,
                           tx_timeout      IN  PLS_INTEGER DEFAULT NULL,
                           wallet_path     IN  VARCHAR2 DEFAULT NULL,
                           wallet_password IN  VARCHAR2 DEFAULT NULL,
                           secure_connection_before_smtp IN BOOLEAN DEFAULT FALSE,
                           secure_host     IN  VARCHAR2 DEFAULT NULL)
                           RETURN connection;


  FUNCTION command(c    IN OUT NOCOPY connection,
                   cmd  IN            VARCHAR2,
                   arg  IN            VARCHAR2 DEFAULT NULL)
                   RETURN reply;

  PROCEDURE command(c     IN OUT NOCOPY connection,
                    cmd   IN            VARCHAR2,
                    arg   IN            VARCHAR2 DEFAULT NULL);


  FUNCTION command_replies(c     IN OUT NOCOPY connection,
                           cmd   IN            VARCHAR2,
                           arg   IN            VARCHAR2 DEFAULT NULL)
                           RETURN replies;


  FUNCTION helo(c       IN OUT NOCOPY connection,
                domain  IN            VARCHAR2) RETURN reply;

  PROCEDURE helo(c       IN OUT NOCOPY connection,
                 domain  IN            VARCHAR2);


  FUNCTION ehlo(c       IN OUT NOCOPY connection,
                domain  IN            VARCHAR2) RETURN replies;

  PROCEDURE ehlo(c       IN OUT NOCOPY connection,
                 domain  IN            VARCHAR2);


  FUNCTION auth(c        IN OUT NOCOPY connection,
                username IN            VARCHAR2,
                password IN            VARCHAR2,
                schemes  IN            VARCHAR2
                                       DEFAULT NON_CLEARTEXT_PASSWORD_SCHEMES)
                RETURN reply;

  PROCEDURE auth(c        IN OUT NOCOPY connection,
                 username IN            VARCHAR2,
                 password IN            VARCHAR2,
                 schemes  IN            VARCHAR2
                                        DEFAULT NON_CLEARTEXT_PASSWORD_SCHEMES);


  FUNCTION mail(c          IN OUT NOCOPY connection,
                sender     IN            VARCHAR2,
                parameters IN            VARCHAR2 DEFAULT NULL) RETURN reply;

  PROCEDURE mail(c          IN OUT NOCOPY connection,
                 sender     IN            VARCHAR2,
                 parameters IN            VARCHAR2 DEFAULT NULL);


  FUNCTION rcpt(c          IN OUT NOCOPY connection,
                recipient  IN            VARCHAR2,
                parameters IN            VARCHAR2 DEFAULT NULL) RETURN reply;

  PROCEDURE rcpt(c          IN OUT NOCOPY connection,
                 recipient  IN            VARCHAR2,
                 parameters IN            VARCHAR2 DEFAULT NULL);


  FUNCTION data(c     IN OUT NOCOPY connection,
                body  IN            VARCHAR2)
                RETURN reply;

  PROCEDURE data(c     IN OUT NOCOPY connection,
                 body  IN            VARCHAR2);


  FUNCTION open_data(c IN OUT NOCOPY connection) RETURN reply;

  PROCEDURE open_data(c IN OUT NOCOPY connection);


  PROCEDURE write_data(c     IN OUT NOCOPY connection,
                       data  IN            VARCHAR2);

  PROCEDURE write_raw_data(c     IN OUT NOCOPY connection,
                           data  IN            RAW);

  FUNCTION close_data(c IN OUT NOCOPY connection) RETURN reply;

  PROCEDURE close_data(c IN OUT NOCOPY connection);


  FUNCTION rset(c IN OUT NOCOPY connection) RETURN reply;

  PROCEDURE rset(c IN OUT NOCOPY connection);


  FUNCTION vrfy(c          IN OUT NOCOPY connection,
                recipient  IN            VARCHAR2) RETURN reply;


  FUNCTION help(c        IN OUT NOCOPY connection,
                command  IN            VARCHAR2 DEFAULT NULL) RETURN replies;


  FUNCTION noop(c IN OUT NOCOPY connection) RETURN reply;

  PROCEDURE noop(c IN OUT NOCOPY connection);


  FUNCTION quit(c IN OUT NOCOPY connection) RETURN reply;

  PROCEDURE quit(c IN OUT NOCOPY connection);


  PROCEDURE close_connection(c IN OUT NOCOPY connection);

END;
//
