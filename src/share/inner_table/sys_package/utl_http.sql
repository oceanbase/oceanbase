#package_name:utl_http
#author: yaojie.lrj

CREATE OR REPLACE PACKAGE UTL_HTTP AS
  TYPE req IS RECORD (
    url           VARCHAR2(32767),
    method        VARCHAR2(64),
    http_version  VARCHAR2(64),
    private_hndl	PLS_INTEGER
  );

  SUBTYPE request_context_key IS PLS_INTEGER;

  TYPE resp IS RECORD (
    status_code    PLS_INTEGER,
    reason_phrase  VARCHAR2(256),
    http_version   VARCHAR2(64),
    private_hndl   PLS_INTEGER
  );

  BAD_ARGUMENT                EXCEPTION;
  BAD_URL                     EXCEPTION;
  END_OF_BODY                 EXCEPTION;
  HTTP_CLIENT_ERROR           EXCEPTION;
  HTTP_SERVER_ERROR           EXCEPTION;
  NETWORK_ACCESS_DENIED       EXCEPTION;
  PARTIAL_MULTIBYTE_EXCEPTION EXCEPTION;
  PROTOCOL_ERROR              EXCEPTION;
  REQUEST_FAILED              EXCEPTION;
  TOO_MANY_REQUESTS           EXCEPTION;
  TRANSFER_TIMEOUT            EXCEPTION;
  UNKNOWN_SCHEME              EXCEPTION;

  PRAGMA EXCEPTION_INIT(BAD_ARGUMENT,                 -9813);
  PRAGMA EXCEPTION_INIT(BAD_URL,                      -9814);
  PRAGMA EXCEPTION_INIT(END_OF_BODY,                  -9815);
  PRAGMA EXCEPTION_INIT(HTTP_CLIENT_ERROR,            -9816);
  PRAGMA EXCEPTION_INIT(HTTP_SERVER_ERROR,            -9817);
  PRAGMA EXCEPTION_INIT(NETWORK_ACCESS_DENIED,        -9818);
  PRAGMA EXCEPTION_INIT(PARTIAL_MULTIBYTE_EXCEPTION,  -9819);
  PRAGMA EXCEPTION_INIT(PROTOCOL_ERROR,               -9820);
  PRAGMA EXCEPTION_INIT(REQUEST_FAILED,               -9821);
  PRAGMA EXCEPTION_INIT(TOO_MANY_REQUESTS,            -9822);
  PRAGMA EXCEPTION_INIT(TRANSFER_TIMEOUT,             -9823);
  PRAGMA EXCEPTION_INIT(UNKNOWN_SCHEME,               -9824);

  FUNCTION BEGIN_REQUEST (
    url               IN  VARCHAR2,
    method            IN  VARCHAR2 DEFAULT 'GET',
    http_version      IN  VARCHAR2 DEFAULT NULL,
    request_context   IN  request_context_key DEFAULT NULL,
    https_host        IN  VARCHAR2 DEFAULT NULL)
  RETURN req;

  PROCEDURE END_REQUEST (
    r  IN OUT NOCOPY req
  );

  FUNCTION GET_RESPONSE (
    r                       IN OUT NOCOPY req,
    return_info_response    IN BOOLEAN DEFAULT FALSE)
  RETURN resp;

  PROCEDURE END_RESPONSE (
    r  IN OUT NOCOPY resp
  );

  PROCEDURE READ_LINE(
    r            IN OUT NOCOPY resp,
    data         OUT NOCOPY  VARCHAR2,
    remove_crlf  IN  BOOLEAN DEFAULT FALSE
  );

  PROCEDURE READ_RAW(
    r     IN OUT NOCOPY resp,
    data  IN OUT NOCOPY RAW,
    len   IN PLS_INTEGER DEFAULT NULL
  );

  PROCEDURE READ_TEXT(
    r     IN OUT NOCOPY resp,
    data  IN OUT NOCOPY VARCHAR2,
    len   IN PLS_INTEGER DEFAULT NULL
  );

  PROCEDURE WRITE_LINE(
    r     IN OUT NOCOPY req,
    data  IN VARCHAR2
  );

  PROCEDURE WRITE_RAW(
    r     IN OUT NOCOPY REQ,
    data  IN            RAW
  );

  PROCEDURE WRITE_TEXT(
    r     IN OUT NOCOPY REQ,
    data  IN            VARCHAR2
  );

  PROCEDURE SET_TRANSFER_TIMEOUT(
    timeout IN PLS_INTEGER DEFAULT 60
  );

  PROCEDURE GET_TRANSFER_TIMEOUT(
    timeout OUT PLS_INTEGER
  );

END utl_http;
//