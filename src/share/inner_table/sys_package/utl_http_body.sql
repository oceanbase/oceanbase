#package_name:utl_http
#author: yaojie.lrj

CREATE OR REPLACE PACKAGE BODY UTL_HTTP IS

  FUNCTION BEGIN_REQUEST_I (
    url               IN  VARCHAR2,
    method            IN  VARCHAR2 DEFAULT 'GET',
    http_version      IN  VARCHAR2 DEFAULT NULL,
    request_context   IN  request_context_key DEFAULT NULL,
    https_host        IN  VARCHAR2 DEFAULT NULL)
  RETURN PLS_INTEGER;
  PRAGMA INTERFACE(c, UTL_HTTP_BEGIN_REQUEST);

  FUNCTION BEGIN_REQUEST (
    url               IN  VARCHAR2,
    method            IN  VARCHAR2 DEFAULT 'GET',
    http_version      IN  VARCHAR2 DEFAULT NULL,
    request_context   IN  request_context_key DEFAULT NULL,
    https_host        IN  VARCHAR2 DEFAULT NULL)
  RETURN req AS
  r req;
  BEGIN
    r.private_hndl := BEGIN_REQUEST_I(
      url, method, http_version, request_context, https_host
    );
    r.url := url;
    r.method := method;
    r.http_version := http_version;

    RETURN r;
  END;

  PROCEDURE END_REQUEST_I (
    private_hndl        IN  PLS_INTEGER
  );
  PRAGMA INTERFACE(c, UTL_HTTP_END_REQUEST);

  PROCEDURE END_REQUEST (
    r  IN OUT NOCOPY req
  ) AS
  BEGIN
    END_REQUEST_I(r.private_hndl);
  END;

  FUNCTION GET_RESPONSE_I (
    private_hndl             IN PLS_INTEGER,
    reason_phrase            IN OUT VARCHAR2,
    http_version             IN OUT VARCHAR2,
    return_info_response     IN BOOLEAN DEFAULT FALSE)
  RETURN PLS_INTEGER;
  PRAGMA INTERFACE(c, UTL_HTTP_GET_RESPONSE);

  FUNCTION GET_RESPONSE (
    r                       IN OUT NOCOPY req,
    return_info_response    IN BOOLEAN DEFAULT FALSE)
  RETURN resp AS
  res resp;
  BEGIN
    res.status_code := GET_RESPONSE_I(r.private_hndl, res.reason_phrase, res.http_version, return_info_response);
    res.private_hndl := r.private_hndl;
    RETURN res;
  END;

  PROCEDURE END_RESPONSE_I (
    private_hndl  IN PLS_INTEGER
  );
  PRAGMA INTERFACE(c, UTL_HTTP_END_RESPONSE);

  PROCEDURE END_RESPONSE (
    r  IN OUT NOCOPY resp
  ) AS
  BEGIN
    END_RESPONSE_I(r.private_hndl);
  END;

  PROCEDURE READ_LINE_I(
    private_hndl IN PLS_INTEGER,
    data         OUT NOCOPY  VARCHAR2 CHARACTER SET ANY_CS,
    remove_crlf  IN  BOOLEAN DEFAULT FALSE
  );
  PRAGMA INTERFACE(c, UTL_HTTP_READ_LINE);

  PROCEDURE READ_LINE(
    r            IN OUT NOCOPY resp,
    data         OUT NOCOPY  VARCHAR2,
    remove_crlf  IN  BOOLEAN DEFAULT FALSE
  ) AS
  BEGIN
    READ_LINE_I(r.private_hndl, data, remove_crlf);
  END;

  PROCEDURE READ_RAW_I(
    private_hndl      IN PLS_INTEGER,
    data              IN OUT NOCOPY RAW,
    len               IN PLS_INTEGER DEFAULT NULL
  );
  PRAGMA INTERFACE(c, UTL_HTTP_READ_RAW);

  PROCEDURE READ_RAW(
    r     IN OUT NOCOPY resp,
    data  IN OUT NOCOPY RAW,
    len   IN PLS_INTEGER DEFAULT NULL
  )AS
  BEGIN
    READ_RAW_I(r.private_hndl, data, len);
  END;

  PROCEDURE READ_TEXT_I(
    private_hndl      IN PLS_INTEGER,
    data              IN OUT NOCOPY VARCHAR2 CHARACTER SET ANY_CS,
    len               IN PLS_INTEGER DEFAULT NULL
  );
  PRAGMA INTERFACE(c, UTL_HTTP_READ_TEXT);

  PROCEDURE READ_TEXT(
    r     IN OUT NOCOPY resp,
    data  IN OUT NOCOPY VARCHAR2,
    len   IN PLS_INTEGER DEFAULT NULL
  ) AS
  BEGIN
    READ_TEXT_I(r.private_hndl, data, len);
  END;

  PROCEDURE WRITE_LINE_I(
    private_hndl     IN PLS_INTEGER,
    data             IN VARCHAR2 CHARACTER SET ANY_CS
  );
  PRAGMA INTERFACE(c, UTL_HTTP_WRITE_LINE);

  PROCEDURE WRITE_LINE(
    r     IN OUT NOCOPY req,
    data  IN VARCHAR2
  ) AS
  BEGIN
    WRITE_LINE_I(r.private_hndl, data);
  END;

  PROCEDURE WRITE_RAW_I(
    private_hndl     IN PLS_INTEGER,
    data             IN RAW
  );
  PRAGMA INTERFACE(c, UTL_HTTP_WRITE_RAW);

  PROCEDURE WRITE_RAW(
    r     IN OUT NOCOPY REQ,
    data  IN            RAW
  ) AS
  BEGIN
    WRITE_RAW_I(r.private_hndl, data);
  END;

  PROCEDURE WRITE_TEXT_I(
    private_hndl        IN PLS_INTEGER,
    data  IN            VARCHAR2 CHARACTER SET ANY_CS
  );
  PRAGMA INTERFACE(c, UTL_HTTP_WRITE_TEXT);

  PROCEDURE WRITE_TEXT(
    r     IN OUT NOCOPY REQ,
    data  IN            VARCHAR2
  ) AS
  BEGIN
    WRITE_TEXT_I(r.private_hndl, data);
  END;

  PROCEDURE SET_TRANSFER_TIMEOUT(
    timeout IN PLS_INTEGER DEFAULT 60
  );
  PRAGMA INTERFACE(c, UTL_HTTP_SET_TRANSFER_TIMEOUT);

  PROCEDURE GET_TRANSFER_TIMEOUT(
    timeout OUT PLS_INTEGER
  );
  PRAGMA INTERFACE(c, UTL_HTTP_GET_TRANSFER_TIMEOUT);

END UTL_HTTP;
//