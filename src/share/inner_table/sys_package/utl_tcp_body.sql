#package_name:utl_tcp
#auth: xinning.lf

CREATE OR REPLACE PACKAGE BODY UTL_TCP IS

  FUNCTION OPEN_CONNECTION_I(REMOTE_HOST     IN VARCHAR2,
                             REMOTE_PORT     IN PLS_INTEGER,
                             IN_BUFFER_SIZE  IN PLS_INTEGER,
                             OUT_BUFFER_SIZE IN PLS_INTEGER,
                             CHARSET         IN VARCHAR2,
                             NEWLINE         IN VARCHAR2,
                             TX_TIMEOUT      IN PLS_INTEGER,
                             WALLET_PATH     IN VARCHAR2,
                             WALLET_PASSWORD IN VARCHAR2)
                             RETURN PLS_INTEGER;
  PRAGMA INTERFACE(c, utl_tcp_open_connection);

  FUNCTION OPEN_CONNECTION(REMOTE_HOST     IN VARCHAR2,
                           REMOTE_PORT     IN PLS_INTEGER,
                           LOCAL_HOST      IN VARCHAR2    DEFAULT NULL,
                           LOCAL_PORT      IN PLS_INTEGER DEFAULT NULL,
                           IN_BUFFER_SIZE  IN PLS_INTEGER DEFAULT NULL,
                           OUT_BUFFER_SIZE IN PLS_INTEGER DEFAULT NULL,
                           CHARSET         IN VARCHAR2    DEFAULT NULL,
                           NEWLINE         IN VARCHAR2    DEFAULT CRLF,
                           TX_TIMEOUT      IN PLS_INTEGER DEFAULT NULL,
                           WALLET_PATH     IN VARCHAR2    DEFAULT NULL,
                           WALLET_PASSWORD IN VARCHAR2    DEFAULT NULL)
                           RETURN CONNECTION AS

  C CONNECTION;
  BEGIN
    C.PRIVATE_SD := OPEN_CONNECTION_I(REMOTE_HOST, REMOTE_PORT, IN_BUFFER_SIZE, OUT_BUFFER_SIZE,
                                     CHARSET, NEWLINE, TX_TIMEOUT, WALLET_PATH, WALLET_PASSWORD);
    C.LOCAL_HOST := NULL;
    C.LOCAL_PORT := NULL;
    C.REMOTE_HOST := REMOTE_HOST;
    C.REMOTE_PORT := REMOTE_PORT;
    C.CHARSET := CHARSET;
    C.NEWLINE := NEWLINE;

    RETURN C;
  END;

  PROCEDURE CLOSE_CONNECTION_I(SOCK IN PLS_INTEGER);
  PRAGMA INTERFACE(C, utl_tcp_close_connection);

  PROCEDURE CLOSE_CONNECTION(C IN OUT NOCOPY CONNECTION) IS
  BEGIN
    CLOSE_CONNECTION_I(C.PRIVATE_SD);
    C.PRIVATE_SD := NULL;
    C.REMOTE_HOST := NULL;
    C.REMOTE_PORT := NULL;
    C.LOCAL_HOST := NULL;
    C.LOCAL_PORT := NULL;
    C.CHARSET := NULL;
  END;

  PROCEDURE CLOSE_ALL_CONNECTIONS_I;
  PRAGMA INTERFACE(C, utl_tcp_close_all_connections);

  PROCEDURE CLOSE_ALL_CONNECTIONS AS
  BEGIN
    CLOSE_ALL_CONNECTIONS_I;
  END;

  FUNCTION WRITE_LINE_I(SOCK    IN PLS_INTEGER,
                        DATA    IN VARCHAR2 CHARACTER SET ANY_CS,
                        NEWLINE IN VARCHAR2)
                        RETURN PLS_INTEGER;
  PRAGMA INTERFACE(C, utl_tcp_write_line);

  FUNCTION WRITE_LINE(C    IN OUT NOCOPY CONNECTION,
                      DATA IN            VARCHAR2 DEFAULT NULL)
                      RETURN PLS_INTEGER IS
  BEGIN
    RETURN WRITE_LINE_I(C.PRIVATE_SD, DATA, C.NEWLINE);
  END;

  FUNCTION WRITE_TEXT_I(SOCK IN PLS_INTEGER,
                        DATA IN VARCHAR2 CHARACTER SET ANY_CS,
                        LEN  IN PLS_INTEGER)
                        RETURN PLS_INTEGER;
  PRAGMA INTERFACE(c, utl_tcp_write_text);

  FUNCTION WRITE_TEXT(C    IN OUT NOCOPY CONNECTION,
                      DATA IN VARCHAR2,
                      LEN  IN PLS_INTEGER DEFAULT NULL)
                      RETURN PLS_INTEGER IS
  BEGIN
    RETURN WRITE_TEXT_I(C.PRIVATE_SD, DATA, LEN);
  END;

  FUNCTION WRITE_RAW_I(SOCK IN  PLS_INTEGER,
                       DATA IN  RAW,
                       LEN  IN  PLS_INTEGER)
                       RETURN PLS_INTEGER;
  PRAGMA INTERFACE(c, utl_tcp_write_raw);

  FUNCTION WRITE_RAW(C    IN OUT NOCOPY CONNECTION,
                     DATA IN            RAW,
                     LEN  IN            PLS_INTEGER DEFAULT NULL)
                     RETURN PLS_INTEGER IS
  BEGIN
    RETURN WRITE_RAW_I(C.PRIVATE_SD, DATA, LEN);
  END;

  FUNCTION READ_LINE_I(SOCK        IN PLS_INTEGER,
                       DATA        IN OUT NOCOPY VARCHAR2 CHARACTER SET ANY_CS,
                       REMOVE_CRLF IN            BOOLEAN,
                       PEEK        IN            BOOLEAN)
                       RETURN PLS_INTEGER;
  PRAGMA INTERFACE(c, utl_tcp_read_line);

  FUNCTION GET_LINE(C        IN OUT NOCOPY CONNECTION,
                    REMOVE_CRLF IN            BOOLEAN DEFAULT FALSE,
                    PEEK        IN            BOOLEAN DEFAULT FALSE)
                    RETURN VARCHAR2 IS
    BUF   VARCHAR2(32767);
    DUMMY_RET PLS_INTEGER;
  BEGIN
    DUMMY_RET := READ_LINE_I(C.PRIVATE_SD, BUF, REMOVE_CRLF, PEEK);
    return BUF;
  END;

  FUNCTION READ_TEXT_I(SOCK IN            PLS_INTEGER,
                       DATA IN OUT NOCOPY VARCHAR2 CHARACTER SET ANY_CS,
                       LEN  IN            PLS_INTEGER,
                       PEEK IN            BOOLEAN)
                       RETURN PLS_INTEGER;
  PRAGMA INTERFACE(c, utl_tcp_read_text);

  FUNCTION GET_TEXT(C    IN OUT NOCOPY CONNECTION,
                    LEN  IN            PLS_INTEGER DEFAULT 1,
                    PEEK IN            BOOLEAN     DEFAULT FALSE)
                    RETURN VARCHAR2 IS
    BUF   VARCHAR2(32767);
    DUMMY_RET PLS_INTEGER;
  BEGIN
    DUMMY_RET := READ_TEXT_I(C.PRIVATE_SD, BUF, LEN, PEEK);
    RETURN BUF;
  END;

END UTL_TCP;
//