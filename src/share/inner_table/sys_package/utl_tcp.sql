#package_name:utl_tcp
#author: xinning.lf

CREATE OR REPLACE PACKAGE utl_tcp AUTHID CURRENT_USER AS

  TYPE connection IS RECORD (
    remote_host   VARCHAR2(255),
    remote_port   PLS_INTEGER,
    local_host    VARCHAR2(255),
    local_port    PLS_INTEGER,
    charset       VARCHAR2(30),
    newline       VARCHAR2(2),
    tx_timeout    PLS_INTEGER,
    private_sd    PLS_INTEGER
  );

  CRLF CONSTANT VARCHAR2(2 CHAR) := unistr('\000D\000A');

  buffer_too_small       EXCEPTION;
  end_of_input           EXCEPTION;
  network_error          EXCEPTION;
  bad_argument           EXCEPTION;
  partial_multibyte_char EXCEPTION;
  transfer_timeout       EXCEPTION;
  network_access_denied  EXCEPTION;

  buffer_too_small_errcode       CONSTANT PLS_INTEGER:= -9785;
  end_of_input_errcode           CONSTANT PLS_INTEGER:= -9786;
  network_error_errcode          CONSTANT PLS_INTEGER:= -9787;
  bad_argument_errcode           CONSTANT PLS_INTEGER:= -9788;
  partial_multibyte_char_errcode CONSTANT PLS_INTEGER:= -9789;
  transfer_timeout_errcode       CONSTANT PLS_INTEGER:= -9790;
  network_access_denied_errcode  CONSTANT PLS_INTEGER:= -9791;

  PRAGMA EXCEPTION_INIT(buffer_too_small,      -9785);
  PRAGMA EXCEPTION_INIT(end_of_input,          -9786);
  PRAGMA EXCEPTION_INIT(network_error,         -9787);
  PRAGMA EXCEPTION_INIT(bad_argument,          -9788);
  PRAGMA EXCEPTION_INIT(partial_multibyte_char,-9789);
  PRAGMA EXCEPTION_INIT(transfer_timeout,      -9790);
  PRAGMA EXCEPTION_INIT(network_access_denied, -9791);

  FUNCTION open_connection(remote_host     IN VARCHAR2,
                           remote_port     IN PLS_INTEGER,
                           local_host      IN VARCHAR2    DEFAULT NULL,
                           local_port      IN PLS_INTEGER DEFAULT NULL,
                           in_buffer_size  IN PLS_INTEGER DEFAULT NULL,
                           out_buffer_size IN PLS_INTEGER DEFAULT NULL,
                           charset         IN VARCHAR2    DEFAULT NULL,
                           newline         IN VARCHAR2    DEFAULT CRLF,
                           tx_timeout      IN PLS_INTEGER DEFAULT NULL,
                           wallet_path     IN VARCHAR2    DEFAULT NULL,
                           wallet_password IN VARCHAR2    DEFAULT NULL)
                           RETURN connection;



  FUNCTION write_line(c    IN OUT NOCOPY connection,
                      data IN            VARCHAR2 DEFAULT NULL)
                      RETURN PLS_INTEGER;

  FUNCTION write_text(c    IN OUT NOCOPY connection,
                      data IN            VARCHAR2,
                      len  IN            PLS_INTEGER DEFAULT NULL)
                      RETURN PLS_INTEGER;

  FUNCTION write_raw(c    IN OUT NOCOPY connection,
                     data IN            RAW,
                     len  IN            PLS_INTEGER DEFAULT NULL)
                     RETURN PLS_INTEGER;

  FUNCTION get_line(c           IN OUT NOCOPY connection,
                    remove_crlf IN            BOOLEAN DEFAULT false,
                    peek        IN            BOOLEAN DEFAULT FALSE)
                    RETURN VARCHAR2;

  FUNCTION get_text(c    IN OUT NOCOPY connection,
                    len  IN            PLS_INTEGER DEFAULT 1,
                    peek IN            BOOLEAN     DEFAULT FALSE)
                    RETURN VARCHAR2;

  PROCEDURE close_connection(c IN OUT NOCOPY connection);

  PROCEDURE close_all_connections;

END UTL_TCP;
//
