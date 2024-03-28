-- package_name: utl_recomp
-- author: webber.wb

CREATE OR REPLACE PACKAGE UTL_RECOMP AS
  USE_EXIST_TABLE   CONSTANT PLS_INTEGER := 512;
  DROP_EXIST_TABLE  CONSTANT PLS_INTEGER := 1024;

  PROCEDURE recomp_parallel(threads PLS_INTEGER := NULL,
                            schema  VARCHAR2    := NULL,
                            flags   PLS_INTEGER := 0);
  PROCEDURE recomp_serial(schema VARCHAR2 := NULL,
                          flags PLS_INTEGER := 0);
  PROCEDURE parallel_slave(flags PLS_INTEGER);
  PROCEDURE truncate_utl_recomp_skip_list(flags PLS_INTEGER := 0);
  PROCEDURE populate_utl_recomp_skip_list(flags PLS_INTEGER := 0);
END;
//
