#package_name:DBMS_RLS
#author: sean.yyj

CREATE OR REPLACE PACKAGE DBMS_RLS AUTHID CURRENT_USER AS

  STATIC                     CONSTANT   BINARY_INTEGER := 1;
  SHARED_STATIC              CONSTANT   BINARY_INTEGER := 2;
  CONTEXT_SENSITIVE          CONSTANT   BINARY_INTEGER := 3;
  SHARED_CONTEXT_SENSITIVE   CONSTANT   BINARY_INTEGER := 4;
  DYNAMIC                    CONSTANT   BINARY_INTEGER := 5;
  XDS1                       CONSTANT   BINARY_INTEGER := 6;
  XDS2                       CONSTANT   BINARY_INTEGER := 7;
  XDS3                       CONSTANT   BINARY_INTEGER := 8;
  OLS                        CONSTANT   BINARY_INTEGER := 9;

  ALL_ROWS                   CONSTANT   BINARY_INTEGER := 1;

  XDS_ON_COMMIT_MV  CONSTANT BINARY_INTEGER := 0;
  XDS_ON_DEMAND_MV  CONSTANT BINARY_INTEGER := 1;
  XDS_SCHEDULED_MV  CONSTANT BINARY_INTEGER := 2;

  XDS_SYSTEM_GENERATED_MV  CONSTANT BINARY_INTEGER := 0;
  XDS_USER_SPECIFIED_MV    CONSTANT BINARY_INTEGER := 1;

  ADD_ATTRIBUTE_ASSOCIATION       CONSTANT   BINARY_INTEGER := 1;
  REMOVE_ATTRIBUTE_ASSOCIATION    CONSTANT   BINARY_INTEGER := 2;

  PROCEDURE add_grouped_policy(object_schema   IN VARCHAR2 := NULL,
                               object_name     IN VARCHAR2,
                               policy_group    IN VARCHAR2 := 'SYS_DEFAULT',
                               policy_name     IN VARCHAR2,
                               function_schema IN VARCHAR2 := NULL,
                               policy_function IN VARCHAR2,
                               statement_types IN VARCHAR2 := NULL,
                               update_check    IN BOOLEAN  := FALSE,
                               enable          IN BOOLEAN  := TRUE,
                               static_policy   IN BOOLEAN  := FALSE,
                               policy_type     IN BINARY_INTEGER := NULL,
                               long_predicate  IN BOOLEAN  := FALSE,
                               sec_relevant_cols IN VARCHAR2  := NULL,
                               sec_relevant_cols_opt IN BINARY_INTEGER := NULL,
                               namespace       IN VARCHAR2 := NULL,
                               attribute       IN VARCHAR2 := NULL);

  PROCEDURE add_policy(object_schema   IN VARCHAR2 := NULL,
                       object_name     IN VARCHAR2,
                       policy_name     IN VARCHAR2,
                       function_schema IN VARCHAR2 := NULL,
                       policy_function IN VARCHAR2,
                       statement_types IN VARCHAR2 := NULL,
                       update_check    IN BOOLEAN  := FALSE,
                       enable          IN BOOLEAN  := TRUE,
                       static_policy   IN BOOLEAN  := FALSE,
                       policy_type     IN BINARY_INTEGER := NULL,
                       long_predicate  IN BOOLEAN  := FALSE,
                       sec_relevant_cols IN VARCHAR2  := NULL,
                       sec_relevant_cols_opt IN BINARY_INTEGER := NULL,
                       namespace       IN VARCHAR2 := NULL,
                       attribute       IN VARCHAR2 := NULL);

  PROCEDURE add_policy_context(object_schema   IN VARCHAR2 := NULL,
                               object_name     IN VARCHAR2,
                               namespace       IN VARCHAR2,
                               attribute       IN VARCHAR2);

  PROCEDURE alter_policy(object_schema IN VARCHAR2 := NULL,
                         object_name     IN VARCHAR2,
                         policy_name     IN VARCHAR2,
                         alter_option    IN BINARY_INTEGER := NULL,
                         namespace       IN VARCHAR2,
                         attribute       IN VARCHAR2);

  PROCEDURE alter_grouped_policy(object_schema   IN VARCHAR2 := NULL,
                                 object_name     IN VARCHAR2,
                                 policy_group    IN VARCHAR2 := 'SYS_DEFAULT',
                                 policy_name     IN VARCHAR2,
                                 alter_option    IN BINARY_INTEGER := NULL,
                                 namespace       IN VARCHAR2,
                                 attribute       IN VARCHAR2);

  PROCEDURE create_policy_group(object_schema IN VARCHAR2 := NULL,
                                object_name   IN VARCHAR2,
                                policy_group  IN VARCHAR2);

  PROCEDURE delete_policy_group(object_schema IN VARCHAR2 := NULL,
                                object_name   IN VARCHAR2,
                                policy_group  IN VARCHAR2);

  PROCEDURE disable_grouped_policy(object_schema IN VARCHAR2 := NULL,
                                   object_name   IN VARCHAR2,
                                   group_name    IN VARCHAR2,
                                   policy_name   IN VARCHAR2);

  PROCEDURE drop_grouped_policy(object_schema IN VARCHAR2 := NULL,
                                object_name   IN VARCHAR2,
                                policy_group  IN VARCHAR2 := 'SYS_DEFAULT',
                                policy_name   IN VARCHAR2);

  PROCEDURE drop_policy(object_schema IN VARCHAR2 := NULL,
                        object_name   IN VARCHAR2,
                        policy_name   IN VARCHAR2);

  PROCEDURE drop_policy_context(object_schema   IN VARCHAR2 := NULL,
                                object_name     IN VARCHAR2,
                                namespace       IN VARCHAR2,
                                attribute       IN VARCHAR2);

  PROCEDURE enable_grouped_policy(object_schema IN VARCHAR2 := NULL,
                                  object_name   IN VARCHAR2,
                                  group_name    IN VARCHAR2,
                                  policy_name   IN VARCHAR2,
                                  enable        IN BOOLEAN := TRUE);

  PROCEDURE enable_policy(object_schema IN VARCHAR2 := NULL,
                          object_name   IN VARCHAR2,
                          policy_name   IN VARCHAR2,
                          enable        IN BOOLEAN := TRUE );

  PROCEDURE refresh_grouped_policy(object_schema IN VARCHAR2 := NULL,
                                   object_name   IN VARCHAR2 := NULL,
                                   group_name    IN VARCHAR2 := NULL,
                                   policy_name   IN VARCHAR2 := NULL);

  PROCEDURE refresh_policy(object_schema IN VARCHAR2 := NULL,
                           object_name   IN VARCHAR2 := NULL,
                           policy_name   IN VARCHAR2 := NULL);

END DBMS_RLS;
//
