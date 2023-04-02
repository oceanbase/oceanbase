#package_name:DBMS_RLS
#author: sean.yyj

CREATE OR REPLACE PACKAGE BODY DBMS_RLS AS

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
  PRAGMA INTERFACE(c, rls_add_grouped_policy);

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
  PRAGMA INTERFACE(c, rls_add_policy);

  PROCEDURE add_policy_context(object_schema   IN VARCHAR2 := NULL,
                               object_name     IN VARCHAR2,
                               namespace       IN VARCHAR2,
                               attribute       IN VARCHAR2);
  PRAGMA INTERFACE(c, rls_add_policy_context);

  PROCEDURE alter_policy(object_schema IN VARCHAR2 := NULL,
                         object_name     IN VARCHAR2,
                         policy_name     IN VARCHAR2,
                         alter_option    IN BINARY_INTEGER := NULL,
                         namespace       IN VARCHAR2,
                         attribute       IN VARCHAR2);
  PRAGMA INTERFACE(c, rls_alter_policy);

  PROCEDURE alter_grouped_policy(object_schema   IN VARCHAR2 := NULL,
                                 object_name     IN VARCHAR2,
                                 policy_group    IN VARCHAR2 := 'SYS_DEFAULT',
                                 policy_name     IN VARCHAR2,
                                 alter_option    IN BINARY_INTEGER := NULL,
                                 namespace       IN VARCHAR2,
                                 attribute       IN VARCHAR2);
  PRAGMA INTERFACE(c, rls_alter_grouped_policy);

  PROCEDURE create_policy_group(object_schema IN VARCHAR2 := NULL,
                                object_name   IN VARCHAR2,
                                policy_group  IN VARCHAR2);
  PRAGMA INTERFACE(c, rls_create_policy_group);

  PROCEDURE delete_policy_group(object_schema IN VARCHAR2 := NULL,
                                object_name   IN VARCHAR2,
                                policy_group  IN VARCHAR2);
  PRAGMA INTERFACE(c, rls_delete_policy_group);

  PROCEDURE disable_grouped_policy(object_schema IN VARCHAR2 := NULL,
                                   object_name   IN VARCHAR2,
                                   group_name    IN VARCHAR2,
                                   policy_name   IN VARCHAR2);
  PRAGMA INTERFACE(c, rls_disable_grouped_policy);

  PROCEDURE drop_grouped_policy(object_schema IN VARCHAR2 := NULL,
                                object_name   IN VARCHAR2,
                                policy_group  IN VARCHAR2 := 'SYS_DEFAULT',
                                policy_name   IN VARCHAR2);
  PRAGMA INTERFACE(c, rls_drop_grouped_policy);

  PROCEDURE drop_policy(object_schema IN VARCHAR2 := NULL,
                        object_name   IN VARCHAR2,
                        policy_name   IN VARCHAR2);
  PRAGMA INTERFACE(c, rls_drop_policy);

  PROCEDURE drop_policy_context(object_schema   IN VARCHAR2 := NULL,
                                object_name     IN VARCHAR2,
                                namespace       IN VARCHAR2,
                                attribute       IN VARCHAR2);
  PRAGMA INTERFACE(c, rls_drop_policy_context);

  PROCEDURE enable_grouped_policy(object_schema IN VARCHAR2 := NULL,
                                  object_name   IN VARCHAR2,
                                  group_name    IN VARCHAR2,
                                  policy_name   IN VARCHAR2,
                                  enable        IN BOOLEAN := TRUE);
  PRAGMA INTERFACE(c, rls_enable_grouped_policy);

  PROCEDURE enable_policy(object_schema IN VARCHAR2 := NULL,
                          object_name   IN VARCHAR2,
                          policy_name   IN VARCHAR2,
                          enable        IN BOOLEAN := TRUE );
  PRAGMA INTERFACE(c, rls_enable_policy);

  PROCEDURE refresh_grouped_policy(object_schema IN VARCHAR2 := NULL,
                                   object_name   IN VARCHAR2 := NULL,
                                   group_name    IN VARCHAR2 := NULL,
                                   policy_name   IN VARCHAR2 := NULL);
  PRAGMA INTERFACE(c, rls_refresh_grouped_policy);

  PROCEDURE refresh_policy(object_schema IN VARCHAR2 := NULL,
                           object_name   IN VARCHAR2 := NULL,
                           policy_name   IN VARCHAR2 := NULL);
  PRAGMA INTERFACE(c, rls_refresh_policy);

END DBMS_RLS
//
