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

  -- security relevant columns options, default is null
  ALL_ROWS                   CONSTANT   BINARY_INTEGER := 1;

  -- Type of refresh on static acl mv
  XDS_ON_COMMIT_MV  CONSTANT BINARY_INTEGER := 0;
  XDS_ON_DEMAND_MV  CONSTANT BINARY_INTEGER := 1;
  XDS_SCHEDULED_MV  CONSTANT BINARY_INTEGER := 2;

  -- Type of static acl mv
  XDS_SYSTEM_GENERATED_MV  CONSTANT BINARY_INTEGER := 0;
  XDS_USER_SPECIFIED_MV    CONSTANT BINARY_INTEGER := 1;

  -- alter options for a row level security policy
  ADD_ATTRIBUTE_ASSOCIATION       CONSTANT   BINARY_INTEGER := 1;
  REMOVE_ATTRIBUTE_ASSOCIATION    CONSTANT   BINARY_INTEGER := 2;


  -- ------------------------------------------------------------------------
  -- add_grouped_policy -  add a row level security policy to a policy group
  --                        for a table or view
  --
  -- INPUT PARAMETERS
  --   object_schema   - schema owning the table/view, current user if NULL
  --   object_name     - name of table or view
  --   policy_group    - name of group of the policy to be added
  --   policy_name     - name of policy to be added
  --   function_schema - schema of the policy function, current user if NULL
  --   policy_function - function to generate predicates for this policy
  --   statement_types - statement type that the policy apply, default is any
  --   update_check    - policy checked against updated or inserted value?
  --   enable          - policy is enabled?
  --   static_policy   - policy is static (predicate is always the same)?
  --   policy_type     - policy type - overwrite static_policy if non-null
  --   long_predicate  - max predicate length 4000 bytes (default) or 32K
  --   sec_relevant_cols - list of security relevant columns
  --   sec_relevant_cols_opt - security relevant columns option
  --   namespace       - name of application context namespace
  --   attribute       - name of application context attribute

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

  -- ------------------------------------------------------------------------
  -- add_policy -  add a row level security policy to a table or view
  --
  -- INPUT PARAMETERS
  --   object_schema   - schema owning the table/view, current user if NULL
  --   object_name     - name of table or view
  --   policy_name     - name of policy to be added
  --   function_schema - schema of the policy function, current user if NULL
  --   policy_function - function to generate predicates for this policy
  --   statement_types - statement type that the policy apply, default is any
  --   update_check    - policy checked against updated or inserted value?
  --   enable          - policy is enabled?
  --   static_policy   - policy is static (predicate is always the same)?
  --   policy_type     - policy type - overwrite static_policy if non-null
  --   long_predicate  - max predicate length 4000 bytes (default) or 32K
  --   sec_relevant_cols - list of security relevant columns
  --   sec_relevant_cols_opt - security relevant column option
  --   namespace       - name of application context namespace
  --   attribute       - name of application context attribute

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

  -- ------------------------------------------------------------------------
  -- add_policy_context -  add a driving context to a table or view
  --
  -- INPUT PARAMETERS
  --   object_schema   - schema owning the table/view, current user if NULL
  --   object_name     - name of table or view
  --   namespace       - namespace of driving context
  --   attribute       - attribute of driving context

  PROCEDURE add_policy_context(object_schema   IN VARCHAR2 := NULL,
                               object_name     IN VARCHAR2,
                               namespace       IN VARCHAR2,
                               attribute       IN VARCHAR2);

  -- ------------------------------------------------------------------------
  -- alter_policy -  alter a row level security policy
  --
  -- INPUT PARAMETERS
  --   object_schema   - schema owning the table/view, current user if NULL
  --   object_name     - name of table or view
  --   policy_name     - name of policy to be added
  --   alter_option    - addition/removal of attribute association
  --   namespace       - name of application context namespace
  --   attribute       - name of application context attribute

  PROCEDURE alter_policy(object_schema IN VARCHAR2 := NULL,
                         object_name     IN VARCHAR2,
                         policy_name     IN VARCHAR2,
                         alter_option    IN BINARY_INTEGER := NULL,
                         namespace       IN VARCHAR2,
                         attribute       IN VARCHAR2);

  -- ------------------------------------------------------------------------
  -- alter_grouped_policy -  alter a row level security policy of a
  --                         policy group
  --
  -- INPUT PARAMETERS
  --   object_schema   - schema owning the table/view, current user if NULL
  --   object_name     - name of table or view
  --   policy_group    - name of group of the policy to be altered
  --   policy_name     - name of policy to be altered
  --   alter_option    - addition/removal of attribute association
  --   namespace       - name of application context namespace
  --   attribute       - name of application context attribute

  PROCEDURE alter_grouped_policy(object_schema   IN VARCHAR2 := NULL,
                                 object_name     IN VARCHAR2,
                                 policy_group    IN VARCHAR2 := 'SYS_DEFAULT',
                                 policy_name     IN VARCHAR2,
                                 alter_option    IN BINARY_INTEGER := NULL,
                                 namespace       IN VARCHAR2,
                                 attribute       IN VARCHAR2);

  -- ------------------------------------------------------------------------
  -- create_policy_group - create a policy group for a table or view
  --
  -- INPUT PARAMETERS
  --   object_schema   - schema owning the table/view, current user if NULL
  --   object_name     - name of table or view
  --   policy_group    - name of policy group to be created

  PROCEDURE create_policy_group(object_schema IN VARCHAR2 := NULL,
                                object_name   IN VARCHAR2,
                                policy_group  IN VARCHAR2);

  -- ------------------------------------------------------------------------
  -- delete_policy_group - drop a policy group for a table or view
  --
  -- INPUT PARAMETERS
  --   object_schema   - schema owning the table/view, current user if NULL
  --   object_name     - name of table or view
  --   policy_group    - name of policy group to be dropped

  PROCEDURE delete_policy_group(object_schema IN VARCHAR2 := NULL,
                                object_name   IN VARCHAR2,
                                policy_group  IN VARCHAR2);

  -- ------------------------------------------------------------------------
  -- disable_grouped_policy - enable or disable a policy for a table or view
  --
  -- INPUT PARAMETERS
  --   object_schema   - schema owning the table/view, current user if NULL
  --   object_name     - name of table or view
  --   group_name    - name of group of the policy to be refreshed
  --   policy_name     - name of policy to be enabled or disabled

  PROCEDURE disable_grouped_policy(object_schema IN VARCHAR2 := NULL,
                                   object_name   IN VARCHAR2,
                                   group_name    IN VARCHAR2,
                                   policy_name   IN VARCHAR2);

  -- ------------------------------------------------------------------------
  -- drop_grouped_policy - drop a row level security policy from a policy
  --                          group of a table or view
  --
  -- INPUT PARAMETERS
  --   object_schema   - schema owning the table/view, current user if NULL
  --   object_name     - name of table or view
  --   policy_group    - name of policy to be dropped
  --   policy_name     - name of policy to be dropped

  PROCEDURE drop_grouped_policy(object_schema IN VARCHAR2 := NULL,
                                object_name   IN VARCHAR2,
                                policy_group  IN VARCHAR2 := 'SYS_DEFAULT',
                                policy_name   IN VARCHAR2);

  -- ------------------------------------------------------------------------
  -- drop_policy - drop a row level security policy from a table or view
  --
  -- INPUT PARAMETERS
  --   object_schema   - schema owning the table/view, current user if NULL
  --   object_name     - name of table or view
  --   policy_name     - name of policy to be dropped

  PROCEDURE drop_policy(object_schema IN VARCHAR2 := NULL,
                        object_name   IN VARCHAR2,
                        policy_name   IN VARCHAR2);

  -- ------------------------------------------------------------------------
  -- drop_policy_context -  drop a driving context from a table or view
  --
  -- INPUT PARAMETERS
  --   object_schema   - schema owning the table/view, current user if NULL
  --   object_name     - name of table or view
  --   namespace       - namespace of driving context
  --   attribute       - attribute of driving context

  PROCEDURE drop_policy_context(object_schema   IN VARCHAR2 := NULL,
                                object_name     IN VARCHAR2,
                                namespace       IN VARCHAR2,
                                attribute       IN VARCHAR2);

  -- ------------------------------------------------------------------------
  -- enable_grouped_policy - enable or disable a policy for a table or view
  --
  -- INPUT PARAMETERS
  --   object_schema   - schema owning the table/view, current user if NULL
  --   object_name     - name of table or view
  --   group_name      - name of group of the policy to be enabled or disabled
  --   policy_name     - name of policy to be enabled or disabled
  --   enable          - TRUE to enable the policy, FALSE to disable the policy

  PROCEDURE enable_grouped_policy(object_schema IN VARCHAR2 := NULL,
                                  object_name   IN VARCHAR2,
                                  group_name    IN VARCHAR2,
                                  policy_name   IN VARCHAR2,
                                  enable        IN BOOLEAN := TRUE);

  -- ------------------------------------------------------------------------
  -- enable_policy - enable or disable a security policy for a table or view
  --
  -- INPUT PARAMETERS
  --   object_schema   - schema owning the table/view, current user if NULL
  --   object_name     - name of table or view
  --   policy_name     - name of policy to be enabled or disabled
  --   enable          - TRUE to enable the policy, FALSE to disable the policy

  PROCEDURE enable_policy(object_schema IN VARCHAR2 := NULL,
                          object_name   IN VARCHAR2,
                          policy_name   IN VARCHAR2,
                          enable        IN BOOLEAN := TRUE );

  -- ------------------------------------------------------------------------
  -- refresh_grouped_policy - invalidate all cursors associated with the policy
  --                  if no argument provides, all cursors with
  --                  policies involved will be invalidated
  --
  -- INPUT PARAMETERS
  --   object_schema   - schema owning the table/view, current user if NULL
  --   object_name     - name of table or view
  --   group_name      - name of group of the policy to be refreshed
  --   policy_name     - name of policy to be refreshed

  PROCEDURE refresh_grouped_policy(object_schema IN VARCHAR2 := NULL,
                                   object_name   IN VARCHAR2 := NULL,
                                   group_name    IN VARCHAR2 := NULL,
                                   policy_name   IN VARCHAR2 := NULL);

  -- ------------------------------------------------------------------------
  -- refresh_policy - invalidate all cursors associated with the policy
  --                  if no argument provides, all cursors with
  --                  policies involved will be invalidated
  --
  -- INPUT PARAMETERS
  --   object_schema   - schema owning the table/view, current user if NULL
  --   object_name     - name of table or view
  --   policy_name     - name of policy to be refreshed

  PROCEDURE refresh_policy(object_schema IN VARCHAR2 := NULL,
                           object_name   IN VARCHAR2 := NULL,
                           policy_name   IN VARCHAR2 := NULL);

END DBMS_RLS;
//
