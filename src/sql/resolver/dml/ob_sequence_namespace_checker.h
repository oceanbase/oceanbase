/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DML_OB_SEQUENCE_NAMESPACE_CHECKER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DML_OB_SEQUENCE_NAMESPACE_CHECKER_H_
#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "pl/dblink/ob_pl_dblink_guard.h"
namespace oceanbase
{
namespace sql
{
struct ObResolverParams;
class ObDMLStmt;
class ObSynonymChecker;
class ObSQLSessionInfo;
class ObSchemaChecker;
struct ObQualifiedName;
class ObSequenceNamespaceChecker
{
public:
  explicit ObSequenceNamespaceChecker(ObResolverParams &resolver_params);
  explicit ObSequenceNamespaceChecker(const ObSchemaChecker *schema_checker,
                                      const ObSQLSessionInfo *session_info);
  ~ObSequenceNamespaceChecker() {};
  int check_sequence_namespace(const ObQualifiedName &q_name,
                               ObSynonymChecker &syn_checker,
                               uint64_t &sequence_id,
                               uint64_t *dblink_id=NULL);
  int check_sequence_namespace(const ObQualifiedName &q_name,
                               ObSynonymChecker &syn_checker,
                               const ObSQLSessionInfo *session_info,
                               const ObSchemaChecker *schema_checker,
                               uint64_t &sequence_id,
                               uint64_t *dblink_id=NULL);
  inline static bool is_curr_or_next_val(const common::ObString &s)
  {
    return 0 == s.case_compare("nextval")  || 0 == s.case_compare("currval");
  }
private:
  int check_sequence_with_synonym_recursively(const uint64_t tenant_id,
                                              const uint64_t database_id,
                                              const common::ObString &sequence_name,
                                              ObSynonymChecker &syn_checker,
                                              const ObSchemaChecker *schema_checker,
                                              bool &exists,
                                              uint64_t &sequence_id);
  int check_link_sequence_exists(const pl::ObDbLinkSchema *dblink_schema,
                                sql::ObSQLSessionInfo *session_info,
                                const ObString &database_name,
                                const ObString &sequence_name,
                                bool &exists,
                                bool &has_currval,
                                number::ObNumber &currval);
  int fetch_dblink_sequence_schema(const uint64_t tenant_id,
                                  const uint64_t db_id,
                                  const uint64_t dblink_id,
                                  const common::ObString &sequence_name,
                                  ObSQLSessionInfo *session_info,
                                  uint64_t &sequence_id);
  const ObSchemaChecker *schema_checker_;
  const ObSQLSessionInfo *session_info_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_DML_OB_SEQUENCE_NAMESPACE_CHECKER_H_ */
