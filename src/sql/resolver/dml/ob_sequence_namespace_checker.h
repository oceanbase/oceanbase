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
namespace oceanbase {
namespace sql {
class ObResolverParams;
class ObDMLStmt;
class ObSynonymChecker;
class ObSQLSessionInfo;
class ObSchemaChecker;
struct ObQualifiedName;
class ObSequenceNamespaceChecker {
public:
  explicit ObSequenceNamespaceChecker(ObResolverParams& resolver_params) : params_(resolver_params)
  {}
  ~ObSequenceNamespaceChecker(){};
  int check_sequence_namespace(const ObQualifiedName& q_name, ObSynonymChecker& syn_checker, uint64_t& sequence_id);
  int check_sequence_namespace(const ObQualifiedName& q_name, ObSynonymChecker& syn_checker,
      ObSQLSessionInfo* session_info, ObSchemaChecker* schema_checker, uint64_t& sequence_id);
  inline static bool is_curr_or_next_val(const common::ObString& s)
  {
    return 0 == s.case_compare("nextval") || 0 == s.case_compare("currval");
  }

private:
  int check_sequence_with_synonym_recursively(const uint64_t tenant_id, const uint64_t database_id,
      const common::ObString& sequence_name, ObSynonymChecker& syn_checker, ObSchemaChecker* schema_checker,
      bool& exists, uint64_t& sequence_id);
  ObResolverParams& params_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_DML_OB_SEQUENCE_NAMESPACE_CHECKER_H_ */
