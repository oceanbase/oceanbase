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

#ifndef OCEANBASE_SQL_RESOLVER_DCL_OB_DCL_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_DCL_OB_DCL_RESOLVER_
#include "sql/resolver/ob_stmt_resolver.h"
#include "share/schema/ob_table_schema.h"
namespace oceanbase {
namespace sql {
class ObDCLResolver : public ObStmtResolver {
public:
  explicit ObDCLResolver(ObResolverParams& params) : ObStmtResolver(params)
  {}
  virtual ~ObDCLResolver()
  {}

protected:
  int check_and_convert_name(common::ObString& db, common::ObString& table);
  int check_password_strength(common::ObString& password, common::ObString& user_name);
  int check_number_count(common::ObString& password, const int64_t& number_count);
  int check_special_char_count(common::ObString& password, const int64_t& special_char_count);
  int check_mixed_case_count(common::ObString& password, const int64_t& mix_case_count);
  int check_user_name(common::ObString& password, common::ObString& user_name);
  int check_password_len(common::ObString& password, const int64_t& password_len);
  int check_oracle_password_strength(
      int64_t tenant_id, int64_t profile_id, common::ObString& password, common::ObString& user_name);
  enum ObPasswordPolicy { LOW = 0, MEDIUM };
  static const char password_mask_ = '*';

private:
  DISALLOW_COPY_AND_ASSIGN(ObDCLResolver);
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_RESOLVER_DCL_OB_DCL_RESOLVER_
