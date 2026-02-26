/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_PRIVILEGE_CHECK_OB_AI_MODEL_PRIV_UTIL_
#define OCEANBASE_SQL_PRIVILEGE_CHECK_OB_AI_MODEL_PRIV_UTIL_

#include "sql/resolver/ob_schema_checker.h"
#include "share/schema/ob_priv_type.h"

namespace oceanbase
{
namespace sql
{


class ObAIServiceEndpointPrivUtil
{
public:
  explicit ObAIServiceEndpointPrivUtil(share::schema::ObSchemaGetterGuard &schema_guard);
  ~ObAIServiceEndpointPrivUtil() {}

  int check_create_ai_model_priv(
    ObIAllocator &allocator,
    const share::schema::ObSessionPrivInfo &session_priv,
    bool &has_priv);

  int check_alter_ai_model_priv(
    ObIAllocator &allocator,
    const share::schema::ObSessionPrivInfo &session_priv,
    bool &has_priv);

  int check_drop_ai_model_priv(
    ObIAllocator &allocator,
    const share::schema::ObSessionPrivInfo &session_priv,
    bool &has_priv);

  int check_access_ai_model_priv(
    ObIAllocator &allocator,
    const share::schema::ObSessionPrivInfo &session_priv,
    bool &has_priv);

private:
  enum ObAIServiceEndpointPrivType {
    AI_PRIV_CREATE = OB_PRIV_CREATE_AI_MODEL,
    AI_PRIV_ALTER  = OB_PRIV_ALTER_AI_MODEL,
    AI_PRIV_DROP   = OB_PRIV_DROP_AI_MODEL,
    AI_PRIV_ACCESS = OB_PRIV_ACCESS_AI_MODEL
  };

  int check_ai_model_priv(
    ObIAllocator &allocator,
    const share::schema::ObSessionPrivInfo &session_priv,
    ObPrivSet ai_priv_type,
    bool &has_priv);

private:
  share::schema::ObSchemaGetterGuard &schema_guard_;
};


} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_PRIVILEGE_CHECK_OB_AI_MODEL_PRIV_UTIL_