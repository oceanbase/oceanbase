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


 #define USING_LOG_PREFIX SQL_SESSION

#include "share/schema/ob_schema_struct.h"
#include "ob_ora_priv_check.h"
#include "ob_ai_model_priv_util.h"

namespace oceanbase
{
namespace sql
{
ObAIServiceEndpointPrivUtil::ObAIServiceEndpointPrivUtil(share::schema::ObSchemaGetterGuard &schema_guard)
  : schema_guard_(schema_guard)
{
}

int ObAIServiceEndpointPrivUtil::check_ai_model_priv(ObIAllocator &allocator,
                                    const share::schema::ObSessionPrivInfo &session_priv,
                                    ObPrivSet ai_priv_type,
                                    bool &has_priv)
{
  int ret = OB_SUCCESS;

  has_priv = false;
  ObSchemaChecker schema_checker;
  if (OB_FAIL(schema_checker.init(schema_guard_))) {
    LOG_WARN("failed to init schema checker", K(ret));
  } else {
    share::schema::ObNeedPriv need_priv;
    need_priv.priv_level_ = share::schema::OB_PRIV_USER_LEVEL;
    need_priv.priv_set_ = ai_priv_type;
    common::ObSEArray<uint64_t, 1> unused_enable_role_id_array;
    share::schema::ObStmtNeedPrivs need_privs(allocator);
    if (OB_FAIL(need_privs.need_privs_.init(1))) {
      LOG_WARN("failed to init need privs", K(ret));
    } else if (OB_FAIL(need_privs.need_privs_.push_back(need_priv))) {
      LOG_WARN("failed to push back need priv", K(ret), K(need_priv));
    } else if (OB_FAIL(schema_checker.check_priv(session_priv, unused_enable_role_id_array, need_privs))) {
      if (OB_ERR_NO_PRIVILEGE == ret) {
        has_priv = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to check ai model privilege", K(ret),
                 K(ai_priv_type));
      }
    } else {
      has_priv = true;
    }
  }

  return ret;
}

int ObAIServiceEndpointPrivUtil::check_create_ai_model_priv(
    ObIAllocator &allocator,
    const share::schema::ObSessionPrivInfo &session_priv,
    bool &has_priv)
{
  return check_ai_model_priv(allocator, session_priv,
                                       AI_PRIV_CREATE, has_priv);
}

int ObAIServiceEndpointPrivUtil::check_alter_ai_model_priv(
    ObIAllocator &allocator,
    const share::schema::ObSessionPrivInfo &session_priv,
    bool &has_priv)
{
  return check_ai_model_priv(allocator, session_priv,
                                       AI_PRIV_ALTER, has_priv);
}

int ObAIServiceEndpointPrivUtil::check_drop_ai_model_priv(
    ObIAllocator &allocator,
    const share::schema::ObSessionPrivInfo &session_priv,
    bool &has_priv)
{
  return check_ai_model_priv(allocator, session_priv,
                                       AI_PRIV_DROP, has_priv);
}

int ObAIServiceEndpointPrivUtil::check_access_ai_model_priv(
    ObIAllocator &allocator,
    const share::schema::ObSessionPrivInfo &session_priv,
    bool &has_priv)
{
  return check_ai_model_priv(allocator, session_priv,
                                       AI_PRIV_ACCESS, has_priv);
}

}
}