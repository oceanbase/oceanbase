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

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_KILL_SESSION_ARG_H__
#define OCEANBASE_SQL_ENGINE_CMD_OB_KILL_SESSION_ARG_H__

#include <cstdint>
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObKillStmt;

class ObKillSessionArg
{
  OB_UNIS_VERSION(1);
public:
  ObKillSessionArg()
      : sess_id_(0),
      tenant_id_(common::OB_INVALID_TENANT_ID),
      user_id_(common::OB_INVALID_ID),
      is_query_(false),
      has_user_super_privilege_(false)
      {
      }
  ~ObKillSessionArg() {}
  int init(ObExecContext &ctx, const ObKillStmt &stmt);
  int calculate_sessid(ObExecContext &ctx, const ObKillStmt &stmt);
  TO_STRING_KV(K(sess_id_),
               K(tenant_id_),
               K(user_id_),
               K(is_query_),
               K(has_user_super_privilege_));
public:
  uint32_t sess_id_;
  uint64_t tenant_id_;
  uint64_t user_id_;
  bool is_query_;
  bool has_user_super_privilege_;
};
}//sql
}//oceanbase
#endif
