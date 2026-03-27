/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  int check_auth_for_kill(uint64_t kill_tid, uint64_t kill_uid) const;
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
