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

#ifndef SQL_RESOLVER_CMD_OB_TRIGGER_STORAGE_CACHE_STMT_H_
#define SQL_RESOLVER_CMD_OB_TRIGGER_STORAGE_CACHE_STMT_H_

#include "sql/resolver/cmd/ob_cmd_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObTriggerStorageCacheStmt : public ObCMDStmt
{
public:
  ObTriggerStorageCacheStmt() : 
      ObCMDStmt(stmt::T_TRIGGER_STORAGE_CACHE),
      rpc_arg_()
  {}
  virtual ~ObTriggerStorageCacheStmt() {}
  obrpc::ObTriggerStorageCacheArg &get_rpc_arg() { return rpc_arg_; }
  void set_storage_cache_op(const obrpc::ObTriggerStorageCacheArg::ObStorageCacheOp op) { rpc_arg_.op_ = op; }
  void set_tenant_id(const uint64_t tenant_id) { rpc_arg_.tenant_id_ = tenant_id; }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
public:
  obrpc::ObTriggerStorageCacheArg rpc_arg_;
};
} /* sql */
} /* oceanbase */

#endif //OCEANBASE_SQL_OB_TENANT_SNAPSHOT_STMT_H_