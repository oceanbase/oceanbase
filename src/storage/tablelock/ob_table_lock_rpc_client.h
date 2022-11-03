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

#ifndef OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_RPC_CLIENT_H_
#define OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_RPC_CLIENT_H_

#include <stdint.h>

#include "common/ob_tablet_id.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/tablelock/ob_table_lock_rpc_proxy.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"

namespace oceanbase
{
namespace transaction
{
namespace tablelock
{

class ObTableLockRpcClient
{
public:
  static ObTableLockRpcClient &get_instance();

  int init();

  int lock_table(const uint64_t table_id,
                const ObTableLockMode lock_mode,
                const ObTableLockOwnerID lock_owner,
                const int64_t timeout_us,
                const int64_t tenant_id);
  int unlock_table(const uint64_t table_id,
                const ObTableLockMode lock_mode,
                const ObTableLockOwnerID lock_owner,
                const int64_t timeout_us,
                const int64_t tenant_id);

private:
  // TODO: yanyuan.cxf use parallel rpc and modify this to 5s
  static const int64_t DEFAULT_TIMEOUT_US = 15L * 1000L * 1000L; // 15s
  ObTableLockRpcClient() : table_lock_rpc_proxy_() {}
  ~ObTableLockRpcClient() {}

private:
  obrpc::ObTableLockRpcProxy table_lock_rpc_proxy_;
};

}
}
}
#endif
