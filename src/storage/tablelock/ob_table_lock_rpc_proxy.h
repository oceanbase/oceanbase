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

#ifndef OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_RPC_PROXY_H_
#define OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_RPC_PROXY_H_

#include "observer/ob_server_struct.h"
#include "share/rpc/ob_async_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"

namespace oceanbase
{

namespace obrpc
{

RPC_F(OB_TABLE_LOCK_TASK, transaction::tablelock::ObTableLockTaskRequest,
        transaction::tablelock::ObTableLockTaskResult, ObTableLockProxy);
RPC_F(OB_BATCH_TABLE_LOCK_TASK, transaction::tablelock::ObLockTaskBatchRequest,
      transaction::tablelock::ObTableLockTaskResult, ObBatchLockProxy);
RPC_F(OB_HIGH_PRIORITY_TABLE_LOCK_TASK, transaction::tablelock::ObTableLockTaskRequest,
        transaction::tablelock::ObTableLockTaskResult, ObHighPriorityTableLockProxy);
RPC_F(OB_HIGH_PRIORITY_BATCH_TABLE_LOCK_TASK, transaction::tablelock::ObLockTaskBatchRequest,
      transaction::tablelock::ObTableLockTaskResult, ObHighPriorityBatchLockProxy);

class ObTableLockRpcProxy: public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObTableLockRpcProxy);
  RPC_S(PR5 lock_table, OB_OUT_TRANS_LOCK_TABLE, (transaction::tablelock::ObOutTransLockTableRequest));
  RPC_S(PR4 unlock_table, OB_OUT_TRANS_UNLOCK_TABLE, (transaction::tablelock::ObOutTransUnLockTableRequest));
};

}
}

#endif /* OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_RPC_PROXY_H_ */
