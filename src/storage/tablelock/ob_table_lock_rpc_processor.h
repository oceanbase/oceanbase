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

#ifndef OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_RPC_PROCESSOR_H_
#define OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_RPC_PROCESSOR_H_

#include <stdint.h>

#include "storage/tablelock/ob_table_lock_rpc_proxy.h"
#include "observer/ob_rpc_processor_simple.h"

namespace oceanbase
{
namespace observer
{
OB_DEFINE_PROCESSOR_S(Srv, OB_TABLE_LOCK_TASK, ObTableLockTaskP);
OB_DEFINE_PROCESSOR_S(Srv, OB_HIGH_PRIORITY_TABLE_LOCK_TASK, ObHighPriorityTableLockTaskP);

 OB_DEFINE_PROCESSOR_S(Srv, OB_BATCH_TABLE_LOCK_TASK, ObBatchLockTaskP);
 OB_DEFINE_PROCESSOR_S(Srv, OB_HIGH_PRIORITY_BATCH_TABLE_LOCK_TASK, ObHighPriorityBatchLockTaskP);

OB_DEFINE_PROCESSOR_S(TableLock, OB_OUT_TRANS_LOCK_TABLE, ObOutTransLockTableP);
OB_DEFINE_PROCESSOR_S(TableLock, OB_OUT_TRANS_UNLOCK_TABLE, ObOutTransUnlockTableP);

class ObAdminRemoveLockP : public obrpc::ObRpcProcessor<obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_REMOVE_OBJ_LOCK>>
{
public:
  explicit ObAdminRemoveLockP() {}
  ~ObAdminRemoveLockP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminRemoveLockP);
};

class ObAdminUpdateLockP : public obrpc::ObRpcProcessor<obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_UPDATE_OBJ_LOCK>>
{
public:
  explicit ObAdminUpdateLockP() {}
  ~ObAdminUpdateLockP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminUpdateLockP);
};

}
}

#endif /* OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_RPC_PROCESSOR_H_ */
