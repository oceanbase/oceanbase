/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_RPC_OB_BATCH_PROCESSOR_H_
#define OCEANBASE_RPC_OB_BATCH_PROCESSOR_H_

#include "rpc/obrpc/ob_rpc_processor.h"
#include "ob_batch_proxy.h"

namespace oceanbase
{
namespace obrpc
{
class ObBatchP : public ObRpcProcessor< obrpc::ObBatchRpcProxy::ObRpc<OB_BATCH> >
{
public:
  ObBatchP() {}
  ~ObBatchP() {}
  int check_timeout() { return common::OB_SUCCESS; }
protected:
  int process();
  int handle_trx_req(common::ObAddr& sender, int type, const char* buf, int32_t size);
  int handle_tx_req(int type, const char* buf, int32_t size);
  int handle_sql_req(common::ObAddr& sender, int type, const char* buf, int32_t size);
  int handle_log_req(const common::ObAddr& sender, int type, const share::ObLSID &ls_id, const char* buf, int32_t size);
  int handle_lock_wait_mgr_req(int type, const char* buf, int32_t size);
private:
  DISALLOW_COPY_AND_ASSIGN(ObBatchP);
};

}; // end namespace rpc
}; // end namespace oceanbase

#endif /* OCEANBASE_RPC_OB_BATCH_PROCESSOR_H_ */
