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

#ifndef OCEANBASE_RPC_OB_BATCH_PROCESSOR_H_
#define OCEANBASE_RPC_OB_BATCH_PROCESSOR_H_

#include "rpc/obrpc/ob_rpc_processor.h"
#include "ob_batch_proxy.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace obrpc {
class ObBatchP : public ObRpcProcessor<obrpc::ObBatchRpcProxy::ObRpc<OB_BATCH> > {
public:
  ObBatchP(storage::ObPartitionService* partition_service) : ps_(partition_service)
  {}
  ~ObBatchP()
  {}
  int check_timeout()
  {
    return common::OB_SUCCESS;
  }

protected:
  int process();
  int handle_clog_req(common::ObAddr& sender, const int64_t src_cluster_id, int type, common::ObPartitionKey& pkey,
      const char* buf, int32_t size);
  int handle_election_req(
      common::ObAddr& sender, int type, common::ObPartitionKey& pkey, const char* buf, int32_t size);
  int handle_election_group_req(common::ObAddr& sender, int type, const char* buf, int32_t size);
  int handle_trx_req(common::ObAddr& sender, int type, common::ObPartitionKey& pkey, const char* buf, int32_t size);
  int handle_sql_req(common::ObAddr& sender, int type, common::ObPartitionKey& pkey, const char* buf, int32_t size);

private:
  storage::ObPartitionService* ps_;
  DISALLOW_COPY_AND_ASSIGN(ObBatchP);
};

};  // namespace obrpc
};  // end namespace oceanbase

#endif /* OCEANBASE_RPC_OB_BATCH_PROCESSOR_H_ */
