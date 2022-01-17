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

#ifndef OCEANBASE_CLOG_OB_LOG_RPC_PROCESSOR_
#define OCEANBASE_CLOG_OB_LOG_RPC_PROCESSOR_

#include "rpc/obrpc/ob_rpc_processor.h"
#include "ob_log_rpc_proxy.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace clog {
class ObLogRpcProcessor : public obrpc::ObRpcProcessor<obrpc::ObLogRpcProxy::ObRpc<obrpc::OB_CLOG> > {
public:
  ObLogRpcProcessor(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  virtual ~ObLogRpcProcessor()
  {}

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};
};  // end namespace clog
};  // end namespace oceanbase

#endif
