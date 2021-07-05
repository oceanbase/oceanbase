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

#ifndef OCEANBASE_CLOG_OB_CLOG_SYNC_RPC_
#define OCEANBASE_CLOG_OB_CLOG_SYNC_RPC_
#include "ob_log_define.h"
#include "ob_log_rpc_proxy.h"
#include "ob_clog_sync_msg.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace obrpc {
class ObLogGetMCTsProcessor : public ObRpcProcessor<obrpc::ObLogRpcProxy::ObRpc<OB_LOG_GET_MC_TS> > {
public:
  ObLogGetMCTsProcessor(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  ~ObLogGetMCTsProcessor()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogGetMCTsProcessor);
};

class ObLogGetMcCtxArrayProcessor : public ObRpcProcessor<obrpc::ObLogRpcProxy::ObRpc<OB_LOG_GET_MC_CTX_ARRAY> > {
public:
  ObLogGetMcCtxArrayProcessor(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  ~ObLogGetMcCtxArrayProcessor()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogGetMcCtxArrayProcessor);
};

class ObLogGetPriorityArrayProcessor : public ObRpcProcessor<obrpc::ObLogRpcProxy::ObRpc<OB_LOG_GET_PRIORITY_ARRAY> > {
public:
  ObLogGetPriorityArrayProcessor(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  ~ObLogGetPriorityArrayProcessor()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogGetPriorityArrayProcessor);
};

class ObLogGetRemoteLogProcessor : public ObRpcProcessor<obrpc::ObLogRpcProxy::ObRpc<OB_LOG_GET_REMOTE_LOG> > {
public:
  ObLogGetRemoteLogProcessor(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  ~ObLogGetRemoteLogProcessor()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogGetRemoteLogProcessor);
};
}  // namespace obrpc
}  // namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_CLOG_SYNC_RPC_
