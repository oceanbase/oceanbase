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

#ifndef OCEANBASE_TRANSACTION_OB_GTS_MGR_
#define OCEANBASE_TRANSACTION_OB_GTS_MGR_

#include <stdint.h>
#include "share/ob_define.h"
#include "lib/net/ob_addr.h"
#include "ob_gts_msg.h"
#include "ob_gts_rpc.h"
#include "storage/transaction/ob_trans_factory.h"

namespace oceanbase {
namespace rpc {
namespace frame {
class ObReqTransport;
}
}  // namespace rpc
namespace transaction {
class ObIGtsResponseRpc;
class ObITimestampService {
public:
  virtual int get_timestamp(const common::ObPartitionKey& partition, int64_t& gts, common::ObAddr& leader) const = 0;
};

class ObIGlobalTimestampService {
public:
  virtual int handle_request(const ObGtsRequest& request, obrpc::ObGtsRpcResult& result) = 0;
  virtual int get_gts(const ObPartitionKey& gts_pkey, ObAddr& leader, int64_t& gts) = 0;
};

class ObGlobalTimestampService : public ObIGlobalTimestampService {
public:
  ObGlobalTimestampService() : is_inited_(false), is_running_(false), ts_service_(NULL), rpc_(NULL)
  {}
  ~ObGlobalTimestampService()
  {
    destroy();
  }
  int init(const ObITimestampService* ts_service, ObIGtsResponseRpc* rpc, const common::ObAddr& self);
  int start();
  int stop();
  int wait();
  void destroy();

public:
  int handle_request(const ObGtsRequest& request, obrpc::ObGtsRpcResult& result);
  int get_gts(const ObPartitionKey& gts_pkey, ObAddr& leader, int64_t& gts);

private:
  int handle_local_request_(const ObGtsRequest& request, obrpc::ObGtsRpcResult& result);
  bool is_inited_;
  bool is_running_;
  const ObITimestampService* ts_service_;
  ObIGtsResponseRpc* rpc_;
  common::ObAddr self_;
};
}  // namespace transaction
}  // end of namespace oceanbase
#endif  // OCEANBASE_TRANSACTION_OB_GTS_MGR_
