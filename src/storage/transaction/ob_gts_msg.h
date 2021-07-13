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

#ifndef OCEANBASE_TRANSACTION_OB_GTS_MSG_
#define OCEANBASE_TRANSACTION_OB_GTS_MSG_

#include "share/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/net/ob_addr.h"
#include "common/ob_partition_key.h"
#include "ob_gts_define.h"

namespace oceanbase {
namespace obrpc {
class ObGtsRpcResult;
}
namespace transaction {
class ObGtsRequest {
  OB_UNIS_VERSION(1);

public:
  ObGtsRequest() : tenant_id_(0), srr_(0), range_size_(0), gts_pkey_(), sender_()
  {}
  ~ObGtsRequest()
  {}
  int init(const uint64_t tenant_id, const MonotonicTs srr, const int64_t range_size,
      const common::ObPartitionKey& gts_pkey, const common::ObAddr& sender);
  bool is_valid() const;

public:
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  MonotonicTs get_srr() const
  {
    return srr_;
  }
  const common::ObPartitionKey& get_gts_pkey() const
  {
    return gts_pkey_;
  }
  const common::ObAddr& get_sender() const
  {
    return sender_;
  }
  TO_STRING_KV(K_(tenant_id), K_(srr), K_(range_size), K_(gts_pkey), K_(sender));

private:
  uint64_t tenant_id_;
  MonotonicTs srr_;
  int64_t range_size_;
  common::ObPartitionKey gts_pkey_;
  common::ObAddr sender_;
};

class ObGtsErrResponse {
  OB_UNIS_VERSION(1);

public:
  ObGtsErrResponse() : tenant_id_(0), srr_(0), status_(0), leader_(), sender_()
  {}
  ~ObGtsErrResponse()
  {}
  int init(const uint64_t tenant_id, const MonotonicTs srr, const int status, const common::ObAddr& leader,
      const common::ObAddr& sender);
  bool is_valid() const;

public:
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  MonotonicTs get_srr() const
  {
    return srr_;
  }
  int get_status() const
  {
    return status_;
  }
  const common::ObAddr& get_leader() const
  {
    return leader_;
  }
  const common::ObAddr& get_sender() const
  {
    return sender_;
  }
  TO_STRING_KV(K_(tenant_id), K_(srr), K_(status), K_(leader), K_(sender));

private:
  uint64_t tenant_id_;
  MonotonicTs srr_;
  int status_;
  common::ObAddr leader_;
  common::ObAddr sender_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_GTS_MSG_
