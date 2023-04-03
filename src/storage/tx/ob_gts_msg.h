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
#include "ob_gts_define.h"

namespace oceanbase
{
namespace obrpc
{
class ObGtsRpcResult;
}
namespace transaction
{
class ObGtsRequest
{
  OB_UNIS_VERSION(1);
public:
  ObGtsRequest() : tenant_id_(0), srr_(0), range_size_(0), sender_() {}
  ~ObGtsRequest() {}
  int init(const uint64_t tenant_id, const MonotonicTs srr, const int64_t range_size,
      const common::ObAddr &sender);
  bool is_valid() const;
public:
  uint64_t get_tenant_id() const { return tenant_id_; }
  MonotonicTs get_srr() const { return srr_; }
  const common::ObAddr &get_sender() const { return sender_; }
  TO_STRING_KV(K_(tenant_id), K_(srr), K_(range_size), K_(sender));
private:
  uint64_t tenant_id_;
  MonotonicTs srr_;
  int64_t range_size_;
  common::ObAddr sender_;
};

class ObGtsErrResponse
{
  OB_UNIS_VERSION(1);
public:
  ObGtsErrResponse() : tenant_id_(0), srr_(0), status_(0), sender_() {}
  ~ObGtsErrResponse() {}
  int init(const uint64_t tenant_id, const MonotonicTs srr, const int status,
      const common::ObAddr &sender);
  bool is_valid() const;
public:
  uint64_t get_tenant_id() const { return tenant_id_; }
  MonotonicTs get_srr() const { return srr_; }
  int get_status() const { return status_; }
  const common::ObAddr &get_sender() const { return sender_; }
  TO_STRING_KV(K_(tenant_id), K_(srr), K_(status), K_(sender));
private:
  uint64_t tenant_id_;
  MonotonicTs srr_;
  int status_;
  common::ObAddr sender_;
};

} // transaction
} // oceanbase

#endif // OCEANBASE_TRANSACTION_OB_GTS_MSG_
