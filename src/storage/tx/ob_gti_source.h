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

#ifndef OCEANBASE_TRANSACTION_OB_GTI_SOURCE_
#define OCEANBASE_TRANSACTION_OB_GTI_SOURCE_

#include "lib/net/ob_addr.h"

namespace oceanbase
{

namespace obrpc
{
class ObGtiRpcProxy;
}

namespace rpc
{
namespace frame
{
class ObReqTransport;
}
}

namespace transaction
{
class ObGtiRequestRpc;
class ObGtiErrResponse;

struct IdCache
{
  int64_t start_id;
  int64_t end_id;
  TO_STRING_KV(K(start_id), K(end_id));
};

class ObIGtiSource
{
public:
  virtual int start() { return OB_SUCCESS; }
  virtual void stop() {}
  virtual void wait() {}
  virtual void destroy() {}
  virtual void reset() {}
  virtual int get_trans_id(int64_t &trans_id) = 0;
};

class ObGtiSource : public ObIGtiSource
{
public:
  ObGtiSource() { reset(); }
  ~ObGtiSource() { destroy(); }
  int init(const common::ObAddr &server, rpc::frame::ObReqTransport *req_transport);
  virtual int start();
  virtual void stop();
  virtual void wait();
  virtual void destroy();
  virtual void reset();
  int refresh_gti_location();
  int update_trans_id(const int64_t start_id, const int64_t end_id);
  virtual int get_trans_id(int64_t &trans_id);
private:
  void update_preallocate_count_();
  int64_t get_preallocate_count_();
public:
  TO_STRING_KV(K_(is_inited), K_(is_running), K_(is_requesting),
               K_(server), K_(gti_cache_leader));
public:
  static const int64_t MIN_PREALLOCATE_COUNT = 10000;
  static const int64_t MAX_PREALLOCATE_COUNT = 1000000;
  static const int64_t UPDATE_FACTOR = 4;
  static const int64_t UPDATE_PREALLOCATE_COUNT_INTERVAL = 100000;
  static const int64_t MAX_CACHE_NUM = 16;
  static const int64_t PRE_CACHE_NUM = MAX_CACHE_NUM / 4;
  static const int64_t RETRY_REQUEST_INTERVAL = 100000;
  static const int64_t MAX_RETRY_REQUEST_INTERVAL = RETRY_REQUEST_INTERVAL * 10;
private:
  bool is_inited_;
  bool is_running_;
  bool is_requesting_;
  IdCache id_cache_[MAX_CACHE_NUM];
  int64_t cur_idx_;
  int64_t cache_idx_;
  common::ObAddr server_;
  obrpc::ObGtiRpcProxy *gti_request_rpc_proxy_;
  ObGtiRequestRpc *gti_request_rpc_;
  common::ObAddr gti_cache_leader_;
  int64_t retry_request_cnt_;
  int64_t last_request_ts_;
  // lock for update trans id
  common::ObLatch lock_;
  int64_t preallocate_count_;
  int64_t last_update_ts_;
};

} // transaction
} // oceanbase

#endif //OCEANBASE_TRANSACTION_OB_GTI_SOURCE_
