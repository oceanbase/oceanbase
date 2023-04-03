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

#ifndef OCEANBASE_OB_DAS_ID_CACHE_H
#define OCEANBASE_OB_DAS_ID_CACHE_H

namespace oceanbase
{
namespace obrpc
{
class ObDASIDRpcProxy;
}
namespace rpc
{
namespace frame
{
class ObReqTransport;
}
}
namespace sql
{
class ObDASIDRequestRpc;

struct IdCache
{
  int64_t start_id;
  int64_t end_id;
  TO_STRING_KV(K(start_id), K(end_id));
};
class ObDASIDCache
{
public:
  ObDASIDCache() { reset(); }
  ~ObDASIDCache() { destroy(); }
  int init(const common::ObAddr &server, rpc::frame::ObReqTransport *req_transport);
  void destroy();
  void reset();
  int refresh_id_service_location();
  int update_das_id(const int64_t start_id, const int64_t end_id);
  int get_das_id(int64_t &das_id, const bool force_renew);
private:
  void update_preallocate_count_();
  int64_t get_preallocate_count_();
public:
  TO_STRING_KV(K_(is_inited), K_(cur_idx), K_(cache_idx), K_(id_service_leader));
public:
  static const int64_t MIN_PREALLOCATE_COUNT = 1000000; // 1 million
  static const int64_t MAX_PREALLOCATE_COUNT = MIN_PREALLOCATE_COUNT * 10;
  static const int64_t UPDATE_FACTOR = 4;
  static const int64_t MAX_CACHE_NUM = 16;
  static const int64_t PRE_CACHE_NUM = MAX_CACHE_NUM / 4;
  static const int64_t OB_DAS_ID_RPC_TIMEOUT_MIN = 100 * 1000L;       // 100ms
  static const int64_t OB_DAS_ID_RPC_TIMEOUT_MAX = 2 * 1000L * 1000L; // 2s
  static const int64_t UPDATE_PREALLOCATE_COUNT_INTERVAL = OB_DAS_ID_RPC_TIMEOUT_MIN;
private:
  bool is_inited_;
  bool is_requesting_;
  IdCache id_cache_[MAX_CACHE_NUM]; //将Cache进行分段打散，避免多个线程并发争抢，造成访问热点
  int64_t cur_idx_;
  int64_t cache_idx_;
  common::ObAddr server_;
  obrpc::ObDASIDRpcProxy *id_rpc_proxy_;
  ObDASIDRequestRpc *id_request_rpc_;
  common::ObAddr id_service_leader_;
  int64_t retry_request_cnt_;
  common::ObLatch lock_;
  int64_t preallocate_count_;
  int64_t last_update_ts_;
  common::ObArenaAllocator alloc_;
};
} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_OB_DAS_ID_CACHE_H
