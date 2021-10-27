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

#ifndef OCEANBASE_SHARE_OB_SEQUENCE_SYNC_PROXY_H_
#define OCEANBASE_SHARE_OB_SEQUENCE_SYNC_PROXY_H_

#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_rpc_struct.h"
#include "lib/hash/ob_link_hashmap.h"
#include "common/ob_timeout_ctx.h"
#include "share/ob_alive_server_tracer.h"
namespace oceanbase {
namespace share {
struct CacheItemKey;
struct ObSequenceCacheItem;

class ObSequenceSyncProxy {
public:
  typedef common::ObLinkHashMap<CacheItemKey, ObSequenceCacheItem> NodeMap;

public:
  ObSequenceSyncProxy();
  ~ObSequenceSyncProxy();
  static ObSequenceSyncProxy& get_instance();
  int init(obrpc::ObSrvRpcProxy* srv_proxy, share::ObAliveServerTracer* server_tracer,
      NodeMap* sequence_cache, lib::ObMutex* cache_mutex);
  int clear_sequence_cache_all(const obrpc::ObSequenceSetValArg& arg);
  int clear_sequence_cache(const obrpc::ObSequenceSetValArg& arg);

private:
  obrpc::ObSrvRpcProxy* srv_proxy_;
  share::ObAliveServerTracer* server_tracer_;
  NodeMap* sequence_cache_;
  lib::ObMutex* cache_mutex_;
};
}  // end namespace share
}  // end namespace oceanbase
#endif
