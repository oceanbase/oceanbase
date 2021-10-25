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

#define USING_LOG_PREFIX SHARE

#include "share/sequence/ob_sequence_sync_proxy.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase {
namespace share {
#ifndef INT24_MIN
#define INT24_MIN (-8388607 - 1)
#endif
#ifndef INT24_MAX
#define INT24_MAX (8388607)
#endif
#ifndef UINT24_MAX
#define UINT24_MAX (16777215U)
#endif

const int64_t SYNC_TIMEOUT_US = 500 * 1000;  // us
const int64_t PARTITION_LOCATION_SET_BUCKET_NUM = 3;
const int64_t SEQ_VALUE_TID = OB_ALL_SEQUENCE_VALUE_TID;

ObSequenceSyncProxy::ObSequenceSyncProxy()
    : srv_proxy_(NULL),
      server_tracer_(NULL),
      sequence_cache_(NULL),
      cache_mutex_(NULL)
{}

ObSequenceSyncProxy::~ObSequenceSyncProxy()
{}

ObSequenceSyncProxy& ObSequenceSyncProxy::get_instance()
{
  static ObSequenceSyncProxy seq_sync_proxy;
  return seq_sync_proxy;
}

int ObSequenceSyncProxy::init(obrpc::ObSrvRpcProxy* srv_proxy,
    share::ObAliveServerTracer* server_tracer,
    NodeMap* sequence_cache, lib::ObMutex* cache_mutex)
{
  int ret = OB_SUCCESS;
  srv_proxy_ = srv_proxy;
  server_tracer_ = server_tracer;
  sequence_cache_ = sequence_cache;
  cache_mutex_ = cache_mutex;
  return ret;
}

int ObSequenceSyncProxy::clear_sequence_cache_all(const uint64_t seq_id)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    LOG_INFO("begin to clear all sever's sequence cache", K(seq_id));
    
    obrpc::UInt64 arg(seq_id);
    common::ObArray<common::ObAddr> servers;
    if (OB_FAIL(server_tracer_->get_active_server_list(servers))) {
      LOG_WARN("get alive server failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < servers.count(); ++i) {
        common::ObAddr& server = servers.at(i);
        LOG_INFO("send rpc call to other observers", "server", server);
        if (OB_FAIL(srv_proxy_->to(server).timeout(SYNC_TIMEOUT_US).clear_sequence_cache(arg))) {
          if (is_timeout_err(ret) || is_server_down_error(ret)) {
            // ignore time out, go on
            LOG_WARN("rpc call time out, ignore the error", "server", server, K(seq_id), K(ret));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to send rpc call", K(seq_id), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObSequenceSyncProxy::clear_sequence_cache(const obrpc::UInt64& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("begin to clear local auto-increment cache", K(arg));

  CacheItemKey key;
  key.key_ = arg;

  lib::ObMutexGuard guard(*cache_mutex_);
  ObSequenceCacheItem* item = nullptr;
  if (OB_FAIL(sequence_cache_->get(key, item))) {
    LOG_WARN("failed to erase sequence cache", K(arg), K(ret));
  } else if (OB_ISNULL(item)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    item->enough_cache_node_ = false;
    item->with_prefetch_node_ = false;
  }

  if (OB_ENTRY_NOT_EXIST == ret) {
    // do nothing; key does not exist
    ret = OB_SUCCESS;
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase
