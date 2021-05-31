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

#include "ob_ilog_cache.h"
#include "lib/allocator/ob_mod_define.h"
#include "ob_log_engine.h"

namespace oceanbase {
using namespace common;
namespace clog {

// ---- wrapper ----
ObIlogPerFileCacheNode::ObIlogPerFileCacheNode()
    : node_status_(NODE_STATUS_INVALID), seq_(alloc_seq()), cache_wrapper_(NULL), wrapper_allocator_(NULL)
{
  // do nothing
}

ObIlogPerFileCacheNode::~ObIlogPerFileCacheNode()
{}

// node is copied from or into node_map_
ObIlogPerFileCacheNode& ObIlogPerFileCacheNode::operator=(const ObIlogPerFileCacheNode& node)
{
  cache_wrapper_ = node.get_cache_wrapper();
  ATOMIC_STORE(&node_status_, node.get_status());
  seq_ = node.get_seq();
  wrapper_allocator_ = node.get_wrapper_allocator();
  return *this;
}

int ObIlogPerFileCacheNode::set_cache_wrapper(
    ObIlogPerFileCacheWrapper* cache_wrapper, common::ObSmallAllocator* wrapper_allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == cache_wrapper) || OB_UNLIKELY(NULL == wrapper_allocator)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    cache_wrapper_ = cache_wrapper;
    wrapper_allocator_ = wrapper_allocator;
    CLOG_LOG(INFO, "set_cache_wrapper", KP(cache_wrapper_), KP(wrapper_allocator_));
  }
  return ret;
}

void ObIlogPerFileCacheNode::reset()
{
  ATOMIC_STORE(&node_status_, NODE_STATUS_INVALID);
  cache_wrapper_ = NULL;
}

void ObIlogPerFileCacheNode::destroy_wrapper()
{
  if (NULL != cache_wrapper_) {
    CSR_LOG(INFO, "cache_wrapper destroy", KP(cache_wrapper_), KP(wrapper_allocator_));
    cache_wrapper_->cache_.destroy();
    cache_wrapper_->page_arena_.free();
    wrapper_allocator_->free(cache_wrapper_);
    cache_wrapper_ = NULL;
    wrapper_allocator_ = NULL;
  }
}

int ObIlogPerFileCacheNode::mark_loading(const file_id_t file_id)
{
  UNUSED(file_id);  // just for debug
  int ret = OB_SUCCESS;
  // INVALID -> LOADING -> READY
  if (ATOMIC_BCAS(&node_status_, NODE_STATUS_INVALID, NODE_STATUS_LOADING)) {
    CSR_LOG(INFO, "[ILOG_PERF_FILE_CACHE] set_status from INVALID to LOADING", K(file_id), K_(seq));
  } else {
    ret = OB_STATE_NOT_MATCH;
    CSR_LOG(WARN, "set_status error", K(ret), K(file_id), "node_status", ATOMIC_LOAD(&node_status_), K_(seq));
  }
  return ret;
}

void ObIlogPerFileCacheNode::mark_invalid(const file_id_t file_id)
{
  UNUSED(file_id);  // just for debug
  ATOMIC_STORE(&node_status_, NODE_STATUS_INVALID);
  CSR_LOG(TRACE, "node mark invalid", K(file_id));
}

int ObIlogPerFileCacheNode::mark_ready(const file_id_t file_id)
{
  UNUSED(file_id);  // just for debug
  int ret = OB_SUCCESS;
  // INVALID -> LOADING -> READY
  if (ATOMIC_BCAS(&node_status_, NODE_STATUS_LOADING, NODE_STATUS_READY)) {
    CSR_LOG(INFO, "[ILOG_PERF_FILE_CACHE] set_status from LOADING to READY", K(file_id), K_(seq));
  } else {
    ret = OB_STATE_NOT_MATCH;
    CSR_LOG(WARN, "set_status error", K(ret), "node_status_", ATOMIC_LOAD(&node_status_));
  }
  return ret;
}

// ---- ilog cache ----
int ObIlogCache::init(const ObIlogCacheConfig& config, ObIlogPerFileCacheBuilder* pf_cache_builder)
{
  int ret = OB_SUCCESS;

  const int64_t WRAPPER_ALLOC_SIZE = sizeof(ObIlogPerFileCacheWrapper);
  constexpr const char* WRAPPER_ALLOCATOR_LABEL = ObModIds::OB_CSR_PER_FILE_NODE_WRAPPER;
  const uint64_t WRAPPER_ALLOCATOR_TENANT_ID = OB_SERVER_TENANT_ID;
  const int64_t WRAPPER_BLOCK_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
  const int64_t WRAPPER_ALLOCATOR_MIN_OBJ_COUNT_ON_BLOCK = 128;
  const int64_t WRAPPER_ALLOCATOR_LIMIT_NUM = 1L * 1024L * 1024L * 1024L;

  constexpr const char* NODE_MAP_LABEL = ObModIds::OB_CSR_PER_FILE_NODE_MAP;
  const int64_t NODE_MAP_TENANT_ID = OB_SERVER_TENANT_ID;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (!config.is_valid() || NULL == pf_cache_builder) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(WARN, "ivnalid config for ObIlogCache", K(ret), K(config), KP(pf_cache_builder));
  } else if (OB_FAIL(wrapper_allocator_.init(WRAPPER_ALLOC_SIZE,
                 WRAPPER_ALLOCATOR_LABEL,
                 WRAPPER_ALLOCATOR_TENANT_ID,
                 WRAPPER_BLOCK_SIZE,
                 WRAPPER_ALLOCATOR_MIN_OBJ_COUNT_ON_BLOCK,
                 WRAPPER_ALLOCATOR_LIMIT_NUM))) {
    CSR_LOG(WARN, "wrapper_allocator_ init error", K(ret));
  } else if (OB_FAIL(node_map_.init(NODE_MAP_LABEL, NODE_MAP_TENANT_ID))) {
    CSR_LOG(WARN, "node_map_ init error", K(ret));
  } else {
    config_ = config;
    pf_cache_builder_ = pf_cache_builder;
    is_inited_ = true;
  }
  return ret;
}

bool ObIlogCache::RemoveNodeFunctor::operator()(const file_id_t file_id, ObIlogPerFileCacheNode& node)
{
  UNUSED(file_id);
  CSR_LOG(INFO, "[ILOG_PERF_FILE_CACHE] destroy node", K(file_id), K(node), K(count_));
  count_++;
  node.dec_wrapper_ref();
  return true;
}

void ObIlogCache::destroy()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    is_inited_ = false;
    RemoveNodeFunctor remove_fn;
    if (OB_FAIL(node_map_.remove_if(remove_fn))) {
      CSR_LOG(WARN, "node_map_ destroy each node erorr", K(ret));
    }
    (void)node_map_.destroy();
    statistic_.reset();
    (void)wrapper_allocator_.destroy();
  }
}

// if return OB_SUCCESS, target_node will be the correct node
int ObIlogCache::prepare_cache_node(const file_id_t file_id, ObIlogPerFileCacheNode& target_node,
    ObIlogStorageQueryCost& csr_cost, const bool is_preload)
{
  int ret = OB_SUCCESS;
  ObIlogPerFileCacheNode prepared_node;
  GetNodeFunctor get_fn1;
  ret = node_map_.operate(file_id, get_fn1);
  if (OB_SUCCESS == ret) {
    // do nothing
    target_node = get_fn1.get_node();
    CSR_LOG(TRACE, "prepare_cache_node get target_node success", K(file_id), K(target_node), K(is_preload));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    if (static_cast<int64_t>(node_map_.count()) >= config_.hold_file_count_limit_) {
      // The operation of inserting and count aren't atomic, this can cause memory usage exceed the limit.
      // wash outside
      ret = OB_SIZE_OVERFLOW;
    } else {
      if (OB_FAIL(prepare_loading(file_id, is_preload, prepared_node))) {
        CSR_LOG(WARN, "prepare target_node to load error", K(ret), K(file_id), K(target_node));
      } else if (OB_FAIL(node_map_.insert(file_id, prepared_node))) {
        prepared_node.dec_wrapper_ref();
        if (OB_ENTRY_EXIST == ret) {  // another peer insert win
          GetNodeFunctor get_fn2;
          ret = node_map_.operate(file_id, get_fn2);  // get the winner target_node
          if (OB_SUCC(ret)) {
            // target_node now is the correct one
            target_node = get_fn2.get_node();
            CSR_LOG(TRACE, "get the correct one", K(ret), K(file_id), K(target_node));
          } else if (OB_ENTRY_NOT_EXIST == ret) {
            // the winner is washed, retry outside
            ret = OB_NEED_RETRY;
            CSR_LOG(WARN, "the winner node that just inserted is washed now", K(ret), K(file_id));
          } else {
            CSR_LOG(WARN, "node_map_ get error", K(ret), K(file_id));
          }
        } else {
          CSR_LOG(WARN, "node_map_ insert error", K(ret), K(file_id), K(target_node));
        }
        CSR_LOG(TRACE, "prepare loading finish", K(ret), K(file_id), K(target_node));
      } else {
        // succeed to hold the place, try load
        GetNodeFunctor get_fn3;
        ret = node_map_.operate(file_id, get_fn3);
        if (OB_SUCC(ret)) {
          target_node = get_fn3.get_node();
          if (target_node.get_seq() == prepared_node.get_seq() &&
              OB_FAIL(sync_load_ilog_file(file_id, target_node, csr_cost))) {
            target_node.dec_wrapper_ref();
            CSR_LOG(WARN, "load ilog_file error", K(ret), K(file_id), K(target_node));
          }
        } else if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_NEED_RETRY;
          CSR_LOG(WARN, "the node that just inserted is washed now", K(ret), K(file_id));
        } else {
          CLOG_LOG(WARN, "node_map_ get error", K(ret), K(file_id));
        }
      }
    }
  } else {
    CSR_LOG(WARN, "node_map_ get error", K(ret), K(file_id));
  }
  return ret;
}

int ObIlogCache::get_cursor_from_node(const ObIlogPerFileCacheNode& target_node, const ObPartitionKey& pkey,
    const uint64_t query_log_id, const uint64_t min_log_id, const uint64_t max_log_id,
    const offset_t start_offset_index, ObGetCursorResult& result)
{
  int ret = OB_SUCCESS;
  ObIlogPerFileCacheWrapper* cache_wrapper = target_node.get_cache_wrapper();
  if (OB_ISNULL(cache_wrapper)) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR,
        "null cache_wrapper of a ready or deleting cache node",
        K(ret),
        K(target_node),
        K(pkey),
        K(query_log_id),
        K(min_log_id),
        K(max_log_id),
        K(start_offset_index));
  } else {
    cache_wrapper->last_access_ts_ = ObTimeUtility::current_time();
    ObIlogPerFileCache& pf_cache = cache_wrapper->cache_;
    if (OB_FAIL(pf_cache.query_cursor(pkey, query_log_id, min_log_id, max_log_id, start_offset_index, result))) {
      CSR_LOG(WARN,
          "pf_cache query_cursor failed",
          K(ret),
          K(pkey),
          K(query_log_id),
          K(min_log_id),
          K(max_log_id),
          K(start_offset_index),
          K(result));
    }
  }
  return ret;
}

int ObIlogCache::query_from_node(const file_id_t file_id, ObIlogPerFileCacheNode& target_node,
    const ObPartitionKey& pkey, const uint64_t query_log_id, const uint64_t min_log_id, const uint64_t max_log_id,
    const offset_t start_offset_index, ObGetCursorResult& result)
{
  int ret = OB_SUCCESS;
  NodeStatus node_status = target_node.get_status();
  if (NODE_STATUS_READY == node_status) {
    // blocking wash, safe to read from target_node
    if (OB_FAIL(get_cursor_from_node(
            target_node, pkey, query_log_id, min_log_id, max_log_id, start_offset_index, result))) {
      CSR_LOG(WARN,
          "get cursor from target_node error",
          K(ret),
          K(pkey),
          K(query_log_id),
          K(min_log_id),
          K(max_log_id),
          K(start_offset_index),
          K(result));
    } else {
      // success, do nothing
    }
  } else if (NODE_STATUS_LOADING == node_status) {
    ret = OB_NEED_RETRY;
    CSR_LOG(TRACE, "try to query a loading pf_cache_node", K(ret), K(file_id), K(pkey), K(query_log_id));
  } else {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "unexpect node_status", K(ret), K(file_id), K(node_status));
  }
  return ret;
}

// Return value:
// 1. OB_SUCCESS                target_log is target log
// 2. OB_NEED_RETRY             target_log is invalid
// 4. OB_ERR_OUT_OF_LOWER_BOUND target_log is the lower bound value
// 5. OB_ERR_OUT_OF_UPPER_BOUND target_log is the upper bound value
// 6. Other error code          target_log is invalid
int ObIlogCache::locate_by_timestamp(const file_id_t file_id, const common::ObPartitionKey& pkey,
    const int64_t start_ts, const uint64_t min_log_id, const uint64_t max_log_id, const offset_t start_offset_index,
    uint64_t& target_log_id, int64_t& target_log_timestamp)
{
  int ret = OB_SUCCESS;
  ObIlogStorageQueryCost csr_cost;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_FILE_ID == file_id) || OB_UNLIKELY(!pkey.is_valid()) ||
             OB_UNLIKELY(OB_INVALID_TIMESTAMP == start_ts) || OB_UNLIKELY(OB_INVALID_ID == min_log_id) ||
             OB_UNLIKELY(OB_INVALID_ID == max_log_id) || OB_UNLIKELY(start_offset_index < 0)) {
    CSR_LOG(WARN,
        "invalid argument",
        K(file_id),
        K(pkey),
        K(start_ts),
        K(min_log_id),
        K(max_log_id),
        K(start_offset_index));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(do_locate_by_timestamp_(file_id,
                 pkey,
                 start_ts,
                 min_log_id,
                 max_log_id,
                 start_offset_index,
                 target_log_id,
                 target_log_timestamp,
                 csr_cost))) {
    if (OB_ERR_OUT_OF_UPPER_BOUND == ret || OB_ERR_OUT_OF_LOWER_BOUND == ret) {
      // Expected error code
    } else {
      CSR_LOG(WARN,
          "IlogCache do_locate_by_timestamp fail",
          K(ret),
          K(file_id),
          K(start_ts),
          K(pkey),
          K(min_log_id),
          K(max_log_id),
          K(start_offset_index),
          K(target_log_id),
          K(target_log_timestamp));
    }
  } else {
    // success
  }

  return ret;
}

int ObIlogCache::do_locate_by_timestamp_(const file_id_t file_id, const common::ObPartitionKey& pkey,
    const int64_t start_ts, const uint64_t min_log_id, const uint64_t max_log_id, const offset_t start_offset_index,
    uint64_t& target_log_id, int64_t& target_log_timestamp, ObIlogStorageQueryCost& csr_cost)
{
  int ret = OB_SUCCESS;
  // Don't retry
  // const int64_t SLEEP_INTERVAL_FOR_WAIT_READY = 10 * 1000; // 10 ms
  const int64_t RETRY_COUNT_FOR_WAIT_READY = 0;
  int64_t retry_times = 0;
  do {
    {
      ObIlogPerFileCacheNode target_node;
      target_node.reset();
      if (OB_FAIL(prepare_cache_node(file_id, target_node, csr_cost))) {
        CSR_LOG(WARN, "prepare target cache_node error", K(ret), K(file_id), K(target_node));
      } else {
        if (OB_FAIL(locate_from_node_by_ts_(file_id,
                target_node,
                pkey,
                start_ts,
                min_log_id,
                max_log_id,
                start_offset_index,
                target_log_id,
                target_log_timestamp))) {
          if (OB_NEED_RETRY == ret) {
            CSR_LOG(TRACE,
                "locate from IlogPerFileCache need retry",
                K(ret),
                K(file_id),
                K(target_node),
                K(pkey),
                K(start_ts),
                K(min_log_id),
                K(max_log_id),
                K(start_offset_index));
          } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret || OB_ERR_OUT_OF_UPPER_BOUND == ret) {
          } else {
            CSR_LOG(WARN,
                "locate from IlogPerFileCache error",
                K(ret),
                K(file_id),
                K(target_node),
                K(pkey),
                K(start_ts),
                K(min_log_id),
                K(max_log_id),
                K(start_offset_index),
                K(target_log_id),
                K(target_log_timestamp));
          }
        }
        target_node.dec_wrapper_ref();
      }
    }

    if (OB_NEED_RETRY == ret) {
      // usleep(SLEEP_INTERVAL_FOR_WAIT_READY);
      CSR_LOG(DEBUG,
          "wait for IlogPerFileCache re-loading",
          K(ret),
          K(file_id),
          K(pkey),
          K(start_ts),
          K(min_log_id),
          K(max_log_id),
          K(start_offset_index));
    } else if (OB_SIZE_OVERFLOW == ret) {
      CSR_LOG(INFO,
          "[ILOG_CACHE] prepare ilog node for locate_by_timestamp, "
          "size overflow, force wash",
          K(file_id),
          K(start_ts),
          K(pkey),
          K(min_log_id),
          K(max_log_id),
          K(start_offset_index));

      ret = OB_SUCCESS;
      if (OB_FAIL(force_wash())) {
        CSR_LOG(WARN, "force_wash error", K(ret));
      } else {
        ret = OB_NEED_RETRY;  // memory sequeezed for new file now, retry
      }
    }
  } while (OB_NEED_RETRY == ret && ++retry_times < RETRY_COUNT_FOR_WAIT_READY);

  return ret;
}

int ObIlogCache::locate_from_node_by_ts_(const file_id_t file_id, ObIlogPerFileCacheNode& target_node,
    const common::ObPartitionKey& pkey, const int64_t start_ts, const uint64_t min_log_id, const uint64_t max_log_id,
    const offset_t start_offset_index, uint64_t& target_log_id, int64_t& target_log_timestamp)
{
  int ret = OB_SUCCESS;
  NodeStatus node_status = target_node.get_status();

  // Only query the node which status is NODE_STATUS_READY.
  if (NODE_STATUS_READY == node_status) {
    ObIlogPerFileCacheWrapper* cache_wrapper = target_node.get_cache_wrapper();
    if (OB_ISNULL(cache_wrapper)) {
      ret = OB_ERR_UNEXPECTED;
      CSR_LOG(ERROR,
          "null cache_wrapper of a ready or deleting cache node",
          K(ret),
          K(target_node),
          K(pkey),
          K(start_ts),
          K(min_log_id),
          K(max_log_id),
          K(start_offset_index));
    } else {
      // Update access time
      cache_wrapper->last_access_ts_ = ObTimeUtility::current_time();
      ObIlogPerFileCache& pf_cache = cache_wrapper->cache_;

      ret = pf_cache.locate_by_timestamp(
          pkey, start_ts, min_log_id, max_log_id, start_offset_index, target_log_id, target_log_timestamp);

      if (OB_SUCCESS == ret || OB_ERR_OUT_OF_UPPER_BOUND == ret || OB_ERR_OUT_OF_LOWER_BOUND == ret) {
        // Expected error code.
      } else {
        CSR_LOG(WARN,
            "pf_cache locate_by_timestamp fail",
            K(ret),
            K(file_id),
            K(pkey),
            K(start_ts),
            K(min_log_id),
            K(max_log_id),
            K(start_offset_index),
            K(target_log_id),
            K(target_log_timestamp));
      }
    }
  } else if (NODE_STATUS_LOADING == node_status) {
    ret = OB_NEED_RETRY;
    CSR_LOG(TRACE, "try to query a loading pf_cache_node", K(ret), K(file_id), K(pkey), K(start_ts));
  } else {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "unexpect node_status", K(ret), K(file_id), K(node_status));
  }
  return ret;
}

int ObIlogCache::get_cursor(const file_id_t file_id, const common::ObPartitionKey& pkey, const uint64_t query_log_id,
    const uint64_t min_log_id, const uint64_t max_log_id, const offset_t start_offset_index, ObGetCursorResult& result,
    ObIlogStorageQueryCost& csr_cost)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_FILE_ID == file_id) || OB_UNLIKELY(!pkey.is_valid()) ||
             OB_UNLIKELY(OB_INVALID_ID == query_log_id) || OB_UNLIKELY(OB_INVALID_ID == min_log_id) ||
             OB_UNLIKELY(OB_INVALID_ID == max_log_id) || OB_UNLIKELY(start_offset_index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(WARN,
        "ObIlogCache get_cursor error",
        K(ret),
        K(file_id),
        K(pkey),
        K(query_log_id),
        K(min_log_id),
        K(max_log_id),
        K(start_offset_index));
  } else {
    if (OB_FAIL(
            do_get_cursor(file_id, pkey, query_log_id, min_log_id, max_log_id, start_offset_index, result, csr_cost))) {
      CSR_LOG(WARN,
          "do_get_cursor error",
          K(ret),
          K(pkey),
          K(query_log_id),
          K(min_log_id),
          K(max_log_id),
          K(start_offset_index),
          K(result));
    }
  }
  return ret;
}

int ObIlogCache::prepare_cache_node(const file_id_t file_id)
{
  int ret = OB_SUCCESS;
  ObIlogPerFileCacheNode target_node;
  target_node.reset();
  ObIlogStorageQueryCost csr_cost;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogCache is not inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_FILE_ID == file_id)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(file_id));
  } else if (OB_FAIL(prepare_cache_node(file_id, target_node, csr_cost))) {
    CSR_LOG(WARN, "prepare target cache_node error", K(ret), K(file_id), K(target_node));
  } else {
    target_node.dec_wrapper_ref();
  }

  if (OB_NEED_RETRY == ret) {
    CSR_LOG(INFO, "ilog file is re-loading, need retry", K(ret), K(file_id));
  } else if (OB_SIZE_OVERFLOW == ret) {
    CSR_LOG(INFO, "[ILOG_CACHE] prepare ilog node , size overflow, force wash", K(ret), K(file_id));

    if (OB_FAIL(force_wash())) {
      CSR_LOG(WARN, "force_wash error", K(ret));
    } else {
      ret = OB_NEED_RETRY;  // memory sequeezed for new file now, retry
    }
  }

  return ret;
}

int ObIlogCache::do_get_cursor(const file_id_t file_id, const ObPartitionKey& pkey, const uint64_t query_log_id,
    const uint64_t min_log_id, const uint64_t max_log_id, const offset_t start_offset_index, ObGetCursorResult& result,
    ObIlogStorageQueryCost& csr_cost)
{
  int ret = OB_SUCCESS;

  ObIlogPerFileCacheNode target_node;
  target_node.reset();
  if (OB_FAIL(prepare_cache_node(file_id, target_node, csr_cost))) {
    CSR_LOG(WARN, "prepare target cache_node error", K(ret), K(file_id), K(target_node));
  } else {
    if (OB_FAIL(query_from_node(
            file_id, target_node, pkey, query_log_id, min_log_id, max_log_id, start_offset_index, result))) {
      if (OB_NEED_RETRY == ret) {
        CSR_LOG(TRACE,
            "query from target_node, need retry",
            K(ret),
            K(file_id),
            K(target_node),
            K(pkey),
            K(query_log_id),
            K(min_log_id),
            K(max_log_id),
            K(start_offset_index),
            K(result));
      } else {
        CSR_LOG(WARN,
            "query from target_node error",
            K(ret),
            K(file_id),
            K(target_node),
            K(pkey),
            K(query_log_id),
            K(min_log_id),
            K(max_log_id),
            K(start_offset_index),
            K(result));
      }
    } else if (OB_UNLIKELY(!result.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      CSR_LOG(WARN, "invalid result", K(ret), K(file_id), K(pkey), K(query_log_id), K(result));
    } else {
      CSR_LOG(DEBUG,
          "get cursor success",
          K(file_id),
          K(pkey),
          K(query_log_id),
          K(min_log_id),
          K(max_log_id),
          K(start_offset_index),
          K(result));
    }
    target_node.dec_wrapper_ref();
  }

  if (OB_NEED_RETRY == ret) {
    CSR_LOG(INFO, "ilog file is re-loading, need retry", K(ret), K(file_id), K(pkey), K(query_log_id));
  } else if (OB_SIZE_OVERFLOW == ret) {
    CSR_LOG(INFO,
        "[ILOG_CACHE] prepare ilog node for get_cursor, size overflow, force wash",
        K(file_id),
        K(pkey),
        K(query_log_id),
        K(min_log_id),
        K(max_log_id),
        K(start_offset_index));

    if (OB_FAIL(force_wash())) {
      CSR_LOG(WARN, "force_wash error", K(ret));
    } else {
      ret = OB_NEED_RETRY;  // memory sequeezed for new file now, retry
    }
  }

  return ret;
}

int ObIlogCache::prepare_loading(const file_id_t file_id, const bool is_preload, ObIlogPerFileCacheNode& node)
{
  int ret = OB_SUCCESS;
  // alloc cache_wrapper, mark loading
  char* buf = reinterpret_cast<char*>(wrapper_allocator_.alloc());
  if (OB_UNLIKELY(NULL == buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObIlogPerFileCacheWrapper* cache_wrapper = new (buf) ObIlogPerFileCacheWrapper;
    if (OB_FAIL(node.set_cache_wrapper(cache_wrapper, &wrapper_allocator_))) {
      CSR_LOG(WARN, "set cache_wrapper error", K(ret), K(file_id), K(node));
    } else if (OB_FAIL(node.mark_loading(file_id))) {
      CSR_LOG(WARN, "mark loading error", K(ret), K(file_id), K(node));
    } else {
      cache_wrapper->set_mod_and_tenant(ObModIds::OB_CSR_PER_FILE_BUF_AND_MAP, OB_SERVER_TENANT_ID);
      if (is_preload) {
        cache_wrapper->be_kind_to_preload();
      }
    }
  }
  CSR_LOG(INFO, "[ILOG_CACHE] prepare_loading", K(ret), K(file_id), K(node));
  return ret;
}

bool ObIlogCache::NodeReadyMarker::operator()(const file_id_t file_id, ObIlogPerFileCacheNode& node)
{
  // ObLinearHashMap apply this functor with bucket lock hold
  if (OB_UNLIKELY(OB_INVALID_FILE_ID == file_id) || OB_UNLIKELY(!node.is_valid())) {
    err_ = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid node in node_map_", K(err_));
  } else if (node.get_seq() != owner_seq_) {
    err_ = OB_STATE_NOT_MATCH;
    owner_match_ = false;
    CSR_LOG(WARN, "owner not match", K(err_), K(file_id), K(node), K(owner_seq_));
  } else {
    if (OB_UNLIKELY(OB_SUCCESS != (err_ = node.mark_ready(file_id)))) {
      CSR_LOG(WARN, "node mark_ready error", K(err_), K(file_id), K(node));
    } else {
      CSR_LOG(INFO, "[ILOG_CACHE] mark node ready", K(file_id), K(node));
    }
  }
  node_ = node;
  return true;  // always return true to node_map_.operate
}

bool ObIlogCache::GetNodeFunctor::operator()(const file_id_t file_id, ObIlogPerFileCacheNode& node)
{
  if (OB_UNLIKELY(OB_INVALID_FILE_ID == file_id) || OB_UNLIKELY(!node.is_valid())) {
    err_ = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid node in node_map_", K(err_));
  } else {
    node.inc_wrapper_ref();
  }
  node_ = node;
  return true;
}

bool ObIlogCache::EraseNodeFunctor::operator()(const file_id_t file_id, ObIlogPerFileCacheNode& node)
{
  if (OB_UNLIKELY(OB_INVALID_FILE_ID == file_id) || OB_UNLIKELY(!node.is_valid())) {
    err_ = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid node in node_map_", K(err_));
  } else {
    node.dec_wrapper_ref();
  }
  return true;
}

// NOTE: time-consumed
int ObIlogCache::sync_load_ilog_file(
    const file_id_t file_id, ObIlogPerFileCacheNode& target_node, ObIlogStorageQueryCost& csr_cost)
{
  // my node may be washed during loading. In this case, rollback this load
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!target_node.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(WARN, "invalid target_node", K(ret), K(target_node), K(file_id));
  } else if (OB_ISNULL(pf_cache_builder_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(pf_cache_builder_->build_cache(file_id,
                 &(target_node.get_cache_wrapper()->cache_),
                 target_node.get_cache_wrapper()->page_arena_,
                 csr_cost))) {
    CSR_LOG(WARN, "build pf_cache error", K(ret), K(ret), K(file_id), K(target_node));
    EraseNodeFunctor erase_fn;
    tmp_ret = node_map_.erase_if(file_id, erase_fn);
    if (OB_SUCCESS == tmp_ret) {
      CSR_LOG(INFO, "node_map_ erase success", K(tmp_ret), K(file_id));
    } else if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      // OB_ENTRY_NOT_EXIST: my node (or an invader's node maybe) is washed by some invader
      ret = OB_NEED_RETRY;
      CSR_LOG(WARN, "erase cache_node error, entry not exist, maybe washed", K(tmp_ret), K(file_id));
    } else {
      CSR_LOG(WARN, "erase cache_node error", K(tmp_ret), K(file_id));
    }
  } else {
    NodeReadyMarker node_ready_marker(target_node.get_seq());
    if (OB_FAIL(node_map_.operate(file_id, node_ready_marker))) {
      CSR_LOG(WARN, "node_map_ apply node_ready_marker error", K(ret), K(file_id), K(node_ready_marker));
    } else if (OB_FAIL(node_ready_marker.get_err())) {
      CSR_LOG(WARN, "mark ready error", K(ret), K(file_id), K(target_node), K(node_ready_marker));
    } else {
      target_node = node_ready_marker.get_marked_node();
      CSR_LOG(TRACE, "mark ready success", K(file_id), K(target_node));
    }
    if (OB_STATE_NOT_MATCH == ret || OB_ENTRY_NOT_EXIST == ret) {
      // OB_STATE_NOT_MATCH: node in map is not mine, mine is occupied by an invader.
      ret = OB_NEED_RETRY;
    }
  }
  const int64_t end_ts = ObTimeUtility::current_time();
  if (OB_SUCC(ret)) {
    CSR_LOG(
        INFO, "[ILOG_CACHE] sync load ilog_file succ", K(file_id), "load_time_us", end_ts - start_ts, K(target_node));
  } else {
    CSR_LOG(WARN, "sync load ilog_file erorr", K(ret), K(file_id), K(target_node));
  }
  return ret;
}

bool ObIlogCache::VictimPicker::operator()(const file_id_t file_id, ObIlogPerFileCacheNode& node)
{
  int ret = OB_SUCCESS;
  ObIlogPerFileCacheWrapper* cache_wrapper = NULL;
  if (OB_UNLIKELY(OB_INVALID_FILE_ID == file_id) || OB_UNLIKELY(!node.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "invalid item in node_map_", K(ret), K(file_id), K(node));
  } else if (OB_ISNULL(cache_wrapper = node.get_cache_wrapper())) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "null cache_wrapper_", K(ret), K(file_id), K(node));
  } else {
    int64_t cur_last_access_ts = cache_wrapper->last_access_ts_;
    if (cur_last_access_ts < min_access_ts_) {
      min_access_ts_ = cur_last_access_ts;
      victim_ = file_id;
    }
    count_++;
    CSR_LOG(DEBUG, "victim_picker iterate", K(file_id), K(min_access_ts_), K(cur_last_access_ts), K(count_));
  }
  err_ = ret;
  return OB_SUCCESS == ret;
}

int ObIlogCache::get_victim_file_id(file_id_t& ret_victim_file_id, int64_t& min_access_ts)
{
  int ret = OB_SUCCESS;
  VictimPicker victim_picker;
  if (OB_FAIL(node_map_.for_each(victim_picker))) {
    CSR_LOG(WARN, "apply victim_picker on node_map_ error", K(ret), K(victim_picker));
  } else if (OB_FAIL(victim_picker.get_err())) {
    CSR_LOG(WARN, "victim_picker exec error", K(ret), K(victim_picker));
  } else {
    file_id_t victim_file_id = victim_picker.get_victim_file_id();
    if (OB_INVALID_FILE_ID == victim_file_id) {
      if (0 == victim_picker.get_count()) {
        ret = OB_SUCCESS;
        CSR_LOG(TRACE, "empty node_map_", K(victim_picker));
      } else {
        ret = OB_ERR_UNEXPECTED;
        CSR_LOG(ERROR, "invalid victim_file_id file_id", K(ret), K(victim_picker));
      }
    }
    ret_victim_file_id = victim_file_id;
    min_access_ts = victim_picker.get_min_access_ts();
  }
  CSR_LOG(TRACE, "get_victim_file_id finish", K(ret), K(ret_victim_file_id), K(min_access_ts));
  return ret;
}

int ObIlogCache::wash_victim(const file_id_t victim_file_id)
{
  int ret = OB_SUCCESS;
  // Concurrency control
  // Steps: erase -> wait -> destroy
  // Note 1: Load ilog file is finished in prepare_cache_node.
  // Note 2: Erasing victim_file_id from map immediately, the quere in flying will insert a new node into map.
  // the memory of two nodes and their per_file_cache are independent.
  EraseNodeFunctor erase_fn;
  if (OB_FAIL(node_map_.erase_if(victim_file_id, erase_fn))) {
    CSR_LOG(WARN, "erase victim_node error", K(ret), K(victim_file_id));
  } else {
    CSR_LOG(INFO, "[ILOG_CACHE] wash_victim erase node success, start wait qs", K(victim_file_id));
  }
  return ret;
}

int ObIlogCache::force_wash()
{
  int ret = OB_SUCCESS;
  file_id_t victim_file_id = OB_INVALID_FILE_ID;
  int64_t min_access_ts = OB_INVALID_TIMESTAMP;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_victim_file_id(victim_file_id, min_access_ts))) {
    CSR_LOG(WARN, "get_victim_file_id error", K(ret));
  } else if (OB_INVALID_FILE_ID == victim_file_id) {
    // return success
    CSR_LOG(WARN, "[ILOG_CACHE] no victim_file_id file_id picked when force_wash", K(victim_file_id));
  } else {
    if (OB_FAIL(wash_victim(victim_file_id))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // another faster cleaner
        ret = OB_SUCCESS;
      } else if (OB_STATE_NOT_MATCH == ret) {
        CSR_LOG(WARN, "wash_victim error, not a ready node", K(ret), K(victim_file_id));
      } else {
        CSR_LOG(WARN, "wash_victim error", K(ret), K(victim_file_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    CSR_LOG(INFO, "[ILOG_CACHE] ObIlogCache wash success", K(victim_file_id), K(min_access_ts));
  } else {
    CSR_LOG(WARN, "[ILOG_CACHE] ObIlogCache wash error", K(ret), K(victim_file_id), K(min_access_ts));
  }
  return ret;
}

bool ObIlogCache::ExpiredNodesPicker::operator()(const file_id_t file_id, ObIlogPerFileCacheNode& node)
{
  int ret = OB_SUCCESS;
  ObIlogPerFileCacheWrapper* cache_wrapper = NULL;
  if (OB_UNLIKELY(OB_INVALID_FILE_ID == file_id) || OB_UNLIKELY(!node.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "invalid item in node_map_", K(ret), K(file_id), K(node));
  } else if (OB_ISNULL(cache_wrapper = node.get_cache_wrapper())) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "null cache_wrapper_", K(ret), K(file_id), K(node));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    if (now - cache_wrapper->last_access_ts_ > EXPIRED_INVTERVAL) {
      if (OB_FAIL(expired_file_id_arr_.push_back(file_id))) {
        CSR_LOG(WARN, "expired_file_id_arr_ push error", K(ret), K(file_id), K(now), K(node));
      }
    } else {
      CSR_LOG(TRACE, "active ilog cache node", K(file_id), K(now), "last_access_ts", cache_wrapper->last_access_ts_);
    }
  }
  err_ = ret;
  return OB_SUCCESS == ret;
}

int ObIlogCache::get_expired_arr(FileIdArray& file_id_arr)
{
  int ret = OB_SUCCESS;
  ExpiredNodesPicker expired_node_picker(file_id_arr);
  if (OB_FAIL(node_map_.for_each(expired_node_picker))) {
    CSR_LOG(WARN, "apply expired_node_picker on node_map_ error", K(ret), K(expired_node_picker));
  } else if (OB_FAIL(expired_node_picker.get_err())) {
    CSR_LOG(WARN, "expired_node_picker exec error", K(ret), K(expired_node_picker));
  } else {
    // do nothing
  }
  return ret;
}

int ObIlogCache::timer_wash()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  FileIdArray expired_file_id_arr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_expired_arr(expired_file_id_arr))) {
    CSR_LOG(WARN, "get_expired_arr error", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < expired_file_id_arr.count(); i++) {
      const file_id_t victim = expired_file_id_arr[i];
      if (OB_SUCCESS != (tmp_ret = wash_victim(victim))) {
        CSR_LOG(WARN, "timer_wash wash some victim error", K(tmp_ret), K(victim), K(i), K(expired_file_id_arr));
      } else {
        CSR_LOG(INFO, "timer_wash wash some victim success", K(victim), K(i), K(expired_file_id_arr));
      }
    }
    if (OB_SUCC(ret)) {
      CSR_LOG(INFO,
          "[ILOG_CACHE] timer_wash wash expired file success",
          "count",
          expired_file_id_arr.count(),
          K(expired_file_id_arr));
    } else {
      CSR_LOG(WARN,
          "[ILOG_CACHE] timer_wash wash expired file error",
          K(ret),
          "count",
          expired_file_id_arr.count(),
          K(expired_file_id_arr));
    }
  }
  return ret;
}

bool ObIlogCache::AllFileIdGetter::operator()(const file_id_t file_id, ObIlogPerFileCacheNode& node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_FILE_ID == file_id)) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "invalid item in node_map_", K(ret), K(file_id), K(node));
  } else if (OB_FAIL(all_file_id_arr_.push_back(file_id))) {
    CSR_LOG(WARN, "all_file_id_arr_ push error", K(ret), K(file_id), K(node));
  }
  err_ = ret;
  return OB_SUCCESS == ret;
}

int ObIlogCache::get_all_file_id_arr(FileIdArray& all_file_id_arr)
{
  int ret = OB_SUCCESS;
  AllFileIdGetter all_file_id_getter(all_file_id_arr);
  if (OB_FAIL(node_map_.for_each(all_file_id_getter))) {
    CSR_LOG(WARN, "apply expired_node_picker on node_map_ error", K(ret), K(all_file_id_getter));
  } else if (OB_FAIL(all_file_id_getter.get_err())) {
    CSR_LOG(WARN, "expired_node_picker exec error", K(ret), K(all_file_id_getter));
  } else {
    // do nothing
  }
  return ret;
}

int ObIlogCache::admin_wash()
{
  CSR_LOG(INFO, "[ILOG_CACHE] admin wash start");
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  FileIdArray all_file_id_arr;
  int64_t washed_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_all_file_id_arr(all_file_id_arr))) {
    CSR_LOG(WARN, "get_all_file_id_arr error", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < all_file_id_arr.count(); i++) {
      const file_id_t victim = all_file_id_arr[i];
      if (OB_SUCCESS != (tmp_ret = wash_victim(victim))) {
        CSR_LOG(WARN, "admin wash some victim error", K(tmp_ret), K(victim), K(i), K(all_file_id_arr));
      } else {
        CSR_LOG(INFO, "[ILOG_CACHE] admin wash some victim success", K(victim), K(i), K(all_file_id_arr));
        washed_count++;
      }
    }
  }
  CSR_LOG(INFO, "[ILOG_CACHE] admin wash", K(ret), K(washed_count), K(all_file_id_arr));
  return ret;
}

int ObIlogCache::admin_wash(const file_id_t file_id)
{
  CSR_LOG(INFO, "admin wash start", K(file_id));
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_FILE_ID == file_id)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(WARN, "admin wash file id error, invalid file id", K(ret), K(file_id));
  } else if (OB_FAIL(wash_victim(file_id))) {
    CSR_LOG(WARN, "wash victim error", K(ret), K(file_id));
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  } else {
  }
  CSR_LOG(INFO, "[ILOG_CACHE] admin wash", K(ret), K(file_id));
  return ret;
}

// called in timer
int ObIlogCache::report()
{
  int ret = OB_SUCCESS;
  NodeReporter node_reporter;
  if (OB_FAIL(node_map_.for_each(node_reporter))) {
    CSR_LOG(WARN, "node_map_ apply NodeReporter error", K(ret));
  } else {
    CSR_LOG(INFO, "[ILOG_CACHE] ObIlogCache Report", K(node_reporter));
  }
  return ret;
}

int64_t ObIlogCache::NodeReporter::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t count = arr_.count();
  databuff_printf(buf, buf_len, pos, "ObIlogCache NodeReport, count=%ld, {", count);
  for (int i = 0; i < count && pos < buf_len; i++) {
    pos += arr_[i].to_string(buf + pos, buf_len - pos);
  }
  databuff_printf(buf, buf_len, pos, "}");
  return pos;
}

}  // namespace clog
}  // namespace oceanbase
