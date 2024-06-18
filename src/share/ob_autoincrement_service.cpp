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

#include "share/ob_autoincrement_service.h"
#include <algorithm>
#include <stdint.h>
#include "lib/worker.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/time/ob_time_utility.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/ash/ob_active_session_guard.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/partition_table/ob_partition_location.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_schema_status_proxy.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_sql_client_decorator.h"

using namespace oceanbase::obrpc;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace share
{
#ifndef INT24_MIN
#define INT24_MIN     (-8388607 - 1)
#endif
#ifndef INT24_MAX
#define INT24_MAX     (8388607)
#endif
#ifndef UINT24_MAX
#define UINT24_MAX    (16777215U)
#endif

static bool is_rpc_error(int error_code)
{
  return (is_timeout_err(error_code) || is_server_down_error(error_code));
}

int CacheNode::combine_cache_node(CacheNode &new_node)
{
  int ret = OB_SUCCESS;
  if (new_node.cache_start_ > 0) {
    if (cache_end_ < cache_start_
        || new_node.cache_end_ < new_node.cache_start_
        || cache_end_ > new_node.cache_start_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cache node is invalid", K(*this), K(new_node), K(ret));
    } else {
      if (cache_end_ > 0 && cache_end_ == new_node.cache_start_ - 1) {
        cache_end_ = new_node.cache_end_;
      } else {
        cache_start_ = new_node.cache_start_;
        cache_end_ = new_node.cache_end_;
      }
      new_node.reset();
    }
  }
  return ret;
}

int TableNode::alloc_handle(ObSmallAllocator &allocator,
                            const uint64_t offset,
                            const uint64_t increment,
                            const uint64_t desired_count,
                            const uint64_t max_value,
                            CacheHandle *&handle,
                            const bool is_retry_alloc)
{
  int ret = OB_SUCCESS;
  CacheNode node = curr_node_;
  uint64_t min_value = 0;
  const uint64_t local_sync = ATOMIC_LOAD(&local_sync_);
  if (local_sync < next_value_) {
    min_value = next_value_;
  } else {
    if (local_sync >= max_value) {
      min_value = max_value;
    } else {
      min_value = local_sync + 1;
    }
  }
  if (min_value < node.cache_start_) {
    min_value = node.cache_start_;
  }
  uint64_t new_next_value = 0;
  uint64_t needed_interval = 0;
  if (curr_node_state_is_pending_) {
    ret = OB_SIZE_OVERFLOW;
  } else if (min_value >= max_value) {
    new_next_value = max_value;
    needed_interval = max_value;
  } else if (min_value > node.cache_end_) {
    ret = OB_SIZE_OVERFLOW;
    LOG_TRACE("fail to alloc handle; cache is not enough", K(min_value), K(max_value), K(node), K(*this), K(ret));
  } else {
    ret = ObAutoincrementService::calc_next_value(min_value,
                                                  offset,
                                                  increment,
                                                  new_next_value);
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to calc next value", K(ret));
    } else {
      bool reach_upper_limit = false;
      if (max_value == node.cache_end_) {
        // won't get cache from global again
        reach_upper_limit = true;
      }
      if (new_next_value > node.cache_end_) {
        if (reach_upper_limit) {
          new_next_value = max_value;
          needed_interval = max_value;
        } else {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("fail to alloc handle; cache is not enough",
                   K(node), K(min_value), K(offset), K(increment), K(max_value), K(new_next_value),
                   K(*this), K(ret));
        }
      } else {
        needed_interval = new_next_value + increment * (desired_count - 1);
        // check overflow
        if (needed_interval < new_next_value) {
          needed_interval = UINT64_MAX;
        }
        if (needed_interval > node.cache_end_) {
          if (reach_upper_limit) {
            needed_interval = max_value;
          } else {
            ret = OB_SIZE_OVERFLOW;
            // don't print warn log for common buffer burnout case, as we will fetch next buffer
            if (is_retry_alloc) {
              LOG_WARN("fail to alloc handle; cache is not enough", K(*this), K(ret));
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    handle = static_cast<CacheHandle *>(allocator.alloc());
    if (NULL == handle) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to alloc cache handle", K(ret));
    } else {
      if (UINT64_MAX == needed_interval) {
        // compatible with MySQL; return error when reach UINT64_MAX
        ret = OB_ERR_REACH_AUTOINC_MAX;
        LOG_WARN("reach UINT64_MAX", K(ret));
      } else {
        handle = new (handle) CacheHandle;
        handle->offset_ = offset;
        handle->increment_ = increment;
        handle->next_value_ = new_next_value;
        handle->prefetch_start_ = new_next_value;
        handle->prefetch_end_ = needed_interval;
        handle->max_value_ = max_value;
        next_value_ = needed_interval + increment;
        LOG_TRACE("succ to allocate cache handle", K(*handle), K(ret));
      }
    }
  } else if (OB_SIZE_OVERFLOW == ret) {
    if (prefetch_node_.cache_start_ > 0) {
      if (OB_FAIL(curr_node_.combine_cache_node(prefetch_node_))) {
        LOG_WARN("failed to combine cache node", K(ret));
      } else if (OB_FAIL(alloc_handle(allocator, offset, increment, desired_count, max_value, handle))) {
        LOG_WARN("failed to allocate cache handle", K(offset), K(increment), K(desired_count), K(ret));
      }
    }
  } else {
    LOG_WARN("unexpected error", K(ret));
  }

  return ret;
}

int TableNode::init(int64_t autoinc_table_part_num)
{
  UNUSED(autoinc_table_part_num);
  return OB_SUCCESS;
}

int CacheHandle::next_value(uint64_t &next_value)
{
  int ret = OB_SUCCESS;
  if (last_row_dup_flag_ && 0 != last_value_to_confirm_) {
    // use last auto-increment value
    next_value = last_value_to_confirm_;
    last_row_dup_flag_ = false;
  } else {
    if (next_value_ >= max_value_) {
      if (OB_FAIL(ObAutoincrementService::calc_prev_value(max_value_, offset_, increment_, next_value))) {
        LOG_WARN("failed to calc previous value", K(ret));
      }
    } else {
      if (next_value_ > prefetch_end_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_DEBUG("no available value in CacheHandle", K_(next_value), K(ret));
      } else {
        next_value = next_value_;
        // for insert...on duplicate key update
        // if last row is duplicate, auto-increment value should be used in next row
        last_value_to_confirm_ = next_value_;
        last_row_dup_flag_ = false;

        next_value_ += increment_;
      }
    }
  }
  return ret;
}

ObAutoincrementService::ObAutoincrementService()
  : node_allocator_(),
    handle_allocator_(),
    mysql_proxy_(NULL),
    srv_proxy_(NULL),
    schema_service_(NULL),
    map_mutex_(common::ObLatchIds::AUTO_INCREMENT_INIT_LOCK)
{
}

ObAutoincrementService::~ObAutoincrementService()
{
}

ObAutoincrementService &ObAutoincrementService::get_instance()
{
  static ObAutoincrementService autoinc_service;
  return autoinc_service;
}

int ObAutoincrementService::init(ObAddr &addr,
                                 ObMySQLProxy *mysql_proxy,
                                 ObSrvRpcProxy *srv_proxy,
                                 ObMultiVersionSchemaService *schema_service,
                                 rpc::frame::ObReqTransport *req_transport)
{
  int ret = OB_SUCCESS;
  my_addr_ = addr;
  mysql_proxy_ = mysql_proxy;
  srv_proxy_ = srv_proxy;
  schema_service_ = schema_service;

  ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::OB_AUTOINCREMENT);
  SET_USE_500(attr);
  if (OB_FAIL(distributed_autoinc_service_.init(mysql_proxy))) {
    LOG_WARN("fail init distributed_autoinc_service_ service", K(ret));
  } else if (OB_FAIL(global_autoinc_service_.init(my_addr_, req_transport))) {
    LOG_WARN("fail init auto inc global service", K(ret));
  } else if (OB_FAIL(node_allocator_.init(sizeof(TableNode), attr))) {
    LOG_WARN("failed to init table node allocator", K(ret));
  } else if (OB_FAIL(handle_allocator_.init(sizeof(CacheHandle), attr))) {
    LOG_WARN("failed to init cache handle allocator", K(ret));
  } else if (OB_FAIL(node_map_.init(attr))) {
    LOG_WARN("failed to init table node map", K(ret));
  } else {
    for (int64_t i = 0; i < INIT_NODE_MUTEX_NUM; ++i) {
      init_node_mutex_[i].set_latch_id(common::ObLatchIds::AUTO_INCREMENT_INIT_LOCK);
    }
  }
  return ret;
}

//only used for logic backup
int ObAutoincrementService::init_for_backup(ObAddr &addr,
                                            ObMySQLProxy *mysql_proxy,
                                            ObSrvRpcProxy *srv_proxy,
                                            ObMultiVersionSchemaService *schema_service,
                                            rpc::frame::ObReqTransport *req_transport)
{
  int ret = OB_SUCCESS;
  my_addr_ = addr;
  mysql_proxy_ = mysql_proxy;
  srv_proxy_ = srv_proxy;
  schema_service_ = schema_service;
  OZ(distributed_autoinc_service_.init(mysql_proxy));
  OZ(global_autoinc_service_.init(my_addr_, req_transport));
  return ret;
}

int ObAutoincrementService::get_handle(AutoincParam &param,
                                       CacheHandle *&handle)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_sequence_load);
  int ret = param.autoinc_mode_is_order_ ?
    get_handle_order(param, handle) : get_handle_noorder(param, handle);
  return ret;
}

int ObAutoincrementService::get_handle(const ObSequenceSchema &schema, ObSequenceValue &nextval)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_sequence_load);
  int ret = OB_SUCCESS;

  if (OB_FAIL(global_autoinc_service_.get_sequence_next_value(
        schema, nextval))) {
    LOG_WARN("fail get value", K(ret));
  } else {
    LOG_TRACE("succ to allocate cache handle", K(nextval), K(ret));
  }
  return ret;
}

int ObAutoincrementService::get_handle_order(AutoincParam &param, CacheHandle *&handle)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id     = param.tenant_id_;
  const uint64_t table_id      = param.autoinc_table_id_;
  const uint64_t column_id     = param.autoinc_col_id_;
  const ObObjType column_type  = param.autoinc_col_type_;
  const uint64_t offset        = param.autoinc_offset_;
  const uint64_t increment     = param.autoinc_increment_;
  const uint64_t max_value     = get_max_value(column_type);
  const int64_t auto_increment_cache_size = param.auto_increment_cache_size_;
  const int64_t autoinc_version = param.autoinc_version_;

  uint64_t desired_count = 0;
  // calc nb_desired_values in MySQL
  if (0 == param.autoinc_intervals_count_) {
    desired_count = param.total_value_count_;
  } else if (param.autoinc_intervals_count_ <= AUTO_INC_DEFAULT_NB_MAX_BITS) {
    desired_count = AUTO_INC_DEFAULT_NB_ROWS * (1 << param.autoinc_intervals_count_);
    if (desired_count > AUTO_INC_DEFAULT_NB_MAX) {
      desired_count = AUTO_INC_DEFAULT_NB_MAX;
    }
  } else {
    desired_count = AUTO_INC_DEFAULT_NB_MAX;
  }

  // allocate auto-increment value first time
  if (0 == param.autoinc_desired_count_) {
    param.autoinc_desired_count_ = desired_count;
  }

  desired_count = param.autoinc_desired_count_;
  const uint64_t batch_count = increment * desired_count;

  AutoincKey key(tenant_id, table_id, column_id);
  uint64_t table_auto_increment = param.autoinc_auto_increment_;
  if (OB_UNLIKELY(offset > 1 && increment >= offset)) {
    // If auto_increment_offset has been set, the base_value should be the maximum value of
    // offset and table_auto_increment.
    table_auto_increment = MAX(table_auto_increment, offset);
  }
  uint64_t start_inclusive = 0;
  uint64_t end_inclusive = 0;
  uint64_t sync_value = 0;
  if (OB_UNLIKELY(table_auto_increment > max_value)) {
    // During the generation of the auto-increment column, if the user-specified value exceeds
    // the maximum value, `OB_DATA_OUT_OF_RANGE` is returned, otherwise `OB_ERR_REACH_AUTOINC_MAX`.
    ret = param.autoinc_auto_increment_ > max_value ?
      OB_ERR_REACH_AUTOINC_MAX : OB_DATA_OUT_OF_RANGE;
    LOG_WARN("reach max autoinc", K(ret), K(table_auto_increment), K(max_value));
  } else if (OB_FAIL(global_autoinc_service_.get_value(key, offset, increment, max_value,
                                                       table_auto_increment, batch_count,
                                                       auto_increment_cache_size, autoinc_version,
                                                       sync_value, start_inclusive, end_inclusive))) {
    LOG_WARN("fail get value", K(ret));
  } else if (OB_UNLIKELY(sync_value > max_value || start_inclusive > max_value)) {
    ret = OB_ERR_REACH_AUTOINC_MAX;
    LOG_WARN("reach max autoinc", K(start_inclusive), K(max_value), K(ret));
  } else {
    uint64_t new_next_value = 0;
    uint64_t needed_interval = 0;
    if (start_inclusive >= max_value) {
      new_next_value = max_value;
      needed_interval = max_value;
    } else if (OB_FAIL(calc_next_value(start_inclusive, offset, increment, new_next_value))) {
      LOG_WARN("fail to calc next value", K(ret));
    } else if (OB_UNLIKELY(new_next_value > end_inclusive)) {
      if (max_value == end_inclusive) {
        new_next_value = max_value;
        needed_interval = max_value;
      } else {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("fail to alloc handle; cache is not enough",
                K(start_inclusive), K(end_inclusive), K(offset), K(increment), K(max_value),
                K(new_next_value), K(ret));
      }
    } else {
      needed_interval = new_next_value + increment * (desired_count - 1);
      // check overflow
      if (needed_interval < new_next_value) {
        needed_interval = UINT64_MAX;
      }
      if (needed_interval > end_inclusive) {
        if (max_value == end_inclusive) {
          needed_interval = max_value;
        } else {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("fail to alloc handle; cache is not enough", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(UINT64_MAX == needed_interval)) {
        // compatible with MySQL; return error when reach UINT64_MAX
        ret = OB_ERR_REACH_AUTOINC_MAX;
        LOG_WARN("reach UINT64_MAX", K(ret));
      } else if (OB_ISNULL(handle = static_cast<CacheHandle *>(handle_allocator_.alloc()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to alloc cache handle", K(ret));
      } else {
        handle = new (handle) CacheHandle;
        handle->offset_ = offset;
        handle->increment_ = increment;
        handle->next_value_ = new_next_value;
        handle->prefetch_start_ = new_next_value;
        handle->prefetch_end_ = needed_interval;
        handle->max_value_ = max_value;
        LOG_TRACE("succ to allocate cache handle", K(*handle), K(ret));
      }
    }
  }
  return ret;
}

int ObAutoincrementService::get_handle_noorder(AutoincParam &param, CacheHandle *&handle)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id     = param.tenant_id_;
  const uint64_t table_id      = param.autoinc_table_id_;
  const uint64_t column_id     = param.autoinc_col_id_;
  const ObObjType column_type  = param.autoinc_col_type_;
  const uint64_t offset        = param.autoinc_offset_;
  const uint64_t increment     = param.autoinc_increment_;
  const uint64_t max_value     = get_max_value(column_type);

  uint64_t desired_count = 0;
  // calc nb_desired_values in MySQL
  if (0 == param.autoinc_intervals_count_) {
    desired_count = param.total_value_count_;
  } else if (param.autoinc_intervals_count_ <= AUTO_INC_DEFAULT_NB_MAX_BITS) {
    desired_count = AUTO_INC_DEFAULT_NB_ROWS * (1 << param.autoinc_intervals_count_);
    if (desired_count > AUTO_INC_DEFAULT_NB_MAX) {
      desired_count = AUTO_INC_DEFAULT_NB_MAX;
    }
  } else {
    desired_count = AUTO_INC_DEFAULT_NB_MAX;
  }

  // allocate auto-increment value first time
  if (0 == param.autoinc_desired_count_) {
    param.autoinc_desired_count_ = desired_count;
  }

  desired_count = param.autoinc_desired_count_;

  TableNode *table_node = NULL;
  LOG_DEBUG("begin to get cache handle", K(param));
  if (OB_FAIL(get_table_node(param, table_node))) {
    LOG_WARN("failed to get table node", K(param), K(ret));
  }

  if (OB_SUCC(ret)) {
    int ignore_ret = try_periodic_refresh_global_sync_value(tenant_id,
                                                            table_id,
                                                            column_id,
                                                            *table_node);
    if (OB_SUCCESS != ignore_ret) {
      LOG_INFO("fail refresh global sync value. ignore this failure.", K(ignore_ret));
    }
  }

  // alloc handle
  bool need_prefetch = false;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(alloc_autoinc_try_lock(table_node->alloc_mutex_))) {
      LOG_WARN("failed to get alloc lock", K(param), K(*table_node), K(ret));
    } else {
      if (OB_SIZE_OVERFLOW == (ret = table_node->alloc_handle(handle_allocator_,
                                                                   offset,
                                                                   increment,
                                                                   desired_count,
                                                                   max_value,
                                                                   handle))) {
        TableNode mock_node;
        if (param.autoinc_desired_count_ <= 0) {
          // do nothing
        } else if (OB_FAIL(fetch_table_node(param, &mock_node))) {
          LOG_WARN("failed to fetch table node", K(param), K(ret));
        } else {
          LOG_TRACE("fetch table node success", K(param), K(mock_node), K(*table_node));
          atomic_update(table_node->local_sync_, mock_node.local_sync_);
          atomic_update(table_node->last_refresh_ts_, mock_node.last_refresh_ts_);
          table_node->prefetch_node_.reset();
          if (mock_node.curr_node_.cache_start_ == table_node->curr_node_.cache_end_ + 1) {
            // when the above condition is true, it means that no other thread has consume
            // any cache. intra-partition ascending property can be kept
            table_node->curr_node_.cache_end_ = mock_node.curr_node_.cache_end_;
          } else {
            table_node->curr_node_.cache_start_ = mock_node.curr_node_.cache_start_;
            table_node->curr_node_.cache_end_ = mock_node.curr_node_.cache_end_;
          }
          table_node->curr_node_state_is_pending_ = false;
          // in order mode, we always trust the next_value returned by global service,
          // because the local next_value may be calculated with the previous increment params.
          // and the increment_increment and increment_offset may be changed.
          if (param.autoinc_mode_is_order_) {
            table_node->next_value_ = mock_node.next_value_;
          }
          if (OB_FAIL(table_node->alloc_handle(handle_allocator_,
                                                offset, increment,
                                                desired_count, max_value, handle))) {
            LOG_WARN("failed to alloc cache handle", K(param), K(ret));
          } else {
            LOG_DEBUG("succ to get cache handle", K(param), K(*handle), K(ret));
          }
        }
      }
      table_node->alloc_mutex_.unlock();
    }
    if (OB_SUCC(ret) && table_node->prefetch_condition() && !table_node->prefetching_) {
      need_prefetch = true;
      table_node->prefetching_ = true;
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(need_prefetch)) {
      LOG_DEBUG("begin to prefetch table node", K(param), K(ret));
      // ensure single thread to prefetch
      TableNode mock_node;
      if (OB_FAIL(fetch_table_node(param, &mock_node, true))) {
        LOG_WARN("failed to fetch table node", K(param), K(ret));
      } else if (OB_FAIL(alloc_autoinc_try_lock(table_node->alloc_mutex_))) {
        LOG_WARN("failed to get alloc mutex lock", K(ret));
      } else {
        LOG_INFO("fetch table node success", K(param), K(mock_node), K(*table_node));
        if (table_node->prefetch_node_.cache_start_ != 0 ||
            mock_node.prefetch_node_.cache_start_ <= table_node->curr_node_.cache_end_) {
          LOG_TRACE("new table_node has been fetched by other, ignore");
        } else {
          atomic_update(table_node->local_sync_, mock_node.local_sync_);
          atomic_update(table_node->last_refresh_ts_, mock_node.last_refresh_ts_);
          table_node->prefetch_node_.cache_start_ = mock_node.prefetch_node_.cache_start_;
          table_node->prefetch_node_.cache_end_ = mock_node.prefetch_node_.cache_end_;
        }
        table_node->prefetching_ = false;
        table_node->alloc_mutex_.unlock();
      }
    }
  }

  // table node must be reverted after get to decrement reference count
  if (OB_UNLIKELY(NULL != table_node)) {
    node_map_.revert(table_node);
  }
  return ret;
}

int ObAutoincrementService::try_periodic_refresh_global_sync_value(
    uint64_t tenant_id,
    uint64_t table_id,
    uint64_t column_id,
    TableNode &table_node)
{
  int ret = OB_SUCCESS;
  const int64_t cur_time = ObTimeUtility::current_time();
  const int64_t cache_refresh_interval = GCONF.autoinc_cache_refresh_interval;
  int64_t delta = cur_time - ATOMIC_LOAD(&table_node.last_refresh_ts_);
  if (OB_LIKELY(delta * PRE_OP_THRESHOLD < cache_refresh_interval)) {
    // do nothing
  } else if (delta < cache_refresh_interval) {
    // try to sync
    if (OB_FAIL(table_node.sync_mutex_.trylock())) {
      // other thread is doing sync; pass through
      ret = OB_SUCCESS;
    } else {
      delta = cur_time - ATOMIC_LOAD(&table_node.last_refresh_ts_);
      if (delta * PRE_OP_THRESHOLD < cache_refresh_interval) {
        // do nothing; refreshed already
      } else if (OB_FAIL(fetch_global_sync(tenant_id, table_id, column_id, table_node, true))) {
        LOG_WARN("failed to get global sync", K(table_node), K(ret));
      }
      table_node.sync_mutex_.unlock();
    }
  } else {
    // must sync
    if (OB_FAIL(table_node.sync_mutex_.lock())) {
      LOG_WARN("failed to get sync lock", K(table_node), K(ret));
    } else {
      delta = cur_time - ATOMIC_LOAD(&table_node.last_refresh_ts_);
      if (delta * PRE_OP_THRESHOLD < cache_refresh_interval) {
        // do nothing; refreshed already
      } else if (OB_FAIL(fetch_global_sync(tenant_id, table_id, column_id, table_node))) {
        LOG_WARN("failed to get global sync", K(table_node), K(ret));
      }
      table_node.sync_mutex_.unlock();
    }
  }
  return ret;
}

void ObAutoincrementService::release_handle(CacheHandle *&handle)
{
  handle_allocator_.free(handle);
  // bug#8200783, should reset handle here
  handle = NULL;
}

int ObAutoincrementService::refresh_sync_value(const obrpc::ObAutoincSyncArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("begin to get global sync", K(arg));
  TableNode *table_node = NULL;
  AutoincKey key;
  key.tenant_id_ = arg.tenant_id_;
  key.table_id_  = arg.table_id_;
  key.column_id_ = arg.column_id_;
  if (OB_ENTRY_NOT_EXIST == (ret = node_map_.get(key, table_node))) {
    LOG_TRACE("there is no cache here", K(arg));
    ret = OB_SUCCESS;
  } else if (OB_SUCC(ret)) {
    const uint64_t sync_value = arg.sync_value_;
    lib::ObMutexGuard guard(table_node->sync_mutex_);
    atomic_update(table_node->local_sync_, sync_value);
    atomic_update(table_node->last_refresh_ts_, ObTimeUtility::current_time());
  } else {
    LOG_WARN("failed to do get table node", K(ret));
  }
  // table node must be reverted after get to decrement reference count
  if (NULL != table_node) {
    node_map_.revert(table_node);
  }
  return ret;
}

int ObAutoincrementService::lock_autoinc_row(const uint64_t &tenant_id,
                                             const uint64_t &table_id,
                                             const uint64_t &column_id,
                                             common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString lock_sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    ObISQLClient *sql_client = &trans;
    if (OB_FAIL(lock_sql.assign_fmt("SELECT sequence_key, sequence_value, sync_value "
                                    "FROM %s WHERE tenant_id = %lu AND sequence_key = %lu "
                                    "AND column_id = %lu FOR UPDATE",
                                    OB_ALL_AUTO_INCREMENT_TNAME,
                                    ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                    ObSchemaUtils::get_extract_schema_id(tenant_id, table_id),
                                    column_id))) {
      LOG_WARN("failed to assign sql", KR(ret));
    } else if (OB_FAIL(trans.read(res, tenant_id, lock_sql.ptr()))) {
      LOG_WARN("failded to execute sql", KR(ret), K(lock_sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get result, result is NULL", KR(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        LOG_WARN("autoincrement not exist", KR(ret), K(lock_sql));
      } else {
        LOG_WARN("iterate next result fail", KR(ret), K(lock_sql));
      }
    }
  }
  return ret;
}

//This implement is only for Truncate, table need to reset autoinc version after truncate
int ObAutoincrementService::reset_autoinc_row(const uint64_t &tenant_id,
                                              const uint64_t &table_id,
                                              const uint64_t &column_id,
                                              const int64_t &autoinc_version,
                                              common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString update_sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(update_sql.assign_fmt("UPDATE %s SET sequence_value = 1, sync_value = 0, truncate_version = %ld",
                                    OB_ALL_AUTO_INCREMENT_TNAME,
                                    autoinc_version))) {
    LOG_WARN("failed to assign sql", KR(ret));
  } else if (OB_FAIL(update_sql.append_fmt(" WHERE sequence_key = %lu AND tenant_id = %lu AND column_id = %lu",
                                            ObSchemaUtils::get_extract_schema_id(tenant_id, table_id),
                                            ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                            column_id))) {
    LOG_WARN("failed to append sql", KR(ret));
  } else if (OB_FAIL(trans.write(tenant_id, update_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to update __all_auto_increment", KR(ret), K(update_sql));
  } else if (OB_UNLIKELY(affected_rows > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(table_id), K(affected_rows), K(update_sql));
  }
  return ret;
}

// for new truncate table
int ObAutoincrementService::reinit_autoinc_row(const uint64_t &tenant_id,
                                               const uint64_t &table_id,
                                               const uint64_t &column_id,
                                               const int64_t &autoinc_version,
                                               common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(lock_autoinc_row(tenant_id, table_id, column_id, trans))) {
    LOG_WARN("failed to select for update", KR(ret));
  } else if (OB_FAIL(reset_autoinc_row(tenant_id, table_id, column_id, autoinc_version, trans))) {
    LOG_WARN("failed to update auto increment", KR(ret));
  }
  return ret;
}

// use for alter table add autoincrement
int ObAutoincrementService::try_lock_autoinc_row(const uint64_t &tenant_id,
                                                 const uint64_t &table_id,
                                                 const uint64_t &column_id,
                                                 const int64_t &autoinc_version,
                                                 bool &need_update_inner_table,
                                                 common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString lock_sql;
  need_update_inner_table = false;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                    || OB_INVALID_ID == table_id
                    || 0 == column_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("arg is not invalid", KR(ret), K(tenant_id), K(table_id), K(column_id));
    } else if (OB_FAIL(lock_sql.assign_fmt("SELECT truncate_version "
                                    "FROM %s WHERE tenant_id = %lu AND sequence_key = %lu "
                                    "AND column_id = %lu FOR UPDATE",
                                    OB_ALL_AUTO_INCREMENT_TNAME,
                                    ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                    ObSchemaUtils::get_extract_schema_id(tenant_id, table_id),
                                    column_id))) {
      LOG_WARN("failed to assign sql", KR(ret));
    } else if (OB_FAIL(trans.read(res, tenant_id, lock_sql.ptr()))) {
      LOG_WARN("failded to execute sql", KR(ret), K(lock_sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get result, result is NULL", KR(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("autoinc row not exist", K(tenant_id), K(table_id), K(column_id));
      } else {
        LOG_WARN("iterate next result fail", KR(ret), K(lock_sql));
      }
    } else {
      int64_t inner_autoinc_version = OB_INVALID_VERSION;
      if (OB_FAIL(result->get_int(0l, inner_autoinc_version))) {
        LOG_WARN("fail to get truncate_version", KR(ret));
      } else if (inner_autoinc_version > autoinc_version) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("autoincrement's newest version can not less than inner version",
                  KR(ret), K(tenant_id), K(table_id), K(column_id),
                  K(inner_autoinc_version), K(autoinc_version));
      } else if (inner_autoinc_version < autoinc_version) {
        need_update_inner_table = true;
        LOG_INFO("inner autoinc version is old, we need to update inner table",
                  K(tenant_id), K(table_id), K(column_id), K(inner_autoinc_version), K(autoinc_version));
      } else {
        LOG_TRACE("inner autoinc version is equal, not need to update inner table",
                   K(tenant_id), K(table_id), K(column_id));
      }
    }
  }
  return ret;
}

int ObAutoincrementService::clear_autoinc_cache_all(const uint64_t tenant_id,
                                                    const uint64_t table_id,
                                                    const uint64_t column_id,
                                                    const bool autoinc_is_order,
                                                    const bool ignore_rpc_errors /*true*/)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    // sync observer where table partitions exist
    LOG_INFO("begin to clear all sever's auto-increment cache",
             K(tenant_id), K(table_id), K(column_id), K(autoinc_is_order));
    ObAutoincSyncArg arg;
    arg.tenant_id_ = tenant_id;
    arg.table_id_  = table_id;
    arg.column_id_ = column_id;
    ObHashSet<ObAddr> server_set;
    if (OB_FAIL(server_set.create(PARTITION_LOCATION_SET_BUCKET_NUM))) {
      LOG_WARN("failed to create hash set", K(ret));
    } else if (OB_FAIL(get_server_set(tenant_id, table_id, server_set, true))) {
      SHARE_LOG(WARN, "failed to get table partitions server set", K(ret));
    }
    if (OB_SUCC(ret)) {
      const int64_t sync_timeout = SYNC_OP_TIMEOUT + TIME_SKEW;
      ObHashSet<ObAddr>::iterator iter;
      for(iter = server_set.begin(); OB_SUCC(ret) && iter != server_set.end(); ++iter) {
        LOG_INFO("send rpc call to other observers", "server", iter->first, K(tenant_id), K(table_id));
        if (OB_FAIL(srv_proxy_->to(iter->first)
                              .by(tenant_id)
                              .timeout(sync_timeout)
                              .clear_autoinc_cache(arg))) {
          if (ignore_rpc_errors && (is_rpc_error(ret) || autoinc_is_order)) {
            // ignore time out and clear ordered auto increment cache error, go on
            LOG_WARN("rpc call time out, ignore the error", "server", iter->first,
                     K(tenant_id), K(table_id), K(autoinc_is_order), K(ret));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to send rpc call", K(tenant_id), K(table_id), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObAutoincrementService::clear_autoinc_cache(const obrpc::ObAutoincSyncArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("begin to clear local auto-increment cache", K(arg));
  // auto-increment key
  AutoincKey key;
  key.tenant_id_ = arg.tenant_id_;
  key.table_id_  = arg.table_id_;
  key.column_id_ = arg.column_id_;

  map_mutex_.lock();
  if (OB_FAIL(node_map_.del(key))) {
    LOG_WARN("failed to erase table node", K(arg), K(ret));
  }
  map_mutex_.unlock();

  if (OB_ENTRY_NOT_EXIST == ret) {
    // do nothing; key does not exist
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret) && OB_FAIL(global_autoinc_service_.clear_global_autoinc_cache(key))) {
    LOG_WARN("failed to clear global autoinc cache", K(ret));
  }
  return ret;
}

uint64_t ObAutoincrementService::get_max_value(const common::ObObjType type)
{
  static const uint64_t type_max_value[] =
  {
    // null
    0,
    // signed
    INT8_MAX,               // tiny int   127
    INT16_MAX,              // short      32767
    INT24_MAX,              // medium int 8388607
    INT32_MAX,              // int        2147483647
    INT64_MAX,              // bigint     9223372036854775807
    // unsigned
    UINT8_MAX,              //            255
    UINT16_MAX,             //            65535
    UINT24_MAX,             //            16777215
    UINT32_MAX,             //            4294967295
    UINT64_MAX,             //            18446744073709551615
    // float
    0x1000000ULL,           // float      /* We use the maximum as per IEEE754-2008 standard, 2^24; compatible with MySQL */
    0x20000000000000ULL,    // double     /* We use the maximum as per IEEE754-2008 standard, 2^53; compatible with MySQL */
    0x1000000ULL,           // ufloat
    0x20000000000000ULL,    // udouble

    // do not support
//    "NUMBER",
//    "NUMBER UNSIGNED",
//
//    "DATETIME",
//    "TIMESTAMP",
//    "DATE",
//    "TIME",
//    "YEAR",
//
//    "VARCHAR",
//    "CHAR",
//    "VARBINARY",
//    "BINARY",
//
//    "EXT",
//    "UNKNOWN",
    0
  };
  return type_max_value[OB_LIKELY(type < ObNumberType) ? type : ObNumberType];
}

int ObAutoincrementService::get_table_node(const AutoincParam &param, TableNode *&table_node)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id     = param.tenant_id_;
  uint64_t table_id      = param.autoinc_table_id_;
  uint64_t column_id     = param.autoinc_col_id_;
  // auto-increment key
  AutoincKey key;
  key.tenant_id_ = tenant_id;
  key.table_id_  = table_id;
  key.column_id_ = column_id;
  int64_t autoinc_version = get_modify_autoinc_version(param.autoinc_version_);
  if (OB_FAIL(node_map_.get(key, table_node))) {
    if (ret != OB_ENTRY_NOT_EXIST) {
      LOG_ERROR("get from map failed", K(ret));
    } else {
      lib::ObMutex &mutex = init_node_mutex_[table_id % INIT_NODE_MUTEX_NUM];
      if (OB_FAIL(mutex.lock())) {
        LOG_WARN("failed to get lock", K(ret));
      } else {
        if (OB_ENTRY_NOT_EXIST == (ret = node_map_.get(key, table_node))) {
          LOG_INFO("alloc table node for auto increment key", K(key));
          if (OB_FAIL(node_map_.alloc_value(table_node))) {
            LOG_ERROR("failed to alloc table node", K(param), K(ret));
          } else if (OB_FAIL(table_node->init(param.autoinc_table_part_num_))) {
            LOG_ERROR("failed to init table node", K(param), K(ret));
          } else {
            table_node->prefetch_node_.reset();
            table_node->autoinc_version_ = autoinc_version;
            lib::ObMutexGuard guard(map_mutex_);
            if (OB_FAIL(node_map_.insert_and_get(key, table_node))) {
              LOG_WARN("failed to create table node", K(param), K(ret));
            }
          }
          if (OB_FAIL(ret) && table_node != nullptr) {
            node_map_.free_value(table_node);
            table_node = NULL;
          }
        }
        mutex.unlock();
      }
    }
  } else {
    if (OB_FAIL(alloc_autoinc_try_lock(table_node->alloc_mutex_))) {
      LOG_WARN("failed to get lock", K(ret));
    } else {
      //  local cache is expired
       if (OB_UNLIKELY(autoinc_version > table_node->autoinc_version_)) {
        LOG_INFO("start to reset table node", K(*table_node), K(param));
        table_node->next_value_ = 0;
        table_node->local_sync_ = 0;
        table_node->curr_node_.reset();
        table_node->prefetch_node_.reset();
        table_node->prefetching_ = false;
        table_node->curr_node_state_is_pending_ = true;
        table_node->autoinc_version_ = autoinc_version;
      // old request cannot get table node, it should retry
      } else if (OB_UNLIKELY(autoinc_version < table_node->autoinc_version_)) {
        ret = OB_AUTOINC_CACHE_NOT_EQUAL;
        LOG_WARN("old reqeust can not get table node, it should retry", KR(ret), K(autoinc_version), K(table_node->autoinc_version_));
      }
      table_node->alloc_mutex_.unlock();
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("succ to get table node", K(param), KPC(table_node), K(ret));
  } else {
    LOG_WARN("failed to get table node", K(param), K(ret));
  }
  return ret;
}

int ObAutoincrementService::alloc_autoinc_try_lock(lib::ObMutex &alloc_mutex)
{
  int ret = OB_SUCCESS;
  static const int64_t SLEEP_TS_US = 10;
  while (OB_SUCC(ret) && OB_FAIL(alloc_mutex.trylock())) {
    if (OB_EAGAIN == ret) {
      THIS_WORKER.check_wait();
      ob_usleep(SLEEP_TS_US);
      if (OB_FAIL(THIS_WORKER.check_status())) { // override ret code
        LOG_WARN("fail to check worker status", K(ret));
      }
    } else {
      LOG_WARN("fail to try lock mutex", K(ret));
    }
  }
  return ret;
}

int ObAutoincrementService::fetch_table_node(const AutoincParam &param,
                                             TableNode *table_node,
                                             const bool fetch_prefetch)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id     = param.tenant_id_;
  const uint64_t table_id      = param.autoinc_table_id_;
  const uint64_t column_id     = param.autoinc_col_id_;
  const ObObjType column_type  = param.autoinc_col_type_;
  const uint64_t part_num      = param.autoinc_table_part_num_;
  const uint64_t desired_count = param.autoinc_desired_count_;
  const uint64_t offset        = param.autoinc_offset_;
  const uint64_t increment     = param.autoinc_increment_;
  const int64_t autoinc_version = param.autoinc_version_;
  if (part_num <= 0 || ObNullType == column_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(part_num), K(column_type), K(ret));
  } else {
    uint64_t sync_value = 0;
    uint64_t max_value = get_max_value(column_type);
    int64_t auto_increment_cache_size = param.auto_increment_cache_size_;
    uint64_t start_inclusive = 0;
    uint64_t end_inclusive = 0;
    if (auto_increment_cache_size < 1 || auto_increment_cache_size > MAX_INCREMENT_CACHE_SIZE) {
      // no ret, just log error
      LOG_ERROR("unexpected auto_increment_cache_size", K(auto_increment_cache_size));
      auto_increment_cache_size = DEFAULT_INCREMENT_CACHE_SIZE;
    }
    // For ORDER mode, the local cache_size is always 1, and the central(remote) cache_size
    // is the configuration value.
    const uint64_t local_cache_size = auto_increment_cache_size;
    uint64_t prefetch_count = std::min(max_value / 100 / part_num, local_cache_size);
    uint64_t batch_count = 0;
    if (prefetch_count > 1) {
      batch_count = std::max(increment * prefetch_count, increment * desired_count);
    } else {
      batch_count = increment * desired_count;
    }
    AutoincKey key(tenant_id, table_id, column_id);
    uint64_t table_auto_increment = param.autoinc_auto_increment_;
    if (OB_UNLIKELY(table_auto_increment > max_value)) {
      ret = OB_ERR_REACH_AUTOINC_MAX;
      LOG_WARN("reach max autoinc", K(ret), K(table_auto_increment));
    } else if (OB_FAIL(distributed_autoinc_service_.get_value(
                          key, offset, increment, max_value, table_auto_increment,
                          batch_count, auto_increment_cache_size, autoinc_version, sync_value,
                          start_inclusive, end_inclusive))) {
      LOG_WARN("fail get value", K(ret));
    } else if (sync_value > max_value || start_inclusive > max_value) {
      ret = OB_ERR_REACH_AUTOINC_MAX;
      LOG_WARN("reach max autoinc", K(start_inclusive), K(max_value), K(ret));
    }

    if (OB_SUCC(ret)) {
      atomic_update(table_node->local_sync_, sync_value);
      atomic_update(table_node->last_refresh_ts_, ObTimeUtility::current_time());
      if (fetch_prefetch) {
        table_node->prefetch_node_.cache_start_ = start_inclusive;
        table_node->prefetch_node_.cache_end_ = std::min(max_value, end_inclusive);
      } else {
        // there is no prefetch_node here
        // because we must have tried to allocate cache handle from curr_node and prefetch_node
        // before allocate new cache node
        // if we allocate new cache node, curr_node and prefetch_node should have been combined;
        // and prefetch_node should have been reset
        CacheNode new_node;
        new_node.cache_start_ = start_inclusive;
        new_node.cache_end_   = std::min(max_value, end_inclusive);
        if (OB_FAIL(table_node->curr_node_.combine_cache_node(new_node))) {
          LOG_WARN("failed to combine cache node", K(*table_node), K(new_node), K(ret));
        } else if (0 == table_node->next_value_) {
          table_node->next_value_ = start_inclusive;
        }
      }
    }

    // ignore error for prefetch, cache is enough here
    // other thread will try next time
    if (fetch_prefetch && OB_FAIL(ret)) {
      LOG_WARN("failed to prefetch; ignore this", K(ret));
      ret = OB_SUCCESS;
    }
  }

  return ret;
}


int ObAutoincrementService::set_pre_op_timeout(common::ObTimeoutCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t abs_timeout_us = ctx.get_abs_timeout();

  if (abs_timeout_us < 0) {
    abs_timeout_us = ObTimeUtility::current_time() + PRE_OP_TIMEOUT;
  }
  if (THIS_WORKER.get_timeout_ts() > 0 && THIS_WORKER.get_timeout_ts() < abs_timeout_us) {
    abs_timeout_us = THIS_WORKER.get_timeout_ts();
  }

  if (OB_FAIL(ctx.set_abs_timeout(abs_timeout_us))) {
    LOG_WARN("set timeout failed", K(ret), K(abs_timeout_us));
  } else  if (ctx.is_timeouted()) {
    ret = OB_TIMEOUT;
    LOG_WARN("is timeout",
             K(ret),
             "abs_timeout", ctx.get_abs_timeout(),
             "this worker timeout ts", THIS_WORKER.get_timeout_ts());
  }

  return ret;
}
//FIXME: use location_service instead
// TODO shengle
int ObAutoincrementService::get_server_set(const uint64_t tenant_id,
                                           const uint64_t table_id,
                                           common::hash::ObHashSet<ObAddr> &server_set,
                                           const bool get_follower)
{
  int ret = OB_SUCCESS;
  const int64_t expire_renew_time = INT64_MAX; // means must renew location
  bool is_cache_hit = false;
  ObSchemaGetterGuard schema_guard;
  bool is_found = false;
  ObLSID ls_id;
  ObLSLocation ls_loc;
  ObSEArray<ObTabletID, 4> tablet_ids;
  ObSEArray<ObLSLocation, 4> ls_locations;
  const ObSimpleTableSchemaV2 *table_schema = nullptr;

  if (OB_ISNULL(schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(schema_service_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id,
                                                              schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret),K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("fail to get table schema", KR(ret));
  } else if (OB_FAIL(table_schema->get_tablet_ids(tablet_ids))) {
    LOG_WARN("fail to get tablet ids", K(ret));
  } else {
    ObLSLocation ls_loc;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
      if (OB_FAIL(GCTX.location_service_->get(tenant_id,
                                              tablet_ids.at(i),
                                              expire_renew_time,
                                              is_cache_hit,
                                              ls_id))) {
        LOG_WARN("fail to get ls id", K(ret), K(tenant_id), K(tablet_ids.at(i)));
      } else if (OB_FAIL(GCTX.location_service_->get(GCONF.cluster_id,
                                                    tenant_id,
                                                    ls_id,
                                                    0, /*not force to renew*/
                                                    is_cache_hit,
                                                    ls_loc))) {
        LOG_WARN("failed to get all partitions' locations", K(table_id), K(ret));
      }
      OZ(ls_locations.push_back(ls_loc));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("get partition locations", K(ls_locations), K(ret));
    ObLSReplicaLocation location;
    for(int64_t i = 0; OB_SUCC(ret) && i < ls_locations.count(); ++i) {
      if (!get_follower) {
        location.reset();
        if (OB_FAIL(ls_locations.at(i).get_leader(location))) {
          LOG_WARN("failed to get partition leader", "location", ls_locations.at(i), K(ret));
        } else {
          int err = OB_SUCCESS;
          err = server_set.set_refactored(location.get_server());
          if (OB_SUCCESS != err && OB_HASH_EXIST != err) {
            ret = err;
            LOG_WARN("failed to add element to set", "server", location.get_server(), K(ret));
          }
        }
      } else {
        const ObIArray<ObLSReplicaLocation> &replica_locations = ls_locations.at(i).get_replica_locations();
        for (int j = 0; j < replica_locations.count(); ++j) {
          int err = OB_SUCCESS;
          err = server_set.set_refactored(replica_locations.at(j).get_server());
          if (OB_SUCCESS != err && OB_HASH_EXIST != err) {
            ret = err;
            LOG_WARN("failed to add element to set",
                     "server", replica_locations.at(j).get_server(), K(ret));
          }
        }
      }
    }
  }
  LOG_DEBUG("sync server_set", K(server_set), K(ret));
  return ret;
}

int ObAutoincrementService::sync_insert_value_order(AutoincParam &param,
                                                    CacheHandle *&cache_handle,
                                                    const uint64_t insert_value)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id     = param.tenant_id_;
  const uint64_t table_id      = param.autoinc_table_id_;
  const uint64_t column_id     = param.autoinc_col_id_;
  const ObObjType column_type  = param.autoinc_col_type_;
  const uint64_t max_value     = get_max_value(column_type);
  const uint64_t part_num      = param.autoinc_table_part_num_;
  const int64_t autoinc_version = param.autoinc_version_;

  uint64_t global_sync_value = 0;
  AutoincKey key(tenant_id, table_id, column_id);
  uint64_t value_to_sync = insert_value;
  if (value_to_sync > max_value) {
    value_to_sync = max_value;
  }
  if (OB_FAIL(global_autoinc_service_.local_push_to_global_value(
                key, max_value, value_to_sync, autoinc_version, param.auto_increment_cache_size_,
                global_sync_value))) {
    LOG_WARN("fail sync value to global", K(key), K(insert_value), K(ret));
  } else if (NULL != cache_handle) {
    LOG_DEBUG("insert value, generate next val",
              K(insert_value), K(cache_handle->prefetch_end_), K(cache_handle->next_value_));
    if (insert_value < cache_handle->prefetch_end_) {
      if (insert_value >= cache_handle->next_value_) {
        if (OB_FAIL(calc_next_value(insert_value + 1,
                                    param.autoinc_offset_,
                                    param.autoinc_increment_,
                                    cache_handle->next_value_))) {
          LOG_WARN("failed to calc next value", K(cache_handle), K(param), K(ret));
        }
        LOG_DEBUG("generate next value when sync_insert_value", K(insert_value), K(cache_handle->next_value_));
      }
    } else {
      // release handle No.
      handle_allocator_.free(cache_handle);
      cache_handle = NULL;
      // invalid cache handle; record count
      param.autoinc_intervals_count_++;
    }
  }
  return ret;
}

/* core logic:
 * 1. write insert_value to global storage
 *    - only if insert_value > local_sync_
 * 2. notify other servers that a larger value written to global storage
 * 3. update local table node
 */
int ObAutoincrementService::sync_insert_value_noorder(AutoincParam &param,
                                                      CacheHandle *&cache_handle,
                                                      const uint64_t insert_value)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id     = param.tenant_id_;
  const uint64_t table_id      = param.autoinc_table_id_;
  const uint64_t column_id     = param.autoinc_col_id_;
  const ObObjType column_type  = param.autoinc_col_type_;
  const uint64_t max_value     = get_max_value(column_type);
  const uint64_t part_num      = param.autoinc_table_part_num_;
  const int64_t autoinc_version = param.autoinc_version_;
  TableNode *table_node = NULL;
  LOG_DEBUG("begin to sync insert value globally", K(param), K(insert_value));
  if (OB_FAIL(get_table_node(param, table_node))) {
    LOG_WARN("table node should exist", K(param), K(ret));
  } else {
    // logic:
    // 1. we should not compare insert_value with local cache node. it is meaningless.
    //    e.g. insert_value = 15, A caches [20,30), B caches [0-10), you should not compare
    //         insert_value with A without any information on B
    // 2. local_sync_ semantics：we can make sure that other observer has seen a
    //                           sync value larger than or equal to local_sync.
    //    If observer has synced with global, we can predicate:
    //    a newly inserted value less than local sync don't need to push to global. As we can
    //    make a good guess that a larger value had already pushed to global by someone else.
    // 3. only if insert_value > local_sync_ & insert_value > global_sync:
    //    we can write insert_value into global storage. otherwise, don't sync
    //
    uint64_t global_sync_value = 0; // piggy back global value when we sync out insert_value
    AutoincKey key(tenant_id, table_id, column_id);
    // compare insert_value with local_sync/global_sync
    if (insert_value <= ATOMIC_LOAD(&table_node->local_sync_)) {
      // do nothing
    } else {
      // For NOORDER mode, the central(remote) cache_size is the configuration value.
      const uint64_t local_cache_size = MAX(1, param.auto_increment_cache_size_);
      uint64_t value_to_sync = calc_next_cache_boundary(insert_value, local_cache_size, max_value);
      if (OB_FAIL(distributed_autoinc_service_.local_push_to_global_value(key, max_value,
                                                  value_to_sync, autoinc_version, 0, global_sync_value))) {
        LOG_WARN("fail sync value to global", K(key), K(insert_value), K(ret));
      } else {
        if (OB_FAIL(alloc_autoinc_try_lock(table_node->alloc_mutex_))) {
          LOG_WARN("fail to get alloc mutex", K(ret));
        } else {
          if (value_to_sync <= global_sync_value) {
            atomic_update(table_node->local_sync_, global_sync_value);
            atomic_update(table_node->last_refresh_ts_, ObTimeUtility::current_time());
          } else {
            atomic_update(table_node->local_sync_, value_to_sync);
            if (OB_FAIL(sync_value_to_other_servers(param, value_to_sync))) {
              LOG_WARN("fail sync value to other servers", K(ret));
            }
          }
          table_node->alloc_mutex_.unlock();
        }
      }
    }

    // INSERT INTO t1 VALUES (null), (null), (4), (null), (null);
    //                          1      2      4     5       6
    // assume cache_handle saves [1, 5]
    // in this case, values will be allocated as above. in order to generate the forth value '5',
    // we need following logic:
    if (OB_SUCC(ret)) {
      if (NULL != cache_handle) {
        LOG_DEBUG("insert value, generate next val",
                  K(insert_value), K(cache_handle->prefetch_end_), K(cache_handle->next_value_));
        if (insert_value < cache_handle->prefetch_end_) {
          if (insert_value >= cache_handle->next_value_) {
            if (OB_FAIL(calc_next_value(insert_value + 1,
                                        param.autoinc_offset_,
                                        param.autoinc_increment_,
                                        cache_handle->next_value_))) {
              LOG_WARN("failed to calc next value", K(cache_handle), K(param), K(ret));
            }
            LOG_DEBUG("generate next value when sync_insert_value", K(insert_value), K(cache_handle->next_value_));
          }
        } else {
          // release handle No.
          handle_allocator_.free(cache_handle);
          cache_handle = NULL;
          // invalid cache handle; record count
          param.autoinc_intervals_count_++;
        }
      }
    }

    if (OB_SUCC(ret)) {
      // Note: when insert_value is larger than current cache value, do we really need to
      // refresh local cache? What if other thread in this server is refreshing local cache?
      //   - if we don't, and other thread fetchs a small cache value, it is not OK.
      // SO, we must fetch table node here.
      // syncing insert_value is not the common case. perf acceptable
      if (OB_FAIL(alloc_autoinc_try_lock(table_node->alloc_mutex_))) {
        LOG_WARN("fail to get alloc mutex", K(ret));
      } else {
        if (insert_value >= table_node->curr_node_.cache_end_
            && insert_value >= table_node->prefetch_node_.cache_end_) {
          TableNode mock_node;
          if (param.autoinc_desired_count_ <= 0) {
            // do nothing
          } else if (OB_FAIL(fetch_table_node(param, &mock_node))) {
            LOG_WARN("failed to alloc table node", K(param), K(ret));
          } else {
            table_node->prefetch_node_.reset();
            if (mock_node.curr_node_.cache_end_ > table_node->curr_node_.cache_end_) {
              table_node->curr_node_.cache_start_ = mock_node.curr_node_.cache_start_;
              table_node->curr_node_.cache_end_ = mock_node.curr_node_.cache_end_;
            }
            LOG_INFO("fetch table node success", K(param), K(*table_node));
          }
        }
        table_node->alloc_mutex_.unlock();
      }
    }
  }
  // table node must be reverted after get to decrement reference count
  if (NULL != table_node) {
    node_map_.revert(table_node);
  }
  return ret;
}

int ObAutoincrementService::sync_value_to_other_servers(
    AutoincParam &param,
    uint64_t insert_value)
{
  int ret = OB_SUCCESS;
  // sync observer where table partitions exist
  LOG_DEBUG("begin to sync other servers", K(param));
  ObAutoincSyncArg arg;
  arg.tenant_id_ = param.tenant_id_;
  arg.table_id_  = param.autoinc_table_id_;
  arg.column_id_ = param.autoinc_col_id_;
  arg.sync_value_ = insert_value;
  ObHashSet<ObAddr> server_set;
  if (OB_FAIL(server_set.create(PARTITION_LOCATION_SET_BUCKET_NUM))) {
    LOG_WARN("failed to create hash set", K(param), K(ret));
  } else {
    if (OB_FAIL(get_server_set(param.tenant_id_, param.autoinc_table_id_, server_set))) {
      SHARE_LOG(WARN, "failed to get table partitions server set", K(ret));
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("server_set after remove self", K(server_set), K(ret));
      ObHashSet<ObAddr>::iterator iter;
      // this operation succeeds in two case:
      //   1. sync to all servers (without timeout)
      //   2. reach SYNC_TIMEOUT  (without worker timeout)
      // other cases won't succeed:
      //   1. worker timeout
      //   2. rpc calls return failure (not OB_TIMEOUT)
      int64_t sync_us = 0;
      const int64_t start_us = ObTimeUtility::current_time();
      const int64_t sync_timeout = SYNC_OP_TIMEOUT + TIME_SKEW;
      for(iter = server_set.begin(); OB_SUCC(ret) && iter != server_set.end(); ++iter) {
        LOG_INFO("send rpc call to other observers", "server", iter->first);
        sync_us = THIS_WORKER.get_timeout_remain();
        if (sync_us > sync_timeout) {
          sync_us = sync_timeout;
        }
        // TODO: send sync_value to other servers
        if (OB_FAIL(srv_proxy_->to(iter->first)
                               .timeout(sync_us)
                               .refresh_sync_value(arg))) {
          if (is_rpc_error(ret)) {
            LOG_WARN("sync rpc call time out", "server", iter->first, K(sync_us), K(param), K(ret));
            if (!THIS_WORKER.is_timeout()) {
              // reach SYNC_TIMEOUT, go on
              // ignore time out
              ret = OB_SUCCESS;
              break;
            }
          } else {
            LOG_WARN("failed to send rpc call", K(param), K(ret));
          }
        } else if (ObTimeUtility::current_time() - start_us >= sync_timeout) {
          // reach SYNC_TIMEOUT, go on
          LOG_INFO("reach SYNC_TIMEOUT, go on", "server", iter->first, K(param), K(ret));
          break;
        } else if (THIS_WORKER.is_timeout()) {
          ret = OB_TIMEOUT;
          LOG_WARN("failed to sync insert value; worker timeout", K(param), K(ret));
          break;
        }
      }
    }
  }
  return ret;
}
// sync last user specified value first(compatible with MySQL)
int ObAutoincrementService::sync_insert_value_global(AutoincParam &param)
{
  int ret = OB_SUCCESS;
  if (0 != param.global_value_to_sync_) {
    if (param.global_value_to_sync_ < param.autoinc_auto_increment_) {
      // do nothing, insert value directly
    } else if (param.autoinc_mode_is_order_) {
      if (OB_FAIL(sync_insert_value_order(param,
                                          param.cache_handle_,
                                          param.global_value_to_sync_))) {
        SQL_ENG_LOG(WARN, "failed to sync insert value",
                          "insert_value", param.global_value_to_sync_, K(ret));
      }
    } else {
      if (OB_FAIL(sync_insert_value_noorder(param,
                                          param.cache_handle_,
                                          param.global_value_to_sync_))) {
        SQL_ENG_LOG(WARN, "failed to sync insert value",
                          "insert_value", param.global_value_to_sync_, K(ret));
      }
    }
    param.global_value_to_sync_ = 0;
  }
  return ret;
}

// sync last user specified value in stmt
int ObAutoincrementService::sync_insert_value_local(AutoincParam &param)
{
  int ret = OB_SUCCESS;
  if (param.sync_flag_) {
    if (param.global_value_to_sync_ < param.value_to_sync_) {
      param.global_value_to_sync_ = param.value_to_sync_;
    }
    param.sync_flag_ = false;
  }
  return ret;
}

int ObAutoincrementService::sync_auto_increment_all(const uint64_t tenant_id,
                                                    const uint64_t table_id,
                                                    const uint64_t column_id,
                                                    const uint64_t sync_value)
{
  int ret = OB_SUCCESS;
  LOG_INFO("begin to sync auto_increment value", K(sync_value), K(tenant_id), K(table_id));

  // if (OB_SUCC(ret)) {
  //   AutoincKey key(tenant_id, table_id, column_id);
  //   if(OB_FAIL(sync_table_auto_increment(tenant_id, key, sync_value))) {
  //     LOG_WARN("fail sync table auto increment value", K(ret));
  //   }
  // }

  if (OB_SUCC(ret)) {
    // sync observer where table partitions exist
    ObAutoincSyncArg arg;
    arg.tenant_id_ = tenant_id;
    arg.table_id_  = table_id;
    arg.column_id_ = column_id;
    arg.sync_value_ = sync_value;
    ObHashSet<ObAddr> server_set;
    if (OB_FAIL(server_set.create(PARTITION_LOCATION_SET_BUCKET_NUM))) {
      LOG_WARN("failed to create hash set", K(ret));
    } else {
      if (OB_FAIL(get_server_set(tenant_id, table_id, server_set, true))) {
        SHARE_LOG(WARN, "failed to get table partitions server set", K(ret));
      }
      LOG_DEBUG("sync server_set", K(server_set), K(ret));
      if (OB_SUCC(ret)) {
        const int64_t sync_timeout = SYNC_OP_TIMEOUT + TIME_SKEW;
        ObHashSet<ObAddr>::iterator iter;
        for(iter = server_set.begin(); OB_SUCC(ret) && iter != server_set.end(); ++iter) {
          LOG_DEBUG("send rpc call to other observers", "server", iter->first);
          if (OB_FAIL(srv_proxy_->to(iter->first)
                                .timeout(sync_timeout)
                                .refresh_sync_value(arg))) {
            if (is_rpc_error(ret)) {
              // ignore time out, go on
              LOG_WARN("rpc call time out", "server", iter->first, K(ret));
              if (!THIS_WORKER.is_timeout()) {
                // reach SYNC_TIMEOUT, go on
                // ignore time out
                ret = OB_SUCCESS;
                break;
              }
            } else {
              LOG_WARN("failed to send rpc call", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}


int ObAutoincrementService::fetch_global_sync(const uint64_t tenant_id,
                                              const uint64_t table_id,
                                              const uint64_t column_id,
                                              TableNode &table_node,
                                              const bool sync_presync)
{
  int ret = OB_SUCCESS;
  uint64_t sync_value = 0;

  // set timeout for presync
  ObTimeoutCtx ctx;
  if (sync_presync) {
    if (OB_FAIL(set_pre_op_timeout(ctx))) {
      LOG_WARN("failed to set timeout", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    AutoincKey key(tenant_id, table_id, column_id);
    if (OB_FAIL(distributed_autoinc_service_.local_sync_with_global_value(key, table_node.autoinc_version_, sync_value))) {
      LOG_WARN("fail refresh global value", K(ret));
    } else {
      atomic_update(table_node.local_sync_, sync_value);
      atomic_update(table_node.last_refresh_ts_, ObTimeUtility::current_time());
    }
  }

  // ignore error for presync
  // other thread will try next time
  if (sync_presync && OB_FAIL(ret)) {
    LOG_WARN("failed to presync; ignore this", K(ret));
    ret = OB_SUCCESS;
  }

  return ret;
}

uint64_t ObAutoincrementService::calc_next_cache_boundary(
    uint64_t insert_value,
    uint64_t cache_size,
    uint64_t max_value)
{
  uint64_t new_next_boundary = 0;
  const uint64_t insert_value_bak = insert_value;
  // round up insert_value except cache_size. for example, when cache size is 1000,
  // we will get the following results for different insert_values:
  // insert_value = 999 -> new_next_boundary = 1000;
  // insert_value = 1000 -> new_next_boundary = 1000;
  // insert_value = 1001 -> new_next_boundary = 2000;
  uint64_t v = (insert_value % cache_size == 0) ? 0 : 1;
  insert_value = (insert_value / cache_size + v) * cache_size;
  if (insert_value < insert_value_bak || insert_value > max_value) {
    new_next_boundary = max_value;
  } else {
    new_next_boundary = insert_value;
  }
  return new_next_boundary;
}

int ObAutoincrementService::calc_next_value(const uint64_t last_next_value,
                                            const uint64_t offset,
                                            const uint64_t increment,
                                            uint64_t &new_next_value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(increment <= 0)) {
    //这里有除0错误，需要防御检查
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("increment is invalid", K(ret), K(increment));
  } else {
    uint64_t real_offset = offset;

    if (real_offset > increment) {
      real_offset = 0;
    }
    if (last_next_value <= real_offset) {
      new_next_value = real_offset;
    } else {
      new_next_value = ((last_next_value - real_offset + increment - 1) / increment) * increment + real_offset;
    }
    if (new_next_value < last_next_value) {
      new_next_value = UINT64_MAX;
    }
  }
  LOG_DEBUG("calc next value", K(new_next_value), K(ret));
  return ret;
}

// Tow params control the starting value
// - auto_increment_increment controls the interval between successive column values.
// - auto_increment_offset determines the starting point for the AUTO_INCREMENT column value.
//
// When the auto value reaches its end, it will keep producing the max value.
// The max value is decided by:
//
// If either of these variables is changed, and then new rows inserted into a table containing an
// AUTO_INCREMENT column, the results may seem counterintuitive because the series of AUTO_INCREMENT
// values is calculated without regard to any values already present in the column, and the next
// value inserted is the least value in the series that is greater than the maximum existing value
// in the AUTO_INCREMENT column. The series is calculated like this:
//
// prev_value =  auto_increment_offset + N × auto_increment_increment
//
// More details:
// https://dev.mysql.com/doc/refman/5.6/en/replication-options-master.html#sysvar_auto_increment_increment
//
// The doc does not mention one case: when offset > max_value, the formulator is not right.
// a bug is recorded here:
int ObAutoincrementService::calc_prev_value(const uint64_t max_value,
                                            const uint64_t offset,
                                            const uint64_t increment,
                                            uint64_t &prev_value)
{
  int ret = OB_SUCCESS;
  if (max_value <= offset) {
    prev_value = max_value;
  } else {
    if (0 == increment) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      prev_value = ((max_value - offset) / increment) * increment + offset;
    }
  }
  LOG_INFO("out of range for column. calc prev value",
           K(prev_value), K(max_value), K(offset), K(increment));
  return ret;
}

int ObAutoincrementService::get_sequence_value(const uint64_t tenant_id,
                                               const uint64_t table_id,
                                               const uint64_t column_id,
                                               const bool is_order,
                                               const int64_t autoinc_version,
                                               uint64_t &seq_value)
{
  int ret = OB_SUCCESS;
  AutoincKey key;
  key.tenant_id_ = tenant_id;
  key.table_id_  = table_id;
  key.column_id_ = column_id;
  if (is_order && OB_FAIL(global_autoinc_service_.get_sequence_value(key, autoinc_version, seq_value))) {
    LOG_WARN("global autoinc service get sequence value failed", K(ret));
  } else if (!is_order &&
      OB_FAIL(distributed_autoinc_service_.get_sequence_value(key, autoinc_version, seq_value))) {
    LOG_WARN("distributed autoinc service get sequence value failed", K(ret));
  }
  return ret;
}

// used for __tenant_virtual_all_table only
int ObAutoincrementService::get_sequence_values(
    const uint64_t tenant_id,
    const ObIArray<AutoincKey> &order_autokeys,
    const ObIArray<AutoincKey> &noorder_autokeys,
    const ObIArray<int64_t> &order_autoinc_versions,
    const ObIArray<int64_t> &noorder_autoinc_versions,
    ObHashMap<AutoincKey, uint64_t> &seq_values)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(global_autoinc_service_.get_auto_increment_values(
                tenant_id, order_autokeys, order_autoinc_versions, seq_values))) {
    LOG_WARN("global_autoinc_service_ get sequence values failed", K(ret));
  } else if (OB_FAIL(distributed_autoinc_service_.get_auto_increment_values(
                       tenant_id, noorder_autokeys, noorder_autoinc_versions, seq_values))) {
    LOG_WARN("distributed_autoinc_service_ get sequence values failed", K(ret));
  }
  return ret;
}

int ObInnerTableGlobalAutoIncrementService::get_value(
    const AutoincKey &key,
    const uint64_t offset,
    const uint64_t increment,
    const uint64_t max_value,
    const uint64_t table_auto_increment,
    const uint64_t desired_count,
    const uint64_t cache_size,
    const int64_t &autoinc_version,
    uint64_t &sync_value,
    uint64_t &start_inclusive,
    uint64_t &end_inclusive)
{
  UNUSED(cache_size);
  int ret = OB_SUCCESS;
  uint64_t sync_value_from_inner_table = 0;
  ret = inner_table_proxy_.next_autoinc_value(
          key, offset, increment, table_auto_increment, max_value, desired_count, autoinc_version,
          start_inclusive, end_inclusive, sync_value_from_inner_table);
  if (OB_SUCC(ret)) {
    if (table_auto_increment != 0 && table_auto_increment - 1 > sync_value_from_inner_table) {
      sync_value = table_auto_increment -1;
    } else {
      sync_value = sync_value_from_inner_table;
    }
  }
  return ret;
}

int ObInnerTableGlobalAutoIncrementService::get_sequence_value(const AutoincKey &key,
                                                               const int64_t &autoinc_version,
                                                               uint64_t &sequence_value)
{
  uint64_t sync_value = 0; // unused
  return inner_table_proxy_.get_autoinc_value(key, autoinc_version, sequence_value, sync_value);
}

int ObInnerTableGlobalAutoIncrementService::get_auto_increment_values(
    const uint64_t tenant_id,
    const common::ObIArray<AutoincKey> &autoinc_keys,
    const common::ObIArray<int64_t> &autoinc_versions,
    common::hash::ObHashMap<AutoincKey, uint64_t> &seq_values)
{
  UNUSED(autoinc_versions);
  return inner_table_proxy_.get_autoinc_value_in_batch(tenant_id, autoinc_keys, seq_values);
}

int ObInnerTableGlobalAutoIncrementService::local_push_to_global_value(
    const AutoincKey &key,
    const uint64_t max_value,
    const uint64_t insert_value,
    const int64_t &autoinc_version,
    const int64_t cache_size,
    uint64_t &sync_value)
{
  UNUSED(cache_size);
  uint64_t seq_value = 0; // unused, * MUST * set seq_value to 0 here.
  return inner_table_proxy_.sync_autoinc_value(key, insert_value, max_value, autoinc_version,
                                               seq_value, sync_value);
}

int ObInnerTableGlobalAutoIncrementService::local_sync_with_global_value(
    const AutoincKey &key,
    const int64_t &autoinc_version,
    uint64_t &sync_value)
{
  uint64_t seq_value = 0; // unused
  return inner_table_proxy_.get_autoinc_value(key, autoinc_version, seq_value, sync_value);
}

int ObRpcGlobalAutoIncrementService::init(
    const common::ObAddr &addr,
    rpc::frame::ObReqTransport *req_transport)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRpcGlobalAutoIncrementService inited twice", KR(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(addr));
  } else if (OB_FAIL(gais_request_rpc_proxy_.init(req_transport, addr))) {
    LOG_WARN("rpc proxy init failed", K(ret), K(req_transport), K(addr));
  } else if (OB_FAIL(gais_request_rpc_.init(&gais_request_rpc_proxy_, addr))) {
    LOG_WARN("response rpc init failed", K(ret), K(addr));
  } else if (OB_FAIL(gais_client_.init(addr, &gais_request_rpc_))) {
    LOG_WARN("init client failed", K(ret));
  }
  return ret;
}

int ObRpcGlobalAutoIncrementService::get_value(
    const AutoincKey &key,
    const uint64_t offset,
    const uint64_t increment,
    const uint64_t max_value,
    const uint64_t table_auto_increment,
    const uint64_t desired_count,
    const uint64_t cache_size,
    const int64_t &autoinc_version,
    uint64_t &sync_value,
    uint64_t &start_inclusive,
    uint64_t &end_inclusive)
{
  return gais_client_.get_value(key, offset, increment, max_value, table_auto_increment,
                                desired_count, cache_size, autoinc_version,sync_value,
                                start_inclusive, end_inclusive);
}

int ObRpcGlobalAutoIncrementService::get_sequence_next_value(
    const ObSequenceSchema &schema,
    ObSequenceValue &nextval)
{
  return gais_client_.get_sequence_next_value(schema, nextval);
}

int ObRpcGlobalAutoIncrementService::get_sequence_value(const AutoincKey &key,
                                                        const int64_t &autoinc_version,
                                                        uint64_t &sequence_value)
{
  return gais_client_.get_sequence_value(key, autoinc_version, sequence_value);
}

int ObRpcGlobalAutoIncrementService::get_auto_increment_values(
    const uint64_t tenant_id,
    const common::ObIArray<AutoincKey> &autoinc_keys,
    const common::ObIArray<int64_t> &autoinc_versions,
    common::hash::ObHashMap<AutoincKey, uint64_t> &seq_valuesm)
{
  UNUSED(tenant_id);
  return gais_client_.get_auto_increment_values(autoinc_keys, autoinc_versions, seq_valuesm);
}

int ObRpcGlobalAutoIncrementService::local_push_to_global_value(
    const AutoincKey &key,
    const uint64_t max_value,
    const uint64_t value,
    const int64_t &autoinc_version,
    const int64_t cache_size,
    uint64_t &global_sync_value)
{
  return gais_client_.local_push_to_global_value(key, max_value, value, autoinc_version,
            cache_size,
            global_sync_value);
}

int ObRpcGlobalAutoIncrementService::local_sync_with_global_value(
    const AutoincKey &key,
    const int64_t &autoinc_version,
    uint64_t &value)
{
  return gais_client_.local_sync_with_global_value(key, autoinc_version, value);
}

int ObRpcGlobalAutoIncrementService::clear_global_autoinc_cache(const AutoincKey &key)
{
  return gais_client_.clear_global_autoinc_cache(key);
}

int ObAutoIncInnerTableProxy::check_inner_autoinc_version(const int64_t &request_autoinc_version,
                                                          const int64_t &inner_autoinc_version,
                                                          const AutoincKey &key)
{
  int ret = OB_SUCCESS;
  if (0 == request_autoinc_version || 0 == inner_autoinc_version) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("autoinc version is zero", KR(ret), K(request_autoinc_version), K(inner_autoinc_version));
  // inner table did not update
  } else if (OB_UNLIKELY(inner_autoinc_version < request_autoinc_version)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner_autoinc_version can not less than autoinc_version", KR(ret), K(key),
                                                                        K(inner_autoinc_version), K(request_autoinc_version));
  // old request
  } else if (OB_UNLIKELY(inner_autoinc_version > request_autoinc_version)) {
    ret = OB_AUTOINC_CACHE_NOT_EQUAL;
    LOG_WARN("inner_autoinc_version is greater than autoinc_version, request needs retry", KR(ret), K(key),
                                                                                           K(inner_autoinc_version), K(request_autoinc_version));
  }
  return ret;
}

int ObAutoIncInnerTableProxy::next_autoinc_value(const AutoincKey &key,
                                                 const uint64_t offset,
                                                 const uint64_t increment,
                                                 const uint64_t base_value,
                                                 const uint64_t max_value,
                                                 const uint64_t desired_count,
                                                 const int64_t &autoinc_version,
                                                 uint64_t &start_inclusive,
                                                 uint64_t &end_inclusive,
                                                 uint64_t &sync_value)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  bool with_snap_shot = true;
  const uint64_t tenant_id = key.tenant_id_;
  const uint64_t table_id = key.table_id_;
  const uint64_t column_id = key.column_id_;
  uint64_t sequence_value = 0;
  int64_t inner_autoinc_version = OB_INVALID_VERSION;
  int64_t tmp_autoinc_version = get_modify_autoinc_version(autoinc_version);
  if (OB_ISNULL(mysql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("mysql proxy is null", K(ret));
  } else if (OB_FAIL(trans.start(mysql_proxy_, tenant_id, with_snap_shot))) {
    LOG_WARN("failed to start transaction", K(ret), K(tenant_id));
  } else {
    int sql_len = 0;
    SMART_VAR(char[OB_MAX_SQL_LENGTH], sql) {
      const uint64_t exec_tenant_id = tenant_id;
      const char *table_name = OB_ALL_AUTO_INCREMENT_TNAME;
      sql_len = snprintf(sql, OB_MAX_SQL_LENGTH,
                         " SELECT sequence_key, sequence_value, sync_value, truncate_version FROM %s WHERE tenant_id = %lu AND sequence_key = %lu AND column_id = %lu FOR UPDATE",
                         table_name,
                         ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                         ObSchemaUtils::get_extract_schema_id(tenant_id, table_id),
                         column_id);
      if (sql_len >= OB_MAX_SQL_LENGTH || sql_len <= 0) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("failed to format sql. size not enough", K(ret), K(sql_len));
      } else {
        int64_t fetch_table_id = OB_INVALID_ID;
        { // make sure %res destructed before execute other sql in the same transaction
          SMART_VAR(ObMySQLProxy::MySQLResult, res) {
            ObMySQLResult *result = NULL;
            ObISQLClient *sql_client = &trans;
            uint64_t sequence_table_id = OB_ALL_AUTO_INCREMENT_TID;
            ObSQLClientRetryWeak sql_client_retry_weak(sql_client,
                                                       exec_tenant_id,
                                                       sequence_table_id);
            if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql))) {
              LOG_WARN(" failed to read data", K(ret));
            } else if (NULL == (result = res.get_result())) {
              LOG_WARN("failed to get result", K(ret));
              ret = OB_ERR_UNEXPECTED;
            } else if (OB_FAIL(result->next())) {
              LOG_WARN("failed to get next", K(ret));
              if (OB_ITER_END == ret) {
                // auto-increment column has been deleted
                ret = OB_SCHEMA_ERROR;
                LOG_WARN("failed to get next", K(ret));
              }
            } else {
              if (OB_FAIL(result->get_int(0l, fetch_table_id))) {
                LOG_WARN("fail to get int_value.", K(ret));
              } else if (OB_FAIL(result->get_uint(1l, sequence_value))) {
                LOG_WARN("fail to get int_value.", K(ret));
              } else if (OB_FAIL(result->get_uint(2l, sync_value))) {
                LOG_WARN("fail to get int_value.", K(ret));
              } else if (OB_FAIL(result->get_int(3l, inner_autoinc_version))) {
                LOG_WARN("fail to get inner_autoinc_version.", K(ret));
              } else if (OB_FAIL(check_inner_autoinc_version(tmp_autoinc_version, inner_autoinc_version, key))) {
                LOG_WARN("fail to check inner_autoinc_version", KR(ret));
              } else {
                if (sync_value >= max_value) {
                  sequence_value = max_value;
                } else {
                  sequence_value = std::max(sequence_value, sync_value + 1);
                }
                if (base_value > sequence_value) {
                  sequence_value = base_value;
                }
              }
              if (OB_SUCC(ret)) {
                int tmp_ret = OB_SUCCESS;
                if (OB_ITER_END != (tmp_ret = result->next())) {
                  if (OB_SUCCESS == tmp_ret) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("more than one row", K(ret), K(tenant_id), K(table_id), K(column_id));
                  } else {
                    ret = tmp_ret;
                    LOG_WARN("fail to iter next row", K(ret), K(tenant_id), K(table_id),
                                                      K(column_id));
                  }
                }
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          uint64_t curr_new_value = 0;
          if (OB_FAIL(ObAutoincrementService::calc_next_value(sequence_value, offset, increment, curr_new_value))) {
            LOG_WARN("fail to get next value.", K(ret));
          } else {
            uint64_t next_sequence_value = 0;
            if (max_value < desired_count || curr_new_value >= max_value - desired_count) {
              end_inclusive = max_value;
              next_sequence_value = max_value;
              if (OB_UNLIKELY(curr_new_value > max_value)) {
                curr_new_value = max_value;
              }
            } else {
              end_inclusive = curr_new_value + desired_count - 1;
              next_sequence_value = curr_new_value + desired_count;
              if (OB_UNLIKELY(end_inclusive >= max_value || end_inclusive < curr_new_value /* overflow */)) {
                end_inclusive = max_value;
                next_sequence_value = max_value;
              }
            }
            start_inclusive = curr_new_value;
            LOG_DEBUG("update next value",
                      K(desired_count), K(next_sequence_value), K(start_inclusive), K(end_inclusive));

            sql_len = snprintf(sql, OB_MAX_SQL_LENGTH,
                              "UPDATE %s SET sequence_value = %lu, gmt_modified = now(6)"
                              " WHERE tenant_id = %lu AND sequence_key = %lu AND column_id = %lu AND truncate_version = %ld",
                              table_name,
                              next_sequence_value,
                              OB_INVALID_TENANT_ID,
                              table_id,
                              column_id,
                              inner_autoinc_version);
            int64_t affected_rows = 0;
            if (sql_len >= OB_MAX_SQL_LENGTH || sql_len <= 0) {
              ret = OB_SIZE_OVERFLOW;
              LOG_WARN("failed to format sql. size not enough", K(ret), K(sql_len));
            } else if (GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != exec_tenant_id) {
              ret = OB_OP_NOT_ALLOW;
              LOG_WARN("can't write sys table now", K(ret), K(exec_tenant_id));
            } else if (OB_FAIL(trans.write(exec_tenant_id, sql, affected_rows))) {
              LOG_WARN("failed to write data", K(ret));
            } else if (affected_rows != 1) {
              LOG_WARN("failed to update sequence value",
                      K(tenant_id), K(table_id), K(column_id), K(ret));
            }
          }
        }
      }
    }

    // commit transaction or rollback
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("fail to commit transaction.", K(ret));
      }
    } else {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = trans.end(false))) {
        LOG_WARN("fail to rollback transaction. ", K(err));
      }
    }
  }
  LOG_DEBUG("get_value done", K(max_value), K(sync_value), K(start_inclusive), K(end_inclusive));
  return ret;
}

int ObAutoIncInnerTableProxy::get_autoinc_value(const AutoincKey &key,
                                                const int64_t &autoinc_version,
                                                uint64_t &seq_value,
                                                uint64_t &sync_value)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = key.tenant_id_;
  const int64_t tmp_autoinc_version = get_modify_autoinc_version(autoinc_version);
  SMART_VARS_2((ObMySQLProxy::MySQLResult, res), (char[OB_MAX_SQL_LENGTH], sql)) {
    ObMySQLResult *result = NULL;
    int sql_len = 0;
    const uint64_t exec_tenant_id = tenant_id;
    const char *table_name = OB_ALL_AUTO_INCREMENT_TNAME;
    sql_len = snprintf(sql, OB_MAX_SQL_LENGTH,
                        " SELECT sequence_value, sync_value, truncate_version FROM %s"
                        " WHERE tenant_id = %lu AND sequence_key = %lu AND column_id = %lu",
                        table_name,
                        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                        ObSchemaUtils::get_extract_schema_id(exec_tenant_id, key.table_id_),
                        key.column_id_);
    ObISQLClient *sql_client = mysql_proxy_;
    uint64_t sequence_table_id = OB_ALL_AUTO_INCREMENT_TID;
    ObSQLClientRetryWeak sql_client_retry_weak(sql_client,
                                                exec_tenant_id,
                                                sequence_table_id);
    if (OB_ISNULL(mysql_proxy_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("mysql proxy is null", K(ret));
    } else if (sql_len >= OB_MAX_SQL_LENGTH || sql_len <= 0) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("failed to format sql. size not enough", K(ret), K(sql_len));
    } else if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql))) {
      LOG_WARN(" failed to read data", K(ret));
    } else if (NULL == (result = res.get_result())) {
      LOG_WARN("failed to get result", K(ret));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        LOG_INFO("there is no autoinc column record, return 0 as seq_value by default",
                  K(key), K(ret));
        seq_value = 0;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail get next value", K(key), K(ret));
      }
    } else {
      int64_t inner_autoinc_version = OB_INVALID_VERSION;
      if (OB_FAIL(result->get_uint(0l, seq_value))) {
        LOG_WARN("fail to get int_value.", K(ret));
      } else if (OB_FAIL(result->get_uint(1l, sync_value))) {
        LOG_WARN("fail to get int_value.", K(ret));
      } else if (OB_FAIL(result->get_int(2l, inner_autoinc_version))) {
        LOG_WARN("fail to get truncate_version.", K(ret));
      } else if (OB_FAIL(check_inner_autoinc_version(tmp_autoinc_version, inner_autoinc_version, key))) {
        LOG_WARN("fail to check inner_autoinc_version", KR(ret));
      }
      if (OB_SUCC(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_ITER_END != (tmp_ret = result->next())) {
          if (OB_SUCCESS == tmp_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("more than one row", K(ret), K(key));
          } else {
            ret = tmp_ret;
            LOG_WARN("fail to iter next row", K(ret), K(key));
          }
        }
      }
    }
  }
  return ret;
}

// TODO: (xingrui.cwh)
// If this interface is used by another function except for __tenant_virtual_all_table,
// this interface will need to verfiy autoinc_version for correctness
int ObAutoIncInnerTableProxy::get_autoinc_value_in_batch(
    const uint64_t tenant_id,
    const common::ObIArray<AutoincKey> &keys,
    common::hash::ObHashMap<AutoincKey, uint64_t> &seq_values)
{
  int ret = OB_SUCCESS;
  int64_t N = keys.count() / FETCH_SEQ_NUM_ONCE;
  int64_t M = keys.count() % FETCH_SEQ_NUM_ONCE;
  N += (M == 0) ? 0 : 1;
  ObSqlString sql;
  if (OB_ISNULL(mysql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("mysql proxy is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    sql.reset();
    if (OB_FAIL(sql.assign_fmt(
        " SELECT sequence_key, column_id, sequence_value FROM %s"
        " WHERE (sequence_key, column_id) IN (", OB_ALL_AUTO_INCREMENT_TNAME))) {
      LOG_WARN("failed to append sql", K(ret));
    }

    // last iteration
    int64_t P = (0 != M && N - 1 == i) ? M : FETCH_SEQ_NUM_ONCE;
    for (int64_t j = 0; OB_SUCC(ret) && j < P; ++j) {
      AutoincKey key = keys.at(i * FETCH_SEQ_NUM_ONCE + j);
      if (OB_FAIL(sql.append_fmt("%s(%lu, %lu)",
                                 (0 == j) ? "" : ", ",
                                 key.table_id_,
                                 key.column_id_))) {
        LOG_WARN("failed to append sql", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt(")"))) {
        LOG_WARN("failed to append sql", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObMySQLResult *result = NULL;
        int64_t table_id  = 0;
        int64_t column_id = 0;
        uint64_t seq_value = 0;
        ObISQLClient *sql_client = mysql_proxy_;
        ObSQLClientRetryWeak sql_client_retry_weak(sql_client,
                                                   tenant_id,
                                                   OB_ALL_AUTO_INCREMENT_TID);
        if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
          LOG_WARN(" failed to read data", K(ret));
        } else if (NULL == (result = res.get_result())) {
          LOG_WARN("failed to get result", K(ret));
          ret = OB_ERR_UNEXPECTED;
        } else {
          while(OB_SUCC(ret) && OB_SUCC(result->next())) {
            if (OB_FAIL(result->get_int(0l, table_id))) {
              LOG_WARN("fail to get int_value.", K(ret));
            } else if (OB_FAIL(result->get_int(1l, column_id))) {
              LOG_WARN("fail to get int_value.", K(ret));
            } else if (OB_FAIL(result->get_uint(2l, seq_value))) {
              LOG_WARN("fail to get int_value.", K(ret));
            } else {
              AutoincKey key;
              key.tenant_id_ = tenant_id;
              key.table_id_  = table_id;
              key.column_id_ = static_cast<uint64_t>(column_id);
              if (OB_FAIL(seq_values.set_refactored(key, seq_value))) {
                LOG_WARN("fail to get int_value.", K(ret));
              }
            }
          }
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get next result", K(ret), K(sql));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      break;
    }
  }
  return ret;
}

int ObAutoIncInnerTableProxy::sync_autoinc_value(const AutoincKey &key,
                                                 const uint64_t insert_value,
                                                 const uint64_t max_value,
                                                 const int64_t autoinc_version,
                                                 uint64_t &seq_value,
                                                 uint64_t &sync_value)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = key.tenant_id_;
  const uint64_t table_id = key.table_id_;
  const uint64_t column_id = key.column_id_;
  ObMySQLTransaction trans;
  ObSqlString sql;
  bool with_snap_shot = true;
  uint64_t fetch_seq_value = 0;
  int64_t inner_autoinc_version = OB_INVALID_VERSION;
  int64_t tmp_autoinc_version = get_modify_autoinc_version(autoinc_version);
  if (OB_ISNULL(mysql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("mysql proxy is null", K(ret));
  } else if (OB_FAIL(trans.start(mysql_proxy_, tenant_id, with_snap_shot))) {
    LOG_WARN("failed to start transaction", K(ret), K(tenant_id));
  } else {
    const uint64_t exec_tenant_id = tenant_id;
    const char *table_name = OB_ALL_AUTO_INCREMENT_TNAME;
    int64_t fetch_table_id = OB_INVALID_ID;
    if (OB_FAIL(sql.assign_fmt(" SELECT sequence_key, sequence_value, sync_value, truncate_version FROM %s WHERE tenant_id = %lu AND sequence_key = %lu"
                               " AND column_id = %lu FOR UPDATE",
                               table_name,
                               ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                               ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id),
                               column_id))) {
      LOG_WARN("failed to assign sql", K(ret));
    }
    if (OB_SUCC(ret)) {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObMySQLResult *result = NULL;
        ObISQLClient *sql_client = &trans;
        uint64_t sequence_table_id = OB_ALL_AUTO_INCREMENT_TID;
        ObSQLClientRetryWeak sql_client_retry_weak(sql_client,
                                                   exec_tenant_id,
                                                   sequence_table_id);
        if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
          LOG_WARN("failed to read data", K(ret));
        } else if (NULL == (result = res.get_result())) {
          LOG_WARN("failed to get result", K(ret));
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("failed to get next", K(ret));
          if (OB_ITER_END == ret) {
            // auto-increment column has been deleted
            ret = OB_SCHEMA_ERROR;
            LOG_WARN("failed to get next", K(ret));
          }
        } else if (OB_FAIL(result->get_int(0l, fetch_table_id))) {
          LOG_WARN("failed to get int_value.", K(ret));
        } else if (OB_FAIL(result->get_uint(1l, fetch_seq_value))) {
          LOG_WARN("failed to get int_value.", K(ret));
        } else if (OB_FAIL(result->get_uint(2l, sync_value))) {
          LOG_WARN("failed to get int_value.", K(ret));
        } else if (OB_FAIL(result->get_int(3l, inner_autoinc_version))) {
          LOG_WARN("failed to get inner_autoinc_version.", K(ret));
        } else if (OB_FAIL(check_inner_autoinc_version(tmp_autoinc_version, inner_autoinc_version, key))) {
          LOG_WARN("failed to check inner_autoinc_version", KR(ret));
        }
        if (OB_SUCC(ret)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_ITER_END != (tmp_ret = result->next())) {
            if (OB_SUCCESS == tmp_ret) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("more than one row", K(ret), K(tenant_id), K(table_id), K(column_id));
            } else {
              ret = tmp_ret;
              LOG_WARN("fail to iter next row", K(ret), K(tenant_id), K(table_id), K(column_id));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      uint64_t new_seq_value = 0;
      if (insert_value > sync_value) {
        seq_value = std::max(fetch_seq_value, seq_value + 1);
        sync_value = std::max(fetch_seq_value - 1, insert_value);
        new_seq_value = sync_value >= max_value ? max_value : sync_value + 1;

        // if insert_value > global_sync
        // 2. update __all_sequence(may get global_sync)
        // 3. sync to other server
        // 4. adjust cache_handle(prefetch) and table node
        int64_t affected_rows = 0;
        // NOTE: Why the sequence value is also updated?
        //       > In order to support display correct AUTO_INCREMENT property in SHOW CREATE TABLE
        //       statment.
        //       Why don't we calculate AUTO_INCREMENT in real time when we execute the SHOW
        //       statement?
        //       > I can't get MAX_VALUE in DDL context. auto inc column type is needed.
        if (OB_FAIL(sql.assign_fmt(
                    "UPDATE %s SET sync_value = %lu, sequence_value = %lu, gmt_modified = now(6) "
                    "WHERE tenant_id=%lu AND sequence_key=%lu AND column_id=%lu AND truncate_version=%ld",
                    table_name, sync_value, new_seq_value,
                    OB_INVALID_TENANT_ID, table_id, column_id, inner_autoinc_version))) {
          LOG_WARN("failed to assign sql", K(ret));
        } else if (GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != exec_tenant_id) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("can't write sys table now", K(ret), K(exec_tenant_id));
        } else if (OB_FAIL((trans.write(exec_tenant_id, sql.ptr(), affected_rows)))) {
          LOG_WARN("failed to execute", K(sql), K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", K(affected_rows), K(ret));
        } else {
          LOG_TRACE("sync insert value", K(key), K(insert_value));
        }

        // commit transaction or rollback
        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true))) {
            LOG_WARN("fail to commit transaction.", K(ret));
          }
        } else {
          int err = OB_SUCCESS;
          if (OB_SUCCESS != (err = trans.end(false))) {
            LOG_WARN("fail to rollback transaction. ", K(err));
          }
        }
      } else {
        seq_value = fetch_seq_value;
      }

      // if transactin is started above(but do nothing), end it here
      if (trans.is_started()) {
        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true))) {
            LOG_WARN("fail to commit transaction.", K(ret));
          }
        } else {
          int err = OB_SUCCESS;
          if (OB_SUCCESS != (err = trans.end(false))) {
            LOG_WARN("fail to rollback transaction. ", K(err));
          }
        }
      }
    }
  }
  return ret;
}

int ObAutoIncInnerTableProxy::read_and_push_inner_table(const AutoincKey &key,
                                                        const uint64_t max_value,
                                                        const uint64_t cache_end,
                                                        const int64_t autoinc_version,
                                                        bool &is_valid,
                                                        uint64_t &new_end)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = key.tenant_id_;
  const uint64_t table_id = key.table_id_;
  const uint64_t column_id = key.column_id_;
  is_valid = false;
  ObMySQLTransaction trans;
  ObSqlString sql;
  bool with_snap_shot = true;
  uint64_t fetch_seq_value = 0;
  int64_t inner_autoinc_version = OB_INVALID_VERSION;
  uint64_t sync_value = 0;
  if (OB_ISNULL(mysql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("mysql proxy is null", K(ret));
  } else if (OB_FAIL(trans.start(mysql_proxy_, tenant_id, with_snap_shot))) {
    LOG_WARN("failed to start transaction", K(ret), K(tenant_id));
  } else {
    const uint64_t exec_tenant_id = tenant_id;
    const char *table_name = OB_ALL_AUTO_INCREMENT_TNAME;
    int64_t fetch_table_id = OB_INVALID_ID;
    if (OB_FAIL(sql.assign_fmt(" SELECT sequence_value, truncate_version FROM %s WHERE tenant_id = %lu AND sequence_key = %lu"
                               " AND column_id = %lu FOR UPDATE",
                               table_name,
                               ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                               ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id),
                               column_id))) {
      LOG_WARN("failed to assign sql", K(ret));
    }
    if (OB_SUCC(ret)) {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObMySQLResult *result = NULL;
        ObISQLClient *sql_client = &trans;
        uint64_t sequence_table_id = OB_ALL_AUTO_INCREMENT_TID;
        ObSQLClientRetryWeak sql_client_retry_weak(sql_client,
                                                   exec_tenant_id,
                                                   sequence_table_id);
        if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
          LOG_WARN("failed to read data", K(ret));
        } else if (NULL == (result = res.get_result())) {
          LOG_WARN("failed to get result", K(ret));
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("failed to get next", K(ret));
          if (OB_ITER_END == ret) {
            // auto-increment column has been deleted
            ret = OB_SCHEMA_ERROR;
            LOG_WARN("failed to get next", K(ret));
          }
        } else if (OB_FAIL(result->get_uint(0l, fetch_seq_value))) {
          LOG_WARN("failed to get int_value.", K(ret));
        } else if (OB_FAIL(result->get_int(1l, inner_autoinc_version))) {
          LOG_WARN("failed to get inner_autoinc_version.", K(ret));
        }
        if (OB_SUCC(ret)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_ITER_END != (tmp_ret = result->next())) {
            if (OB_SUCCESS == tmp_ret) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("more than one row", K(ret), K(tenant_id), K(table_id), K(column_id));
            } else {
              ret = tmp_ret;
              LOG_WARN("fail to iter next row", K(ret), K(tenant_id), K(table_id), K(column_id));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (autoinc_version != inner_autoinc_version) {
        is_valid = false;
      } else if (cache_end == fetch_seq_value && cache_end == max_value) {
        // the column reach max value, keep the maximum value unchanged
        is_valid = true;
        new_end = max_value;
      } else if (cache_end == fetch_seq_value - 1) {
        // The cache is continuous and the verification passes.
        is_valid = true;
        uint64_t new_seq_value = fetch_seq_value;
        if (fetch_seq_value >= max_value) {
          new_end = max_value;
        } else {
          new_end = fetch_seq_value;
          new_seq_value += 1;
          // push new seq value to inner table
          int64_t affected_rows = 0;
          if (OB_FAIL(sql.assign_fmt(
                      "UPDATE %s SET sequence_value = %lu, gmt_modified = now(6) "
                      "WHERE tenant_id=%lu AND sequence_key=%lu AND column_id=%lu AND truncate_version=%ld",
                      table_name, new_seq_value,
                      OB_INVALID_TENANT_ID, table_id, column_id, inner_autoinc_version))) {
            LOG_WARN("failed to assign sql", K(ret));
          } else if (GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != exec_tenant_id) {
            ret = OB_OP_NOT_ALLOW;
            LOG_WARN("can't write sys table now", K(ret), K(exec_tenant_id));
          } else if (OB_FAIL((trans.write(exec_tenant_id, sql.ptr(), affected_rows)))) {
            LOG_WARN("failed to execute", K(sql), K(ret));
          } else if (!is_single_row(affected_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error", K(affected_rows), K(ret));
          } else {
          }

          // commit transaction or rollback
          if (OB_SUCC(ret)) {
            if (OB_FAIL(trans.end(true))) {
              LOG_WARN("fail to commit transaction.", K(ret));
            }
          } else {
            int err = OB_SUCCESS;
            if (OB_SUCCESS != (err = trans.end(false))) {
              LOG_WARN("fail to rollback transaction. ", K(err));
            }
          }
        }
      }

      // if transactin is started above(but do nothing), end it here
      if (trans.is_started()) {
        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true))) {
            LOG_WARN("fail to commit transaction.", K(ret));
          }
        } else {
          int err = OB_SUCCESS;
          if (OB_SUCCESS != (err = trans.end(false))) {
            LOG_WARN("fail to rollback transaction. ", K(err));
          }
        }
      }
    }
  }
  return ret;
}

}//end namespace share
}//end namespace oceanbase
