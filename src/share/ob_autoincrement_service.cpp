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
#include "share/ob_worker.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/time/ob_time_utility.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/partition_table/ob_partition_location.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_utils.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_i_partition_group.h"
#include "share/ob_schema_status_proxy.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_sql_client_decorator.h"
#include "sql/ob_sql_partition_location_cache.h"

using namespace oceanbase::obrpc;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::common::sqlclient;
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

int CacheNode::combine_cache_node(CacheNode& new_node)
{
  int ret = OB_SUCCESS;
  if (new_node.cache_start_ > 0) {
    if (cache_end_ < cache_start_ || new_node.cache_end_ < new_node.cache_start_ ||
        cache_end_ > new_node.cache_start_) {
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

int TableNode::alloc_handle(ObSmallAllocator& allocator, const uint64_t offset, const uint64_t increment,
    const uint64_t desired_count, const uint64_t max_value, CacheHandle*& handle)
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
  if (min_value >= max_value) {
    new_next_value = max_value;
    needed_interval = max_value;
  } else if (min_value > node.cache_end_) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("fail to alloc handle; cache is not enough", K(*this), K(ret));
  } else {
    ret = ObAutoincrementService::calc_next_value(min_value, offset, increment, new_next_value);
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
          LOG_WARN("fail to alloc handle; cache is not enough", K(*this), K(ret));
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
            LOG_WARN("fail to alloc handle; cache is not enough", K(*this), K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    handle = static_cast<CacheHandle*>(allocator.alloc());
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
        LOG_INFO("failed to allocate cache handle", K(ret));
      }
    }
  } else {
    LOG_WARN("unexpected error", K(ret));
  }

  return ret;
}

int TableNode::init(int64_t autoinc_table_part_num)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(partition_leader_epoch_map_.create(autoinc_table_part_num, ObModIds::OB_AUTOINCREMENT_PART_MAP))) {
    LOG_WARN("failed to create leader epoch map", K(ret));
  }
  return ret;
}

int TableNode::set_partition_leader_epoch(const int64_t partition_id, const int64_t leader_epoch)
{
  const int overwrite = 1;
  return partition_leader_epoch_map_.set_refactored(partition_id, leader_epoch, overwrite);
}

int TableNode::get_partition_leader_epoch(const int64_t partition_id, int64_t& leader_epoch)
{
  int ret = OB_SUCCESS;
  leader_epoch = 0;
  int hash_ret = partition_leader_epoch_map_.get_refactored(partition_id, leader_epoch);
  if (OB_HASH_NOT_EXIST != hash_ret) {
    ret = hash_ret;
  }
  return ret;
}

int TableNode::update_all_partitions_leader_epoch(const int64_t partition_id, const int64_t leader_epoch)
{
  int ret = set_partition_leader_epoch(partition_id, leader_epoch);
  for (auto iter = partition_leader_epoch_map_.begin(); iter != partition_leader_epoch_map_.end(); ++iter) {
    const int64_t cur_partition_id = iter->first;
    if (cur_partition_id != partition_id) {
      int64_t new_leader_epoch = -1;
      ObPartitionKey pkey(table_id_, cur_partition_id, 0);
      int tmp_ret = ObAutoincrementService::get_instance().get_leader_epoch_id(pkey, new_leader_epoch);
      // just logging, remain return value
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("failed to get leader epoch id", K(tmp_ret), K(pkey));
      } else {
        iter->second = new_leader_epoch;
      }
    }
  }
  return ret;
}

int CacheHandle::next_value(uint64_t& next_value)
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
      location_cache_(NULL),
      schema_service_(NULL),
      ps_(NULL)
{}

ObAutoincrementService::~ObAutoincrementService()
{}

ObAutoincrementService& ObAutoincrementService::get_instance()
{
  static ObAutoincrementService autoinc_service;
  return autoinc_service;
}

int ObAutoincrementService::init(ObAddr& addr, ObMySQLProxy* mysql_proxy, ObSrvRpcProxy* srv_proxy,
    ObPartitionLocationCache* location_cache, ObMultiVersionSchemaService* schema_service,
    storage::ObPartitionService* ps)
{
  int ret = OB_SUCCESS;
  my_addr_ = addr;
  mysql_proxy_ = mysql_proxy;
  srv_proxy_ = srv_proxy;
  location_cache_ = location_cache;
  schema_service_ = schema_service;
  ps_ = ps;

  if (OB_FAIL(node_allocator_.init(sizeof(TableNode), ObModIds::OB_AUTOINCREMENT))) {
    LOG_WARN("failed to init table node allocator", K(ret));
  } else if (OB_FAIL(handle_allocator_.init(sizeof(CacheHandle), ObModIds::OB_AUTOINCREMENT))) {
    LOG_WARN("failed to init cache handle allocator", K(ret));
  } else if (OB_FAIL(node_map_.init())) {
    LOG_WARN("failed to init table node map", K(ret));
  }
  return ret;
}

// only used for logic backup
int ObAutoincrementService::init_for_backup(ObAddr& addr, ObMySQLProxy* mysql_proxy, ObSrvRpcProxy* srv_proxy,
    ObPartitionLocationCache* location_cache, ObMultiVersionSchemaService* schema_service,
    storage::ObPartitionService* ps)
{
  int ret = OB_SUCCESS;
  my_addr_ = addr;
  mysql_proxy_ = mysql_proxy;
  srv_proxy_ = srv_proxy;
  location_cache_ = location_cache;
  schema_service_ = schema_service;
  ps_ = ps;
  return ret;
}

int ObAutoincrementService::get_handle(AutoincParam& param, CacheHandle*& handle)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = param.tenant_id_;
  const uint64_t table_id = param.autoinc_table_id_;
  const uint64_t column_id = param.autoinc_col_id_;
  const ObObjType column_type = param.autoinc_col_type_;
  const uint64_t offset = param.autoinc_offset_;
  const uint64_t increment = param.autoinc_increment_;
  const uint64_t max_value = get_max_value(column_type);

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

  TableNode* table_node = NULL;
  LOG_DEBUG("begin to get cache handle", K(param));
  if (OB_FAIL(get_table_node(param, table_node))) {
    LOG_WARN("failed to get table node", K(param), K(ret));
  } else {
    const int64_t cur_time = ObTimeUtility::current_time();
    const int64_t cache_refresh_interval = GCONF.autoinc_cache_refresh_interval;
    int64_t delta = cur_time - ATOMIC_LOAD(&table_node->last_refresh_ts_);
    if (delta * PRE_OP_THRESHOLD < cache_refresh_interval) {
      // do nothing
    } else if (delta < cache_refresh_interval) {
      // try to sync
      if (OB_FAIL(table_node->sync_mutex_.trylock())) {
        // other thread is doing sync; pass through
        ret = OB_SUCCESS;
      } else {
        delta = cur_time - ATOMIC_LOAD(&table_node->last_refresh_ts_);
        if (delta * PRE_OP_THRESHOLD < cache_refresh_interval) {
          // do nothing; refreshed already
        } else if (OB_FAIL(do_global_sync(tenant_id, table_id, column_id, table_node, true))) {
          LOG_WARN("failed to get global sync", K(param), K(*table_node), K(ret));
        }
        table_node->sync_mutex_.unlock();
      }
    } else {
      // must sync
      if (OB_FAIL(table_node->sync_mutex_.lock())) {
        LOG_WARN("failed to get sync lock", K(param), K(*table_node), K(ret));
      } else {
        delta = cur_time - ATOMIC_LOAD(&table_node->last_refresh_ts_);
        if (delta * PRE_OP_THRESHOLD < cache_refresh_interval) {
          // do nothing; refreshed already
        } else if (OB_FAIL(do_global_sync(tenant_id, table_id, column_id, table_node))) {
          LOG_WARN("failed to get global sync", K(param), K(*table_node), K(ret));
        }
        table_node->sync_mutex_.unlock();
      }
    }
    // alloc handle
    bool need_prefetch = false;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_node->alloc_mutex_.lock())) {
        LOG_WARN("failed to get alloc lock", K(param), K(*table_node), K(ret));
      } else {
        if (OB_SIZE_OVERFLOW ==
            (ret = table_node->alloc_handle(handle_allocator_, offset, increment, desired_count, max_value, handle))) {
          if (OB_FAIL(fetch_table_node(param, table_node))) {
            LOG_WARN("failed to fetch table node", K(param), K(ret));
          } else if (OB_FAIL(table_node->alloc_handle(
                         handle_allocator_, offset, increment, desired_count, max_value, handle))) {
            LOG_WARN("failed to alloc cache handle", K(param), K(ret));
          } else {
            LOG_INFO("succ to get cache handle", K(param), K(*handle), K(ret));
          }
        }
        if (OB_SUCC(ret) && table_node->prefetch_condition() && !table_node->prefetching_) {
          // ensure single thread to prefetch
          need_prefetch = true;
          table_node->prefetching_ = true;
        }
        table_node->alloc_mutex_.unlock();
      }
    }
    // prefetch cache node
    if (OB_SUCC(ret) && need_prefetch) {
      LOG_INFO("begin to prefetch table node", K(param), K(ret));
      TableNode mock_node;
      if (OB_FAIL(fetch_table_node(param, &mock_node, true))) {
        LOG_WARN("failed to fetch table node", K(param), K(ret));
      } else if (OB_FAIL(table_node->alloc_mutex_.lock())) {
        LOG_WARN("failed to get alloc lock", K(param), K(*table_node), K(ret));
      } else {
        LOG_INFO("dump node", K(mock_node), K(*table_node));
        if (table_node->prefetch_node_.cache_start_ != 0 ||
            mock_node.prefetch_node_.cache_start_ <= table_node->curr_node_.cache_end_) {
          LOG_INFO("new table_node has been fetched by other, ignore");
        } else {
          atomic_update(table_node->local_sync_, mock_node.local_sync_);
          atomic_update(table_node->last_refresh_ts_, mock_node.last_refresh_ts_);
          table_node->prefetch_node_.cache_start_ = mock_node.prefetch_node_.cache_start_;
          table_node->prefetch_node_.cache_end_ = mock_node.prefetch_node_.cache_end_;
        }
        table_node->alloc_mutex_.unlock();
      }
      table_node->prefetching_ = false;
    }
  }
  // table node must be reverted after get to decrement reference count
  if (NULL != table_node) {
    node_map_.revert(table_node);
  }
  return ret;
}

void ObAutoincrementService::release_handle(CacheHandle*& handle)
{
  handle_allocator_.free(handle);
  // bug#8200783, should reset handle here
  handle = NULL;
}

int ObAutoincrementService::refresh_sync_value(const obrpc::ObAutoincSyncArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("begin to get global sync", K(arg));
  if (OB_FAIL(sync_global_sync(arg.tenant_id_, arg.table_id_, arg.column_id_, arg.sync_value_))) {
    LOG_WARN("failed to get global sync value", K(arg));
  }
  return ret;
}

int ObAutoincrementService::clear_autoinc_cache_all(
    const uint64_t tenant_id, const uint64_t table_id, const uint64_t column_id)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    // sync observer where table partitions exist
    LOG_INFO("begin to clear all sever's auto-increment cache", K(tenant_id), K(table_id));
    ObAutoincSyncArg arg;
    arg.tenant_id_ = tenant_id;
    arg.table_id_ = table_id;
    arg.column_id_ = column_id;
    ObHashSet<ObAddr> server_set;
    if (OB_FAIL(server_set.create(PARTITION_LOCATION_SET_BUCKET_NUM))) {
      LOG_WARN("failed to create hash set", K(ret));
    } else if (OB_FAIL(get_server_set(table_id, server_set, true))) {
      SHARE_LOG(WARN, "failed to get table partitions server set", K(ret));
    } else {
      const int64_t sync_timeout = GCONF.autoinc_cache_refresh_interval + TIME_SKEW;
      ObHashSet<ObAddr>::iterator iter;
      for (iter = server_set.begin(); OB_SUCC(ret) && iter != server_set.end(); ++iter) {
        LOG_INFO("send rpc call to other observers", "server", iter->first);
        if (OB_FAIL(srv_proxy_->to(iter->first).timeout(sync_timeout).clear_autoinc_cache(arg))) {
          if (is_timeout_err(ret) || is_server_down_error(ret)) {
            // ignore time out, go on
            LOG_WARN("rpc call time out, ignore the error", "server", iter->first, K(tenant_id), K(table_id), K(ret));
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

int ObAutoincrementService::clear_autoinc_cache(const obrpc::ObAutoincSyncArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("begin to clear local auto-increment cache", K(arg));
  // auto-increment key
  AutoincKey key;
  key.tenant_id_ = arg.tenant_id_;
  key.table_id_ = arg.table_id_;
  key.column_id_ = arg.column_id_;

  lib::ObMutexGuard guard(map_mutex_);
  if (OB_FAIL(node_map_.del(key))) {
    LOG_WARN("failed to erase table node", K(arg), K(ret));
  }

  if (OB_ENTRY_NOT_EXIST == ret) {
    // do nothing; key does not exist
    ret = OB_SUCCESS;
  }
  return ret;
}

uint64_t ObAutoincrementService::get_max_value(const common::ObObjType type)
{
  static const uint64_t type_max_value[] = {// null
      0,
      // signed
      INT8_MAX,      // tiny int   127
      INT16_MAX,     // short      32767
      INT24_MAX,     // medium int 8388607
      INT32_MAX,     // int        2147483647
      INT64_MAX,     // bigint     9223372036854775807
                     // unsigned
      UINT8_MAX,     //            255
      UINT16_MAX,    //            65535
      UINT24_MAX,    //            16777215
      UINT32_MAX,    //            4294967295
      UINT64_MAX,    //            18446744073709551615
                     // float
      0x1000000ULL,  // float      /* We use the maximum as per IEEE754-2008 standard, 2^24; compatible with MySQL */
      0x20000000000000ULL,  // double     /* We use the maximum as per IEEE754-2008 standard, 2^53; compatible with
                            // MySQL */
      0x1000000ULL,         // ufloat
      0x20000000000000ULL,  // udouble

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
      0};
  return type_max_value[OB_LIKELY(type < ObNumberType) ? type : ObNumberType];
}

int ObAutoincrementService::get_leader_epoch_id(const ObPartitionKey& part_key, int64_t& epoch_id) const
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard guard;
  if (OB_LIKELY(part_key.is_valid())) {
    if (OB_ISNULL(ps_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ps is null", K(ret));
    } else if (OB_FAIL(ps_->get_partition(part_key, guard))) {
      LOG_WARN("get partition failed", K(part_key), K(ret));
    } else if (OB_ISNULL(guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is NULL", K(part_key), K(ret));
    } else if (OB_FAIL(guard.get_partition_group()->get_leader_epoch(epoch_id))) {
      LOG_WARN("get leader epoch failed", K(ret));
    }
  }
  return ret;
}

int ObAutoincrementService::get_table_node(const AutoincParam& param, TableNode*& table_node)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = param.tenant_id_;
  uint64_t table_id = param.autoinc_table_id_;
  uint64_t column_id = param.autoinc_col_id_;
  // auto-increment key
  AutoincKey key;
  key.tenant_id_ = tenant_id;
  key.table_id_ = table_id;
  key.column_id_ = column_id;

  const int64_t partition_id = param.pkey_.is_valid() ? param.pkey_.get_partition_id() : -1;
  int64_t leader_epoch = 0;
  if (OB_FAIL(get_leader_epoch_id(param.pkey_, leader_epoch))) {
    LOG_WARN("get leader epoch id failed", K(ret), K(param));
  } else if (OB_FAIL(node_map_.get(key, table_node))) {
    if (ret != OB_ENTRY_NOT_EXIST) {
      LOG_ERROR("get from map failed", K(ret));
    } else {
      lib::ObMutex& mutex = init_node_mutex_[table_id % INIT_NODE_MUTEX_NUM];
      if (OB_FAIL(mutex.lock())) {
        LOG_WARN("failed to get lock", K(ret));
      } else if (OB_ENTRY_NOT_EXIST == (ret = node_map_.get(key, table_node))) {
        LOG_INFO("alloc table node for auto increment key", K(key));
        if (NULL == (table_node = op_alloc(TableNode))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("failed to alloc table node", K(param), K(ret));
        } else if (OB_FAIL(table_node->init(param.autoinc_table_part_num_))) {
          LOG_ERROR("failed to init table node", K(param), K(ret));
        } else if (OB_FAIL(table_node->set_partition_leader_epoch(partition_id, leader_epoch))) {
          LOG_ERROR("failed to set", K(param), K(ret));
        } else {
          table_node->prefetch_node_.reset();
          if (OB_FAIL(fetch_table_node(param, table_node))) {
            LOG_WARN("failed to fetch table node", K(param), K(ret));
          } else {
            lib::ObMutexGuard guard(map_mutex_);
            if (OB_FAIL(node_map_.insert_and_get(key, table_node))) {
              LOG_WARN("failed to create table node", K(param), K(ret));
            }
          }
        }
        if (OB_FAIL(ret) && table_node != nullptr) {
          op_free(table_node);
          table_node = NULL;
        }
      }
      mutex.unlock();
    }
  } else if (leader_epoch != 0) {
    lib::ObMutexGuard guard(table_node->alloc_mutex_);
    int64_t cur_leader_epoch = 0;
    if (OB_FAIL(table_node->get_partition_leader_epoch(partition_id, cur_leader_epoch))) {
      LOG_ERROR("failed to get", K(param), K(ret));
    } else if (cur_leader_epoch != leader_epoch) {
      LOG_INFO("leader epoch changed", "pkey", param.pkey_, "orgin epoch", cur_leader_epoch, "new epoch", leader_epoch);
      if (OB_FAIL(table_node->update_all_partitions_leader_epoch(partition_id, leader_epoch))) {
        LOG_ERROR("failed to set", K(param), K(ret));
      } else {
        table_node->prefetch_node_.reset();
        if (OB_FAIL(fetch_table_node(param, table_node))) {
          LOG_WARN("failed to fetch table node", K(param), K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("succ to get table node", K(param), K(*table_node), K(ret));
  } else {
    LOG_WARN("failed to get table node", K(param), K(ret));
  }
  return ret;
}

template <typename SchemaType>
int ObAutoincrementService::get_schema(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t schema_id,
    const std::function<int(uint64_t, const SchemaType*&)> get_schema_func, const SchemaType*& schema)
{
  int ret = OB_SUCCESS;
  schema = NULL;
  uint64_t fetch_tenant_id = is_inner_table(schema_id) ? OB_SYS_TENANT_ID : extract_tenant_id(schema_id);

  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(schema_service_), K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(fetch_tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret), K(fetch_tenant_id));
  } else if (OB_FAIL(get_schema_func(schema_id, schema))) {
    LOG_WARN("get table schema failed", K(schema_id), K(ret));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH;
    LOG_WARN(
        "NULL ptr, maybe schema not refreshed in this server", K(fetch_tenant_id), K(schema_id), K(schema), K(ret));
  }

  return ret;
}

int ObAutoincrementService::fetch_table_node(
    const AutoincParam& param, TableNode* table_node, const bool fetch_prefetch)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = param.tenant_id_;
  const uint64_t table_id = param.autoinc_table_id_;
  const uint64_t column_id = param.autoinc_col_id_;
  const ObObjType column_type = param.autoinc_col_type_;
  const uint64_t part_num = param.autoinc_table_part_num_;
  const uint64_t desired_count = param.autoinc_desired_count_;
  const uint64_t offset = param.autoinc_offset_;
  const uint64_t increment = param.autoinc_increment_;
  if (part_num <= 0 || ObNullType == column_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(part_num), K(column_type), K(ret));
  } else {
    // set timeout for prefetch
    ObTimeoutCtx ctx;
    if (fetch_prefetch) {
      if (OB_FAIL(set_pre_op_timeout(ctx))) {
        LOG_WARN("failed to set timeout", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObMySQLTransaction trans;
      bool with_snap_shot = true;

      uint64_t curr_value = 0;
      uint64_t curr_new_value = 0;
      uint64_t next_value = 0;
      uint64_t sync_value = 0;
      uint64_t max_value = get_max_value(column_type);
      uint64_t sequence_value = 0;
      LOG_DEBUG("get max_value according to column type", K(column_type), K(max_value));
      if (OB_FAIL(trans.start(mysql_proxy_, with_snap_shot))) {
        LOG_WARN("failed to start transaction", K(ret));
      } else {
        int sql_len = 0;
        SMART_VAR(char[OB_MAX_SQL_LENGTH], sql)
        {
          const uint64_t exec_tenant_id = tenant_id;
          const char* table_name = OB_ALL_SEQUENCE_V2_TNAME;
          sql_len = snprintf(sql,
              OB_MAX_SQL_LENGTH,
              " SELECT sequence_key, sequence_value, sync_value FROM %s"
              " WHERE tenant_id = %lu AND sequence_key = %lu AND column_id = %lu"
              " FOR UPDATE",
              table_name,
              ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
              ObSchemaUtils::get_extract_schema_id(tenant_id, table_id),
              column_id);
          if (sql_len >= OB_MAX_SQL_LENGTH || sql_len <= 0) {
            ret = OB_SIZE_OVERFLOW;
            LOG_WARN("failed to format sql. size not enough");
          } else {
            int64_t fetch_table_id = OB_INVALID_ID;
            {  // make sure %res destructed before execute other sql in the same transaction
              SMART_VAR(ObMySQLProxy::MySQLResult, res)
              {
                ObMySQLResult* result = NULL;
                ObISQLClient* sql_client = &trans;
                uint64_t sequence_table_id = OB_ALL_SEQUENCE_V2_TID;
                ObSQLClientRetryWeak sql_client_retry_weak(sql_client, exec_tenant_id, sequence_table_id);
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
                  } else if (OB_FAIL(result->get_uint(1l, curr_value))) {
                    LOG_WARN("fail to get int_value.", K(ret));
                  } else if (OB_FAIL(result->get_uint(2l, sync_value))) {
                    LOG_WARN("fail to get int_value.", K(ret));
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
                  if (OB_SUCC(ret)) {
                    ObSchemaGetterGuard schema_guard;
                    const ObTableSchema* table = NULL;
                    std::function<int(uint64_t, const ObTableSchema*&)> get_schema_func =
                        std::bind((int (ObSchemaGetterGuard::*)(uint64_t, const ObTableSchema*&)) &
                                      ObSchemaGetterGuard::get_table_schema,
                            std::ref(schema_guard),
                            std::placeholders::_1,
                            std::placeholders::_2);
                    if (OB_FAIL(get_schema(schema_guard, table_id, get_schema_func, table))) {
                      LOG_WARN("fail get table schema", K(table_id), K(ret));
                    } else {
                      const uint64_t auto_increment = table->get_auto_increment();
                      LOG_INFO("dump auto_increment", K(auto_increment), K(curr_value), K(sync_value));
                      if (auto_increment > curr_value) {
                        curr_value = auto_increment;
                      }
                      if (auto_increment != 0 && auto_increment - 1 > sync_value) {
                        sync_value = auto_increment - 1;
                      }
                    }
                  }
                }
              }
            }
            // compatible with MySQL
            // create table t1(c1 int key auto_increment) auto_increment = value_greater_than_max_value;
            // allocation at first time will fail
            if (curr_value > max_value) {
              ret = OB_ERR_REACH_AUTOINC_MAX;
            }
            if (OB_SUCC(ret)) {
              if (sync_value >= max_value) {
                curr_value = max_value;
              } else {
                curr_value = std::max(curr_value, sync_value + 1);
              }
              if (OB_FAIL(calc_next_value(curr_value, offset, increment, curr_new_value))) {
                LOG_WARN("fail to get next value.", K(ret));
              } else {
                bool reach_upper_limit = false;
                if (curr_new_value < max_value) {
                  // TODO overflow check
                  uint64_t batch_count = increment * desired_count;
                  // TODO sanity formula
                  int64_t auto_increment_cache_size = param.auto_increment_cache_size_;
                  if (auto_increment_cache_size < 1 || auto_increment_cache_size > MAX_INCREMENT_CACHE_SIZE) {
                    // no ret, just log error
                    LOG_ERROR("unexpected auto_increment_cache_size", K(auto_increment_cache_size));
                    auto_increment_cache_size = DEFAULT_INCREMENT_CACHE_SIZE;
                  }
                  uint64_t prefetch_count =
                      std::min(max_value / 100 / part_num, static_cast<uint64_t>(auto_increment_cache_size));
                  batch_count = prefetch_count > batch_count ? prefetch_count : batch_count;
                  next_value = curr_new_value + batch_count - 1;
                  if (next_value >= max_value || next_value < curr_new_value) {
                    next_value = max_value;
                    reach_upper_limit = true;
                  }
                } else {
                  reach_upper_limit = true;
                  curr_new_value = max_value;
                  next_value = max_value;
                }

                if (OB_SUCC(ret)) {
                  LOG_DEBUG("cache node upper limit", "upper_limit", next_value);
                  sequence_value = reach_upper_limit ? max_value : next_value + 1;
                  bool use_old_key = OB_INVALID_TENANT_ID != extract_tenant_id(fetch_table_id);
                  sql_len = snprintf(sql,
                      OB_MAX_SQL_LENGTH,
                      " UPDATE %s SET sequence_value = %lu, gmt_modified = now(6)"
                      " WHERE tenant_id = %lu AND sequence_key = %lu AND column_id = %lu",
                      table_name,
                      sequence_value,
                      use_old_key ? tenant_id : OB_INVALID_TENANT_ID,
                      use_old_key ? table_id : extract_pure_id(table_id),
                      column_id);
                  int64_t affected_rows = 0;
                  if (sql_len >= OB_MAX_SQL_LENGTH || sql_len <= 0) {
                    ret = OB_SIZE_OVERFLOW;
                    LOG_WARN("failed to format sql. size not enough");
                  } else if (GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != exec_tenant_id) {
                    ret = OB_OP_NOT_ALLOW;
                    LOG_WARN("can't write sys table now", K(ret), K(exec_tenant_id));
                  } else if (OB_FAIL(trans.write(exec_tenant_id, sql, affected_rows))) {
                    LOG_WARN("failed to write data", K(ret));
                  } else if (affected_rows != 1) {
                    LOG_WARN("failed to update sequence value", K(tenant_id), K(table_id), K(column_id), K(ret));
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

          if (OB_SUCC(ret)) {
            table_node->table_id_ = table_id;
            atomic_update(table_node->local_sync_, sync_value);
            atomic_update(table_node->last_refresh_ts_, ObTimeUtility::current_time());
            if (fetch_prefetch) {
              table_node->prefetch_node_.cache_start_ = curr_new_value;
              table_node->prefetch_node_.cache_end_ = next_value;
            } else {
              // there is no prefetch_node here
              // because we must have tried to allocate cache handle from curr_node and prefetch_node
              // before allocate new cache node
              // if we allocate new cache node, curr_node and prefetch_node should have been combined;
              // and prefetch_node should have been reset
              CacheNode new_node;
              new_node.cache_start_ = curr_new_value;
              new_node.cache_end_ = next_value;
              if (OB_FAIL(table_node->curr_node_.combine_cache_node(new_node))) {
                LOG_WARN("failed to combine cache node", K(*table_node), K(new_node), K(ret));
              } else if (0 == table_node->next_value_) {
                table_node->next_value_ = curr_new_value;
              }
            }
            LOG_INFO("succ to allocate new table node", K(*table_node), K(ret));
          }
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

int ObAutoincrementService::set_pre_op_timeout(common::ObTimeoutCtx& ctx)
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
  } else if (ctx.is_timeouted()) {
    ret = OB_TIMEOUT;
    LOG_WARN("is timeout",
        K(ret),
        "abs_timeout",
        ctx.get_abs_timeout(),
        "this worker timeout ts",
        THIS_WORKER.get_timeout_ts());
  }

  return ret;
}

int ObAutoincrementService::get_server_set(
    const uint64_t table_id, common::hash::ObHashSet<ObAddr>& server_set, const bool get_follower)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObPartitionLocation, 32> locations;
  const int64_t expire_renew_time = INT64_MAX;  // means must renew location
  bool is_cache_hit = false;
  sql::ObSqlPartitionLocationCache sql_partition_location_cache;
  ObSchemaGetterGuard schema_guard;
  uint64_t fetch_tenant_id = is_inner_table(table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(table_id);

  if (OB_ISNULL(schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(schema_service_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(fetch_tenant_id, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret));
  } else if (FALSE_IT(sql_partition_location_cache.init(location_cache_, my_addr_, &schema_guard))) {
    // do nothing
  } else if (OB_FAIL(sql_partition_location_cache.get(table_id, locations, expire_renew_time, is_cache_hit))) {
    LOG_WARN("failed to get all partitions' locations", K(table_id), K(ret));
  } else {
    LOG_TRACE("get partition locations", K(locations), K(ret));
    ObReplicaLocation location;
    for (int64_t i = 0; OB_SUCC(ret) && i < locations.count(); ++i) {
      if (!get_follower) {
        location.reset();
        if (OB_FAIL(locations.at(i).get_strong_leader(location))) {
          LOG_WARN("failed to get partition leader", "location", locations.at(i), K(ret));
        } else {
          int err = OB_SUCCESS;
          err = server_set.set_refactored(location.server_);
          if (OB_SUCCESS != err && OB_HASH_EXIST != err) {
            ret = err;
            LOG_WARN("failed to add element to set", "server", location.server_, K(ret));
          }
        }
      } else {
        const ObIArray<ObReplicaLocation>& replica_locations = locations.at(i).get_replica_locations();
        for (int j = 0; j < replica_locations.count(); ++j) {
          int err = OB_SUCCESS;
          err = server_set.set_refactored(replica_locations.at(j).server_);
          if (OB_SUCCESS != err && OB_HASH_EXIST != err) {
            ret = err;
            LOG_WARN("failed to add element to set", "server", replica_locations.at(j).server_, K(ret));
          }
        }
      }
    }
  }
  LOG_DEBUG("sync server_set", K(server_set), K(ret));
  return ret;
}

int ObAutoincrementService::sync_insert_value(
    AutoincParam& param, CacheHandle*& cache_handle, const uint64_t value_to_sync)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = param.tenant_id_;
  const uint64_t table_id = param.autoinc_table_id_;
  const uint64_t column_id = param.autoinc_col_id_;
  const ObObjType column_type = param.autoinc_col_type_;
  const uint64_t part_num = param.autoinc_table_part_num_;
  TableNode* table_node = NULL;
  LOG_DEBUG("begin to sync insert value globally", K(param), K(value_to_sync));
  if (OB_FAIL(get_table_node(param, table_node))) {
    LOG_WARN("table node should exist", K(param), K(ret));
  } else {
    uint64_t insert_value = value_to_sync;
    // compare insert_value with local_sync/global_sync
    if (insert_value <= ATOMIC_LOAD(&table_node->local_sync_)) {
      // do nothing
    } else {
      // get global sync
      uint64_t sync_value = 0;
      ObMySQLTransaction trans;
      ObSqlString sql;
      bool with_snap_shot = true;
      if (OB_FAIL(trans.start(mysql_proxy_, with_snap_shot))) {
        LOG_WARN("failed to start transaction", K(ret));
      } else {
        const uint64_t exec_tenant_id = tenant_id;
        const char* table_name = OB_ALL_SEQUENCE_V2_TNAME;
        int64_t fetch_table_id = OB_INVALID_ID;
        if (OB_FAIL(sql.assign_fmt(" SELECT sequence_key, sequence_value, sync_value FROM %s"
                                   " WHERE tenant_id = %lu AND sequence_key = %lu"
                                   " AND column_id = %lu"
                                   " FOR UPDATE",
                table_name,
                ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id),
                column_id))) {
          LOG_WARN("failed to assign sql", K(ret));
        }
        if (OB_SUCC(ret)) {
          SMART_VAR(ObMySQLProxy::MySQLResult, res)
          {
            ObMySQLResult* result = NULL;
            ObISQLClient* sql_client = &trans;
            uint64_t sequence_table_id = OB_ALL_SEQUENCE_V2_TID;
            ObSQLClientRetryWeak sql_client_retry_weak(sql_client, exec_tenant_id, sequence_table_id);
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
            } else if (OB_FAIL(result->get_uint(2l, sync_value))) {
              LOG_WARN("failed to get int_value.", K(ret));
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
          LOG_INFO("get global sync", K(sync_value), K(insert_value), K(ret));
          // if insert_value <= global_sync
          if (insert_value <= sync_value) {
            // do not need to sync other servers, update local sync
            atomic_update(table_node->local_sync_, sync_value);
            atomic_update(table_node->last_refresh_ts_, ObTimeUtility::current_time());
          } else {
            // if insert_value > global_sync
            // 1. mock a insert value large enough
            // 2. update __all_sequence(may get global_sync)
            // 3. sync to other server
            // 4. adjust cache_handle(prefetch) and table node

            // mock a insert value large enough
            const uint64_t max_value = get_max_value(column_type);
            int64_t auto_increment_cache_size = param.auto_increment_cache_size_;
            if (auto_increment_cache_size < 1 || auto_increment_cache_size > MAX_INCREMENT_CACHE_SIZE) {
              // no ret, just log error
              LOG_ERROR("unexpected auto_increment_cache_size", K(auto_increment_cache_size));
              auto_increment_cache_size = DEFAULT_INCREMENT_CACHE_SIZE;
            }
            uint64_t fetch_count =
                std::min(max_value / 100 / part_num, static_cast<uint64_t>(auto_increment_cache_size));
            const uint64_t insert_value_bak = insert_value;
            insert_value += fetch_count;
            // check overflow
            if (insert_value >= max_value || insert_value < insert_value_bak) {
              insert_value = max_value;
            }

            int64_t affected_rows = 0;
            bool use_old_key = OB_INVALID_TENANT_ID != extract_tenant_id(fetch_table_id);
            if (OB_FAIL(sql.assign_fmt("UPDATE %s SET sync_value = %lu, gmt_modified = now(6) "
                                       "WHERE tenant_id=%lu AND sequence_key=%lu AND column_id=%lu",
                    table_name,
                    insert_value,
                    use_old_key ? tenant_id : OB_INVALID_TENANT_ID,
                    use_old_key ? table_id : extract_pure_id(table_id),
                    column_id))) {
              LOG_WARN("failed to assign sql", K(ret));
            } else if (GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != exec_tenant_id) {
              ret = OB_OP_NOT_ALLOW;
              LOG_WARN("can't write sys table now", K(ret), K(exec_tenant_id));
            } else if (OB_FAIL((trans.write(exec_tenant_id, sql.ptr(), affected_rows)))) {
              LOG_WARN("failed to execute", K(sql), K(ret));
            } else if (!is_single_row(affected_rows)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected error", K(affected_rows), K(ret));
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

            if (OB_SUCC(ret)) {
              // sync observer where table partitions exist
              LOG_DEBUG("begin to sync other servers", K(param));
              ObAutoincSyncArg arg;
              arg.tenant_id_ = tenant_id;
              arg.table_id_ = table_id;
              arg.column_id_ = column_id;
              arg.sync_value_ = insert_value;
              ObHashSet<ObAddr> server_set;
              if (OB_FAIL(server_set.create(PARTITION_LOCATION_SET_BUCKET_NUM))) {
                LOG_WARN("failed to create hash set", K(param), K(ret));
              } else {
                if (OB_FAIL(get_server_set(table_id, server_set))) {
                  SHARE_LOG(WARN, "failed to get table partitions server set", K(ret));
                }

                if (OB_SUCC(ret)) {
                  int err = OB_SUCCESS;
                  err = server_set.erase_refactored(my_addr_);
                  if (OB_SUCCESS != err && OB_HASH_NOT_EXIST != err) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("failed to erase element in set", K_(my_addr), K(ret));
                  } else if (OB_HASH_NOT_EXIST == err) {
                    if (param.pkey_.is_valid()) {
                      // if this server addr is not in server set, partition leader has changed
                      // we think this write transaction should rollback
                      ret = OB_TRANS_NEED_ROLLBACK;
                      LOG_WARN("element dose not exist in set", K_(my_addr), K(ret));
                    } else {
                      // for global index
                      // autoincrement service can fetch sequence at every node, not only at partition leader
                      // so my_addr_ may not be in the server set
                      ret = OB_SUCCESS;
                    }
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
                    const int64_t sync_timeout = GCONF.autoinc_cache_refresh_interval + TIME_SKEW;
                    for (iter = server_set.begin(); OB_SUCC(ret) && iter != server_set.end(); ++iter) {
                      LOG_INFO("send rpc call to other observers", "server", iter->first);
                      sync_us = THIS_WORKER.get_timeout_remain();
                      if (sync_us > sync_timeout) {
                        sync_us = sync_timeout;
                      }
                      // TODO: send sync_value to other servers
                      if (OB_FAIL(srv_proxy_->to(iter->first).timeout(sync_us).refresh_sync_value(arg))) {
                        if (OB_TIMEOUT == ret) {
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
              }
            }
            // update local sync
            if (OB_SUCC(ret)) {
              atomic_update(table_node->local_sync_, insert_value);
              atomic_update(table_node->last_refresh_ts_, ObTimeUtility::current_time());
            }
          }
          // adjust cache_handle(prefetch) and table_node
          if (OB_SUCC(ret)) {
            if (NULL != cache_handle) {
              if (insert_value < cache_handle->prefetch_end_) {
                if (insert_value >= cache_handle->next_value_) {
                  if (OB_FAIL(calc_next_value(insert_value + 1,
                          param.autoinc_offset_,
                          param.autoinc_increment_,
                          cache_handle->next_value_))) {
                    LOG_WARN("failed to calc next value", K(cache_handle), K(param), K(ret));
                  }
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
        if (OB_SUCC(ret)) {
          // re-alloc table node
          lib::ObMutexGuard guard(table_node->alloc_mutex_);
          if (insert_value >= table_node->curr_node_.cache_end_ &&
              insert_value >= table_node->prefetch_node_.cache_end_) {
            table_node->curr_node_.reset();
            table_node->prefetch_node_.reset();
            if (OB_FAIL(fetch_table_node(param, table_node))) {
              LOG_WARN("failed to alloc table node", K(param), K(ret));
            }
          }
        }
      }
    }
  }
  // table node must be reverted after get to decrement reference count
  if (NULL != table_node) {
    node_map_.revert(table_node);
  }
  return ret;
}

// sync last user specified value first(compatible with MySQL)
int ObAutoincrementService::sync_insert_value_global(AutoincParam& param)
{
  int ret = OB_SUCCESS;
  if (0 != param.global_value_to_sync_) {
    if (OB_FAIL(sync_insert_value(param, param.cache_handle_, param.global_value_to_sync_))) {
      SQL_ENG_LOG(WARN, "failed to sync insert value", "insert_value", param.global_value_to_sync_, K(ret));
    } else {
      param.global_value_to_sync_ = 0;
    }
  }
  return ret;
}

// sync last user specified value in stmt
int ObAutoincrementService::sync_insert_value_local(AutoincParam& param)
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

int ObAutoincrementService::sync_auto_increment_all(
    const uint64_t tenant_id, const uint64_t table_id, const uint64_t column_id, const uint64_t sync_value)
{
  int ret = OB_SUCCESS;
  LOG_INFO("begin to sync auto_increment value", K(tenant_id), K(table_id));
  if (OB_SUCC(ret)) {
    // sync observer where table partitions exist
    ObAutoincSyncArg arg;
    arg.tenant_id_ = tenant_id;
    arg.table_id_ = table_id;
    arg.column_id_ = column_id;
    arg.sync_value_ = sync_value;
    ObHashSet<ObAddr> server_set;
    if (OB_FAIL(server_set.create(PARTITION_LOCATION_SET_BUCKET_NUM))) {
      LOG_WARN("failed to create hash set", K(ret));
    } else {
      if (OB_FAIL(get_server_set(table_id, server_set, true))) {
        SHARE_LOG(WARN, "failed to get table partitions server set", K(ret));
      }
      LOG_DEBUG("sync server_set", K(server_set), K(ret));
      if (OB_SUCC(ret)) {
        const int64_t sync_timeout = GCONF.autoinc_cache_refresh_interval + TIME_SKEW;
        ObHashSet<ObAddr>::iterator iter;
        for (iter = server_set.begin(); OB_SUCC(ret) && iter != server_set.end(); ++iter) {
          LOG_DEBUG("send rpc call to other observers", "server", iter->first);
          if (OB_FAIL(srv_proxy_->to(iter->first).timeout(sync_timeout).refresh_sync_value(arg))) {
            if (OB_TIMEOUT == ret) {
              // ignore time out, go on
              LOG_WARN("rpc call time out", "server", iter->first, K(ret));
              ret = OB_SUCCESS;
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

int ObAutoincrementService::sync_global_sync(
    const uint64_t tenant_id, const uint64_t table_id, const uint64_t column_id, const uint64_t sync_value)
{
  int ret = OB_SUCCESS;
  TableNode* table_node = NULL;
  // auto-increment key
  AutoincKey key;
  key.tenant_id_ = tenant_id;
  key.table_id_ = table_id;
  key.column_id_ = column_id;
  if (OB_ENTRY_NOT_EXIST == (ret = node_map_.get(key, table_node))) {
    LOG_INFO("there is no cache here", K(tenant_id), K(table_id), K(ret));
    ret = OB_SUCCESS;
  } else if (OB_SUCC(ret)) {
    lib::ObMutexGuard guard(table_node->sync_mutex_);
    if (OB_FAIL(do_global_sync(tenant_id, table_id, column_id, table_node, false, &sync_value))) {
      LOG_WARN("failed to do global sync", K(ret));
    }
  } else {
    LOG_WARN("failed to do get table node", K(ret));
  }
  // table node must be reverted after get to decrement reference count
  if (NULL != table_node) {
    node_map_.revert(table_node);
  }
  return ret;
}

int ObAutoincrementService::do_global_sync(const uint64_t tenant_id, const uint64_t table_id, const uint64_t column_id,
    TableNode* table_node, const bool sync_presync, const uint64_t* speci_sync_value)
{
  int ret = OB_SUCCESS;

  // set timeout for presync
  ObTimeoutCtx ctx;
  if (sync_presync) {
    if (OB_FAIL(set_pre_op_timeout(ctx))) {
      LOG_WARN("failed to set timeout", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    uint64_t sync_value = 0;
    if (speci_sync_value != NULL) {
      sync_value = *speci_sync_value;
      LOG_INFO("sync value is specified", K(sync_value));
    } else {
      ObMySQLTransaction trans;
      ObSqlString sql;
      bool with_snap_shot = true;
      if (OB_FAIL(trans.start(mysql_proxy_, with_snap_shot))) {
        LOG_WARN("failed to start transaction", K(ret));
      } else {
        SMART_VAR(ObMySQLProxy::MySQLResult, res)
        {
          ObMySQLResult* result = NULL;
          int sql_len = 0;
          SMART_VAR(char[OB_MAX_SQL_LENGTH], sql)
          {
            const uint64_t exec_tenant_id = tenant_id;
            const char* table_name = OB_ALL_SEQUENCE_V2_TNAME;
            sql_len = snprintf(sql,
                OB_MAX_SQL_LENGTH,
                " SELECT sync_value FROM %s"
                " WHERE tenant_id = %lu AND sequence_key = %lu AND column_id = %lu",
                table_name,
                ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id),
                column_id);
            ObISQLClient* sql_client = &trans;
            uint64_t sequence_table_id = OB_ALL_SEQUENCE_V2_TID;
            ObSQLClientRetryWeak sql_client_retry_weak(sql_client, exec_tenant_id, sequence_table_id);
            if (sql_len >= OB_MAX_SQL_LENGTH || sql_len <= 0) {
              ret = OB_SIZE_OVERFLOW;
              LOG_WARN("failed to format sql. size not enough");
            } else if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql))) {
              LOG_WARN(" failed to read data", K(ret));
            } else if (NULL == (result = res.get_result())) {
              LOG_WARN("failed to get result", K(ret));
              ret = OB_ERR_UNEXPECTED;
            } else if (OB_FAIL(result->next())) {
              LOG_WARN("failed to get next", K(ret));
              if (OB_ITER_END == ret) {
                // if sequence record deleted, clear observer's cache
                ObAutoincSyncArg arg;
                arg.tenant_id_ = tenant_id;
                arg.table_id_ = table_id;
                arg.column_id_ = column_id;
                if (OB_FAIL(clear_autoinc_cache(arg))) {
                  LOG_WARN("failed to clear auto-increment cache", K(arg), K(ret));
                } else {
                  // auto-increment column has been deleted
                  ret = OB_SCHEMA_ERROR;
                  LOG_WARN("auto-increment sequence does not exist", K(ret));
                }
              }
            } else {
              if (OB_FAIL(result->get_uint(0l, sync_value))) {
                LOG_WARN("fail to get int_value.", K(ret));
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
    if (OB_SUCC(ret)) {
      if (table_node->local_sync_ < sync_value) {
        table_node->local_sync_ = sync_value;
      }
      table_node->last_refresh_ts_ = ObTimeUtility::current_time();
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

int ObAutoincrementService::calc_next_value(
    const uint64_t last_next_value, const uint64_t offset, const uint64_t increment, uint64_t& new_next_value)
{
  int ret = OB_SUCCESS;
  if (increment <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("increment is invalid", K(ret), K(increment));
  } else {
    uint64_t real_offset = offset;

    if (real_offset > increment) {
      real_offset = 0;
    }
    // TODO check argument and overflow
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

int ObAutoincrementService::calc_prev_value(
    const uint64_t max_value, const uint64_t offset, const uint64_t increment, uint64_t& prev_value)
{
  int ret = OB_SUCCESS;
  if (max_value <= offset) {
    prev_value = max_value;
  } else {
    prev_value = ((max_value - offset) / increment) * increment + offset;
  }
  LOG_INFO("out of range for column. calc prev value", K(prev_value), K(max_value), K(offset), K(increment));
  return ret;
}

int ObAutoincrementService::get_sequence_value(
    const uint64_t tenant_id, const uint64_t table_id, const uint64_t column_id, uint64_t& seq_value)
{
  int ret = OB_SUCCESS;
  int sql_len = 0;
  SMART_VAR(char[OB_MAX_SQL_LENGTH], sql)
  {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      ObMySQLResult* result = NULL;
      const uint64_t exec_tenant_id = tenant_id;
      sql_len = snprintf(sql,
          OB_MAX_SQL_LENGTH,
          " SELECT sequence_value, sync_value FROM %s"
          " WHERE tenant_id = %lu AND sequence_key = %lu AND column_id = %lu",
          OB_ALL_SEQUENCE_V2_TNAME,
          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
          ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id),
          column_id);
      ObISQLClient* sql_client = mysql_proxy_;
      uint64_t sequence_table_id = OB_ALL_SEQUENCE_V2_TID;
      ObSQLClientRetryWeak sql_client_retry_weak(sql_client, exec_tenant_id, sequence_table_id);
      if (sql_len >= OB_MAX_SQL_LENGTH || sql_len <= 0) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("failed to format sql. size not enough");
      } else if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql))) {
        LOG_WARN(" failed to read data", K(ret));
      } else if (NULL == (result = res.get_result())) {
        LOG_WARN("failed to get result", K(ret));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
        LOG_INFO("there is no sequence record", K(ret));
        seq_value = 0;
        ret = OB_SUCCESS;
      } else if (OB_FAIL(result->get_uint(0l, seq_value))) {
        LOG_WARN("fail to get int_value.", K(ret));
      } else {
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
  return ret;
}

int ObAutoincrementService::get_sequence_value(
    const uint64_t tenant_id, const ObIArray<AutoincKey>& autoinc_keys, ObHashMap<AutoincKey, uint64_t>& seq_values)
{
  int ret = OB_SUCCESS;
  int64_t N = autoinc_keys.count() / FETCH_SEQ_NUM_ONCE;
  int64_t M = autoinc_keys.count() % FETCH_SEQ_NUM_ONCE;
  N += (M == 0) ? 0 : 1;
  LOG_INFO("fetch table option auto_increment", "batch_count", N);
  ObSqlString sql;
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    sql.reset();
    if (OB_FAIL(sql.assign_fmt(" SELECT sequence_key, column_id, sequence_value FROM %s"
                               " WHERE (sequence_key, column_id) IN (",
            OB_ALL_SEQUENCE_V2_TNAME))) {
      LOG_WARN("failed to append sql", K(ret));
    }

    // last iteration
    int64_t P = (0 != M && N - 1 == i) ? M : FETCH_SEQ_NUM_ONCE;
    for (int64_t j = 0; OB_SUCC(ret) && j < P; ++j) {
      AutoincKey key = autoinc_keys.at(i * FETCH_SEQ_NUM_ONCE + j);
      if (OB_FAIL(
              sql.append_fmt("%s(%lu, %lu)", (0 == j) ? "" : ", ", extract_pure_id(key.table_id_), key.column_id_))) {
        LOG_WARN("failed to append sql", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt(")"))) {
        LOG_WARN("failed to append sql", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        ObMySQLResult* result = NULL;
        int64_t table_id = 0;
        int64_t column_id = 0;
        uint64_t seq_value = 0;
        ObISQLClient* sql_client = mysql_proxy_;
        ObSQLClientRetryWeak sql_client_retry_weak(sql_client, tenant_id, OB_ALL_SEQUENCE_V2_TID);
        if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
          LOG_WARN(" failed to read data", K(ret));
        } else if (NULL == (result = res.get_result())) {
          LOG_WARN("failed to get result", K(ret));
          ret = OB_ERR_UNEXPECTED;
        } else {
          while (OB_SUCC(ret) && OB_SUCC(result->next())) {
            if (OB_FAIL(result->get_int(0l, table_id))) {
              LOG_WARN("fail to get int_value.", K(ret));
            } else if (OB_FAIL(result->get_int(1l, column_id))) {
              LOG_WARN("fail to get int_value.", K(ret));
            } else if (OB_FAIL(result->get_uint(2l, seq_value))) {
              LOG_WARN("fail to get int_value.", K(ret));
            } else {
              AutoincKey key;
              key.tenant_id_ = tenant_id;
              key.table_id_ = combine_id(tenant_id, table_id);
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

}  // end namespace share
}  // end namespace oceanbase
