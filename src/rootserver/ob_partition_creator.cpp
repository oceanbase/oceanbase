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

#define USING_LOG_PREFIX RS

#include "ob_root_utils.h"
#include "ob_partition_creator.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/worker.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_partition_table_proxy.h"
#include "share/config/ob_server_config.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_debug_sync.h"
#include "share/ob_cluster_version.h"
#include "share/config/ob_server_config.h"
#include "share/ob_partition_modify.h"

namespace oceanbase {
using namespace common;
using namespace share;

namespace rootserver {
int64_t ObPartitionCreator::flag_replica_batch_update_cnt = 256;

ObPartitionCreator::ObPartitionCreator(obrpc::ObSrvRpcProxy& rpc_proxy, ObPartitionTableOperator& pt_operator,
    ObServerManager* server_mgr, const bool binding, const bool ignore_member_list, common::ObMySQLProxy* sql_proxy)
    : proxy_(rpc_proxy, &obrpc::ObSrvRpcProxy::create_partition),
      pt_operator_(pt_operator),
      proxy_batch_(rpc_proxy, &obrpc::ObSrvRpcProxy::create_partition_batch),
      server_mgr_(server_mgr),
      split_info_(),
      create_mode_(obrpc::ObCreateTableMode::OB_CREATE_TABLE_MODE_LOOSE),
      binding_(binding),
      sql_proxy_(sql_proxy),
      set_member_list_proxy_(rpc_proxy, &obrpc::ObSrvRpcProxy::batch_set_member_list),
      ignore_member_list_(ignore_member_list),
      paxos_replica_num_(OB_INVALID_COUNT),
      member_lists_(),
      partition_limit_proxy_(rpc_proxy, &obrpc::ObSrvRpcProxy::reach_partition_limit)
{}

int ObPartitionCreator::add_flag_replica(const ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid() || !replica.is_flag_replica()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else if (!replicas_.empty() &&
             extract_tenant_id(replica.table_id_) != extract_tenant_id(replicas_.at(0).table_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("replica belong to different tenant",
        K(ret),
        "tenant_id",
        extract_tenant_id(replicas_.at(0).table_id_),
        K(replica));
  } else if (OB_FAIL(replicas_.push_back(replica))) {
    LOG_WARN("add replica failed", K(ret));
  }
  return ret;
}

int ObPartitionCreator::add_create_partition_arg(const ObAddr& dest, const obrpc::ObCreatePartitionArg& arg)
{
  int ret = OB_SUCCESS;
  if (!dest.is_valid() || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest), K(arg));
  } else if (OB_FAIL(dests_.push_back(dest))) {
    LOG_WARN("add address failed", K(ret));
  } else if (OB_FAIL(args_.push_back(arg))) {
    LOG_WARN("add create partition argument failed", K(ret));
  }
  return ret;
}

int ObPartitionCreator::execute()
{
  int ret = OB_SUCCESS;
  RS_TRACE(alloc_partition_begin);
  if (dests_.count() != args_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "dest count", dests_.count(), "arg count", args_.count());
  } else if (args_.empty()) {
    // do nothing for empty create partition request.
  } else {
    if (!replicas_.empty()) {
      if (OB_FAIL(update_flag_replica())) {
        LOG_WARN("update flag replicas failed", K(ret));
      }
      DEBUG_SYNC(AFTER_INSERT_FLAG_REPLICA);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(create_partitions())) {
        LOG_WARN("create partition failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_CREATE_TENANT_END_PERSIST_MEMBER_LIST) OB_SUCCESS;
  }
  if (OB_FAIL(ret)) {
  } else if (!ignore_member_list_) {
  } else if (OB_FAIL(send_set_member_list_rpc())) {
    LOG_WARN("failed to set member list", K(ret));
  }
  RS_TRACE(alloc_partition_end);
  return ret;
}

int ObPartitionCreator::execute(const ObSplitInfo& split_info)
{
  int ret = OB_SUCCESS;
  RS_TRACE(alloc_partition_begin);
  if (dests_.count() != args_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "dest count", dests_.count(), "arg count", args_.count());
  } else if (args_.empty()) {
    // do nothing for empty create partition request.
  } else if (OB_FAIL(split_info_.assign(split_info))) {
    LOG_WARN("fail to assign", K(ret));
  } else {
    if (!replicas_.empty()) {
      if (OB_FAIL(update_flag_replica())) {
        LOG_WARN("update flag replicas failed", K(ret));
      }
      DEBUG_SYNC(AFTER_INSERT_FLAG_REPLICA);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(create_partitions())) {
        LOG_WARN("create partition failed", K(ret));
      }
    }
  }
  RS_TRACE(alloc_partition_end);
  return ret;
}

int ObPartitionCreator::update_flag_replica()
{
  int ret = OB_SUCCESS;
  if (replicas_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no replica", K(ret));
  } else {
    LOG_INFO("start insert flag replicas", "replica_cnt", replicas_.count());
    // To simplify implement only all replicas belong to __all_root_table
    // or all belong to one tenant's __all_tenant_meta_table can be batch updated.
    // TODO:  batch_update different pt
    bool can_batch_update = flag_replica_batch_update_cnt > 1;
    const uint64_t pt_tid = ObIPartitionTable::get_partition_table_id(replicas_.at(0).table_id_);
    const uint64_t tenant_id = extract_tenant_id(replicas_.at(0).table_id_);
    if (OB_ALL_ROOT_TABLE_TID != pt_tid && OB_ALL_META_TABLE_TID != pt_tid && OB_ALL_TENANT_META_TABLE_TID != pt_tid) {
      can_batch_update = false;
    } else {
      FOREACH_CNT_X(r, replicas_, can_batch_update)
      {
        if (pt_tid != ObIPartitionTable::get_partition_table_id(r->table_id_)) {
          can_batch_update = false;
        } else if (OB_ALL_TENANT_META_TABLE_TID == extract_pure_id(pt_tid) &&
                   tenant_id != extract_tenant_id(r->table_id_)) {
          can_batch_update = false;
        }
      }
    }

    if (can_batch_update) {
      if (OB_FAIL(batch_update_flag_replica())) {
        LOG_WARN("batch update flag replica failed", K(ret));
      }
    } else {
      RS_TRACE(update_flag_replica_begin);
      FOREACH_CNT_X(r, replicas_, OB_SUCC(ret))
      {
        if (OB_FAIL(pt_operator_.update(*r))) {
          LOG_WARN("update partition failed", K(ret), "replica", *r);
        }
      }
      RS_TRACE(update_flag_replica_end);
    }
    LOG_INFO("end insert flag replicas", K(ret), "replica_cnt", replicas_.count(), K(can_batch_update));
  }
  return ret;
}

int ObPartitionCreator::batch_update_flag_replica()
{
  int ret = OB_SUCCESS;
  RS_TRACE(update_flag_replica_begin);
  if (replicas_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no replica", K(ret));
  } else if (OB_ALL_ROOT_TABLE_TID != ObIPartitionTable::get_partition_table_id(replicas_.at(0).table_id_) &&
             OB_ALL_META_TABLE_TID != ObIPartitionTable::get_partition_table_id(replicas_.at(0).table_id_) &&
             OB_ALL_TENANT_META_TABLE_TID != ObIPartitionTable::get_partition_table_id(replicas_.at(0).table_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("replica not belong to __all_root_table or __all_tenant_meta_table",
        K(ret),
        "partition_table_id",
        ObIPartitionTable::get_partition_table_id(replicas_.at(0).table_id_));
  } else {
    int64_t begin = 0;
    int64_t timeout_us = OB_INVALID_TIMESTAMP;
    for (int64_t i = 0; i < replicas_.count() && OB_SUCC(ret); ++i) {
      timeout_us = THIS_WORKER.get_timeout_remain();
      if (timeout_us <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("timeouted", K(ret), K(timeout_us));
      } else if (replicas_.count() == (i + 1) || i + 1 - begin >= flag_replica_batch_update_cnt) {
        if (OB_FAIL(batch_update_flag_replica(begin, i + 1))) {
          LOG_WARN("batch update flag replica failed", K(ret), "batch count", i + 1 - begin);
        } else {
          begin = i + 1;
        }
      }
    }
  }
  RS_TRACE(update_flag_replica_end);
  return ret;
}

int ObPartitionCreator::batch_update_flag_replica(const int64_t begin, const int64_t end)
{
  int ret = OB_SUCCESS;
  RS_TRACE(update_flag_replica_begin);
  const char* partition_table_name = NULL;
  uint64_t sql_tenant_id = OB_INVALID_TENANT_ID;
  ObISQLClient* sql_proxy = pt_operator_.get_persistent_table().get_sql_proxy();
  if (begin < 0 || end <= begin || end > replicas_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(begin), K(end), "replica count", replicas_.count());
  } else if (NULL == sql_proxy) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get NULL sql proxy", K(ret));
  } else if (OB_FAIL(ObIPartitionTable::get_partition_table_name(
                 replicas_.at(begin).table_id_, partition_table_name, sql_tenant_id))) {
    LOG_WARN("get partition table name failed", K(ret), "table_id", replicas_.at(begin).table_id_);
  } else if (NULL == partition_table_name) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL partition table name", K(ret));
  } else {
    ObSqlString sql;
    ObSqlString values;
    ObDMLSqlSplicer dml;
    // The upper layer ensures that all replicas that can be executed in batch belong to the same Pt
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
      dml.reuse();
      if (OB_FAIL(ObPartitionTableProxy::fill_dml_splicer(replicas_.at(i), dml))) {
        LOG_WARN("fill replica column failed", K(ret));
      } else {
        if (begin == i) {
          if (OB_FAIL(dml.splice_column_names(values))) {
            LOG_WARN("splice replica names failed", K(ret));
          } else if (ignore_member_list_) {
            if (OB_FAIL(sql.assign_fmt(
                    "REPLACE /*+ use_plan_cache(none) */ INTO %s (%s) VALUES", partition_table_name, values.ptr()))) {
              LOG_WARN("assign sql string failed", K(ret));
            }
          } else if (OB_FAIL(sql.assign_fmt("INSERT /*+ use_plan_cache(none) */ INTO %s (%s) VALUES",
                         partition_table_name,
                         values.ptr()))) {
            LOG_WARN("assign sql string failed", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        values.reset();
        if (OB_FAIL(dml.splice_values(values))) {
          LOG_WARN("splice replica values failed", K(ret));
        } else if (OB_FAIL(sql.append_fmt("%s(%s)", (begin == i ? " " : ", "), values.ptr()))) {
          LOG_WARN("append sql string failed", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t replica_cnt = end - begin;
      int64_t affected_rows = 0;
      if (OB_FAIL(sql_proxy->write(sql_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql), K(sql_tenant_id));
      } else if (ignore_member_list_) {
        if (replica_cnt > affected_rows && replica_cnt * 2 < affected_rows) {
          // use replace command, so affected_row != replica_cnt
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows), K(replica_cnt));
        }
      } else if (replica_cnt != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows), K(replica_cnt));
      }
    }
  }
  RS_TRACE(update_flag_replica_end);
  return ret;
}

int ObPartitionCreator::init_batch_args(DestArgMap& dest_arg_map, common::PageArena<>& allocator)
{
  int ret = OB_SUCCESS;
  RS_TRACE(init_batch_args_begin);
  if (dests_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no server", K(ret));
  } else {
    if (false == dest_arg_map.created()) {
      if (OB_FAIL(dest_arg_map.create(HASHMAP_SERVER_CNT, ObModIds::OB_SERVER_ARPCARGS_MAP))) {
        LOG_WARN("create dest_arg_map failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      obrpc::ObCreatePartitionBatchArg* barg = NULL;
      FOREACH_CNT_X(s, dests_, OB_SUCC(ret))
      {
        if (OB_FAIL(dest_arg_map.get_refactored(*s, barg)) && OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("get batch arg from dest_arg_map failed", K(ret));
        } else if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          char* buf = allocator.alloc(sizeof(obrpc::ObCreatePartitionBatchArg));
          if (OB_UNLIKELY(NULL == buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("failed to alloc batch arg", K(ret));
          } else {
            obrpc::ObCreatePartitionBatchArg* tmp_arg = NULL;
            tmp_arg = new (buf) obrpc::ObCreatePartitionBatchArg();
            if (OB_FAIL(dest_arg_map.set_refactored(*s, tmp_arg))) {
              LOG_WARN("insert into hashmap failed", K(ret));
              tmp_arg->~ObCreatePartitionBatchArg();
            }
          }
        }
      }
    }
  }
  RS_TRACE(init_batch_args_end);
  return ret;
}

int ObPartitionCreator::do_check_loose_mode_reach_create_partition_limit(
    const DestCountMap& dest_count_map, const common::ObIArray<int>& ret_array)
{
  int ret = OB_SUCCESS;
  DestRetMap dest_ret_map;
  hash::ObHashMap<ObPartitionKey, int64_t> failed_partition_map;
  hash::ObHashMap<ObPartitionKey, int> check_full_replica_map;
  if (binding_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not be here", K(ret));
  } else if (args_.count() <= 0) {
    // bypass
  } else if (ret_array.count() != dest_count_map.size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count not match", "left_cnt", ret_array.count(), "right_cnt", dest_count_map.size());
  } else if (OB_FAIL(dest_ret_map.create(HASHMAP_SERVER_CNT, ObModIds::OB_SERVER_ARPCARGS_MAP))) {
    LOG_WARN("fail to create dest ret map", K(ret));
  } else if (OB_FAIL(failed_partition_map.create(2 * args_.count(), ObModIds::OB_CREATE_PARTITION_FAILED_LIST))) {
    LOG_WARN("failed to create hash set", K(ret));
  } else if (OB_FAIL(check_full_replica_map.create(
                 2 * args_.count(), ObModIds::OB_CREATE_PARTITION_CHECK_FULL_REPLICA_LIST))) {
    LOG_WARN("failed to create hash set", K(ret));
  } else {
    int64_t index = 0;
    for (auto p = dest_count_map.begin(); OB_SUCC(ret) && p != dest_count_map.end(); ++p) {
      const int32_t overwrite = 0;
      if (OB_UNLIKELY(!p->first.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server addr unexpected", K(ret), "server", p->first);
      } else if (index >= ret_array.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index ret array unexpected", K(ret));
      } else if (OB_FAIL(dest_ret_map.set_refactored(p->first, ret_array.at(index), overwrite))) {
        LOG_WARN("fail to set refactored", K(ret));
      } else {
        ++index;
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < args_.count(); ++j) {
      const ObReplicaType replica_type = args_.at(j).replica_type_;
      int64_t replica_num = args_.at(j).replica_num_;
      const common::ObAddr& dest = dests_.at(j);
      int this_ret = OB_SUCCESS;
      int64_t member_list_count = -1;
      if (OB_FAIL(get_member_list_count(args_.at(j), member_list_count))) {
        LOG_WARN("failed to get member list count", K(ret), K(j));
      } else if (member_list_count < replica_num / 2 + 1) {
        ret = OB_REPLICA_NUM_NOT_ENOUGH;
        LOG_WARN("alloc replica num not enough", K(ret), "arg", args_.at(j), K(member_list_count));
      } else if (OB_FAIL(dest_ret_map.get_refactored(dest, this_ret))) {
        LOG_WARN("fail to get from map", K(ret));
      } else if (OB_SUCCESS != this_ret) {
        if (OB_FAIL(check_majority(args_.at(j).partition_key_,
                this_ret,
                failed_partition_map,
                check_full_replica_map,
                replica_num,
                member_list_count,
                replica_type))) {
          LOG_WARN("failed to create partition", K(ret), "partition key", args_.at(j));
        }
      }
    }
  }
  return ret;
}

int ObPartitionCreator::try_check_reach_server_partition_limit()
{
  int ret = OB_SUCCESS;
  DestCountMap dest_count_map;
  if (binding_) {
    // no need to check with binding true
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2220) {
    // no need to check in upgrading
  } else if (OB_FAIL(dest_count_map.create(HASHMAP_SERVER_CNT, ObModIds::OB_SERVER_ARPCARGS_MAP))) {
    LOG_WARN("fail to create des count map", K(ret));
  } else {
    partition_limit_proxy_.reuse();
    uint64_t tenant_id = OB_INVALID_ID;
    bool is_pg_arg = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < dests_.count(); ++i) {
      int64_t count = 0;
      int tmp_ret = dest_count_map.get_refactored(dests_.at(i), count);
      if (OB_SUCCESS == tmp_ret) {
        const int32_t overwrite = 1;
        if (OB_FAIL(dest_count_map.set_refactored(dests_.at(i), count + 1, overwrite))) {
          LOG_WARN("fail to set refactored", K(ret));
        }
      } else if (OB_HASH_NOT_EXIST == tmp_ret) {
        count = 1;
        if (OB_FAIL(dest_count_map.set_refactored(dests_.at(i), count))) {
          LOG_WARN("fail to set refactored", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get from dest count map", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (0 == i) {
          tenant_id = args_.at(i).partition_key_.get_tenant_id();
          is_pg_arg = args_.at(i).is_create_pg();
        } else {
          if (tenant_id != args_.at(i).partition_key_.get_tenant_id() || is_pg_arg != args_.at(i).is_create_pg()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("info not match",
                K(ret),
                "left_tenant_id",
                tenant_id,
                "right_tenant_id",
                args_.at(i).partition_key_.get_tenant_id(),
                "left_is_pg",
                is_pg_arg,
                "right_is_pg",
                args_.at(i).is_create_pg());
          }
        }
      }
    }
    int64_t rpc_cnt = 0;
    ObArray<int> tmp_ret_array;
    ObArray<int> ret_array;
    for (auto p = dest_count_map.begin(); OB_SUCC(ret) && p != dest_count_map.end(); ++p) {
      if (OB_UNLIKELY(!p->first.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server addr unexpected", K(ret), "server", p->first);
      } else if (rpc_cnt >= MAX_PARTITION_LIMIT_RPC_CNT) {
        if (obrpc::OB_CREATE_TABLE_MODE_LOOSE == create_mode_) {
          if (OB_FAIL(partition_limit_proxy_.wait_all(tmp_ret_array))) {
            LOG_WARN("fail to execute rpc", K(ret));
          } else if (OB_FAIL(append(ret_array, tmp_ret_array))) {
            LOG_WARN("fail to append to ret array", K(ret));
          } else {
            partition_limit_proxy_.reuse();
            tmp_ret_array.reset();
            rpc_cnt = 0;
          }
        } else if (obrpc::OB_CREATE_TABLE_MODE_STRICT == create_mode_) {
          if (OB_FAIL(partition_limit_proxy_.wait())) {
            LOG_WARN("fail to execute rpc", K(ret));
          } else {
            partition_limit_proxy_.reuse();
            rpc_cnt = 0;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected create table mode", K(ret), K(create_mode_));
        }
      }
      if (OB_SUCC(ret)) {
        obrpc::ObReachPartitionLimitArg arg(p->second, tenant_id, is_pg_arg);
        const int64_t timeout_us = THIS_WORKER.get_timeout_remain();
        if (OB_FAIL(partition_limit_proxy_.call(p->first, timeout_us, arg))) {
          LOG_WARN("fail to call rpc", K(ret), K(timeout_us), K(arg), "dst", p->second);
        } else {
          ++rpc_cnt;
        }
      }
    }
    if (obrpc::OB_CREATE_TABLE_MODE_LOOSE == create_mode_) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = partition_limit_proxy_.wait_all(tmp_ret_array))) {
        LOG_WARN("fail to execute rpc", K(tmp_ret));
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      } else if (OB_SUCCESS != ret) {
        // It has failed. It doesn't need to be executed in the future
      } else if (OB_FAIL(append(ret_array, tmp_ret_array))) {
        LOG_WARN("fail to append", K(ret));
      } else if (OB_FAIL(do_check_loose_mode_reach_create_partition_limit(dest_count_map, ret_array))) {
        LOG_WARN("fail to do check loose mode reach create partition limit", K(ret));
      }
    } else if (obrpc::OB_CREATE_TABLE_MODE_STRICT == create_mode_) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = partition_limit_proxy_.wait())) {
        LOG_WARN("fail to execute rpc", K(tmp_ret));
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObPartitionCreator::init_prev_not_processed_idx_array(common::ObIArray<int64_t>& prev_not_processed_idx_array)
{
  int ret = OB_SUCCESS;
  prev_not_processed_idx_array.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < dests_.count(); ++i) {
    if (OB_FAIL(prev_not_processed_idx_array.push_back(i))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  return ret;
}

int ObPartitionCreator::get_whole_partition_idx_scope(
    const common::ObIArray<int64_t>& not_processed_idx_array, const int64_t p_start, int64_t& p_end)
{
  int ret = OB_SUCCESS;
  int64_t start_idx = -1;
  if (p_start >= not_processed_idx_array.count() || p_start < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(p_start), "array_cnt", not_processed_idx_array.count());
  } else if (FALSE_IT(start_idx = not_processed_idx_array.at(p_start))) {
    // will never be here
  } else if (start_idx >= args_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start idx unexpected", K(ret), K(start_idx), "array_cnt", args_.count());
  } else {
    p_end = not_processed_idx_array.count();
    const common::ObPartitionKey& sample_pkey = args_.at(start_idx).partition_key_;
    for (int64_t i = p_start + 1; OB_SUCC(ret) && i < not_processed_idx_array.count(); ++i) {
      const int64_t this_idx = not_processed_idx_array.at(i);
      common::ObPartitionKey this_pkey;
      if (this_idx >= args_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this_idx unexpected", K(ret), K(this_idx), "array_cnt", args_.count());
      } else if (FALSE_IT(this_pkey = args_.at(this_idx).partition_key_)) {
        // shall never be here
      } else if (this_pkey == sample_pkey) {
        // still the same
      } else {
        p_end = i;
        break;
      }
    }
  }
  return ret;
}

int ObPartitionCreator::check_whole_partition_dest_rpc_capacity(
    const common::ObIArray<int64_t>& not_processed_idx_array, const int64_t p_start, const int64_t p_end,
    DestArgMap& dest_arg_map, bool& in_tolerance)
{
  int ret = OB_SUCCESS;
  if (p_start >= p_end || p_start < 0 || p_end < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(p_start), K(p_end));
  } else if (p_start >= not_processed_idx_array.count() || p_end > not_processed_idx_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(p_start), K(p_end));
  } else {
    in_tolerance = true;
    for (int64_t i = p_start; in_tolerance && OB_SUCC(ret) && i < p_end; ++i) {
      const int64_t this_idx = not_processed_idx_array.at(i);
      if (this_idx >= dests_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("idx unexpected", K(ret), K(this_idx), "array_cnt", dests_.count());
      } else {
        obrpc::ObCreatePartitionBatchArg* barg = nullptr;
        ret = dest_arg_map.get_refactored(dests_.at(this_idx), barg);
        if (OB_FAIL(ret) || nullptr == barg) {
          ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
          LOG_WARN("fail to get dest from map", K(ret));
        } else if (barg->args_.count() >= MAX_PARTITION_LIMIT_RPC_CNT) {
          in_tolerance = false;
        } else {
          // go on check next partition
        }
      }
    }
  }
  return ret;
}

int ObPartitionCreator::append_not_processed_idx_array(const common::ObIArray<int64_t>& prev_not_processed_idx_array,
    common::ObIArray<int64_t>& not_processed_idx_array, const int64_t p_start, const int64_t p_end)
{
  int ret = OB_SUCCESS;
  if (p_start >= p_end || p_start < 0 || p_end < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(p_start), K(p_end));
  } else if (p_start >= prev_not_processed_idx_array.count() || p_end > prev_not_processed_idx_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(p_start), K(p_end));
  } else {
    for (int64_t i = p_start; OB_SUCC(ret) && i < p_end; ++i) {
      const int64_t idx = prev_not_processed_idx_array.at(i);
      if (OB_FAIL(not_processed_idx_array.push_back(idx))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionCreator::create_partitions()
{
  int ret = OB_SUCCESS;
  DestArgMap dest_arg_map;
  common::PageArena<> allocator;
  common::ObArray<int64_t> prev_not_processed_idx_array;
  common::ObArray<int64_t> this_not_processed_idx_array;
  if (args_.empty() || args_.count() != dests_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "dest count", dests_.count(), "arg count", args_.count());
  } else if (OB_FAIL(init_batch_args(dest_arg_map, allocator))) {
    LOG_WARN("init dests to args hashmap failed", K(ret));
  } else if (OB_FAIL(init_prev_not_processed_idx_array(prev_not_processed_idx_array))) {
    LOG_WARN("fail to init prev not processed idx array", K(ret));
  } else if (OB_FAIL(try_check_reach_server_partition_limit())) {
    LOG_WARN("fail to try check reach server partition limit", K(ret));
  } else {
    RS_TRACE(for_each_dest_begin);
    while (OB_SUCC(ret) && prev_not_processed_idx_array.count() > 0) {
      this_not_processed_idx_array.reuse();
      int64_t i = 0;
      int64_t replica_cnt = 0;
      while (OB_SUCC(ret) && i < prev_not_processed_idx_array.count()) {
        const int64_t p_start = i;
        int64_t p_end = -1;
        bool in_tolerance = true;
        if (OB_FAIL(get_whole_partition_idx_scope(prev_not_processed_idx_array, p_start, p_end))) {
          LOG_WARN("fail to get whole partition idx scope", K(ret));
        } else if (OB_FAIL(check_whole_partition_dest_rpc_capacity(
                       prev_not_processed_idx_array, p_start, p_end, dest_arg_map, in_tolerance))) {
          LOG_WARN("fail to check whole partition dest rpc capaclity", K(ret));
        } else if (!in_tolerance) {
          if (OB_FAIL(append_not_processed_idx_array(
                  prev_not_processed_idx_array, this_not_processed_idx_array, p_start, p_end))) {
            LOG_WARN("fail to append not processed idx", K(ret));
          }
        } else {
          const int64_t lease_start = ObTimeUtility::current_time();
          replica_cnt += (p_end - p_start);
          for (int64_t j = p_start; OB_SUCC(ret) && j < p_end; ++j) {
            obrpc::ObCreatePartitionBatchArg* barg = nullptr;
            const int64_t idx = prev_not_processed_idx_array.at(j);
            if (idx >= dests_.count()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("idx unexpected", K(idx), "array_cnt", dests_.count());
            } else if (FALSE_IT(ret = dest_arg_map.get_refactored(dests_.at(idx), barg))) {
              // will never be here
            } else if (OB_FAIL(ret) || nullptr == barg) {
              ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
              LOG_WARN("fail to get dest from map", K(ret));
            } else if (FALSE_IT(args_.at(idx).lease_start_ = lease_start)) {
              // will never be here
            } else if (OB_FAIL(barg->args_.push_back(args_.at(idx)))) {
              LOG_WARN("fail to push back", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          i = p_end;
        }
      }
      if (OB_SUCC(ret)) {
        prev_not_processed_idx_array.reuse();
        if (OB_FAIL(prev_not_processed_idx_array.assign(this_not_processed_idx_array))) {
          LOG_WARN("fail to assign", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(batch_create_partitions(dest_arg_map, replica_cnt))) {
        LOG_WARN("fail to batch create partitions", K(ret));
      } else {
        FOREACH(iter, dest_arg_map)
        {
          if (nullptr != iter->second) {
            iter->second->reset();
          }
        }
      }
    }
    RS_TRACE(for_each_dest_end);
  }
  if (OB_FAIL(ret)) {
  } else if (ignore_member_list_) {
    // persist member_list to __all_partition_member_list
    if (OB_FAIL(batch_persist_member_list(args_))) {
      LOG_WARN("failed to persist member_list", K(ret));
    }
  }
  FOREACH(iter, dest_arg_map)
  {
    if (NULL != iter->second) {
      iter->second->~ObCreatePartitionBatchArg();
    }
  }
  dest_arg_map.destroy();
  allocator.free();
  return ret;
}

int ObPartitionCreator::batch_create_partitions(DestArgMap& dest_arg_map, const int64_t replica_cnt)
{
  int ret = OB_SUCCESS;
  RS_TRACE(batch_create_partition_begin);
  LOG_INFO("start batch create partition", "partition_count", replica_cnt);
  ObArray<int> return_code_array;
  obrpc::ObCreatePartitionBatchArg arg;
  common::ObArray<common::ObAddr> binding_inactive_servers;
  common::ObArray<obrpc::ObCreatePartitionBatchArg> binding_inactive_args;
  common::ObArray<obrpc::ObCreatePartitionBatchArg*> inactive_args;

  proxy_batch_.reuse();
  const int64_t log_sync_time = 10 * 1000 * 3;  // 3 times of sata disk sync time
  const int64_t log_sync_count = 3;             //  1 slog + 2 clog
  int64_t max_timeout_us = GCONF.rpc_timeout + replica_cnt * (log_sync_time * log_sync_count);
  // update partition table one by one, count update partition table time.
  const int64_t update_partition_table_time = 200 * 1000;  // 200ms
  max_timeout_us += update_partition_table_time * replica_cnt;
  int64_t timeout_us = std::min(THIS_WORKER.get_timeout_remain(), max_timeout_us);
  if (timeout_us <= 0) {
    ret = OB_TIMEOUT;
    LOG_WARN("timeouted", K(ret), K(timeout_us));
  }
  int64_t rpc_cnt = 0;
  FOREACH_X(p, dest_arg_map, OB_SUCC(ret))
  {
    if (NULL == p->second) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get batch args from dest_arg_map", K(ret), "dest", p->first);
    } else if (0 < p->second->args_.count()) {
      bool is_active = false;
      if (OB_ISNULL(server_mgr_)) {
        is_active = true;
      } else if (OB_FAIL(server_mgr_->check_server_alive(p->first, is_active))) {
        LOG_WARN("check_server_alive failed", K(ret), "server", p->first);
      }
      if (OB_FAIL(ret)) {
        // failed
      } else if (!is_active && binding_) {
        // binding, and not active
        if (OB_FAIL(binding_inactive_servers.push_back(p->first))) {
          LOG_WARN("fail to push back", K(ret));
        } else if (binding_inactive_args.push_back(*(p->second))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else if (!is_active) {
        // not binding and not acitve
        // If create partition in server A and B, cause A is inactive, it will not send RPC to A.
        // If A is not record, successful creating of B will leader to minority.
        // So we need to check the majority with inactive A
        LOG_INFO("server is not active", KR(ret), "server", p->first, "partition_count", p->second->args_.count());
        if (OB_FAIL(inactive_args.push_back(p->second))) {
          LOG_WARN("failed to push back", KR(ret), "server", p->first);
        }
      } else {
        // binding or not binding and active
        // split to SERVER_CONCURRENCY
        STATIC_ASSERT(SERVER_CONCURRENCY > 0 && CREATE_PARTITION_BATCH_CNT > SERVER_CONCURRENCY,
            "invalid SERVER_CONCURRENCY or CREATE_PARTITION_BATCH_CNT");
        const int64_t reserve_cnt = std::min(CREATE_PARTITION_BATCH_CNT / SERVER_CONCURRENCY, p->second->args_.count());
        if (OB_FAIL(arg.args_.reserve(reserve_cnt))) {
          LOG_WARN("reserve array failed", K(ret), K(reserve_cnt));
        } else {
          arg.reuse();
          for (int64_t i = 0; OB_SUCC(ret) && i < p->second->args_.count(); ++i) {
            timeout_us = std::min(THIS_WORKER.get_timeout_remain(), max_timeout_us);
            if (timeout_us <= 0) {
              ret = OB_TIMEOUT;
              LOG_WARN("timeouted", K(ret), K(timeout_us));
            } else if (OB_FAIL(arg.args_.push_back(p->second->args_.at(i)))) {
              LOG_WARN("array push back failed", K(ret));
            } else if (i + 1 == p->second->args_.count() ||
                       arg.args_.count() == CREATE_PARTITION_BATCH_CNT / SERVER_CONCURRENCY) {
              LOG_INFO("start to send rpc for batch_create_partition",
                  K(timeout_us),
                  "dest",
                  p->first,
                  "count",
                  arg.args_.count());
              if (NULL != server_mgr_ && OB_FAIL(server_mgr_->set_with_partition(p->first))) {
                LOG_WARN("set server with partition failed", "server", p->first, K(ret));
              } else if (OB_FAIL(proxy_batch_.call(p->first, timeout_us, arg))) {
                LOG_WARN("call async batch rpc failed", K(ret), K(timeout_us), K(arg));
              } else {
                arg.reuse();
                ++rpc_cnt;
              }
            }
          }
        }
      }
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = proxy_batch_.wait_all(return_code_array))) {
    LOG_WARN("wait batch result failed", K(ret), K(tmp_ret));
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
  }
  if (OB_SUCC(ret)) {
    if (return_code_array.count() != rpc_cnt || proxy_batch_.get_args().count() != rpc_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected batch return code count",
          K(ret),
          "return code count",
          return_code_array.count(),
          "rpc count",
          rpc_cnt,
          "arg count",
          proxy_batch_.get_args().count());
    }
  }

  if (OB_FAIL(ret)) {
    // skip
  } else if (binding_) {
    if (OB_FAIL(check_binding_partition_execution_result(
            proxy_batch_, return_code_array, binding_inactive_servers, binding_inactive_args))) {
      LOG_WARN("fail to check binding partition execution result", K(ret));
    }
  } else {
    if (obrpc::ObCreateTableMode::OB_CREATE_TABLE_MODE_STRICT == create_mode_) {
      if (OB_FAIL(check_partition_in_strict_create_mode(replica_cnt, proxy_batch_, return_code_array))) {
        LOG_WARN("fail to check partition in strict create mode", K(ret));
      }
    } else {
      if (OB_FAIL(
              check_partition_in_non_strict_create_mode(replica_cnt, proxy_batch_, return_code_array, inactive_args))) {
        LOG_WARN("fail to check partition in non-strict create mode", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ret = E(EventTable::EN_CREATE_TENANT_BEFORE_PERSIST_MEMBER_LIST) OB_SUCCESS;
    }
  }

  LOG_INFO("end batch create partition", K(ret), "partition count", replica_cnt);
  RS_TRACE(batch_create_partition_end);
  return ret;
}

int ObPartitionCreator::check_binding_partition_execution_result(ObCreatePartitionBatchProxy& proxy_batch,
    common::ObIArray<int>& return_code_array, common::ObIArray<common::ObAddr>& inactive_servers,
    common::ObIArray<obrpc::ObCreatePartitionBatchArg>& inactive_args)
{
  int ret = OB_SUCCESS;
  const int64_t log_sync_time = 10 * 1000 * 3;  // 3 times of sata disk sync time
  const int64_t log_sync_count = 3;             //  1 slog + 2 clog
  common::ObArray<obrpc::ObCreatePartitionBatchArg> new_arg_array;
  common::ObArray<common::ObAddr> new_dest_array;
  while (OB_SUCC(ret) && THIS_WORKER.get_timeout_remain() > 0) {
    new_arg_array.reset();
    new_dest_array.reset();
    if (OB_FAIL(new_dest_array.assign(inactive_servers))) {
      LOG_WARN("fail to assign new dest array", K(ret));
    } else if (OB_FAIL(new_arg_array.assign(inactive_args))) {
      LOG_WARN("fail to assign new arg array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
        const int ret1 = return_code_array.at(i);
        const obrpc::ObCreatePartitionBatchRes* result = proxy_batch.get_results().at(i);
        const obrpc::ObCreatePartitionBatchArg& arg = proxy_batch.get_args().at(i);
        if (OB_UNLIKELY(nullptr == result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result ptr is null", K(ret));
        } else if (OB_SUCCESS != ret1) {
          ret = ret1;
          LOG_WARN("fail to batch execute rpc", K(ret));
        } else if (arg.args_.count() != result->ret_list_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("arg and result count not match",
              K(ret),
              "arg_cnt",
              arg.args_.count(),
              "result_cnt",
              result->ret_list_.count());
        } else if (OB_FAIL(build_binding_partition_new_arg(arg, *result, new_dest_array, new_arg_array))) {
          LOG_WARN("fail to build binding partition new arg", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        // bypass
      } else if (new_arg_array.count() != new_dest_array.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("count unexpected", K(ret));
      } else if (0 == new_arg_array.count()) {
        return_code_array.reset();
        break;
      } else if (THIS_WORKER.get_timeout_remain() <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("timeout", K(ret));
      } else if (OB_FAIL(do_execute_binding_partition_request(
                     proxy_batch, new_dest_array, new_arg_array, return_code_array, inactive_servers, inactive_args))) {
        LOG_WARN("fail to do execute binding partition request", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && THIS_WORKER.get_timeout_remain() <= 0) {
    ret = OB_TIMEOUT;
    LOG_WARN("execute timeout", K(ret));
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
      ret = return_code_array.at(i);
    }
  }

  return ret;
}

int ObPartitionCreator::build_binding_partition_new_arg(const obrpc::ObCreatePartitionBatchArg& arg,
    const obrpc::ObCreatePartitionBatchRes& result, common::ObIArray<common::ObAddr>& new_dest_array,
    common::ObIArray<obrpc::ObCreatePartitionBatchArg>& new_arg_array)
{
  int ret = OB_SUCCESS;
  for (int64_t j = 0; OB_SUCC(ret) && j < result.ret_list_.count(); ++j) {
    const obrpc::ObCreatePartitionArg& p_arg = arg.args_.at(j);
    const int64_t ret2 = result.ret_list_.at(j);
    if (OB_SUCCESS == ret2) {
      // good
    } else if (OB_NOT_MASTER != ret2 && OB_PARTITION_NOT_EXIST != ret2) {
      ret = ret2;
    } else {
      share::ObPartitionInfo pg_info;
      ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
      pg_info.set_allocator(&allocator);
      while (OB_SUCC(ret) && THIS_WORKER.get_timeout_remain() > 0) {
        const share::ObPartitionReplica* leader_replica = nullptr;
        pg_info.reuse();
        allocator.reuse();
        if (OB_UNLIKELY(!p_arg.pg_key_.is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(p_arg));
        } else if (OB_SUCCESS != pt_operator_.get(  // do not modify ret value
                                     p_arg.pg_key_.get_tablegroup_id(),
                                     p_arg.pg_key_.get_partition_id(),
                                     pg_info)) {
          if (THIS_WORKER.get_timeout_remain() <= 0) {
            ret = OB_TIMEOUT;
            LOG_WARN("get pg leader timeout", K(ret));
          } else {
            usleep(100 * 1000);  // sleep 100ms and retry
          }
        } else if (OB_SUCCESS != pg_info.find_leader_v2(leader_replica)) {  // do not modify ret
          if (THIS_WORKER.get_timeout_remain() <= 0) {
            ret = OB_TIMEOUT;
            LOG_WARN("get pg leader timeout", K(ret));
          } else {
            usleep(100 * 1000);  // sleep 100ms and retry
          }
        } else if (OB_UNLIKELY(nullptr == leader_replica)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("leader replica ptr is null", K(ret));
        } else {
          int64_t index = 0;
          for (/*nop*/; index < new_dest_array.count(); ++index) {
            if (leader_replica->server_ == new_dest_array.at(index)) {
              break;
            }
          }
          if (index >= new_dest_array.count()) {
            obrpc::ObCreatePartitionBatchArg new_arg;
            if (OB_FAIL(new_dest_array.push_back(leader_replica->server_))) {
              LOG_WARN("fail to push back", K(ret));
            } else if (OB_FAIL(new_arg_array.push_back(new_arg))) {
              LOG_WARN("fail to push back", K(ret));
            }
          }
          if (OB_FAIL(ret)) {
            // fail
          } else if (index >= new_arg_array.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("index unexpected", K(ret));
          } else if (OB_FAIL(new_arg_array.at(index).args_.push_back(p_arg))) {
            LOG_WARN("fail to push back", K(ret));
          } else {
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionCreator::do_execute_binding_partition_request(ObCreatePartitionBatchProxy& proxy_batch,
    const common::ObIArray<common::ObAddr>& new_dest_array,
    const common::ObIArray<obrpc::ObCreatePartitionBatchArg>& new_arg_array, common::ObIArray<int>& return_code_array,
    common::ObIArray<common::ObAddr>& inactive_servers,
    common::ObIArray<obrpc::ObCreatePartitionBatchArg>& inactive_args)
{
  int ret = OB_SUCCESS;
  proxy_batch.reuse();
  return_code_array.reset();
  inactive_servers.reset();
  inactive_args.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < new_dest_array.count(); ++i) {
    const common::ObAddr& dest = new_dest_array.at(i);
    const obrpc::ObCreatePartitionBatchArg& arg = new_arg_array.at(i);
    bool is_active = false;
    const int64_t log_sync_time = 10 * 1000 * 3;  // 3 times of sata disk sync time
    const int64_t log_sync_count = 3;             //  1 slog + 2 clog
    int64_t timeout_us = GCONF.rpc_timeout + arg.args_.count() * log_sync_time * log_sync_count;
    timeout_us = std::min(timeout_us, THIS_WORKER.get_timeout_remain());
    if (OB_UNLIKELY(nullptr == server_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server mgr ptr is null", K(ret));
    } else if (OB_FAIL(server_mgr_->check_server_alive(dest, is_active))) {
      LOG_WARN("fail to check server alive", K(ret), K(dest));
    } else if (!is_active) {
      if (OB_FAIL(inactive_servers.push_back(dest))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(inactive_args.push_back(arg))) {
        LOG_WARN("fail to push back", K(ret));
      }
    } else if (OB_FAIL(server_mgr_->set_with_partition(dest))) {
      LOG_WARN("fail to set with partition", K(ret), K(dest));
    } else if (OB_FAIL(proxy_batch_.call(dest, timeout_us, arg))) {
      LOG_WARN("fail to call rpc", K(ret), K(timeout_us), K(arg));
    }
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = proxy_batch_.wait_all(return_code_array))) {
    LOG_WARN("wait batch result failed", K(ret), K(tmp_ret));
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
  }
  if (OB_SUCC(ret)) {
    if (return_code_array.count() != proxy_batch_.get_args().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected batch return code count",
          K(ret),
          "return code count",
          return_code_array.count(),
          "arg count",
          proxy_batch_.get_args().count());
    }
  }
  return ret;
}

// all partition must be created successfully in strict model expect for {ALL_SERVER}@R
int ObPartitionCreator::check_partition_in_strict_create_mode(
    const int64_t replica_cnt, ObCreatePartitionBatchProxy& proxy_batch, ObArray<int>& return_code_array)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<ObPartitionKey, int64_t> partition_map;
  if (replica_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (proxy_batch.get_args().count() != return_code_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("return_code_array count and  args count not matched",
        K(ret),
        "return_code_array_count",
        return_code_array.count(),
        "proxy_batch_args_count",
        proxy_batch.get_args().count());
  } else if (OB_FAIL(
                 partition_map.create(hash::cal_next_prime(replica_cnt), ObModIds::OB_CREATE_PARTITION_CHECK_LIST))) {
    LOG_WARN("failed to create hash set", K(ret));
  }
  if (OB_SUCC(ret)) {
    // Check whether all failed error codes are consistent
    int first_fail_code = OB_SUCCESS;
    bool is_all_fail_code_same = true;
    for (int64_t i = 0; is_all_fail_code_same && i < return_code_array.count(); ++i) {
      int cur_ret_code = return_code_array.at(i);
      if (OB_SUCCESS != cur_ret_code) {
        if (OB_SUCCESS == first_fail_code) {
          first_fail_code = cur_ret_code;
        } else if (first_fail_code != cur_ret_code) {
          is_all_fail_code_same = false;
        }
      }
    }
    if (is_all_fail_code_same) {
      // if all failed error codes are consistent, return the error code
      ret = first_fail_code;
      LOG_WARN("create partition failed", K(ret));
    }
  }
  // check all return codes
  for (int64_t i = 0; OB_SUCC(ret) && i < proxy_batch.get_results().count(); ++i) {
    const obrpc::ObCreatePartitionBatchRes* batch_res = proxy_batch.get_results().at(i);
    const obrpc::ObCreatePartitionBatchArg& batch_arg = proxy_batch.get_args().at(i);
    if (OB_UNLIKELY(NULL == batch_res)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("batch create partition response is NULL", K(ret), K(batch_res));
    } else if (batch_res->ret_list_.count() != batch_arg.args_.count()) {
      ret = OB_REPLICA_NUM_NOT_ENOUGH;
      LOG_WARN("some partitions create failed",
          K(ret),
          "batch_ret_count",
          batch_res->ret_list_.count(),
          "batch_arg_count",
          batch_arg.args_.count());
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < batch_res->ret_list_.count(); ++j) {
        if (OB_SUCCESS != batch_res->ret_list_.at(j)) {
          ret = OB_REPLICA_NUM_NOT_ENOUGH;
          LOG_WARN("some replicas create failed", K(ret));
        } else {
          int64_t remain_replica_num = OB_INVALID_ARGUMENT;
          const ObPartitionKey& partition_key = batch_arg.args_.at(j).partition_key_;
          if (OB_FAIL(partition_map.get_refactored(partition_key, remain_replica_num))) {
            if (OB_HASH_NOT_EXIST == ret) {
              remain_replica_num =
                  batch_arg.args_.at(j).replica_num_ + batch_arg.args_.at(j).non_paxos_replica_num_ - 1;
              if (OB_FAIL(partition_map.set_refactored(partition_key, remain_replica_num))) {
                LOG_WARN("set partition_map failed", K(ret), K(partition_key), K(remain_replica_num));
              }
            } else {
              LOG_WARN("failed to get element from hashmap", K(ret));
            }
          } else {
            const int overwrite = 1;
            --remain_replica_num;
            if (OB_FAIL(partition_map.set_refactored(partition_key, remain_replica_num, overwrite))) {
              LOG_WARN("set partition_map failed", K(ret), K(partition_key), K(remain_replica_num), K(overwrite));
            }
          }
        }
      }
    }
  }
  // check partition num completely matched
  if (OB_SUCC(ret)) {
    FOREACH(iter, partition_map)
    {
      if (0 != iter->second) {
        ret = OB_REPLICA_NUM_NOT_ENOUGH;
        LOG_WARN("partition remain replica num should be zero",
            K(ret),
            "part_key",
            iter->first,
            "remain_replica_num",
            iter->second);
        break;
      }
    }
  }
  partition_map.destroy();
  return ret;
}

// check paxos member successful count >= majority in loose mode
int ObPartitionCreator::check_partition_in_non_strict_create_mode(const int64_t replica_cnt,
    ObCreatePartitionBatchProxy& proxy_batch, ObArray<int>& return_code_array,
    const common::ObIArray<obrpc::ObCreatePartitionBatchArg*>& inactive_args)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<ObPartitionKey, int64_t> failed_partition_map;
  hash::ObHashMap<ObPartitionKey, int> check_full_replica_map;
  int64_t member_list_count = -1;
  if (replica_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica_cnt));
  } else if (proxy_batch.get_args().count() != return_code_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("return_code_array count and  args count not matched",
        K(ret),
        "return_code_array_count",
        return_code_array.count(),
        "proxy_batch_args_count",
        proxy_batch.get_args().count());
  } else if (OB_FAIL(failed_partition_map.create(
                 hash::cal_next_prime(replica_cnt), ObModIds::OB_CREATE_PARTITION_FAILED_LIST))) {
    LOG_WARN("failed to create hash set", K(ret));
  } else if (OB_FAIL(check_full_replica_map.create(
                 hash::cal_next_prime(replica_cnt), ObModIds::OB_CREATE_PARTITION_CHECK_FULL_REPLICA_LIST))) {
    LOG_WARN("failed to create hash set", K(ret));
  }
  // check all replicas in inactive server
  for (int64_t i = 0; OB_SUCC(ret) && i < inactive_args.count(); ++i) {
    const obrpc::ObCreatePartitionBatchArg* batch_arg = inactive_args.at(i);
    if (OB_ISNULL(batch_arg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get arg", KR(ret), K(i));
    } else {
      FOREACH_CNT_X(r, batch_arg->args_, OB_SUCC(ret))
      {
        if (OB_ISNULL(r)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("arg is null", K(ret), K(r), K(batch_arg));
        } else if (OB_FAIL(get_member_list_count(*r, member_list_count))) {
          LOG_WARN("failed to get member list count", K(ret), K(r));
        } else if (OB_FAIL(check_majority(r->partition_key_,
                       OB_SERVER_NOT_ACTIVE,
                       failed_partition_map,
                       check_full_replica_map,
                       r->replica_num_,
                       member_list_count,
                       r->replica_type_))) {
          LOG_WARN("failed to create partition", K(ret), "partition key", r->partition_key_);
        }
      }
    }
  }
  // check all return codes
  for (int64_t i = 0; OB_SUCC(ret) && i < proxy_batch.get_results().count(); ++i) {
    const obrpc::ObCreatePartitionBatchRes* batch_res = proxy_batch.get_results().at(i);
    const obrpc::ObCreatePartitionBatchArg& batch_arg = proxy_batch.get_args().at(i);
    if (OB_UNLIKELY(NULL == batch_res)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("batch create partition response is NULL", K(ret), K(batch_res));
    } else if (batch_res->ret_list_.count() != batch_arg.args_.count()) {
      FOREACH_CNT_X(r, batch_arg.args_, OB_SUCC(ret))
      {
        if (OB_ISNULL(r)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("arg is null", K(ret), K(r), K(batch_arg));
        } else if (OB_FAIL(get_member_list_count(*r, member_list_count))) {
          LOG_WARN("failed to get member list count", K(ret), K(r));
        } else if (OB_FAIL(check_majority(r->partition_key_,
                       return_code_array.at(i),
                       failed_partition_map,
                       check_full_replica_map,
                       r->replica_num_,
                       member_list_count,
                       r->replica_type_))) {
          LOG_WARN("failed to create partition", K(ret), "partition key", r->partition_key_);
        }
      }
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < batch_res->ret_list_.count(); ++j) {
        const ObReplicaType replica_type = batch_arg.args_.at(j).replica_type_;
        int64_t replica_num = batch_arg.args_.at(j).replica_num_;
        if (OB_FAIL(get_member_list_count(batch_arg.args_.at(j), member_list_count))) {
          LOG_WARN("failed to get member list count", K(ret), K(j), K(batch_arg));
        } else if (member_list_count < replica_num / 2 + 1) {
          ret = OB_REPLICA_NUM_NOT_ENOUGH;
          LOG_WARN("alloc replica num not enough", K(ret), "arg", batch_arg.args_.at(j), K(member_list_count));
        } else if (OB_SUCCESS != batch_res->ret_list_.at(j)) {
          if (OB_FAIL(check_majority(batch_arg.args_.at(j).partition_key_,
                  batch_res->ret_list_.at(j),
                  failed_partition_map,
                  check_full_replica_map,
                  replica_num,
                  member_list_count,
                  replica_type))) {
            LOG_WARN("failed to create partition", K(ret), "partition key", batch_arg.args_.at(j));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          const ObPartitionKey& partition_key = batch_arg.args_.at(j).partition_key_;
          if (OB_FAIL(check_full_replica_map.get_refactored(partition_key, tmp_ret))) {
            if (OB_HASH_NOT_EXIST == ret) {
              tmp_ret =
                  ObReplicaTypeCheck::is_paxos_replica_V2(replica_type) ? OB_SUCCESS : OB_FULL_REPLICA_NUM_NOT_ENOUGH;
              if (OB_FAIL(check_full_replica_map.set_refactored(partition_key, tmp_ret))) {
                LOG_WARN("set check_full_replica_map failed", K(ret), K(partition_key), K(tmp_ret));
              }
            } else {
              LOG_WARN("failed to get element from hashmap", K(ret));
            }
          } else if (OB_SUCCESS != tmp_ret && ObReplicaTypeCheck::is_paxos_replica_V2(replica_type)) {
            const int overwrite = 1;
            if (OB_FAIL(check_full_replica_map.set_refactored(partition_key, OB_SUCCESS, overwrite))) {
              LOG_WARN("set check_full_replica_map failed", K(ret), K(partition_key), K(overwrite));
            }
          }
        }
      }
    }
  }
  // check partition at least has one full type replica
  if (OB_SUCC(ret)) {
    FOREACH(iter, check_full_replica_map)
    {
      if (OB_SUCCESS != iter->second) {
        ret = iter->second;
        LOG_WARN("should at least has one full replica", K(ret));
        break;
      }
    }
  }
  check_full_replica_map.destroy();
  failed_partition_map.destroy();
  return ret;
}

int ObPartitionCreator::get_member_list_count(const obrpc::ObCreatePartitionArg& arg, int64_t& member_list_count)
{
  int ret = OB_SUCCESS;
  member_list_count = -1;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", K(ret), K(arg));
  } else if (arg.restore_ >= ObReplicaRestoreStatus::REPLICA_RESTORE_DATA &&
             arg.restore_ < ObReplicaRestoreStatus::REPLICA_RESTORE_MAX) {
    // FIXME:() physical restore
    member_list_count = arg.replica_num_;
  } else if (ignore_member_list_) {
    // get member_list_count from member_list
    if (OB_ISNULL(member_lists_.get(arg.partition_key_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get member list", K(ret), K(arg));
    } else {
      member_list_count = member_lists_.get(arg.partition_key_)->member_list_.get_member_number();
    }
  } else {
    member_list_count = arg.member_list_.get_member_number();
  }
  return ret;
}

/* replica_num: the quorum num of paxos
 * member_cnt:  the current num of paxos members
 * replica_num != member_cnt.
 * e.g.: need to create a partition has 5 replicas. cause has inactive server. the member_list
 *       has only 3 mebers. replica_num = 5, member_cnt = 3.
 *       Here, we need to ensure member_cnt - failed_cnt >= replica_num / 2 + 1
 *       to ensure the number of successes >= majority
 * Entering this function indicates that the creation failed
 */
int ObPartitionCreator::check_majority(const ObPartitionKey& partition_key, const int failed_ret,
    hash::ObHashMap<ObPartitionKey, int64_t>& failed_partition_map,
    hash::ObHashMap<ObPartitionKey, int>& check_full_partition_map, const int64_t replica_num, const int64_t member_cnt,
    ObReplicaType replica_type)
{
  int ret = OB_SUCCESS;
  int64_t failed_cnt = 0;
  // mark partition with full type replica
  int failed_ret_from_map = OB_SUCCESS;
  if (OB_FAIL(check_full_partition_map.get_refactored(partition_key, failed_ret_from_map))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(check_full_partition_map.set_refactored(partition_key, failed_ret))) {
        LOG_WARN("set check_full_partition_map failed", K(ret), K(partition_key), K(failed_ret));
      }
    } else {
      LOG_WARN("failed to get element from hashmap", K(ret));
    }
  }
  // check paxos member cnt >= majority
  if (OB_FAIL(ret)) {
    // skip
  } else if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_type)) {
    if (OB_FAIL(failed_partition_map.get_refactored(partition_key, failed_cnt))) {
      if (OB_HASH_NOT_EXIST == ret) {
        failed_cnt = 1;
        if (OB_FAIL(failed_partition_map.set_refactored(partition_key, failed_cnt))) {
          LOG_WARN("failed to add failed partition to hashmap", K(ret));
        }
      } else {
        LOG_WARN("failed to get element from hashmap", K(ret));
      }
    } else {
      ++failed_cnt;
      const int overwrite = 1;
      if (OB_FAIL(failed_partition_map.set_refactored(partition_key, failed_cnt, overwrite))) {
        LOG_WARN("failed_partition_map overwrite failed", K(ret), K(overwrite), K(partition_key));
      }
    }
    if (OB_SUCC(ret)) {
      if (member_cnt - failed_cnt < replica_num / 2 + 1) {
        ret = failed_ret;
        LOG_WARN("succeed partition count less than majority",
            K(ret),
            K(partition_key),
            K(failed_cnt),
            K(replica_num),
            K(member_cnt));
      }
    }
  }
  return ret;
}

int ObPartitionCreator::create_partition_sync(obrpc::ObSrvRpcProxy& rpc_proxy, ObPartitionTableOperator& pt_operator,
    const ObAddr& dest, const obrpc::ObCreatePartitionArg& arg)
{
  int ret = OB_SUCCESS;
  ObPartitionCreator creator(rpc_proxy, pt_operator, NULL);
  if (!dest.is_valid() || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest), K(arg));
  } else if (OB_FAIL(creator.add_create_partition_arg(dest, arg))) {
    LOG_WARN("create partition failed", K(ret), K(dest), K(arg));
  } else if (OB_FAIL(creator.execute())) {
    LOG_WARN("execute create partition failed", K(ret), K(dest), K(arg));
  }
  return ret;
}

void ObPartitionCreator::reuse()
{
  proxy_.reuse();
  proxy_batch_.reuse();
  replicas_.reuse();
  dests_.reuse();
  args_.reuse();
  set_member_list_proxy_.reuse();
  ignore_member_list_ = false;
  member_lists_.reuse();
}

// Create tenant, the internal table is persisted only after all replicas are created successfully
// So we can get all member_lists of the tenant
int ObPartitionCreator::set_tenant_exist_partition_member_list(const uint64_t tenant_id, const int64_t quorum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ignore_member_list_) || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not ignore member list or tenant_id is invalid", KR(ret), K(ignore_member_list_), K(tenant_id));
  } else {
    // check has persist member_list in table
    ObSqlString sql;
    if (OB_FAIL(
            sql.assign_fmt("select * from %s where tenant_id = %ld", OB_ALL_PARTITION_MEMBER_LIST_TNAME, tenant_id))) {
      LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(construct_member_list_map(sql, quorum))) {
      LOG_WARN("failed to construct member list map", KR(ret), K(tenant_id), K(sql), K(quorum));
    }
  }
  return ret;
}

int ObPartitionCreator::set_exist_partition_member_list(
    const uint64_t tenant_id, const int64_t table_id, const int64_t quorum)
{
  int ret = OB_SUCCESS;
  if (!ignore_member_list_ || OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not ignore member list", K(ret), K(ignore_member_list_), K(tenant_id), K(table_id));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("select * from %s where tenant_id = %lu and table_id = %ld",
            OB_ALL_PARTITION_MEMBER_LIST_TNAME,
            tenant_id,
            table_id))) {
      LOG_WARN("failed to assign sql", KR(ret), K(sql));
    } else if (OB_FAIL(construct_member_list_map(sql, quorum))) {
      LOG_WARN("failed to construct member list map", K(ret), K(sql));
    }
  }
  return ret;
}
int ObPartitionCreator::construct_member_list_map(const ObSqlString& sql, const int64_t quorum)
{
  int ret = OB_SUCCESS;
  if (!ignore_member_list_ || sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not ignore member list", K(ret), K(ignore_member_list_), K(sql));
  } else {
    // set paxos replica num for check majority
    paxos_replica_num_ = quorum;
    // check has persist member_list
    // Not on hotspot path, use HEAP_VAR
    HEAP_VAR(ObMySQLProxy::MySQLResult, res)
    {
      common::sqlclient::ObMySQLResult* result = NULL;
      if (OB_FAIL(sql_proxy_->read(res, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else {
        HEAP_VAR(common::ObFixedLengthString<MAX_MEMBER_LIST_LENGTH>, member_list_str2)
        {
          member_list_str2.reset();
          ObString member_list_str;
          ObPartitionReplica::MemberList member_list;
          int64_t partition_id = OB_INVALID_ID;
          const int64_t partition_cnt = 0;
          int64_t table_id = OB_INVALID_ID;
          obrpc::ObSetMemberListArg arg;
          while (OB_SUCC(ret) && OB_SUCC(result->next())) {
            arg.reset();
            member_list_str.reset();
            member_list.reset();
            char* member_list_cstr = NULL;
            char* lease_start_cstr = NULL;
            EXTRACT_INT_FIELD_MYSQL(*result, "partition_id", partition_id, int64_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, int64_t);
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "member_list", member_list_str);
            if (OB_FAIL(ret)) {
              LOG_WARN("failed to get result", K(ret), K(sql));
            } else if (OB_FAIL(member_list_str2.assign(member_list_str))) {
              LOG_WARN("failed to assign member list str", KR(ret), K(member_list_str));
            } else if (OB_ISNULL(member_list_cstr = (STRTOK_R(member_list_str2.ptr(), "-", &lease_start_cstr)))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to get member_list and lease start", KR(ret), K(member_list_str2));
            } else if (OB_FAIL(ObPartitionReplica::text2member_list(member_list_cstr, member_list))) {
              LOG_WARN("failed to get member_list", K(ret), K(member_list_str));
            } else if (OB_FAIL(arg.key_.init(table_id, partition_id, partition_cnt))) {
              LOG_WARN("failed to init partition key", K(ret), K(table_id), K(partition_id));
            } else if (OB_ISNULL(lease_start_cstr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("lease start is null", KR(ret), K(member_list_str));
            } else if (OB_FAIL(ob_atoll(lease_start_cstr, arg.lease_start_))) {
              LOG_WARN("failed to get int", KR(ret), K(lease_start_cstr), K(member_list_str));
            } else {
              for (int64_t i = 0; OB_SUCC(ret) && i < member_list.count(); ++i) {
                const ObMember member = ObMember(member_list.at(i).server_, member_list.at(i).timestamp_);
                if (OB_FAIL(arg.member_list_.add_member(member))) {
                  LOG_WARN("failed to add member", K(ret), K(i), K(member));
                } else {
                  arg.quorum_ = quorum;
                  arg.leader_ = member_list.at(0).server_;  // leader is first member of member_list
                }
              }
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(build_member_list_map(arg))) {
                LOG_WARN("failed to push back arg", K(ret), K(arg));
              }
            }
          }
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else if (OB_FAIL(ret)) {
            LOG_WARN("get next failed", K(ret), K(sql));
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected ret", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionCreator::build_member_list_map(const obrpc::ObSetMemberListArg& arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid() || !ignore_member_list_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", K(ret), K(ignore_member_list_), K(arg));
  } else {
    if (!member_lists_.created()) {
      const int64_t bucket_num = 5;  // TODO
      if (OB_FAIL(member_lists_.create(bucket_num, ObModIds::OB_CREATE_PARTITION_CHECK_LIST))) {
        LOG_WARN("failed to create hashmap", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(member_lists_.set_refactored(arg.key_, arg))) {
      LOG_WARN("failed ot set member to hashmap", KR(ret), K(arg));
    }
  }
  return ret;
}
// persist member_list to inner table
int ObPartitionCreator::batch_persist_member_list(const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_args)
{
  int ret = OB_SUCCESS;
  if (!ignore_member_list_ || 1 > batch_args.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not ignore member list", K(ret), K(ignore_member_list_), "args_count", batch_args.count());
  } else {
    ObDMLSqlSplicer dml;
    int64_t partition_count = 0;
    common::hash::ObHashSet<ObPartitionKey> keys;  // for remove duplicate batch_arg
    const int64_t bucket_num = 5;                  // TODO
    if (OB_FAIL(keys.create(bucket_num))) {
      LOG_WARN("failed to create partition key set", K(ret), K(bucket_num));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_args.count(); ++i) {
      const obrpc::ObCreatePartitionArg& arg = batch_args.at(i);
      if (!arg.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("arg is invalid", K(ret), "arg", batch_args.at(i));
      } else if (ObReplicaTypeCheck::is_paxos_replica_V2(arg.replica_type_)) {
        const ObPartitionKey& partition_key = arg.partition_key_;
        ret = keys.exist_refactored(partition_key);
        if (OB_SUCC(ret)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to find partition key", K(ret), K(partition_key));
        } else if (OB_HASH_EXIST == ret) {
          // nothing
          ret = OB_SUCCESS;
        } else if (OB_HASH_NOT_EXIST == ret) {
          if (OB_FAIL(fill_dml_splicer(partition_key, arg.schema_version_, dml))) {
            LOG_WARN("failed to fill dml splicer", K(ret), K(partition_key), K(arg));
          } else if (OB_FAIL(dml.finish_row())) {
            LOG_WARN("failed to finish row", K(ret), K(partition_key));
          } else if (OB_FAIL(keys.set_refactored(partition_key))) {
            LOG_WARN("failed to set partition key", K(ret), K(partition_key));
          } else {
            ++partition_count;
          }
        } else if (OB_FAIL(ret)) {
          LOG_WARN("failed to find partition key", K(ret), K(partition_key));
        }
      }
    }  // end for i
    int64_t affected_row = 0;
    if (OB_FAIL(ret)) {
    } else {
      ObSqlString sql;
      if (OB_FAIL(dml.splice_batch_insert_sql(OB_ALL_PARTITION_MEMBER_LIST_TNAME, sql))) {
        LOG_WARN("failed to splice batch insert sql", K(ret), K(sql));
      } else if (OB_FAIL(sql_proxy_->write(sql.ptr(), affected_row))) {
        LOG_WARN("failed to execute", K(ret), K(affected_row), K(sql));
      } else if (partition_count != affected_row) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_row not equal to partition count", K(ret), K(affected_row), K(partition_count));
      }
    }
  }
  return ret;
}
int ObPartitionCreator::fill_dml_splicer(
    const ObPartitionKey& partition_key, const int64_t schema_version, ObDMLSqlSplicer& dml_splicer)
{
  int ret = OB_SUCCESS;
  if (!ignore_member_list_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not ignore member list", K(ret));
  } else if (!partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition key is invalid", K(ret), K(partition_key));
  } else {
    const obrpc::ObSetMemberListArg* arg = member_lists_.get(partition_key);
    const int64_t length = MAX_MEMBER_LIST_LENGTH;
    const int64_t partition_status = 1;
    ObMember ob_member;
    ObPartitionReplica::MemberList new_member_list;
    const int64_t tenant_id = extract_tenant_id(partition_key.get_table_id());
    if (OB_ISNULL(arg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get member list arg", K(ret), K(partition_key));
    } else if (majority(paxos_replica_num_) > arg->member_list_.get_member_number()) {
      ret = OB_REPLICA_NUM_NOT_ENOUGH;
      LOG_WARN("partition count less than majority", KR(ret), KPC(arg), K(paxos_replica_num_));
    } else if (OB_FAIL(arg->member_list_.get_member_by_addr(arg->leader_, ob_member))) {
      LOG_WARN("failed to get leader", KR(ret), KPC(arg));
    } else {
      ObPartitionReplica::Member member(ob_member.get_server(), ob_member.get_timestamp());
      if (OB_FAIL(new_member_list.push_back(member))) {
        LOG_WARN("failed to push back member", KR(ret), KPC(arg), K(member));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < arg->member_list_.get_member_number(); ++i) {
      if (OB_FAIL(arg->member_list_.get_member_by_index(i, ob_member))) {
        LOG_WARN("failed to get member by index", KR(ret), K(i));
      } else if (arg->leader_ == ob_member.get_server()) {
        // leader is first of member_list
      } else {
        ObPartitionReplica::Member member(ob_member.get_server(), ob_member.get_timestamp());
        if (OB_FAIL(new_member_list.push_back(member))) {
          LOG_WARN("failed to push back member", KR(ret), K(i), KPC(arg), K(member));
        }
      }
    }
    if (OB_SUCC(ret)) {
      HEAP_VAR(common::ObFixedLengthString<length>, member_list_str)
      {
        // leader is the first of member_list
        // member_list-lease_start
        if (OB_SUCC(ret)) {
          int64_t pos = 0;
          if (OB_FAIL(ObPartitionReplica::member_list2text(new_member_list, member_list_str.ptr(), length))) {
            LOG_WARN("failed to member list to text", KR(ret), K(new_member_list), K(length), KPC(arg));
          } else {
            pos = member_list_str.size();
            if (OB_FAIL(databuff_printf(member_list_str.ptr(), length, pos, "-%ld", arg->lease_start_))) {
              LOG_WARN("failed to databuff printf", KR(ret), KPC(arg), K(pos), K(length));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", tenant_id)) ||
                   OB_FAIL(dml_splicer.add_pk_column("table_id", partition_key.get_table_id())) ||
                   OB_FAIL(dml_splicer.add_pk_column("partition_id", partition_key.get_partition_id())) ||
                   OB_FAIL(dml_splicer.add_column("member_list", member_list_str)) ||
                   OB_FAIL(dml_splicer.add_column("schema_version", schema_version)) ||
                   OB_FAIL(dml_splicer.add_column("partition_status", partition_status))) {
          LOG_WARN("add column failed", K(ret), K(partition_key), K(member_list_str), K(schema_version));
        }
      }
    }
  }
  return ret;
}

int ObPartitionCreator::check_partition_already_exist(
    const int64_t table_id, const int64_t partition_id, const int64_t paxos_replica_num, bool& partition_already_exist)
{
  int ret = OB_SUCCESS;
  partition_already_exist = false;
  if (OB_INVALID_ID == table_id || OB_INVALID_ID == partition_id || OB_INVALID_ID == paxos_replica_num) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table id or partition id is invalid", K(ret), K(table_id), K(partition_id), K(paxos_replica_num));
  } else if (!ignore_member_list_) {
    partition_already_exist = false;
  } else if (!member_lists_.created()) {
    // first to construct member_list of exist partition
    const int64_t tenant_id = extract_tenant_id(table_id);
    if (OB_FAIL(set_exist_partition_member_list(tenant_id, table_id, paxos_replica_num))) {
      LOG_WARN("failed to set exist member list", KR(ret), K(tenant_id));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (ignore_member_list_) {
    ObPartitionKey key;
    const int64_t partition_cnt = 0;
    if (OB_FAIL(key.init(table_id, partition_id, partition_cnt))) {
      LOG_WARN("failed to init partition key", K(ret), K(table_id), K(partition_id));
    } else if (OB_ISNULL(member_lists_.get(key))) {
      partition_already_exist = false;
    } else {
      partition_already_exist = true;
    }
  }
  return ret;
}
// set member_list to partition
int ObPartitionCreator::send_set_member_list_rpc()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to persist member list");
  if (!ignore_member_list_ || OB_INVALID_ID == paxos_replica_num_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not ignore member list or invalid replica num", K(ret), K(ignore_member_list_), K(paxos_replica_num_));
  } else {
    // construct batch_rpc by server
    DestMemberArgMap arg_map;
    common::PageArena<> allocator;
    if (OB_FAIL(distrubte_for_set_member_list(allocator, arg_map))) {
      LOG_WARN("failed to distrute member list", K(ret));
    } else {
      obrpc::ObSetMemberListArg arg;
      // There is a limit on the number of replicas per batch
      int64_t replica_count = 0;
      // for Mark map iteration complete
      int64_t partition_count = 0;
      obrpc::ObSetMemberListBatchArg* batch_arg = NULL;
      FOREACH_X(it, member_lists_, OB_SUCC(ret))
      {
        const ObPartitionKey& key = it->first;
        const ObMemberList& member_list = it->second.member_list_;
        ObAddr addr;
        if (OB_UNLIKELY(0 >= member_list.get_member_number())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("member list is empty", KR(ret));
        } else {
          // Put each server in the member list in the corresponding place
          // partition_key:KEY, member_list:A:B:C
          // destA: KEY; destB: KEY; destC: KEY;
          ++partition_count;
          for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
            arg.reset();
            if (OB_FAIL(arg.init(key.get_table_id(),
                    key.get_partition_id(),
                    key.get_partition_cnt(),
                    member_list,
                    paxos_replica_num_,
                    it->second.lease_start_,
                    it->second.leader_))) {
              LOG_WARN("failed to init persist member list",
                  KR(ret),
                  K(key),
                  K(member_list),
                  "leader",
                  it->second.leader_,
                  "lease_start",
                  it->second.lease_start_);
            } else if (OB_FAIL(member_list.get_server_by_index(i, addr))) {
              LOG_WARN("failed to get server by index", KR(ret), K(i), K(member_list));
            } else if (OB_FAIL(arg_map.get_refactored(addr, batch_arg))) {
              LOG_WARN("failed to get batch arg", K(ret), K(addr));
            } else if (OB_ISNULL(batch_arg)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("batch arg is null", K(ret), K(addr));
            } else if (OB_FAIL(batch_arg->args_.push_back(arg))) {
              LOG_WARN("failed to push back arg", K(ret), K(arg));
            } else {
              ++replica_count;
            }
          }  // end for
          if (OB_FAIL(ret)) {
            // The replica_count is equal to CREATE_PARTITION_BATCH_CNT or
            // map iteration complete
          } else if (CREATE_PARTITION_BATCH_CNT <= replica_count || member_lists_.size() == partition_count) {
            if (OB_FAIL(batch_send_set_member_list_rpc(replica_count, arg_map))) {
              LOG_WARN("failed to persist member list", K(ret), K(replica_count));
            } else {
              replica_count = 0;
              FOREACH(iter, arg_map)
              {
                if (NULL != iter->second) {
                  iter->second->reset();
                }
              }
            }
          }  // end else
        }
      }  // end for
    }
    arg_map.clear();
    allocator.free();
  }
  return ret;
}
int ObPartitionCreator::distrubte_for_set_member_list(common::PageArena<>& allocate, DestMemberArgMap& dest_arg_map)
{
  int ret = OB_SUCCESS;
  dest_arg_map.clear();
  if (!ignore_member_list_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not ignore_member_list", K(ret));
  } else {

    FOREACH_X(it, member_lists_, OB_SUCC(ret))
    {
      obrpc::ObSetMemberListArg& arg = it->second;
      if (OB_UNLIKELY(0 >= arg.member_list_.get_member_number())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("member list is empty", K(ret), K(arg));
      } else {
        ObMemberList& member_list = arg.member_list_;
        for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
          if (!dest_arg_map.created()) {
            if (OB_FAIL(dest_arg_map.create(HASHMAP_SERVER_CNT, ObModIds::OB_SERVER_ARPCARGS_MAP))) {
              LOG_WARN("create dest_arg_map failed", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            obrpc::ObSetMemberListBatchArg* batch_arg = NULL;
            ObAddr server;
            if (OB_FAIL(member_list.get_server_by_index(i, server))) {
              LOG_WARN("failed to get server", KR(ret), K(i), K(member_list));
            } else if (OB_FAIL(dest_arg_map.get_refactored(server, batch_arg)) && OB_HASH_NOT_EXIST != ret) {
              LOG_WARN("get batch arg from dest_arg_map failed", KR(ret), K(server));
            } else if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
              char* buf = allocate.alloc(sizeof(obrpc::ObSetMemberListBatchArg));
              if (OB_UNLIKELY(NULL == buf)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_ERROR("failed to alloc batch arg", K(ret));
              } else {
                obrpc::ObSetMemberListBatchArg* tmp_arg = NULL;
                tmp_arg = new (buf) obrpc::ObSetMemberListBatchArg();
                if (OB_FAIL(dest_arg_map.set_refactored(server, tmp_arg))) {
                  LOG_WARN("insert into hashmap failed", KR(ret), K(server), K(member_list));
                }
              }
            }
          }
        }  // end for
      }
    }
  }
  return ret;
}
int ObPartitionCreator::batch_send_set_member_list_rpc(const int64_t partition_count, DestMemberArgMap& arg_map)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start batch persist_member_list", "partition count", partition_count);
  if (!ignore_member_list_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not ignore member list", K(ret));
  } else {
    const int64_t log_sync_time = 10 * 1000 * 3;
    const int64_t log_sync_count = 2;  //  2 clog
    int64_t max_timeout_us = GCONF.rpc_timeout + partition_count * log_sync_count * log_sync_time;
    int64_t timeout = std::min(THIS_WORKER.get_timeout_remain(), max_timeout_us);
    obrpc::ObSetMemberListBatchArg arg;
    int64_t rpc_count = 0;
    set_member_list_proxy_.reuse();
    ObArray<obrpc::ObSetMemberListBatchArg*> inactive_args;

    FOREACH_X(iter, arg_map, OB_SUCC(ret))
    {
      if (NULL == iter->second) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get batch args from dest_arg_map", K(ret), "dest", iter->first);
      } else if (0 < iter->second->args_.count()) {
        bool is_active = false;
        if (OB_ISNULL(server_mgr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("server mgr is null", K(ret));
        } else if (OB_FAIL(server_mgr_->check_server_alive(iter->first, is_active))) {
          LOG_WARN("check_server_alive failed", K(ret), "server", iter->first);
        } else if (!is_active) {
          LOG_WARN("server in not alive", K(ret), "server", iter->first);
          if (OB_FAIL(inactive_args.push_back(iter->second))) {
            LOG_WARN("failed to push back arg", KR(ret), "server", iter->first);
          }
        } else {
          STATIC_ASSERT(SERVER_CONCURRENCY > 0 && CREATE_PARTITION_BATCH_CNT > SERVER_CONCURRENCY,
              "invalid SERVER_CONCURRENCY or CREATE_PARTITION_BATCH_CNT");
          const int64_t reserve_cnt =
              std::min(CREATE_PARTITION_BATCH_CNT / SERVER_CONCURRENCY, iter->second->args_.count());
          if (OB_FAIL(arg.args_.reserve(reserve_cnt))) {
            LOG_WARN("reserve array failed", K(ret), K(reserve_cnt));
          } else {
            arg.reset();
            for (int64_t i = 0; OB_SUCC(ret) && i < iter->second->args_.count(); ++i) {
              timeout = std::min(THIS_WORKER.get_timeout_remain(), max_timeout_us);
              if (timeout <= 0) {
                ret = OB_TIMEOUT;
                LOG_WARN("timeouted", K(ret), K(timeout));
              } else if (OB_FAIL(arg.args_.push_back(iter->second->args_.at(i)))) {
                LOG_WARN("array push back failed", K(ret));
              } else if (i + 1 == iter->second->args_.count() ||
                         arg.args_.count() == CREATE_PARTITION_BATCH_CNT / SERVER_CONCURRENCY) {
                LOG_INFO("start to send rpc for batch_create_partition",
                    K(timeout),
                    "dest",
                    iter->first,
                    "count",
                    arg.args_.count());
                if (OB_FAIL(set_member_list_proxy_.call(iter->first, timeout, arg))) {
                  LOG_WARN("call async batch rpc failed", K(ret), K(timeout), K(arg));
                } else {
                  ++rpc_count;
                  arg.reset();
                }
              }
            }  // end for send rpc
          }
        }
      }
    }  // end for send all server
    int tmp_ret = OB_SUCCESS;
    ObArray<int> return_code_array;
    if (OB_SUCCESS != (tmp_ret = set_member_list_proxy_.wait_all(return_code_array))) {
      LOG_WARN("wait batch result failed", K(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    } else if (return_code_array.count() != rpc_count || set_member_list_proxy_.get_args().count() != rpc_count) {
      tmp_ret = OB_ERR_UNEXPECTED;
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      LOG_WARN("unexpected batch return code count",
          K(ret),
          "return code count",
          return_code_array.count(),
          "rpc count",
          rpc_count,
          "arg count",
          proxy_batch_.get_args().count());
    } else if (OB_FAIL(check_set_member_list_succ(partition_count, return_code_array, inactive_args))) {
      LOG_WARN("fail to check partition in non-strict create mode",
          K(ret),
          K(partition_count),
          K(paxos_replica_num_),
          K(return_code_array));
    }
  }
  return ret;
}

// It is OK the majority of partition set member_list successfully
int ObPartitionCreator::check_set_member_list_succ(const int64_t partition_count,
    const common::ObIArray<int>& return_code_array,
    const common::ObIArray<obrpc::ObSetMemberListBatchArg*>& inactive_args)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<ObPartitionKey, int64_t> failed_partition_map;
  hash::ObHashMap<ObPartitionKey, int> check_full_replica_map;
  if (0 == partition_count || !ignore_member_list_ || OB_INVALID_ID == paxos_replica_num_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition_count), K(paxos_replica_num_), K(ignore_member_list_));
  } else if (set_member_list_proxy_.get_args().count() != return_code_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("return_code_array count and  args count not matched",
        K(ret),
        "return_code_array_count",
        return_code_array.count(),
        "proxy_batch_args_count",
        set_member_list_proxy_.get_args().count());
  } else if (OB_FAIL(failed_partition_map.create(
                 hash::cal_next_prime(partition_count), ObModIds::OB_CREATE_PARTITION_FAILED_LIST))) {
    LOG_WARN("failed to create hash set", K(ret));
  } else if (OB_FAIL(check_full_replica_map.create(
                 hash::cal_next_prime(partition_count), ObModIds::OB_CREATE_PARTITION_CHECK_FULL_REPLICA_LIST))) {
    LOG_WARN("failed to create hash set", K(ret));
  }
  // check replicas in inactive server
  for (int64_t i = 0; OB_SUCC(ret) && i < inactive_args.count(); ++i) {
    const obrpc::ObSetMemberListBatchArg* batch_arg = inactive_args.at(i);
    if (OB_ISNULL(batch_arg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get arg", KR(ret), K(i));
    } else {
      FOREACH_CNT_X(r, batch_arg->args_, OB_SUCC(ret))
      {
        if (OB_ISNULL(r)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("arg is null", K(ret), K(r), K(batch_arg));
        } else if (OB_FAIL(check_majority(r->key_,
                       OB_SERVER_NOT_ACTIVE,
                       failed_partition_map,
                       check_full_replica_map,
                       paxos_replica_num_,
                       r->member_list_.get_member_number(),
                       REPLICA_TYPE_FULL))) {
          LOG_WARN("failed to create partition", K(ret), "arg", *r, K(paxos_replica_num_));
        }
      }
    }
  }

  // check all return codes
  for (int64_t i = 0; OB_SUCC(ret) && i < set_member_list_proxy_.get_results().count(); ++i) {
    const obrpc::ObCreatePartitionBatchRes* batch_res = set_member_list_proxy_.get_results().at(i);
    const obrpc::ObSetMemberListBatchArg& batch_arg = set_member_list_proxy_.get_args().at(i);
    if (OB_UNLIKELY(NULL == batch_res)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("batch create partition response is NULL", K(ret), K(batch_res));
    } else if (batch_res->ret_list_.count() != batch_arg.args_.count()) {
      FOREACH_CNT_X(r, batch_arg.args_, OB_SUCC(ret))
      {
        if (OB_FAIL(check_majority(r->key_,
                return_code_array.at(i),
                failed_partition_map,
                check_full_replica_map,
                paxos_replica_num_,
                r->member_list_.get_member_number(),
                REPLICA_TYPE_FULL))) {
          LOG_WARN("failed to create partition", K(ret), "arg", *r, K(paxos_replica_num_));
        }
      }
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < batch_res->ret_list_.count(); ++j) {
        int64_t member_list_count = batch_arg.args_.at(j).member_list_.get_member_number();
        if (member_list_count < paxos_replica_num_ / 2 + 1) {
          ret = OB_REPLICA_NUM_NOT_ENOUGH;
          LOG_WARN("alloc replica num not enough", K(ret), "arg", batch_arg.args_.at(j), K(member_list_count));
        } else if (OB_SUCCESS != batch_res->ret_list_.at(j)) {
          if (OB_FAIL(check_majority(batch_arg.args_.at(j).key_,
                  batch_res->ret_list_.at(j),
                  failed_partition_map,
                  check_full_replica_map,
                  paxos_replica_num_,
                  member_list_count,
                  REPLICA_TYPE_FULL))) {
            LOG_WARN("failed to create partition", K(ret), "arg", batch_arg.args_.at(j), K(paxos_replica_num_));
          }
        }
      }  // end for
    }
  }
  check_full_replica_map.destroy();
  failed_partition_map.destroy();
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
