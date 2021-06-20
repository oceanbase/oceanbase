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

#ifndef OCEANBASE_SHARE_OB_AUTOINCREMENT_SERVICE_H_
#define OCEANBASE_SHARE_OB_AUTOINCREMENT_SERVICE_H_

#include <functional>
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/allocator/ob_small_allocator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "common/ob_timeout_ctx.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_autoincrement_param.h"
#include "share/ob_rpc_struct.h"
#include "share/partition_table/ob_partition_location_cache.h"
#include "share/schema/ob_multi_version_schema_service.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace share {
namespace schema {
class ObTableSchema;
}
static const int64_t TIME_SKEW = 100 * 1000;       // 100ms, time skew
static const int64_t PRE_OP_TIMEOUT = 500 * 1000;  // 500ms, for prefetch or presync
static const int PRE_OP_THRESHOLD = 4;             // for prefetch or presync
static const int64_t PARTITION_LOCATION_SET_BUCKET_NUM = 3;
static const int64_t FETCH_SEQ_NUM_ONCE = 1000;
static const uint64_t AUTO_INC_DEFAULT_NB_MAX_BITS = 16;                                  // from MySQL
static const uint64_t AUTO_INC_DEFAULT_NB_MAX = (1 << AUTO_INC_DEFAULT_NB_MAX_BITS) - 1;  // from MySQL
static const uint64_t AUTO_INC_DEFAULT_NB_ROWS = 1;                                       // from MySQL

struct CacheNode {
  CacheNode() : cache_start_(0), cache_end_(0)
  {}

  void reset()
  {
    cache_start_ = 0;
    cache_end_ = 0;
  }

  TO_STRING_KV(K_(cache_start), K_(cache_end));

  // combine two cache node if they are valid and continuous
  // otherwise use new_node if it is valid
  int combine_cache_node(CacheNode& new_node);

  uint64_t cache_start_;
  uint64_t cache_end_;
  // uint64_t cache_count_;
};

struct CacheHandle {
  CacheHandle()
      : prefetch_start_(0),
        prefetch_end_(0),
        next_value_(0),
        offset_(0),
        increment_(0),
        max_value_(0),
        last_value_to_confirm_(0),
        last_row_dup_flag_(false)

  {}

  TO_STRING_KV(K_(prefetch_start), K_(prefetch_end), K_(next_value), K_(offset), K_(increment), K_(max_value));

  uint64_t prefetch_start_;
  uint64_t prefetch_end_;
  // uint64_t prefetch_count_;
  uint64_t next_value_;
  uint64_t offset_;
  uint64_t increment_;
  uint64_t max_value_;
  uint64_t last_value_to_confirm_;
  bool last_row_dup_flag_;

  int next_value(uint64_t& next_value);
  bool in_range(const uint64_t value) const
  {
    return ((value >= prefetch_start_) && (value <= prefetch_end_));
  }
};

struct TableNode : public common::LinkHashValue<AutoincKey> {
  TableNode() : table_id_(0), next_value_(0), local_sync_(0), last_refresh_ts_(0), prefetching_(false)
  {}
  virtual ~TableNode()
  {
    destroy();
  }
  int init(int64_t autoinc_table_part_num);

  TO_STRING_KV(KT_(table_id), K_(next_value), K_(local_sync), K_(last_refresh_ts), K_(curr_node), K_(prefetch_node),
      K_(prefetching));

  int alloc_handle(common::ObSmallAllocator& allocator, const uint64_t offset, const uint64_t increment,
      const uint64_t desired_count, const uint64_t max_value, CacheHandle*& handle);

  bool prefetch_condition()
  {
    return 0 == prefetch_node_.cache_start_ &&
           (next_value_ - curr_node_.cache_start_) * PRE_OP_THRESHOLD > curr_node_.cache_end_ - curr_node_.cache_start_;
  }
  int set_partition_leader_epoch(const int64_t partition_id, const int64_t leader_epoch);
  int update_all_partitions_leader_epoch(const int64_t partition_id, const int64_t leader_epoch);
  int get_partition_leader_epoch(const int64_t partition_id, int64_t& leader_epoch);
  void destroy()
  {
    if (partition_leader_epoch_map_.created()) {
      partition_leader_epoch_map_.destroy();
    }
  }
  lib::ObMutex sync_mutex_;
  lib::ObMutex alloc_mutex_;
  uint64_t table_id_;
  uint64_t next_value_;
  uint64_t local_sync_;
  int64_t last_refresh_ts_;
  CacheNode curr_node_;
  CacheNode prefetch_node_;
  bool prefetching_;
  common::hash::ObHashMap<int64_t, int64_t> partition_leader_epoch_map_;
};

// atomic update if greater than origin value
template <typename T>
inline void atomic_update(T& v, T new_v)
{
  while (true) {
    T cur_v = v;
    if (new_v <= cur_v) {
      break;
    } else if (ATOMIC_BCAS(&v, cur_v, new_v)) {
      break;
    }
  }
}

class ObAutoincrementService {
public:
  static const int64_t DEFAULT_TABLE_NODE_NUM = 1024;
  //  static const int64_t BATCH_FETCH_COUNT = 1024;
  typedef common::ObLinkHashMap<AutoincKey, TableNode> NodeMap;

public:
  ObAutoincrementService();
  ~ObAutoincrementService();
  static ObAutoincrementService& get_instance();
  int init(common::ObAddr& addr, common::ObMySQLProxy* mysql_proxy, obrpc::ObSrvRpcProxy* srv_proxy,
      ObPartitionLocationCache* location_cache, share::schema::ObMultiVersionSchemaService* schema_service,
      storage::ObPartitionService* ps);
  int init_for_backup(common::ObAddr& addr, common::ObMySQLProxy* mysql_proxy, obrpc::ObSrvRpcProxy* srv_proxy,
      ObPartitionLocationCache* location_cache, share::schema::ObMultiVersionSchemaService* schema_service,
      storage::ObPartitionService* ps);
  int get_handle(AutoincParam& param, CacheHandle*& handle);
  void release_handle(CacheHandle*& handle);

  int sync_insert_value(AutoincParam& param, CacheHandle*& cache_handle, const uint64_t value_to_sync);
  int sync_insert_value_global(AutoincParam& param);

  int sync_insert_value_local(AutoincParam& param);

  int sync_auto_increment_all(
      const uint64_t tenant_id, const uint64_t table_id, const uint64_t column_id, const uint64_t sync_value);
  int refresh_sync_value(const obrpc::ObAutoincSyncArg& arg);

  int clear_autoinc_cache_all(const uint64_t tenant_id, const uint64_t table_id, const uint64_t column_id);
  int clear_autoinc_cache(const obrpc::ObAutoincSyncArg& arg);

  static int calc_next_value(
      const uint64_t last_next_value, const uint64_t offset, const uint64_t increment, uint64_t& new_next_value);
  static int calc_prev_value(
      const uint64_t last_next_value, const uint64_t offset, const uint64_t increment, uint64_t& prev_value);
  int get_sequence_value(
      const uint64_t tenant_id, const uint64_t table_id, const uint64_t column_id, uint64_t& seq_value);

  int get_sequence_value(const uint64_t tenant_id, const common::ObIArray<AutoincKey>& autoinc_keys,
      common::hash::ObHashMap<AutoincKey, uint64_t>& seq_values);

  int get_leader_epoch_id(const common::ObPartitionKey& part_key, int64_t& epoch_id) const;

private:
  uint64_t get_max_value(const common::ObObjType type);
  int get_table_node(const AutoincParam& param, TableNode*& table_node);
  int fetch_table_node(const AutoincParam& param, TableNode* table_node, const bool fetch_prefetch = false);
  int sync_global_sync(
      const uint64_t tenant_id, const uint64_t table_id, const uint64_t column_id, const uint64_t sync_value);
  int do_global_sync(const uint64_t tenant_id, const uint64_t table_id, const uint64_t column_id, TableNode* table_node,
      const bool sync_presync = false, const uint64_t* sync_value = NULL);
  int get_server_set(
      const uint64_t table_id, common::hash::ObHashSet<common::ObAddr>& server_set, const bool get_follower = false);
  // for prefetch or presync
  int set_pre_op_timeout(common::ObTimeoutCtx& ctx);
  template <typename SchemaType>
  int get_schema(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t schema_id,
      const std::function<int(uint64_t, const SchemaType*&)> get_schema_func, const SchemaType*& schema);

private:
  common::ObSmallAllocator node_allocator_;
  common::ObSmallAllocator handle_allocator_;
  common::ObAddr my_addr_;
  common::ObMySQLProxy* mysql_proxy_;
  obrpc::ObSrvRpcProxy* srv_proxy_;
  ObPartitionLocationCache* location_cache_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  storage::ObPartitionService* ps_;
  lib::ObMutex map_mutex_;
  // common::hash::ObHashMap<AutoincKey, TableNode*> node_map_;
  NodeMap node_map_;
  const static int INIT_NODE_MUTEX_NUM = 1024;
  lib::ObMutex init_node_mutex_[INIT_NODE_MUTEX_NUM];
};
}  // end namespace share
}  // end namespace oceanbase
#endif
