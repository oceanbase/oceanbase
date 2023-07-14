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
#include "share/ob_gais_client.h"
#include "share/ob_i_global_autoincrement_service.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
}
static const int64_t  TIME_SKEW = 100 * 1000;                 // 100ms, time skew
static const int64_t  PRE_OP_TIMEOUT = 500 * 1000;            // 500ms, for prefetch or presync
static const int64_t  SYNC_OP_TIMEOUT = 1000 * 1000;          // 1000ms, for first sync
static const int      PRE_OP_THRESHOLD = 4;                   // for prefetch or presync
static const int64_t  PARTITION_LOCATION_SET_BUCKET_NUM = 3;
static const int64_t  FETCH_SEQ_NUM_ONCE = 1000;
static const uint64_t AUTO_INC_DEFAULT_NB_MAX_BITS = 16;                                  // from MySQL
static const uint64_t AUTO_INC_DEFAULT_NB_MAX = (1 << AUTO_INC_DEFAULT_NB_MAX_BITS) - 1;  // from MySQL
static const uint64_t AUTO_INC_DEFAULT_NB_ROWS = 1;                                       // from MySQL


struct CacheNode
{
  CacheNode() : cache_start_(0), cache_end_(0) {}

  void reset() { cache_start_ = 0; cache_end_ = 0; }

  TO_STRING_KV(K_(cache_start),
               K_(cache_end));

  // combine two cache node if they are valid and continuous
  // otherwise use new_node if it is valid
  int combine_cache_node(CacheNode &new_node);

  uint64_t cache_start_; // inclusive
  uint64_t cache_end_; // inclusive!
  //uint64_t cache_count_;
};

struct CacheHandle
{
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

  TO_STRING_KV(K_(prefetch_start),
               K_(prefetch_end),
               K_(next_value),
               K_(offset),
               K_(increment),
               K_(max_value));

  // CacheHandle represent value acuquision for one query.
  // when a insert stmt has multiple rows,
  // prefetch_start_ represent the first value for first row, prefetch_end_ for the last row
  uint64_t prefetch_start_;
  uint64_t prefetch_end_;
  //uint64_t prefetch_count_;
  uint64_t next_value_;
  uint64_t offset_;
  uint64_t increment_;
  uint64_t max_value_;
  uint64_t last_value_to_confirm_;
  bool     last_row_dup_flag_;

  int next_value(uint64_t &next_value);
  bool in_range(const uint64_t value) const
  { return ((value >= prefetch_start_) && (value <= prefetch_end_)); }

};

struct TableNode: public common::LinkHashValue<AutoincKey>
{
  TableNode()
    : sync_mutex_(common::ObLatchIds::AUTO_INCREMENT_SYNC_LOCK),
      alloc_mutex_(common::ObLatchIds::AUTO_INCREMENT_ALLOC_LOCK),
      table_id_(0),
      next_value_(0),
      local_sync_(0),
      last_refresh_ts_(common::ObTimeUtility::current_time()),
      prefetching_(false),
      curr_node_state_is_pending_(false),
      autoinc_version_(OB_INVALID_VERSION)
  {}
  virtual ~TableNode()
  {
    destroy();
  }
  int init(int64_t autoinc_table_part_num);

  TO_STRING_KV(KT_(table_id),
               K_(next_value),
               K_(local_sync),
               K_(last_refresh_ts),
               K_(curr_node),
               K_(prefetch_node),
               K_(prefetching),
               K_(autoinc_version));

  int alloc_handle(common::ObSmallAllocator &allocator,
                   const uint64_t offset,
                   const uint64_t increment,
                   const uint64_t desired_count,
                   const uint64_t max_value,
                   CacheHandle *&handle,
                   const bool is_retry_alloc = false);

  bool prefetch_condition()
  {
    //return 0 == prefetch_node_.cache_start_ &&
    //    (next_value_ - curr_node_.cache_start_) * PRE_OP_THRESHOLD > curr_node_.cache_end_ - curr_node_.cache_start_;
    return false;
  }
  void destroy()
  {
  }
  lib::ObMutex sync_mutex_;
  lib::ObMutex alloc_mutex_;
  uint64_t table_id_;
  uint64_t next_value_;
  // local_sync_ semantics：we can make sure that other observer has seen a
  //                        sync value larger than or equal to local_sync.
  // purpose:
  //  If observer has synced with global, we can predicate:
  //  a newly inserted value less than local sync don't need to push to global.
  //  as we can make a good guess that a larger value had already pushed to global by someone else
  uint64_t local_sync_;
  int64_t  last_refresh_ts_;
  CacheNode curr_node_;
  CacheNode prefetch_node_;
  bool prefetching_;
  // we are not sure if curr_node is avaliable.
  // it will become avaliable again after fetch a new node
  // and combine them together.
  // ref:
  bool curr_node_state_is_pending_;
  int64_t  autoinc_version_;
};

// atomic update if greater than origin value
template<typename T>
inline void atomic_update(T &v, T new_v)
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

class ObAutoIncInnerTableProxy
{
public:
  ObAutoIncInnerTableProxy() : mysql_proxy_(nullptr) {}
  ~ObAutoIncInnerTableProxy() {}
  int init(common::ObMySQLProxy *mysql_proxy)
  {
    mysql_proxy_ = mysql_proxy;
    return common::OB_SUCCESS;
  }

  void reset()
  {
    mysql_proxy_ = NULL;
  }

public:
  int next_autoinc_value(const AutoincKey &key,
                         const uint64_t offset,
                         const uint64_t increment,
                         const uint64_t base_value,
                         const uint64_t max_value,
                         const uint64_t desired_count,
                         const int64_t &inner_autoinc_version,
                         uint64_t &start_inclusive,
                         uint64_t &end_inclusive,
                         uint64_t &sync_value );

  int get_autoinc_value(const AutoincKey &key, const int64_t &autoinc_version, uint64_t &seq_value, uint64_t &sync_value);

  int get_autoinc_value_in_batch(const uint64_t tenant_id,
                                 const common::ObIArray<AutoincKey> &keys,
                                 common::hash::ObHashMap<AutoincKey, uint64_t> &seq_values);

  int sync_autoinc_value(const AutoincKey &key,
                         const uint64_t insert_value,
                         const uint64_t max_value,
                         const int64_t autoinc_version,
                         uint64_t &seq_value,
                         uint64_t &sync_value);
private:
  int check_inner_autoinc_version(const int64_t &request_autoinc_version,
                                  const int64_t &inner_autoinc_version,
                                  const AutoincKey &key);
private:
  common::ObMySQLProxy *mysql_proxy_;
};

class ObInnerTableGlobalAutoIncrementService : public ObIGlobalAutoIncrementService
{
public:
  ObInnerTableGlobalAutoIncrementService() {}
  virtual ~ObInnerTableGlobalAutoIncrementService() = default;

  int init(common::ObMySQLProxy *mysql_proxy)
  {
    return inner_table_proxy_.init(mysql_proxy);
  }

  virtual int get_value(
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
      uint64_t &end_inclusive) override;

  virtual int get_sequence_value(const AutoincKey &key, const int64_t &autoinc_version, uint64_t &sequence_value) override;

  virtual int get_auto_increment_values(
      const uint64_t tenant_id,
      const common::ObIArray<AutoincKey> &autoinc_keys,
      const common::ObIArray<int64_t> &autoinc_versions,
      common::hash::ObHashMap<AutoincKey, uint64_t> &seq_values) override;

  // when we push local value to global, we may find in global end that the local value
  // is obsolete. we will piggy back the larger global value to caller via global_sync_value,
  // which will be used to push up sync_value by local
  virtual int local_push_to_global_value(
      const AutoincKey &key,
      const uint64_t max_value,
      const uint64_t value,
      const int64_t &autoinc_version,
      uint64_t &global_sync_value) override;

  virtual int local_sync_with_global_value(const AutoincKey &key, const int64_t &autoinc_version, uint64_t &value) override;
private:
  ObAutoIncInnerTableProxy inner_table_proxy_;
};

class ObRpcGlobalAutoIncrementService : public ObIGlobalAutoIncrementService
{
public:
  ObRpcGlobalAutoIncrementService() : is_inited_(false), gais_request_rpc_proxy_(nullptr) {}
  virtual ~ObRpcGlobalAutoIncrementService() = default;

  int init(const common::ObAddr &addr,
           rpc::frame::ObReqTransport *req_transport);

  virtual int get_value(
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
      uint64_t &end_inclusive) override;

  virtual int get_sequence_value(const AutoincKey &key, const int64_t &autoinc_version, uint64_t &sequence_value) override;

  virtual int get_auto_increment_values(
      const uint64_t tenant_id,
      const common::ObIArray<AutoincKey> &autoinc_keys,
      const common::ObIArray<int64_t> &autoinc_versions,
      common::hash::ObHashMap<AutoincKey, uint64_t> &seq_values) override;

  // when we push local value to global, we may find in global end that the local value
  // is obsolete. we will piggy back the larger global value to caller via global_sync_value,
  // which will be used to push up sync_value by local
  virtual int local_push_to_global_value(
      const AutoincKey &key,
      const uint64_t max_value,
      const uint64_t value,
      const int64_t &autoinc_version,
      uint64_t &global_sync_value) override;

  virtual int local_sync_with_global_value(const AutoincKey &key, const int64_t &autoinc_version, uint64_t &value) override;

  int clear_global_autoinc_cache(const AutoincKey &key);

private:
  bool is_inited_;
  ObGAISClient gais_client_;
  obrpc::ObGAISRpcProxy gais_request_rpc_proxy_;
  ObGAISRequestRpc gais_request_rpc_;
};

class ObAutoincrementService
{
public:
  static const int64_t DEFAULT_TABLE_NODE_NUM = 1024;
//  static const int64_t BATCH_FETCH_COUNT = 1024;
  typedef common::ObLinkHashMap<AutoincKey, TableNode> NodeMap;
public:
  ObAutoincrementService();
  ~ObAutoincrementService();
  static ObAutoincrementService &get_instance();
  int init(common::ObAddr &addr,
           common::ObMySQLProxy *mysql_proxy,
           obrpc::ObSrvRpcProxy *srv_proxy,
           share::schema::ObMultiVersionSchemaService *schema_service,
           rpc::frame::ObReqTransport *req_transport);
  int init_for_backup(common::ObAddr &addr,
                      common::ObMySQLProxy *mysql_proxy,
                      obrpc::ObSrvRpcProxy *srv_proxy,
                      share::schema::ObMultiVersionSchemaService *schema_service,
                      rpc::frame::ObReqTransport *req_transport);
  int get_handle(AutoincParam &param, CacheHandle *&handle);
  void release_handle(CacheHandle *&handle);

  int sync_insert_value_global(AutoincParam &param);

  int sync_insert_value_local(AutoincParam &param);

  int sync_auto_increment_all(const uint64_t tenant_id,
                              const uint64_t table_id,
                              const uint64_t column_id,
                              const uint64_t sync_value);
  // int sync_table_auto_increment(
  //     uint64_t tenant_id,
  //     AutoincKey &key,
  //     uint64_t auto_increment);
  int refresh_sync_value(const obrpc::ObAutoincSyncArg &arg);

  int clear_autoinc_cache_all(const uint64_t tenant_id,
                              const uint64_t table_id,
                              const uint64_t column_id,
                              const bool autoinc_mode_is_order);
  int clear_autoinc_cache(const obrpc::ObAutoincSyncArg &arg);

  int get_sequence_value(const uint64_t tenant_id,
                         const uint64_t table_id,
                         const uint64_t column_id,
                         const bool is_order,
                         const int64_t autoinc_version,
                         uint64_t &seq_value);

  int get_sequence_values(const uint64_t tenant_id,
                          const common::ObIArray<AutoincKey> &order_autokeys,
                          const common::ObIArray<AutoincKey> &noorder_autokeys,
                          const common::ObIArray<int64_t> &order_autoinc_versions,
                          const common::ObIArray<int64_t> &noorder_autoinc_versions,
                          common::hash::ObHashMap<AutoincKey, uint64_t> &seq_values);
  int reinit_autoinc_row(const uint64_t &tenant_id,
                         const uint64_t &table_id,
                         const uint64_t &column_id,
                         const int64_t &autoinc_version,
                         common::ObMySQLTransaction &trans);
  int lock_autoinc_row(const uint64_t &tenant_id,
                       const uint64_t &table_id,
                       const uint64_t &column_id,
                       common::ObMySQLTransaction &trans);
  int reset_autoinc_row(const uint64_t &tenant_id,
                        const uint64_t &table_id,
                        const uint64_t &column_id,
                        const int64_t &autoinc_version,
                        common::ObMySQLTransaction &trans);
  static int calc_next_value(const uint64_t last_next_value,
                             const uint64_t offset,
                             const uint64_t increment,
                             uint64_t &new_next_value);
  static int calc_prev_value(const uint64_t last_next_value,
                             const uint64_t offset,
                             const uint64_t increment,
                             uint64_t &prev_value);
private:
  int get_handle_order(AutoincParam &param, CacheHandle *&handle);
  int get_handle_noorder(AutoincParam &param, CacheHandle *&handle);
  int sync_insert_value_order(AutoincParam &param, CacheHandle *&cache_handle,
                              const uint64_t value_to_sync);
  int sync_insert_value_noorder(AutoincParam &param, CacheHandle *&cache_handle,
                               const uint64_t value_to_sync);

private:
  uint64_t get_max_value(const common::ObObjType type);
  int get_table_node(const AutoincParam &param, TableNode *&table_node);
  int fetch_table_node(const AutoincParam &param,
                       TableNode *table_node,
                       const bool fetch_prefetch = false);
  int fetch_global_sync(const uint64_t tenant_id,
                        const uint64_t table_id,
                        const uint64_t column_id,
                        TableNode &table_node,
                        const bool sync_presync = false);
  int get_server_set(const uint64_t tenant_id,
                     const uint64_t table_id,
                     common::hash::ObHashSet<common::ObAddr> &server_set,
                     const bool get_follower = false);
  int sync_value_to_other_servers(
      AutoincParam &param,
      uint64_t insert_value);

  int try_periodic_refresh_global_sync_value(
      uint64_t tenant_id,
      uint64_t table_id,
      uint64_t column_id,
      TableNode &table_node);

  // align insert value to next cache boundary (end)
  uint64_t calc_next_cache_boundary(
      uint64_t insert_value,
      uint64_t cache_size,
      uint64_t max_value);
  // for prefetch or presync
  int set_pre_op_timeout(common::ObTimeoutCtx &ctx);
  int alloc_autoinc_try_lock(lib::ObMutex &alloc_mutex);

private:
  common::ObSmallAllocator node_allocator_;
  common::ObSmallAllocator handle_allocator_;
  common::ObAddr           my_addr_;
  common::ObMySQLProxy     *mysql_proxy_;
  obrpc::ObSrvRpcProxy     *srv_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObRpcGlobalAutoIncrementService global_autoinc_service_;             // for order increment mode
  ObInnerTableGlobalAutoIncrementService distributed_autoinc_service_; // for noorder increment mode
  lib::ObMutex             map_mutex_;
  //common::hash::ObHashMap<AutoincKey, TableNode*> node_map_;
  NodeMap node_map_;
  const static int INIT_NODE_MUTEX_NUM = 1024;
  lib::ObMutex init_node_mutex_[INIT_NODE_MUTEX_NUM];
};
}//end namespace share
}//end namespace oceanbase
#endif
