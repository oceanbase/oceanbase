/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_HNSW_INDEX_HNSW_INDEX_CACHE_H
#define OCEANBASE_SHARE_HNSW_INDEX_HNSW_INDEX_CACHE_H

#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/vector_index/ob_hnsw.h"

namespace oceanbase {
namespace share {
class ObHNSWIndexBuilder;
class ObHNSWIndexCache {
  friend class ObHNSWIndexRingedCache;
  friend class ObHNSWIndexSwitchableRingedCache;

public:
  ObHNSWIndexCache(int cache_lv, int64_t tenant_id, int64_t pkey_count,
                   common::ObSafeArenaAllocator *allocator,
                   const ObString &index_db_name,
                   const ObString &hnsw_index_table_name,
                   const ObString &vector_column_name)
      : is_inited_(false), cache_lv_(cache_lv), tenant_id_(tenant_id),
        pkey_count_(pkey_count), allocator_(allocator),
        index_db_name_(index_db_name),
        hnsw_index_table_name_(hnsw_index_table_name),
        vector_column_name_(vector_column_name) {}
  ~ObHNSWIndexCache() {}
  int init();
  int reset();
  int get_element_in_neighbor_without_load(ObHNSWNeighbor *nb,
                                           ObHNSWElement *&ele);
  int set_element_for_insert(sqlclient::ObMySQLResult *res, ObHNSWElement *&ele,
                             int64_t ele_max_level);
  int set_deep_copy_element(const ObHNSWElement &other, int skip_cnt,
                            int64_t cache_epoch, ObHNSWElement *&new_ele);

private:
  int get_or_alloc_lv_neighbors(ObHNSWElement *element, int64_t e_lv,
                                ObHNSWLevelNeighbors *&lv_neighbors);
  int load_element(common::ObMySQLTransaction *trans, ObHNSWCPoolPtr pkey_ptr,
                   int skip_cnt, int64_t cache_epoch, ObHNSWElement *&ele);
  int load_ep_element(common::ObMySQLTransaction *trans, ObSqlString &sql,
                      ObHNSWElement *&ele);
  int copy_objs_to_const_pool(sqlclient::ObMySQLResult *res, int64_t start_idx,
                              int64_t end_idx, ObHNSWCPoolPtr &pk_ptr);
  int copy_vector_to_const_pool(sqlclient::ObMySQLResult *res, int64_t idx,
                                common::ObTypeVector &vector);
  int64_t get_element_idx_size() {
    return element_index_.size() * (sizeof(ObString) + sizeof(ObHNSWElement *));
  }
  bool is_inited_;
  int cache_lv_;
  int64_t tenant_id_;
  int64_t pkey_count_;
  common::ObSafeArenaAllocator *allocator_;
  hash::ObHashMap<ObString, ObHNSWElement *, common::hash::NoPthreadDefendMode>
      element_index_;
  const ObString index_db_name_;
  const ObString hnsw_index_table_name_;
  const ObString vector_column_name_;
};

class ObHNSWIndexRingedCache {
  friend class ObHNSWIndexSwitchableRingedCache;

public:
  struct AddrInt {
    AddrInt() { addr_int_ = 0; }
    AddrInt(void *addr) { addr_int_ = reinterpret_cast<uint64_t>(addr); }
    uint64_t hash() const { return addr_int_; }
    int hash(uint64_t &hash_val) const {
      hash_val = addr_int_;
      return OB_SUCCESS;
    }
    int64_t to_string(char *buf, const int64_t buf_len) const {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "%lu", addr_int_);
      return pos;
    }
    bool operator==(const AddrInt &other) const {
      return addr_int_ == other.addr_int_;
    }
    uint64_t addr_int_;
  };
  struct ObHNSWSQLDeleteRow {
    ObHNSWCPoolPtr base_pk_ptr_;
    int64_t level_;
    ObHNSWCPoolPtr ref_pk_ptr_;
    TO_STRING_KV(K_(base_pk_ptr), K_(level), K_(ref_pk_ptr));
  };
  struct ObHNSWSQLInsertRow {
    ObHNSWCPoolPtr base_pk_ptr_;
    int64_t level_;
    ObHNSWCPoolPtr ref_pk_ptr_;
    common::ObTypeVector data_;
    int is_valid_;
    double distance_;
    TO_STRING_KV(K_(base_pk_ptr), K_(level), K_(ref_pk_ptr), K_(is_valid),
                 K_(distance));
  };
  class BatchTransInsertFunctor final {
  public:
    BatchTransInsertFunctor(common::ObMySQLTransaction *trans,
                            ObHNSWIndexRingedCache *ring_cache, int64_t bach_sz)
        : ret_code_(common::OB_SUCCESS), trans_(trans), ring_cache_(ring_cache),
          batch_size_(bach_sz) {}
    ~BatchTransInsertFunctor() {}
    bool operator()(const AddrInt &key, const ObHNSWSQLInsertRow &value);
    int get_ret_code() const { return ret_code_; }

  private:
    int ret_code_;
    common::ObMySQLTransaction *trans_;
    ObHNSWIndexRingedCache *ring_cache_;
    int64_t batch_size_;
  };
  ObHNSWIndexRingedCache(int ring_radius, int64_t tenant_id, int64_t pkey_count,
                         common::ObSafeArenaAllocator *in_ring_allocator,
                         common::ObSafeArenaAllocator *out_ring_allocator,
                         const ObString &index_db_name,
                         const ObString &hnsw_index_table_name,
                         const ObString &vector_column_name,
                         int64_t delete_batch_threshold,
                         int64_t insert_batch_threshold)
      : is_inited_(false), ring_radius_(ring_radius),
        in_ring_cache_(0, tenant_id, pkey_count, in_ring_allocator,
                       index_db_name, hnsw_index_table_name,
                       vector_column_name),
        out_ring_cache_(1, tenant_id, pkey_count, out_ring_allocator,
                        index_db_name, hnsw_index_table_name,
                        vector_column_name) {
    delete_batch_threshold_ = delete_batch_threshold;
    insert_batch_threshold_ = insert_batch_threshold;
  }
  ~ObHNSWIndexRingedCache() {}
  int init(bool insert_mode = false);
  int all_reset(common::ObMySQLTransaction *trans);
  int partial_reset(common::ObMySQLTransaction *trans);
  int get_element_in_neighbor(common::ObMySQLTransaction *trans,
                              ObHNSWNeighbor *nb, int cur_ele_skip_cnt,
                              uint64_t global_cache_epoch, ObHNSWElement *&ele);
  int set_element_for_insert(
      ObHNSWIndexBuilder *idx_builder, sqlclient::ObMySQLResult *res,
      ObHNSWElement *&ele, int64_t ele_max_level, int skip_cnt,
      uint64_t global_cache_epoch,
      common::ObArray<common::ObArray<ObHNSWCandidate>> &selected_neighbors_lv);
  int load_ep_element(common::ObMySQLTransaction *trans, ObSqlString &sql,
                      ObHNSWElement *&ele) {
    return in_ring_cache_.load_ep_element(trans, sql, ele);
  }

private:
  int add_connection_for_inserted_element(ObHNSWElement *element,
                                          common::ObArray<ObHNSWCandidate> &cds,
                                          int64_t lv);
  int update_connection_for_neighbors(ObHNSWIndexBuilder *idx_builder,
                                      int cur_ele_skip_cnt,
                                      uint64_t global_cache_epoch,
                                      ObHNSWElement *element, int64_t lv);
  int get_neighbors(ObHNSWElement *ele, int64_t lv,
                    ObHNSWLevelNeighbors *&neighbors);
  int parse_delete_row_to_delete_where_clause(ObHNSWSQLDeleteRow &delete_row,
                                              ObSqlString &sql);
  int print_vector_to_sql(ObSqlString &sql, const ObTypeVector &vector,
                          bool print_comma);
  int parse_insert_row_to_insert_values(const ObHNSWSQLInsertRow &insert_row,
                                        ObSqlString &sql,
                                        bool first_value_list);
  int do_deletes(common::ObMySQLTransaction *trans,
                 int64_t real_delete_bach_sz);
  int do_inserts(common::ObMySQLTransaction *trans,
                 int64_t real_insert_bach_sz);
  int write_all_updates(common::ObMySQLTransaction *trans);
  bool valid_neighbor_element_ptr(ObHNSWNeighbor *nb, int cur_ele_skip_cnt,
                                  uint64_t global_cache_epoch);
  int get_cache_lv(int skip_cnt) { return skip_cnt > ring_radius_; }
  int init_insert_column_sql_str();
  bool is_escape_connection(int from_skip_cnt, int to_skip_cnt) {
    return 0 == get_cache_lv(from_skip_cnt) && 1 == get_cache_lv(to_skip_cnt);
  }
  int64_t get_update_batch_mem_usage() {
    return (sizeof(AddrInt) + sizeof(ObHNSWSQLInsertRow)) * inserts_v2_.size() +
           sizeof(deletes_) * deletes_.count();
  }
  bool is_inited_;
  int ring_radius_;
  ObHNSWIndexCache in_ring_cache_;
  ObHNSWIndexCache out_ring_cache_;
  int64_t delete_batch_threshold_;
  common::ObArray<ObHNSWSQLDeleteRow> deletes_;
  int64_t insert_batch_threshold_;
  hash::ObHashMap<AddrInt, ObHNSWSQLInsertRow,
                  common::hash::NoPthreadDefendMode>
      inserts_v2_;
  ObSqlString insert_column_sql_str_;
};

class ObHNSWIndexSwitchableRingedCache {
public:
  enum HNSWIndexCacheMode {
    NoThreadDefendMode = 0,
    ThreadSafeMode,
  };
  ObHNSWIndexSwitchableRingedCache(
      int ring_radius, int64_t tenant_id, int64_t pkey_count,
      common::ObSafeArenaAllocator *in_ring_allocator1,
      common::ObSafeArenaAllocator *out_ring_allocator1,
      common::ObSafeArenaAllocator *in_ring_allocator2,
      common::ObSafeArenaAllocator *out_ring_allocator2,
      const ObString &index_db_name, const ObString &hnsw_index_table_name,
      const ObString &vector_column_name, int64_t delete_batch_threshold = 500,
      int64_t insert_batch_threshold = 500)
      : is_inited_(false), cache_mode_(NoThreadDefendMode),
        global_cache_epoch_(0),
        cache1_(ring_radius, tenant_id, pkey_count, in_ring_allocator1,
                out_ring_allocator1, index_db_name, hnsw_index_table_name,
                vector_column_name, delete_batch_threshold,
                insert_batch_threshold),
        cache2_(ring_radius, tenant_id, pkey_count, in_ring_allocator2,
                out_ring_allocator2, index_db_name, hnsw_index_table_name,
                vector_column_name, delete_batch_threshold,
                insert_batch_threshold) {
    caches_[0] = &cache1_;
    caches_[1] = &cache2_;
    cur_cache_id_ = 0;
  }
  ~ObHNSWIndexSwitchableRingedCache() {}
  int init(bool insert_mode);
  int write_all_updates(common::ObMySQLTransaction *trans);
  int all_reset(common::ObMySQLTransaction *trans);
  int prune_out_ring_cache(common::ObMySQLTransaction *trans);
  int get_neighbors(ObHNSWElement *ele, int64_t lv,
                    ObHNSWLevelNeighbors *&neighbors);
  int get_element_in_neighbor(common::ObMySQLTransaction *trans,
                              ObHNSWNeighbor *nb, int cur_ele_skip_cnt,
                              ObHNSWElement *&ele);
  int set_element_for_insert(
      ObHNSWIndexBuilder *idx_builder, sqlclient::ObMySQLResult *res,
      ObHNSWElement *&ele, int64_t ele_max_level, int skip_cnt,
      common::ObArray<common::ObArray<ObHNSWCandidate>> &selected_neighbors_lv);
  int set_new_ep(
      ObHNSWIndexBuilder *idx_builder, sqlclient::ObMySQLResult *res,
      ObHNSWElement *&ele, int64_t ele_max_level,
      common::ObArray<common::ObArray<ObHNSWCandidate>> &selected_neighbors_lv);
  int load_ep_element(common::ObMySQLTransaction *trans, ObSqlString &sql,
                      ObHNSWElement *&ele);
  int64_t get_cur_cache_out_ring_mem_usage() {
    ObHNSWIndexRingedCache *cur_cache = caches_[cur_cache_id_];
    return cur_cache->out_ring_cache_.allocator_->used();
  }
  int64_t get_cur_cache_in_ring_mem_usage() {
    ObHNSWIndexRingedCache *cur_cache = caches_[cur_cache_id_];
    return cur_cache->in_ring_cache_.allocator_->used();
  }
  int64_t get_cur_cache_mem_usage() {
    ObHNSWIndexRingedCache *cur_cache = caches_[cur_cache_id_];
    return cur_cache->out_ring_cache_.allocator_->used() +
           cur_cache->in_ring_cache_.allocator_->used() +
           cur_cache->out_ring_cache_.get_element_idx_size() +
           cur_cache->in_ring_cache_.get_element_idx_size() +
           cur_cache->get_update_batch_mem_usage();
  }
  int64_t get_cur_cache_out_ring_vector_cnt() {
    ObHNSWIndexRingedCache *cur_cache = caches_[cur_cache_id_];
    return cur_cache->out_ring_cache_.element_index_.size();
  }
  void show_cache_info();

private:
  int partial_reset(int cache_idx,
                    common::ObMySQLTransaction *trans); // for switch_cache
  // typedef common::SpinRWLock RWLock;
  // typedef common::SpinRLockGuard  RLockGuard;
  // typedef common::SpinWLockGuard  WLockGuard;
  bool is_inited_;
  HNSWIndexCacheMode cache_mode_;
  uint64_t global_cache_epoch_;
  ObHNSWIndexRingedCache cache1_;
  ObHNSWIndexRingedCache cache2_;
  ObHNSWIndexRingedCache *caches_[2];
  int cur_cache_id_;
};

} // namespace share
} // namespace oceanbase

#endif