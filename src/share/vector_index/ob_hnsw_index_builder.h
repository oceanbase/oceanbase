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

#ifndef OCEANBASE_SHARE_HNSW_INDEX_HNSW_INDEX_BUILDER_H
#define OCEANBASE_SHARE_HNSW_INDEX_HNSW_INDEX_BUILDER_H

#include "share/schema/ob_table_schema.h"
#include "share/vector_index/ob_hnsw.h"
#include "share/vector_index/ob_hnsw_index_cache.h"
#include <random>

namespace oceanbase {
namespace share {

class ObHNSWIndexBuilder {
public:
  struct ObHNSWPruneCandidate {
    int64_t idx_;
    ObHNSWElement *ele_;
    double distance_;
    TO_STRING_KV(K_(idx), K_(distance));
    bool operator<(const ObHNSWPruneCandidate &cd) const {
      return distance_ < cd.distance_;
    }
    bool operator>(const ObHNSWPruneCandidate &cd) const {
      return distance_ > cd.distance_;
    }
  };
  ObHNSWIndexBuilder(
      int64_t m, int64_t ef_construction, ObHNSWOption::DistanceFunc dfunc,
      const ObString &index_table_name, const ObString &index_db_name,
      common::ObArenaAllocator *allocator, int64_t pkey_count,
      const ObString &vector_column_name, common::ObMySQLTransaction *trans,
      int64_t tenant_id, common::ObSafeArenaAllocator *in_ring_allocator1,
      common::ObSafeArenaAllocator *out_ring_allocator1,
      common::ObSafeArenaAllocator *in_ring_allocator2,
      common::ObSafeArenaAllocator *out_ring_allocator2, int ring_radius = 3,
      int64_t delete_batch_threshold = 500,
      int64_t insert_batch_threshold = 500, size_t random_seed = 100)
      : opt_(m, ef_construction, dfunc),
        hnsw_index_table_name_(index_table_name), index_db_name_(index_db_name),
        allocator_(allocator),
        sr_cache_(ring_radius, tenant_id, pkey_count, in_ring_allocator1,
                  out_ring_allocator1, in_ring_allocator2, out_ring_allocator2,
                  index_db_name, index_table_name, vector_column_name,
                  delete_batch_threshold, insert_batch_threshold) {
    is_inited_ = false;
    opt_.ml_ = 1.0 / log(opt_.m_);
    ep_ = nullptr;
    cur_max_level_ = -1;
    pkey_count_ = pkey_count;
    vector_column_name_.assign_ptr(vector_column_name.ptr(),
                                   vector_column_name.length());
    trans_ = trans;
    tenant_id_ = tenant_id;
    vector_dim_ = -1;
    lv_idx_ = 0;
    level_generator_.seed(random_seed);
  }
  ~ObHNSWIndexBuilder() {}
  int init(bool insert_mode = true);
  int reset();
  int write_all_updates();
  int insert_vector(sqlclient::ObMySQLResult *res);
  int delete_vector(sqlclient::ObMySQLResult *res);
  int64_t get_tenant_id() const { return tenant_id_; }
  void set_hnsw_vector_dim(int64_t hnsw_dim) { vector_dim_ = hnsw_dim; }
  int64_t get_hnsw_vector_dim() const { return vector_dim_; }
  void set_max_ep_level(int64_t hnsw_entry_level) {
    cur_max_level_ = hnsw_entry_level;
  }
  int64_t get_max_ep_level() const { return cur_max_level_; }
  const ObHNSWElement *get_ep() const { return ep_; }
  void set_ep_pkeys_for_search(common::ObArray<ObString> *rowkeys) {
    OB_ASSERT(rowkeys->count() == pkey_count_);
    ep_pkeys_for_search_ = rowkeys;
  }
  common::ObMySQLTransaction *get_trans() { return trans_; }
  ObHNSWOption get_opt() { return opt_; }
  int prune_connections(ObHNSWIndexRingedCache *ringed_cache,
                        uint64_t global_cache_epoch,
                        ObHNSWLevelNeighbors *lv_nbs, int64_t lv,
                        ObHNSWElement *new_cand_e, double distance,
                        int cur_skip_cnt, int64_t &prune_idx, int64_t debug_lv);
  int64_t get_out_ring_cache_vector_cnt() {
    return sr_cache_.get_cur_cache_out_ring_vector_cnt();
  }
  int64_t get_cache_mem_usage() { return sr_cache_.get_cur_cache_mem_usage(); }
  void show_cache_info() { sr_cache_.show_cache_info(); }

private:
  int get_vector_distance_with_cache(ObHNSWElement *e1, ObHNSWElement *e2,
                                     int64_t lv, double &distance);
  int get_vector_distance(const ObTypeVector &v1, const ObTypeVector &v2,
                          double &distance);
  int check_element_closerV2(ObHNSWPruneCandidate cd,
                             common::ObArray<ObHNSWPruneCandidate> &r,
                             int64_t lv, bool &is_closer);
  int hnsw_select_neighbors_simple(
      common::ObArray<ObHNSWCandidate> &cds, int64_t m,
      common::ObArray<ObHNSWCandidate> &selected_neighbors,
      int64_t &min_skip_cnt);
  int hnsw_search_layer(common::ObTypeVector &q,
                        common::ObArray<ObHNSWCandidate> &ep, int64_t ef,
                        int64_t lv);
  int hnsw_insert_elementV2(common::ObTypeVector &q, int64_t e_lv,
                            ObHNSWElement *ep, sqlclient::ObMySQLResult *res,
                            ObHNSWElement *&ele);
  int hnsw_get_or_alloc_lv_neighbors(ObHNSWElement *element, int64_t e_lv,
                                     ObHNSWLevelNeighbors *&lv_neighbors);
  int copy_objs_to_const_pool(sqlclient::ObMySQLResult *res, int64_t start_idx,
                              int64_t end_idx, ObHNSWCPoolPtr &pk_ptr);
  int borrow_vector_from_mysql_res(sqlclient::ObMySQLResult *res, int64_t idx,
                                   common::ObTypeVector &vector);
  int copy_vector_to_const_pool(sqlclient::ObMySQLResult *res, int64_t idx,
                                common::ObTypeVector &vector);
  bool is_inited_;
  ObHNSWOption opt_;
  const ObString hnsw_index_table_name_;
  const ObString index_db_name_;
  common::ObArenaAllocator *allocator_;
  // current entry point
  ObHNSWElement *ep_;
  int64_t cur_max_level_;
  int64_t pkey_count_;
  hash::ObHashMap<ObString, ObHNSWElement *, common::hash::NoPthreadDefendMode>
      element_index_v2_; // move to cache
  ObString vector_column_name_;
  common::ObMySQLTransaction *trans_;
  int64_t tenant_id_;
  int64_t vector_dim_;
  common::ObArray<ObString> *ep_pkeys_for_search_;
  ObHNSWIndexSwitchableRingedCache sr_cache_;
  int64_t lv_idx_;
  std::default_random_engine level_generator_;
};

} // namespace share
} // namespace oceanbase

#endif