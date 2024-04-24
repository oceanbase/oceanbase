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

#ifndef OCEANBASE_SHARE_HNSW_INDEX_HNSW_INDEX_READER_H
#define OCEANBASE_SHARE_HNSW_INDEX_HNSW_INDEX_READER_H

#include "share/schema/ob_table_schema.h"
#include "share/vector_index/ob_hnsw.h"
#include "share/vector_index/ob_hnsw_index_cache.h"

namespace oceanbase {
namespace share {

class ObHNSWIndexReader {
public:
  static const double ann_search_mem_percentage;
  ObHNSWIndexReader(int64_t m, int64_t ef_construction,
                    ObHNSWOption::DistanceFunc dfunc,
                    const ObString &index_table_name,
                    const ObString &index_db_name, int64_t pkey_count,
                    const ObString &vector_column_name, int64_t tenant_id,
                    const ObMemAttr &mem_attr, int ring_radius = 4,
                    int64_t delete_batch_threshold = 500,
                    int64_t insert_batch_threshold = 500)
      : opt_(m, ef_construction, dfunc),
        hnsw_index_table_name_(index_table_name), index_db_name_(index_db_name),
        unsafe_in_ring_allocator1_(mem_attr),
        unsafe_out_ring_allocator1_(mem_attr),
        unsafe_in_ring_allocator2_(mem_attr),
        unsafe_out_ring_allocator2_(mem_attr),
        in_ring_allocator1_(unsafe_in_ring_allocator1_),
        out_ring_allocator1_(unsafe_out_ring_allocator1_),
        in_ring_allocator2_(unsafe_in_ring_allocator2_),
        out_ring_allocator2_(unsafe_out_ring_allocator2_),
        sr_cache_(ring_radius, tenant_id, pkey_count, &in_ring_allocator1_,
                  &out_ring_allocator1_, &in_ring_allocator2_,
                  &out_ring_allocator2_, index_db_name, index_table_name,
                  vector_column_name, delete_batch_threshold,
                  insert_batch_threshold) {
    is_inited_ = false;
    opt_.ml_ = 1.0 / log(1.0 * opt_.m_);
    ep_ = nullptr;
    cur_max_level_ = -1;
    pkey_count_ = pkey_count;
    vector_column_name_.assign_ptr(vector_column_name.ptr(),
                                   vector_column_name.length());
    tenant_id_ = tenant_id;
    vector_dim_ = -1;
    ep_pkeys_for_search_ = nullptr;
  }
  ~ObHNSWIndexReader() {}
  int init();
  int reset();
  int search_vector(common::ObMySQLTransaction *trans, ObTypeVector &q,
                    int64_t k, int64_t ef_search,
                    ObArray<ObHNSWCandidate> &res);
  int init_ep_for_search(common::ObMySQLTransaction *trans);
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

private:
  int get_vector_distance(const ObTypeVector &v1, const ObTypeVector &v2,
                          double &distance);
  int get_ep_with_sql_for_search(common::ObMySQLTransaction *trans);
  int hnsw_search_layer(common::ObMySQLTransaction *trans,
                        common::ObTypeVector &q,
                        common::ObArray<ObHNSWCandidate> &ep, int64_t ef,
                        int64_t lv);
  bool is_inited_;
  ObHNSWOption opt_;
  const ObString hnsw_index_table_name_;
  const ObString index_db_name_;
  // current entry point
  ObHNSWElement *ep_;
  int64_t cur_max_level_;
  int64_t pkey_count_;
  ObString vector_column_name_;
  int64_t tenant_id_;
  int64_t vector_dim_;
  common::ObArray<ObString> *ep_pkeys_for_search_;
  common::ObArenaAllocator unsafe_in_ring_allocator1_;
  common::ObArenaAllocator unsafe_out_ring_allocator1_;
  common::ObArenaAllocator unsafe_in_ring_allocator2_;
  common::ObArenaAllocator unsafe_out_ring_allocator2_;
  common::ObSafeArenaAllocator in_ring_allocator1_;
  common::ObSafeArenaAllocator out_ring_allocator1_;
  common::ObSafeArenaAllocator in_ring_allocator2_;
  common::ObSafeArenaAllocator out_ring_allocator2_;
  ObHNSWIndexSwitchableRingedCache sr_cache_;
  int64_t mem_allow_limit_;
};

} // namespace share
} // namespace oceanbase

#endif