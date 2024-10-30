/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SRC_SHARE_VECTOR_INDEX_OB_TENANT_PQ_CENTER_CACHE_H_
#define SRC_SHARE_VECTOR_INDEX_OB_TENANT_PQ_CENTER_CACHE_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/vector/ob_vector.h"
#include "lib/hash/ob_hashmap.h"
#include "share/schema/ob_table_schema.h"
#include "share/vector_index/ob_tenant_ivf_center_cache.h"

namespace oceanbase
{
namespace share
{
class ObTablePQCenters
{
public:
  ObTablePQCenters()
    : count_(0),
      seg_(OB_DEFAULT_VECTOR_PQ_SEG),
      dis_type_(INVALID_DISTANCE_TYPE),
      seg_center_count_(nullptr),
      pq_centers_(nullptr),
      allocator_()
  {}
  ~ObTablePQCenters() { destroy(); }

  int init(const int64_t tenant_id, const ObArray<common::ObIArray<ObTypeVector *> *> &array);
  int init(const int64_t tenant_id, const int64_t count);
  int add(const int64_t seg_idx, const int64_t center_idx, const ObTypeVector &vector);
  void destroy();
  int64_t get_seg() const { return seg_; }
  int64_t *get_seg_center_count() const { return seg_center_count_; }
  void set_dis_type(const ObVectorDistanceType dis_type) { dis_type_ = dis_type; }
  ObVectorDistanceType get_dis_type() const { return dis_type_; }
  const ObTypeVector &at(const int64_t seg_idx, const int64_t center_idx) const;
  const ObTypeVector &at(const int64_t idx) const;
  TO_STRING_KV(KP_(pq_centers));
private:
  int64_t count_;
  int64_t seg_;
  ObVectorDistanceType dis_type_;
  int64_t *seg_center_count_; // 按seg_idx顺序排列
  ObTypeVector *pq_centers_; // 按seg_idx和center_idx顺序展开排列
  ObArenaAllocator allocator_; // 当前实现centers不改变，因此使用ArenaAllocator
};


class ObTenantPQCenterCache
{
public:
  static int mtl_init(ObTenantPQCenterCache *&pq_center_cache);
  ObTenantPQCenterCache()
    : is_inited_(false),
      allocator_(),
      rwlock_()
  {}
  ~ObTenantPQCenterCache() { destroy(); }

  int init(const int64_t tenant_id);
  void destroy();

  // TODO(@jingshui): 需要考虑回收问题，在读取状态要禁止回收；put的并发问题；put的覆盖写问题
  // 当前只考虑了并发问题
  int put(
      const int64_t table_id,
      const int64_t partition_id,
      const ObVectorDistanceType dis_type,
      const ObArray<common::ObIArray<ObTypeVector *> *> &array);
  int get(const int64_t table_id, const int64_t partition_id, ObTablePQCenters *&centers);
  int drop(const int64_t table_id, const int64_t part_count);
  int convert_to_pq_index_vec(
      ObIAllocator &allocator,
      const ObTypeVector &qvector,
      const int64_t table_id,
      const ObTabletID &tablet_id,
      ObObj &cell);

  static int set_partition_name(
      const int64_t tenant_id,
      const int64_t table_id,
      const common::ObTabletID &tablet_id,
      ObIAllocator &allocator,
      ObString &partition_name,
      int64_t &partition_index);
  TO_STRING_KV(K_(is_inited));
private:
  int get_pq_centers(
      ObTablePQCenters *&pq_centers,
      const uint64_t tenant_id,
      const int64_t table_id,
      const int64_t partition_idx,
      const ObString partition_name,
      const schema::ObTableSchema *index_table_schema,
      const schema::ObTableSchema *second_container_table_schema);
  int create_map_entry(const ObArray<common::ObIArray<ObTypeVector *> *> &array, ObTablePQCenters *&entry);
  int create_map_entry(const int64_t count, ObTablePQCenters *&entry);
  int erase_map_entry(const int64_t table_id, const int64_t part_idx);
private:
  static const int64_t BUCKET_LIMIT = 1000;
  static const int64_t PAGE_SIZE = (1 << 12); // 4KB
  static const int64_t MAX_SIZE = (1 << 31); // 2GB
  typedef common::hash::ObHashMap<ObTableCenterKey, ObTablePQCenters*> TablePQCenterMap;
private:
  bool is_inited_;
  common::ObFIFOAllocator allocator_;
  TablePQCenterMap map_; // key = index_table_id
  common::SpinRWLock rwlock_;
};
} // share
} // oceanbase
#endif
