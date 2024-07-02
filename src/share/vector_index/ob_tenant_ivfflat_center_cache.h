/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SRC_SHARE_VECTOR_INDEX_OB_TENANT_IVFFLAT_CENTER_CACHE_H_
#define SRC_SHARE_VECTOR_INDEX_OB_TENANT_IVFFLAT_CENTER_CACHE_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/vector/ob_vector.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace share
{
class ObTableIvfflatCenters
{
public:
  ObTableIvfflatCenters()
    : count_(0),
      dis_type_(INVALID_DISTANCE_TYPE),
      centers_(nullptr),
      allocator_()
  {}
  ~ObTableIvfflatCenters() { destroy(); }

  int init(const int64_t tenant_id, const common::ObIArray<ObTypeVector *> &array);
  int init(const int64_t tenant_id, const int64_t count);
  int add(const int64_t center_idx, const ObTypeVector &vector);
  void destroy();
  int64_t count() const { return count_; }
  void set_dis_type(const ObVectorDistanceType dis_type) { dis_type_ = dis_type; }
  ObVectorDistanceType get_dis_type() const { return dis_type_; }
  const ObTypeVector &at(const int64_t idx) const;
  TO_STRING_KV(KP_(centers));
private:
  int64_t count_;
  ObVectorDistanceType dis_type_;
  ObTypeVector *centers_; // 按center_idx顺序排列
  ObArenaAllocator allocator_; // 当前实现centers不改变，因此使用ArenaAllocator
};

struct ObTableCenterKey {
  ObTableCenterKey()
    : table_id_(0),
      partition_id_(-1)
  {}
  ObTableCenterKey(const int64_t table_id, const int64_t partition_id)
    : table_id_(table_id),
      partition_id_(partition_id)
  {}
  inline int64_t hash() const
  {
    int64_t hash_value = 0;
    hash_value = common::murmurhash(&table_id_, sizeof(uint64_t), hash_value);
    hash_value = common::murmurhash(&partition_id_, sizeof(uint64_t), hash_value);
    return hash_value;
  }
  inline bool equals(const ObTableCenterKey &trace_id) const
  {
    return table_id_ == trace_id.table_id_ && partition_id_ == trace_id.partition_id_;
  }
  inline bool operator == (const ObTableCenterKey &other) const
  {
    return equals(other);
  }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; };
  TO_STRING_KV(K_(table_id), K_(partition_id));
  int64_t table_id_;
  int64_t partition_id_;
};

class ObTenantIvfflatCenterCache
{
public:
  static int mtl_init(ObTenantIvfflatCenterCache *&ivfflat_center_cache);
  ObTenantIvfflatCenterCache()
    : is_inited_(false),
      allocator_(),
      rwlock_()
  {}
  ~ObTenantIvfflatCenterCache() { destroy(); }

  int init(const int64_t tenant_id);
  void destroy();
  static int decode_centers(
      common::ObIArray<ObTypeVector *> &array,
      ObIAllocator &allocator,
      ObVectorArray &vec_array,
      const int64_t dims,
      const int64_t index);
  int put(const schema::ObTableSchema &table_schema);
  // TODO(@jingshui): 需要考虑回收问题，在读取状态要禁止回收；put的并发问题；put的覆盖写问题
  // 当前只考虑了并发问题
  int put(
      const int64_t table_id,
      const int64_t partition_id,
      const ObVectorDistanceType dis_type,
      const common::ObIArray<ObTypeVector *> &array);
  int get(const int64_t table_id, const int64_t partition_id, ObTableIvfflatCenters *&centers);
  int drop(const int64_t table_id, const int64_t part_count);
  int get_nearest_center(
      const ObTypeVector &qvector,
      const int64_t table_id,
      const ObTabletID &tablet_id,
      ObObj &cell);
    
  static int check_ivfflat_index_table_id_valid(
    const int64_t tenant_id,
    const int64_t ivfflat_index_table_id,
    const int64_t base_table_id
  );

  static int get_partition_index_with_tablet_id(
      const int64_t tenant_id,
      const int64_t table_id,
      const uint64_t tablet_id,
      int64_t &partition_index
  );

  static int set_partition_name(
      const int64_t tenant_id,
      const int64_t table_id,
      const common::ObTabletID &tablet_id,
      ObIAllocator &allocator,
      ObString &partition_name,
      int64_t &partition_index);
  TO_STRING_KV(K_(is_inited));
private:
  int create_map_entry(const common::ObIArray<ObTypeVector *> &array, ObTableIvfflatCenters *&entry);
  int create_map_entry(const int64_t count, ObTableIvfflatCenters *&entry);
  int erase_map_entry(const int64_t table_id, const int64_t part_idx);
private:
  static const int64_t BUCKET_LIMIT = 1000;
  static const int64_t PAGE_SIZE = (1 << 12); // 4KB
  static const int64_t MAX_SIZE = (1 << 31); // 2GB
  typedef common::hash::ObHashMap<ObTableCenterKey, ObTableIvfflatCenters*> TableCenterMap;
private:
  bool is_inited_;
  common::ObFIFOAllocator allocator_;
  TableCenterMap map_; // key = index_table_id
  common::SpinRWLock rwlock_;
};
} // share
} // oceanbase
#endif