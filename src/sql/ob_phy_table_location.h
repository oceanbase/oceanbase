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

#ifndef OCEANBASE_SQL_OB_PHY_TABLE_LOCATION_
#define OCEANBASE_SQL_OB_PHY_TABLE_LOCATION_

#include "common/ob_range.h"
#include "share/partition_table/ob_partition_location.h"

namespace oceanbase
{
namespace sql
{
class ObCandiTableLoc;
class ObCandiTabletLoc;

typedef common::ObIArray<share::ObPartitionLocation> ObPartitionLocationIArray;
typedef common::ObSEArray<share::ObPartitionLocation, 1> ObPartitionLocationSEArray;

typedef common::ObIArray<share::ObPartitionReplicaLocation> ObPartitionReplicaLocationIArray;
typedef common::ObSEArray<share::ObPartitionReplicaLocation, 1> ObPartitionReplicaLocationSEArray;

enum class ObDuplicateType : int64_t
{
  NOT_DUPLICATE = 0, //非复制表
  DUPLICATE,         //复制表, 可以选择任意副本
  DUPLICATE_IN_DML,  //被DML更改的复制表, 此时只能选leader副本
};

class ObPhyTableLocation final
{
  OB_UNIS_VERSION(1);
public:
  static bool compare_phy_part_loc_info_asc(const ObCandiTabletLoc *&left,
                                            const ObCandiTabletLoc *&right);
  static bool compare_phy_part_loc_info_desc(const ObCandiTabletLoc *&left,
                                             const ObCandiTabletLoc *&right);
public:
  ObPhyTableLocation();
  void reset();
  int assign(const ObPhyTableLocation &other);
  int append(const ObPhyTableLocation &other);
  int assign_from_phy_table_loc_info(const ObCandiTableLoc &other);
  inline bool operator== (const ObPhyTableLocation &other) const
  {
    return table_location_key_ == other.table_location_key_ &&  ref_table_id_ == other.ref_table_id_;
  }

  inline void set_table_location_key(uint64_t table_location_key, uint64_t ref_table_id)
  {
    table_location_key_ = table_location_key;
    ref_table_id_ = ref_table_id;
  }
  inline uint64_t get_table_location_key() const { return table_location_key_; }
  inline uint64_t get_ref_table_id() const { return ref_table_id_; }

  int add_partition_locations(const ObCandiTableLoc &phy_location_info);
  int add_partition_location(const ObPartitionReplicaLocationIArray &partition_locations);
  int add_partition_location(const share::ObPartitionReplicaLocation &partition_location);
  inline const ObPartitionReplicaLocationIArray &get_partition_location_list() const
  {
    return partition_location_list_;
  }
  // 危险的接口
  inline ObPartitionReplicaLocationIArray &get_partition_location_list()
  {
    return const_cast<ObPartitionReplicaLocationIArray &>
       ((static_cast<const ObPhyTableLocation&>(*this)).get_partition_location_list());
  }

  int64_t get_partition_cnt() const { return partition_location_list_.count(); }
  TO_STRING_KV(K_(table_location_key),
               K_(ref_table_id),
               K_(partition_location_list),
               K_(duplicate_type));
  const share::ObPartitionReplicaLocation *get_part_replic_by_part_id(int64_t part_id) const;
  template<typename SrcArray, typename DstArray>
  int find_not_include_part_ids(const SrcArray &all_part_ids, DstArray &expected_part_ids);
  int erase_partition_location(int64_t partition_id);
  const share::ObPartitionReplicaLocation *get_part_replic_by_index(int64_t part_idx) const;
  int get_location_idx_by_part_id(int64_t part_id, int64_t &loc_idx) const;
  bool is_duplicate_table() const { return ObDuplicateType::NOT_DUPLICATE != duplicate_type_; }
  bool is_duplicate_table_not_in_dml() const { return ObDuplicateType::DUPLICATE == duplicate_type_; }
  void set_duplicate_type(ObDuplicateType v) { duplicate_type_ = v; }
  ObDuplicateType get_duplicate_type() const { return duplicate_type_; }
private:
  int try_build_location_idx_map();
private:
  /* 用于表ID(可能是generated alias id)寻址location */
  uint64_t table_location_key_;
  /* 用于获取实际的物理表ID */
  uint64_t ref_table_id_;

  // The following two list has one element for each partition
  ObPartitionReplicaLocationSEArray partition_location_list_;
  ObDuplicateType duplicate_type_;

  // for lookup performance
  static const int FAST_LOOKUP_LOC_IDX_SIZE_THRES = 3;
  common::hash::ObHashMap<int64_t, int64_t, common::hash::NoPthreadDefendMode> location_idx_map_;
};

template<typename SrcArray, typename DstArray>
int ObPhyTableLocation::find_not_include_part_ids(const SrcArray &all_part_ids,
                                                  DstArray &expected_part_ids)
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < all_part_ids.count(); ++i) {
    if (OB_ISNULL(get_part_replic_by_part_id(all_part_ids.at(i)))) {
      if (OB_FAIL(expected_part_ids.push_back(all_part_ids.at(i)))) {
        SQL_LOG(WARN, "push back not included partition id failed", K(ret));
      }
    }
  }
  return ret;
}

class ObPhyTableLocationGuard
{
public:
  ObPhyTableLocationGuard() : loc_(nullptr) {};
  ~ObPhyTableLocationGuard()
  {
    if (loc_) {
      loc_->~ObPhyTableLocation();
      loc_ = nullptr;
    }
  }
  int new_location(common::ObIAllocator &allocator)
  {
    int ret = common::OB_SUCCESS;
    void *buf = nullptr;
    if (OB_NOT_NULL(loc_)) {
      // init twice
      ret = common::OB_ERR_UNEXPECTED;
    } else if (nullptr == (buf = allocator.alloc(sizeof(ObPhyTableLocation)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else if (NULL == (loc_ = new(buf)ObPhyTableLocation())) {
      ret = common::OB_ERR_UNEXPECTED;
    }
    return ret;
  }
  // caller must ensure that the loc_ is not NULL before call get_loc()
  ObPhyTableLocation *get_loc() { return loc_; }
private:
  ObPhyTableLocation *loc_;
};

}
}
#endif /* OCEANBASE_SQL_OB_PHY_TABLE_LOCATION_ */
