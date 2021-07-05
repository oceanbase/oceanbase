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

namespace oceanbase {
namespace sql {
class ObPhyTableLocationInfo;
class ObPhyPartitionLocationInfo;

typedef common::ObIArray<share::ObPartitionLocation> ObPartitionLocationIArray;
typedef common::ObSEArray<share::ObPartitionLocation, 1> ObPartitionLocationSEArray;

typedef common::ObIArray<share::ObPartitionReplicaLocation> ObPartitionReplicaLocationIArray;
typedef common::ObSEArray<share::ObPartitionReplicaLocation, 1> ObPartitionReplicaLocationSEArray;

class ObSplittedRanges {
  OB_UNIS_VERSION(1);

public:
  const common::ObIArray<common::ObNewRange>& get_ranges() const
  {
    return ranges_;
  }

  common::ObIArray<common::ObNewRange>& get_ranges()
  {
    return const_cast<common::ObIArray<common::ObNewRange>&>(static_cast<const ObSplittedRanges&>(*this).get_ranges());
  }

  int64_t get_offset(int64_t idx) const
  {
    return offsets_.at(idx);
  }

  const common::ObIArray<int64_t>& get_offsets() const
  {
    return offsets_;
  }

  common::ObIArray<int64_t>& get_offsets()
  {
    return const_cast<common::ObIArray<int64_t>&>(static_cast<const ObSplittedRanges&>(*this).get_offsets());
  }

  TO_STRING_KV(K_(ranges), K_(offsets));

private:
  common::ObSEArray<common::ObNewRange, 1> ranges_;
  common::ObSEArray<int64_t, 1> offsets_;
};

typedef common::ObIArray<ObSplittedRanges> ObSplittedRangesIArray;
typedef common::ObSEArray<ObSplittedRanges, 1> ObSplittedRangesSEArray;

enum class ObDuplicateType : int64_t {
  NOT_DUPLICATE = 0,  // non duplicate table
  DUPLICATE,          // normal duplicate table, can choose any replica
  DUPLICATE_IN_DML,   // duplicate table in DML, only can choose leader
};

class ObPhyTableLocation final {
  OB_UNIS_VERSION(1);

public:
  static bool compare_phy_part_loc_info_asc(
      const ObPhyPartitionLocationInfo*& left, const ObPhyPartitionLocationInfo*& right);
  static bool compare_phy_part_loc_info_desc(
      const ObPhyPartitionLocationInfo*& left, const ObPhyPartitionLocationInfo*& right);

public:
  ObPhyTableLocation();
  void reset();
  int assign(const ObPhyTableLocation& other);
  int append(const ObPhyTableLocation& other);
  int assign_from_phy_table_loc_info(const ObPhyTableLocationInfo& other);
  inline bool operator==(const ObPhyTableLocation& other) const
  {
    return table_location_key_ == other.table_location_key_ && ref_table_id_ == other.ref_table_id_;
  }

  inline void set_table_location_key(uint64_t table_location_key, uint64_t ref_table_id)
  {
    table_location_key_ = table_location_key;
    ref_table_id_ = ref_table_id;
  }
  inline uint64_t get_table_location_key() const
  {
    return table_location_key_;
  }
  inline uint64_t get_ref_table_id() const
  {
    return ref_table_id_;
  }

  int add_partition_locations(const ObPhyTableLocationInfo& phy_location_info);
  int add_partition_location(const ObPartitionReplicaLocationIArray& partition_locations);
  int add_partition_location(const share::ObPartitionReplicaLocation& partition_location);
  inline const ObPartitionReplicaLocationIArray& get_partition_location_list() const
  {
    return partition_location_list_;
  }

  inline ObPartitionReplicaLocationIArray& get_partition_location_list()
  {
    return const_cast<ObPartitionReplicaLocationIArray&>(
        (static_cast<const ObPhyTableLocation&>(*this)).get_partition_location_list());
  }

  int64_t get_partition_cnt() const
  {
    return partition_location_list_.count();
  }
  TO_STRING_KV(K_(table_location_key), K_(ref_table_id), K_(partition_location_list), K_(splitted_range_list),
      K_(duplicate_type));
  const share::ObPartitionReplicaLocation* get_part_replic_by_part_id(int64_t part_id) const;
  template <typename SrcArray, typename DstArray>
  int find_not_include_part_ids(const SrcArray& all_part_ids, DstArray& expected_part_ids);
  const ObSplittedRangesIArray& get_splitted_ranges_list() const
  {
    return splitted_range_list_;
  }

  ObSplittedRangesIArray& get_splitted_ranges_list()
  {
    return const_cast<common::ObIArray<ObSplittedRanges>&>(
        static_cast<const ObPhyTableLocation&>(*this).get_splitted_ranges_list());
  }
  const share::ObPartitionReplicaLocation* get_part_replic_by_index(int64_t part_idx) const;
  bool is_duplicate_table() const
  {
    return ObDuplicateType::NOT_DUPLICATE != duplicate_type_;
  }
  bool is_duplicate_table_not_in_dml() const
  {
    return ObDuplicateType::DUPLICATE == duplicate_type_;
  }
  void set_duplicate_type(ObDuplicateType v)
  {
    duplicate_type_ = v;
  }
  ObDuplicateType get_duplicate_type() const
  {
    return duplicate_type_;
  }
  static common::ObPartitionType get_partition_type(const common::ObPartitionKey& pkey,
      const common::ObIArray<ObPhyTableLocation>& table_locations, bool is_retry_for_dup_tbl);

private:
  uint64_t table_location_key_;
  uint64_t ref_table_id_;
  /* locations */

  ObPartitionLocationSEArray part_loc_list_;

  // The following two list has one element for each partition
  ObPartitionReplicaLocationSEArray partition_location_list_;
  ObSplittedRangesSEArray splitted_range_list_;  // corresponding to partition list
  ObDuplicateType duplicate_type_;
};

template <typename SrcArray, typename DstArray>
int ObPhyTableLocation::find_not_include_part_ids(const SrcArray& all_part_ids, DstArray& expected_part_ids)
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
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_OB_PHY_TABLE_LOCATION_ */
