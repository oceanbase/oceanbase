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

#ifndef OB_STORAGE_COMPACTION_MEDIUM_COMPACTION_INFO_H_
#define OB_STORAGE_COMPACTION_MEDIUM_COMPACTION_INFO_H_

#include "storage/ob_storage_schema.h"
#include "lib/container/ob_array_array.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace storage
{
class ObTablet;
}
namespace compaction
{

struct ObParallelMergeInfo
{
public:
  ObParallelMergeInfo()
   : parallel_info_(0),
     parallel_end_key_list_(nullptr),
     allocator_(nullptr)
  {}
  ~ObParallelMergeInfo() { destroy(); } // attention!!! use destroy to free memory
  int init(common::ObIAllocator &allocator, const ObParallelMergeInfo &other);
  void destroy();
  bool is_valid() const
  {
    return list_size_ == 0 || nullptr != parallel_end_key_list_;
  }

  // serialize & deserialize
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;

  int generate_from_range_array(
      ObIAllocator &allocator,
      common::ObArrayArray<ObStoreRange> &paral_range);

  int64_t to_string(char* buf, const int64_t buf_len) const;
  static const int64_t MAX_PARALLEL_RANGE_SERIALIZE_LEN = 1 * 1024 * 1024;
  static const int64_t VALID_CONCURRENT_CNT = 1;

  union {
    uint32_t parallel_info_;
    struct {
      uint32_t compat_          : 4;
      uint32_t list_size_       : 8;
      uint32_t reserved_        : 20;
    };
  };
  ObStoreRowkey *parallel_end_key_list_; // concurrent_cnt - 1

  ObIAllocator *allocator_;
};

struct ObMediumCompactionInfo final : public memtable::ObIMultiSourceDataUnit
{
public:
  enum ObCompactionType
  {
    MEDIUM_COMPACTION = 0,
    MAJOR_COMPACTION = 1,
    COMPACTION_TYPE_MAX,
  };
  const static char *ObCompactionTypeStr[];
  const static char *get_compaction_type_str(enum ObCompactionType type);
public:
  ObMediumCompactionInfo();
  virtual ~ObMediumCompactionInfo();

  int init(ObIAllocator &allocator, const ObMediumCompactionInfo &medium_info);
  int save_storage_schema(ObIAllocator &allocator, const storage::ObStorageSchema &storage_schema);
  int gene_parallel_info(
      ObIAllocator &allocator,
      common::ObArrayArray<ObStoreRange> &paral_range);
  static inline bool is_valid_compaction_type(const ObCompactionType type) { return MEDIUM_COMPACTION <= type && type < COMPACTION_TYPE_MAX; }
  static inline bool is_medium_compaction(const ObCompactionType type) { return MEDIUM_COMPACTION == type; }
  static inline bool is_major_compaction(const ObCompactionType type) { return MAJOR_COMPACTION == type; }
  inline bool is_major_compaction() const { return is_major_compaction((ObCompactionType)compaction_type_); }
  inline bool is_medium_compaction() const { return is_medium_compaction((ObCompactionType)compaction_type_); }
  inline void clear_parallel_range()
  {
    parallel_merge_info_.list_size_ = 0;
    parallel_merge_info_.parallel_end_key_list_ = nullptr;
    contain_parallel_range_ = false;
  }

  // ObIMultiSourceDataUnit section
  virtual int deep_copy(const ObIMultiSourceDataUnit *src, ObIAllocator *allocator) override;
  virtual void reset() override;
  virtual bool is_valid() const override;
  virtual inline int64_t get_data_size() const override { return sizeof(ObMediumCompactionInfo); }
  virtual inline memtable::MultiSourceDataUnitType type() const override
  {
    return memtable::MultiSourceDataUnitType::MEDIUM_COMPACTION_INFO;
  }
  virtual int64_t get_version() const override { return medium_snapshot_; }
  virtual bool is_save_last() const override { return false; }
  bool from_cur_cluster() const { return cluster_id_ == GCONF.cluster_id; }

  // serialize & deserialize
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;

  void gene_info(char* buf, const int64_t buf_len, int64_t &pos) const;
  TO_STRING_KV(K_(cluster_id), K_(medium_compat_version), K_(data_version),
      "compaction_type", ObMediumCompactionInfo::get_compaction_type_str((ObCompactionType)compaction_type_),
      "medium_merge_reason", ObAdaptiveMergePolicy::merge_reason_to_str(medium_merge_reason_), K_(cluster_id),
      K_(medium_snapshot), K_(storage_schema),
      K_(contain_parallel_range), K_(parallel_merge_info));
public:
  static const int64_t MEIDUM_COMPAT_VERSION = 1;

private:
  static const int32_t SCS_ONE_BIT = 1;
  static const int32_t SCS_RESERVED_BITS = 49;

public:
  union {
    uint64_t info_;
    struct {
      uint64_t medium_compat_version_           : 4;
      uint64_t compaction_type_                 : 2;
      uint64_t contain_parallel_range_          : SCS_ONE_BIT;
      uint64_t medium_merge_reason_             : 8;
      uint64_t reserved_                        : SCS_RESERVED_BITS;
    };
  };

  uint64_t cluster_id_; // for backup database to throw MEDIUM_COMPACTION clog
  uint64_t data_version_;
  int64_t medium_snapshot_;
  storage::ObStorageSchema storage_schema_;
  ObParallelMergeInfo parallel_merge_info_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_MEDIUM_COMPACTION_INFO_H_
