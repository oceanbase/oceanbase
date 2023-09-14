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
   : compat_(PARALLEL_INFO_VERSION_V1),
     list_size_(0),
     reserved_(0),
     parallel_store_rowkey_list_(nullptr),
     parallel_datum_rowkey_list_(nullptr),
     allocator_(nullptr)
  {}
  ~ObParallelMergeInfo() { destroy(); } // attention!!! use destroy to free memory
  int init(common::ObIAllocator &allocator, const ObParallelMergeInfo &other);
  void destroy();
  void clear()
  {
    list_size_ = 0;
    parallel_store_rowkey_list_ = nullptr;
    parallel_datum_rowkey_list_ = nullptr;
  }
  int64_t get_size() const { return list_size_; }
  bool is_valid() const
  {
    return list_size_ == 0
      || (PARALLEL_INFO_VERSION_V0 == compat_ && nullptr != parallel_store_rowkey_list_)
      || (PARALLEL_INFO_VERSION_V1 == compat_ && nullptr != parallel_datum_rowkey_list_);
  }

  template<typename T>
  int deep_copy_list(common::ObIAllocator &allocator, const T *src, T *&dst);
  template<typename T>
  void destroy(T *&array);
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
  int deep_copy_datum_rowkey(
    const int64_t idx,
    ObIAllocator &allocator,
    blocksstable::ObDatumRowkey &rowkey) const;
public:
  int64_t to_string(char* buf, const int64_t buf_len) const;
  static const int64_t MAX_PARALLEL_RANGE_SERIALIZE_LEN = 1 * 1024 * 1024;
  static const int64_t VALID_CONCURRENT_CNT = 1;
  static const int64_t PARALLEL_INFO_VERSION_V0 = 0; // StoreRowkey
  static const int64_t PARALLEL_INFO_VERSION_V1 = 1; // DatumRowkey
private:
  int generate_datum_rowkey_list(
    ObIAllocator &allocator,
    ObArrayArray<ObStoreRange> &paral_range);
  int generate_store_rowkey_list(
    ObIAllocator &allocator,
    ObArrayArray<ObStoreRange> &paral_range);

  union {
    uint32_t parallel_info_;
    struct {
      uint32_t compat_          : 4;
      uint32_t list_size_       : 8;
      uint32_t reserved_        : 20;
    };
  };
  // concurrent_cnt - 1; valid when compat_ = PARALLEL_INFO_VERSION_V0
  ObStoreRowkey *parallel_store_rowkey_list_;
  // concurrent_cnt - 1; valid when compat_ = PARALLEL_INFO_VERSION_V1
  blocksstable::ObDatumRowkey *parallel_datum_rowkey_list_;
  ObIAllocator *allocator_;
};


struct ObMediumCompactionInfoKey final
{
public:
  OB_UNIS_VERSION(1);
public:
  ObMediumCompactionInfoKey()
    : medium_snapshot_(0)
  {}
  ObMediumCompactionInfoKey(const ObMediumCompactionInfoKey &other)
    : medium_snapshot_(other.medium_snapshot_)
  {}
  ObMediumCompactionInfoKey(const int64_t medium_snapshot)
    : medium_snapshot_(medium_snapshot)
  {}
  ObMediumCompactionInfoKey &operator=(const ObMediumCompactionInfoKey &other)
  {
    medium_snapshot_ = other.medium_snapshot_;
    return *this;
  }
  ~ObMediumCompactionInfoKey() = default;

  void reset() { medium_snapshot_ = 0; }
  bool is_valid() const { return medium_snapshot_ > 0; }
  ObMediumCompactionInfoKey &operator=(const int64_t medium_snapshot)
  {
    medium_snapshot_ = medium_snapshot;
    return *this;
  }

  bool operator<(const ObMediumCompactionInfoKey& rhs) const
  {
    return medium_snapshot_ < rhs.medium_snapshot_;
  }
  bool operator<=(const ObMediumCompactionInfoKey& rhs) const
  {
    return medium_snapshot_ <= rhs.medium_snapshot_;
  }
  bool operator>(const ObMediumCompactionInfoKey& rhs) const
  {
    return medium_snapshot_ > rhs.medium_snapshot_;
  }
  bool operator>=(const ObMediumCompactionInfoKey& rhs) const
  {
    return medium_snapshot_ >= rhs.medium_snapshot_;
  }
  bool operator==(const ObMediumCompactionInfoKey& rhs) const
  {
    return medium_snapshot_ == rhs.medium_snapshot_;
  }
  bool operator!=(const ObMediumCompactionInfoKey& rhs) const
  {
    return medium_snapshot_ != rhs.medium_snapshot_;
  }
  int64_t get_medium_snapshot() const { return medium_snapshot_; }
  void set_medium_snapshot(const int64_t medium_snapshot) { medium_snapshot_ = medium_snapshot; }

  TO_STRING_KV(K_(medium_snapshot));
private:
  int64_t medium_snapshot_;
};

struct ObMediumCompactionInfo final : public common::ObDLinkBase<ObMediumCompactionInfo>
{
public:
  enum ObCompactionType
  {
    MEDIUM_COMPACTION = 0,
    MAJOR_COMPACTION = 1,
    COMPACTION_TYPE_MAX,
  };
  static const char *ObCompactionTypeStr[];
  static const char *get_compaction_type_str(enum ObCompactionType type);
public:
  ObMediumCompactionInfo();
  ~ObMediumCompactionInfo();

  int assign(ObIAllocator &allocator, const ObMediumCompactionInfo &medium_info);
  int init(ObIAllocator &allocator, const ObMediumCompactionInfo &medium_info);
  int init_data_version();
  int gene_parallel_info(
      ObIAllocator &allocator,
      common::ObArrayArray<ObStoreRange> &paral_range);
  static inline bool is_valid_compaction_type(const ObCompactionType type) { return MEDIUM_COMPACTION <= type && type < COMPACTION_TYPE_MAX; }
  static inline bool is_medium_compaction(const ObCompactionType type) { return MEDIUM_COMPACTION == type; }
  static inline bool is_major_compaction(const ObCompactionType type) { return MAJOR_COMPACTION == type; }
  inline bool is_major_compaction() const { return is_major_compaction((ObCompactionType)compaction_type_); }
  inline bool is_medium_compaction() const { return is_medium_compaction((ObCompactionType)compaction_type_); }
  void clear_parallel_range()
  {
    parallel_merge_info_.clear();
    contain_parallel_range_ = false;
  }
  void reset();
  bool is_valid() const;
  bool from_cur_cluster() const { return cluster_id_ == GCONF.cluster_id && tenant_id_ == MTL_ID(); }
  bool cluster_id_equal() const { return cluster_id_ == GCONF.cluster_id; } // for compat
  bool should_throw_for_standby_cluster() const;
  // serialize & deserialize
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;

  void gene_info(char* buf, const int64_t buf_len, int64_t &pos) const;
  int64_t to_string(char* buf, const int64_t buf_len) const;
public:
  static const int64_t MEDIUM_COMPAT_VERSION = 1;
  static const int64_t MEDIUM_COMPAT_VERSION_V2 = 2; // for add last_medium_snapshot_
  static const int64_t MEDIUM_COMPAT_VERSION_V3 = 3; // for stanby tenant, not throw medium info
private:
  static const int32_t SCS_ONE_BIT = 1;
  static const int32_t SCS_RESERVED_BITS = 32;

public:
  union {
    uint64_t info_;
    struct {
      uint64_t medium_compat_version_           : 4;
      uint64_t compaction_type_                 : 2;
      uint64_t contain_parallel_range_          : SCS_ONE_BIT;
      uint64_t medium_merge_reason_             : 8;
      uint64_t is_schema_changed_               : SCS_ONE_BIT;
      uint64_t tenant_id_                       : 16; // record tenant_id of ls primary_leader, just for throw medium
      uint64_t reserved_                        : SCS_RESERVED_BITS;
    };
  };

  uint64_t cluster_id_; // for backup database to throw MEDIUM_COMPACTION clog
  uint64_t data_version_;
  int64_t medium_snapshot_;
  int64_t last_medium_snapshot_;
  storage::ObStorageSchema storage_schema_;
  ObParallelMergeInfo parallel_merge_info_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMediumCompactionInfo);
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_MEDIUM_COMPACTION_INFO_H_
