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

#ifndef SRC_STORAGE_OB_SSTABLE_MERGE_INFO_MGR_H_
#define SRC_STORAGE_OB_SSTABLE_MERGE_INFO_MGR_H_

#include "common/ob_simple_iterator.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/container/ob_array.h"
#include "ob_sstable.h"

namespace oceanbase {
namespace storage {

struct ObSSTableMergeInfoKey {
public:
  ObSSTableMergeInfoKey() : table_id_(common::OB_INVALID_ID), partition_id_(common::OB_INVALID_ID)
  {}
  ObSSTableMergeInfoKey(const int64_t table_id, const int64_t partition_id)
      : table_id_(table_id), partition_id_(partition_id)
  {}
  virtual ~ObSSTableMergeInfoKey()
  {}
  inline uint64_t hash() const;
  inline bool operator==(const ObSSTableMergeInfoKey& other) const;
  inline bool operator!=(const ObSSTableMergeInfoKey& other) const;
  TO_STRING_KV(K_(table_id), K_(partition_id));
  int64_t table_id_;
  int64_t partition_id_;
};

struct ObSSTableMergeInfoValue {
  ObSSTableMergeInfoValue()
      : major_version_(0),
        minor_version_(0),
        snapshot_version_(0),
        insert_row_count_(0),
        update_row_count_(0),
        delete_row_count_(0),
        ref_cnt_(0)
  {}
  virtual ~ObSSTableMergeInfoValue()
  {}
  inline void reset();
  TO_STRING_KV(K_(major_version), K_(minor_version), K_(snapshot_version), K_(insert_row_count), K_(update_row_count),
      K_(delete_row_count), K_(ref_cnt));
  int32_t major_version_;
  int32_t minor_version_;
  int64_t snapshot_version_;
  int64_t insert_row_count_;
  int64_t update_row_count_;
  int64_t delete_row_count_;
  int64_t ref_cnt_;
};

struct ObTableModificationInfo {
public:
  ObTableModificationInfo()
  {
    reset();
  }
  virtual ~ObTableModificationInfo() = default;
  OB_INLINE void reset()
  {
    table_id_ = OB_INVALID_ID;
    partition_id_ = 0;
    insert_row_count_ = 0;
    update_row_count_ = 0;
    delete_row_count_ = 0;
    max_snapshot_version_ = 0;
  }
  TO_STRING_KV(K_(table_id), K_(partition_id), K_(insert_row_count), K_(update_row_count), K_(delete_row_count),
      K_(max_snapshot_version));

public:
  uint64_t table_id_;
  int64_t partition_id_;
  int64_t insert_row_count_;
  int64_t update_row_count_;
  int64_t delete_row_count_;
  int64_t max_snapshot_version_;
};

class ObSSTableMergeInfoIterator {
public:
  ObSSTableMergeInfoIterator();
  virtual ~ObSSTableMergeInfoIterator();
  int open();
  int get_next_merge_info(ObSSTableMergeInfo& merge_info);
  void reset();

private:
  int64_t major_info_idx_;
  int64_t major_info_cnt_;
  int64_t minor_info_idx_;
  int64_t minor_info_cnt_;
  bool is_opened_;
};

class ObSSTableMergeInfoMgr {
public:
  int init(const int64_t memory_limit = MERGE_INFO_MEMORY_LIMIT);
  void destroy();
  static ObSSTableMergeInfoMgr& get_instance();
  int add_sstable_merge_info(const ObSSTableMergeInfo& merge_info);
  int get_modification_infos(common::ObIArray<ObTableModificationInfo>& infos);

private:
  friend class ObSSTableMergeInfoIterator;
  typedef common::hash::ObHashMap<ObSSTableMergeInfoKey, ObSSTableMergeInfoValue*, common::hash::NoPthreadDefendMode>
      ObSSTableMergeInfoMap;
  ObSSTableMergeInfoMgr();
  virtual ~ObSSTableMergeInfoMgr();
  int alloc_info_array(ObSSTableMergeInfo**& merge_infos, const int64_t array_size);
  void release_info(ObSSTableMergeInfo& merge_info);
  int get_major_info(const int64_t idx, ObSSTableMergeInfo& merge_info);
  int get_minor_info(const int64_t idx, ObSSTableMergeInfo& merge_info);
  static const int64_t MERGE_INFO_MEMORY_LIMIT = 64LL * 1024LL * 1024LL;  // 64MB
  bool is_inited_;
  common::SpinRWLock lock_;
  common::ObArenaAllocator allocator_;
  ObSSTableMergeInfoMap merge_info_map_;
  ObSSTableMergeInfo** major_merge_infos_;
  ObSSTableMergeInfo** minor_merge_infos_;
  common::ObArray<ObSSTableMergeInfoValue*> free_info_values_;
  int64_t major_info_max_cnt_;
  int64_t minor_info_max_cnt_;
  int64_t major_info_cnt_;
  int64_t minor_info_cnt_;
  int64_t major_info_idx_;
  int64_t minor_info_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableMergeInfoMgr);
};

/**
 * ----------------------------------------------------------INLINE
 * FUNCTION---------------------------------------------------------------
 */

inline uint64_t ObSSTableMergeInfoKey::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&table_id_, sizeof(int64_t), 0);
  hash_ret = common::murmurhash(&partition_id_, sizeof(int64_t), hash_ret);
  return hash_ret;
}

inline bool ObSSTableMergeInfoKey::operator==(const ObSSTableMergeInfoKey& other) const
{
  return (table_id_ == other.table_id_) && (partition_id_ == other.partition_id_);
}

inline bool ObSSTableMergeInfoKey::operator!=(const ObSSTableMergeInfoKey& other) const
{
  return !(*this == other);
}

inline void ObSSTableMergeInfoValue::reset()
{
  major_version_ = 0;
  minor_version_ = 0;
  snapshot_version_ = 0;
  delete_row_count_ = 0;
  insert_row_count_ = 0;
  update_row_count_ = 0;
  ref_cnt_ = 0;
}

}  // namespace storage
}  // namespace oceanbase

#endif /* SRC_STORAGE_OB_SSTABLE_MERGE_INFO_MGR_H_ */
