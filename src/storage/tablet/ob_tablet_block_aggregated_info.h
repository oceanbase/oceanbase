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

#ifndef OCEANBASE_STORAGE_OB_TABLET_BLOCK_AGGREGATED_INFO
#define OCEANBASE_STORAGE_OB_TABLET_BLOCK_AGGREGATED_INFO

#include <stdint.h>
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase
{
namespace storage
{
class ObLinkedMacroBlockItemWriter;
class ObLinkedMacroBlockItemReader;

enum class ObTabletMacroType : int16_t
{
  INVALID_TYPE = 0,
  META_BLOCK = 1,
  DATA_BLOCK = 2,
  SHARED_META_BLOCK = 3,
  SHARED_DATA_BLOCK = 4,
  LINKED_BLOCK = 5,
  MAX

};
struct ObSharedBlockInfo final
{
public:
  ObSharedBlockInfo()
    : shared_macro_id_(), occupy_size_()
  {
  }
  ObSharedBlockInfo(const blocksstable::MacroBlockId &shared_macro_id, const int64_t occupy_size)
    : shared_macro_id_(shared_macro_id), occupy_size_(occupy_size)
  {
  }
  ~ObSharedBlockInfo()
  {
    reset();
  }
  void reset()
  {
    shared_macro_id_.reset();
    occupy_size_ = 0;
  }
  OB_INLINE bool is_valid()
  {
    return shared_macro_id_.is_valid() && occupy_size_ >= 0;
  }
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;
  TO_STRING_KV(K_(shared_macro_id), K_(occupy_size));
public:
  blocksstable::MacroBlockId shared_macro_id_;
  int64_t occupy_size_;
};

struct ObBlockInfoSet
{
public:
  static const int64_t SHARED_BLOCK_BUCKET_NUM = 10;
  static const int64_t EXCLUSIVE_BLOCK_BUCKET_NUM = 10000;
  static const int64_t MAP_EXTEND_RATIO = 2;

  typedef typename common::hash::ObHashSet<blocksstable::MacroBlockId,
                                          common::hash::NoPthreadDefendMode,
                                          common::hash::hash_func<blocksstable::MacroBlockId>,
                                          common::hash::equal_to<blocksstable::MacroBlockId>,
                                          common::hash::SimpleAllocer<common::hash::HashSetTypes<blocksstable::MacroBlockId>::AllocType>,
                                          common::hash::NormalPointer,
                                          oceanbase::common::ObMalloc,
                                          MAP_EXTEND_RATIO> TabletMacroSet;
  typedef typename common::hash::ObHashMap<blocksstable::MacroBlockId,
                                          int64_t,
                                          common::hash::NoPthreadDefendMode,
                                          common::hash::hash_func<blocksstable::MacroBlockId>,
                                          common::hash::equal_to<blocksstable::MacroBlockId>,
                                          common::hash::SimpleAllocer<common::hash::HashMapTypes<blocksstable::MacroBlockId, int64_t>::AllocType>,
                                          common::hash::NormalPointer,
                                          oceanbase::common::ObMalloc,
                                          MAP_EXTEND_RATIO> TabletMacroMap;
  typedef typename TabletMacroSet::const_iterator SetIterator;
  typedef typename TabletMacroMap::const_iterator MapIterator;
public:
  ObBlockInfoSet()
    : meta_block_info_set_(), data_block_info_set_(), backup_block_info_set_(),
    shared_meta_block_info_set_(), clustered_data_block_info_map_()
  {
  }
  ~ObBlockInfoSet()
  {
    meta_block_info_set_.reuse();
    data_block_info_set_.reuse();
    backup_block_info_set_.reuse();
    shared_meta_block_info_set_.reuse();
    clustered_data_block_info_map_.reuse();
  }
  int init(
      const int64_t meta_bucket_num = EXCLUSIVE_BLOCK_BUCKET_NUM,
      const int64_t data_bucket_num = EXCLUSIVE_BLOCK_BUCKET_NUM,
      const int64_t shared_meta_bucket_num = SHARED_BLOCK_BUCKET_NUM,
      const int64_t shared_data_bucket_num = SHARED_BLOCK_BUCKET_NUM);

public:
  TabletMacroSet meta_block_info_set_; // MacroBlockID of small_sstable->addr, other_block & linked_block in normal sstable
  TabletMacroSet data_block_info_set_; // only data block of sstable, not include index_block and meta_block
  TabletMacroSet backup_block_info_set_;
  TabletMacroSet shared_meta_block_info_set_; // MacroBlockID of 
                                              // (sstable [stable.serialize], sstable->addr[->block_id()], 
                                              //  table_store, auto_inc_seq, storage_schema, dump_kvs, medium_info_list)
  TabletMacroMap clustered_data_block_info_map_; // map<macro_id, used_size (sum of nest_size, less than 2MB)>
                                                 // small_sstable->meta->macro_info_->nested_size
};

class ObTabletMacroInfo final
{
friend class ObMacroInfoIterator;
private:
  template <typename T>
  class ObBlockInfoArray final
  {
  public:
    ObBlockInfoArray();
    ~ObBlockInfoArray();
    void reset();
    int reserve(const int64_t cnt, ObArenaAllocator &allocator);
    int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
    int deserialize(ObArenaAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
    int64_t get_serialize_size() const;
    int64_t get_deep_copy_size() const;
    int deep_copy(char *buf, const int64_t buf_len, int64_t &pos, ObBlockInfoArray &dest_obj) const;
    OB_INLINE bool is_valid() const
    {
      return (0 == cnt_ && nullptr == arr_) || (0 < cnt_ && nullptr != arr_);
    }
    TO_STRING_KV(K_(cnt), KP_(arr), K_(capacity));

  public:
    int64_t cnt_;
    T *arr_;

    // no need to be persisted and only used by iterator
    int64_t capacity_;
  };
public:
  ObTabletMacroInfo();
  ~ObTabletMacroInfo();
  void reset();
  int init(
    ObArenaAllocator &allocator, 
    const ObBlockInfoSet &id_set, 
    ObLinkedMacroBlockItemWriter *linked_writer);
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(ObArenaAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;
  int64_t get_deep_copy_size() const;
  int deep_copy(char *buf, const int64_t buf_len, ObTabletMacroInfo *&dest_obj) const;
  bool is_valid() const;
  TO_STRING_KV(
      K_(entry_block),
      K_(meta_block_info_arr),
      K_(data_block_info_arr),
      K_(shared_meta_block_info_arr),
      K_(shared_data_block_info_arr),
      K_(is_inited));
private:
  int construct_block_id_arr(const ObBlockInfoSet::TabletMacroSet &id_set, ObBlockInfoArray<blocksstable::MacroBlockId> &block_id_arr);
  int construct_block_info_arr(const ObBlockInfoSet::TabletMacroMap &block_info_map, ObBlockInfoArray<ObSharedBlockInfo> &block_info_arr);
  int persist_macro_ids(ObArenaAllocator &allocator, ObLinkedMacroBlockItemWriter &linked_writer);
  int do_flush_ids(
      const ObTabletMacroType macro_type,
      ObBlockInfoArray<blocksstable::MacroBlockId> &block_id_arr,
      ObArenaAllocator &allocator,
      ObLinkedMacroBlockItemWriter &linked_writer);
  int do_flush_ids(
      ObBlockInfoArray<ObSharedBlockInfo> &block_info_arr,
      ObArenaAllocator &allocator,
      ObLinkedMacroBlockItemWriter &linked_writer);
private:
  static const int64_t ID_COUNT_THRESHOLD = 50000; // occupy almost 1.2MB disk space
  static const int32_t TABLET_MACRO_INFO_VERSION = 1;
public:
  blocksstable::MacroBlockId entry_block_;
  ObBlockInfoArray<blocksstable::MacroBlockId> meta_block_info_arr_;
  ObBlockInfoArray<blocksstable::MacroBlockId> data_block_info_arr_;
  ObBlockInfoArray<blocksstable::MacroBlockId> shared_meta_block_info_arr_;
  ObBlockInfoArray<ObSharedBlockInfo> shared_data_block_info_arr_;
  bool is_inited_;
};

/**
 * ---------------------------------------ObBlockInfoArray----------------------------------------
 */
template <typename T>
ObTabletMacroInfo::ObBlockInfoArray<T>::ObBlockInfoArray()
  : cnt_(0), arr_(nullptr), capacity_(0)
{
}

template <typename T>
ObTabletMacroInfo::ObBlockInfoArray<T>::~ObBlockInfoArray()
{
  reset();
}

template <typename T>
void ObTabletMacroInfo::ObBlockInfoArray<T>::reset()
{
  cnt_ = 0;
  capacity_ = 0;
  arr_ = nullptr;
}

template <typename T>
int ObTabletMacroInfo::ObBlockInfoArray<T>::reserve(const int64_t cnt, ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (0 == cnt) {
    // no macro id
    arr_ = nullptr;
  } else if (OB_ISNULL(arr_ = reinterpret_cast<T *>(allocator.alloc(sizeof(T) * cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory", K(ret), K(sizeof(T) * cnt));
  }
  if (OB_SUCC(ret)) {
    cnt_ = cnt;
    capacity_ = cnt;
  }
  return ret;
}

template <typename T>
int ObTabletMacroInfo::ObBlockInfoArray<T>::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, cnt_))) {
    STORAGE_LOG(WARN, "fail to encode count", K(ret), K_(cnt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; i++) {
    if (OB_UNLIKELY(!arr_[i].is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "macro block id is invalid", K(ret), K(i), K(arr_[i]));
    } else if (OB_FAIL(arr_[i].serialize(buf, buf_len, pos))) {
      STORAGE_LOG(WARN, "fail to serialize macro block id", K(ret), K(i), KP(buf), K(buf_len), K(pos));
    }
  }
  return ret;
}

template <typename T>
int ObTabletMacroInfo::ObBlockInfoArray<T>::deserialize(ObArenaAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0 || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &cnt_))) {
    STORAGE_LOG(WARN, "fail to decode count", K(ret), K(data_len), K(pos));
  } else if (0 == cnt_) {
    // no macro id
    arr_ = nullptr;
  } else if (OB_UNLIKELY(cnt_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "array count shouldn't be less than 0", K(ret), K_(cnt));
  } else {
    if (OB_ISNULL(arr_ = static_cast<T *>(allocator.alloc(cnt_ * sizeof(T))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory for macro id array", K(ret), K_(cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; i++) {
      if (OB_FAIL(arr_[i].deserialize(buf, data_len, pos))) {
        STORAGE_LOG(WARN, "fail to deserialize macro block id", K(ret), K(data_len), K(pos));
      } else if (OB_UNLIKELY(!arr_[i].is_valid())) {
        STORAGE_LOG(WARN, "deserialized macro id is invalid", K(ret), K(arr_[i]));
      }
    }
  }
  if (OB_FAIL(ret) && nullptr != arr_) {
    allocator.free(arr_);
    reset();
  } else if (OB_SUCC(ret)) {
    capacity_ = cnt_;
  }
  return ret;
}

template <typename T>
int64_t ObTabletMacroInfo::ObBlockInfoArray<T>::get_serialize_size() const
{
  T block_info;
  return serialization::encoded_length_i64(cnt_) + block_info.get_serialize_size() * cnt_;
}

template <typename T>
int64_t ObTabletMacroInfo::ObBlockInfoArray<T>::get_deep_copy_size() const
{
  return sizeof(T) * cnt_;
}

template <typename T>
int ObTabletMacroInfo::ObBlockInfoArray<T>::deep_copy(char *buf, const int64_t buf_len, int64_t &pos, ObBlockInfoArray &dest_obj) const
{
  int ret = OB_SUCCESS;
  const int64_t memory_size = get_deep_copy_size();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || pos < 0 || buf_len - pos < memory_size)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos), K(memory_size));
  } else if (OB_NOT_NULL(arr_) && 0 != cnt_) {
    dest_obj.arr_ = reinterpret_cast<T *>(buf + pos);
    MEMCPY(dest_obj.arr_, arr_, sizeof(T) * cnt_);
  } else {
    dest_obj.arr_ = nullptr;
  }
  if (OB_SUCC(ret)) {
    dest_obj.cnt_ = cnt_;
    dest_obj.capacity_ = capacity_;
    pos += memory_size;
  }
  return ret;
}

} // storage
} // oceanbase

#endif