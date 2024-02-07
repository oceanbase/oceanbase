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
    : meta_block_info_set_(), data_block_info_set_(), shared_meta_block_info_set_(), shared_data_block_info_map_()
  {
  }
  ~ObBlockInfoSet()
  {
    meta_block_info_set_.reuse();
    data_block_info_set_.reuse();
    shared_meta_block_info_set_.reuse();
    shared_data_block_info_map_.reuse();
  }
  int init(
      const int64_t meta_bucket_num = EXCLUSIVE_BLOCK_BUCKET_NUM,
      const int64_t data_bucket_num = EXCLUSIVE_BLOCK_BUCKET_NUM,
      const int64_t shared_meta_bucket_num = SHARED_BLOCK_BUCKET_NUM,
      const int64_t shared_data_bucket_num = SHARED_BLOCK_BUCKET_NUM);

public:
  TabletMacroSet meta_block_info_set_;
  TabletMacroSet data_block_info_set_;
  TabletMacroSet shared_meta_block_info_set_;
  TabletMacroMap shared_data_block_info_map_;
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
  int init(ObArenaAllocator &allocator, const ObBlockInfoSet &id_set, ObLinkedMacroBlockItemWriter &linked_writer);
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
} // storage
} // oceanbase

#endif