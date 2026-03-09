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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SSTABLE_META_INFO_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SSTABLE_META_INFO_H_

#include "lib/utility/ob_unify_serialize.h"
#include "lib/container/ob_iarray.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"

namespace oceanbase
{
namespace storage
{
class ObLinkedMacroBlockItemReader;
class ObLinkedMacroBlockItemWriter;
struct ObSSTableLinkBlockWriteInfo;
}
namespace blocksstable
{

class ObRootBlockInfo final
{
public:
  ObRootBlockInfo();
  ~ObRootBlockInfo();
  bool is_valid() const;
  void reset();
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObArenaAllocator &allocator,
      const ObMicroBlockDesMeta &des_meta,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;
  int init_root_block_info(
      common::ObArenaAllocator &allocator,
      const storage::ObMetaDiskAddr &addr,
      const ObMicroBlockData &block_data,
      const common::ObRowStoreType row_store_type);
  int load_root_block_data(
      common::ObArenaAllocator &allocator,
      const ObMicroBlockDesMeta &des_meta);
  int transform_root_block_extra_buf(common::ObArenaAllocator &allocator);
  int deep_copy(
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      ObRootBlockInfo &other) const;
  OB_INLINE int64_t get_variable_size() const
  {
    int64_t size = block_data_.total_size();
    if (orig_block_buf_ != nullptr && orig_block_buf_ != block_data_.buf_) {
      size += addr_.size();
    }
    return size;
  }
  OB_INLINE const storage::ObMetaDiskAddr &get_addr() const { return addr_; }
  OB_INLINE const ObMicroBlockData &get_block_data() const { return block_data_; }
  OB_INLINE const char *get_orig_block_buf() const { return orig_block_buf_; }

  TO_STRING_KV(K_(addr), KP_(orig_block_buf), K_(block_data));
private:
  static const int64_t ROOT_BLOCK_INFO_VERSION = 1;
  static int read_block_data(
      const storage::ObMetaDiskAddr &addr,
      char *buf,
      const int64_t buf_len);
  int serialize_(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize_(
      common::ObArenaAllocator &allocator,
      const ObMicroBlockDesMeta &des_meta,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size_() const;
  int transform_cs_encoding_data_buf_(
      common::ObIAllocator *allocator,
      const char *buf,
      const int64_t buf_size,
      const char *&dst_buf,
      int64_t &dst_buf_size);
  int deep_copy_micro_buf(
      const char *src_buf,
      const int64_t src_buf_len,
      char *dst_buf,
      const int64_t dst_buf_len,
      const bool need_deserialize_header) const;

private:
  storage::ObMetaDiskAddr addr_;
  const char *orig_block_buf_; // record the block buf before transform when addr_ is mem addr
  ObMicroBlockData block_data_;
  DISALLOW_COPY_AND_ASSIGN(ObRootBlockInfo);
};

class ObMacroIdIterator final
{
public:
  enum Type : uint8_t
  {
    DATA_BLOCK   = 0,
    OTHER_BLOCK  = 1,
    LINKED_BLOCK = 2,
    MAX = 4,
    NONE = UINT8_MAX,
  };
public:
  ObMacroIdIterator();
  ~ObMacroIdIterator() { reset(); }
  bool is_valid() const
  {
    return is_inited_
           && target_type_ >= Type::DATA_BLOCK
           && target_type_ <= Type::MAX
           && total_count_ >= 0;
  }
  // for non-inline macro info
  int init(const Type type, const MacroBlockId &entry_id);
  // for inline macro info
  int init(const MacroBlockId *ptr, const int64_t count, const Type block_type);
  // for inline macro info
  int init(
    MacroBlockId *data_blk_ids,
    const int64_t data_blk_cnt,
    MacroBlockId *other_blk_ids,
    const int64_t other_blk_cnt);
  void reset();
  int get_next_macro_id(MacroBlockId &macro_id, Type &block_type);
  int get_next_macro_id(MacroBlockId &macro_id);
  int get_block_count(const Type block_type, int64_t &count) const;
  TO_STRING_KV(K_(target_type),
               K(iter_elements_.count()),
               K_(element_idx),
               K_(element_pos),
               K_(iter_cnt),
               K_(total_count),
               K_(is_inited));
public:
  static bool is_block_type_valid(const Type block_type) { return block_type >= Type::DATA_BLOCK && block_type < Type::MAX; }
  static bool is_meta_block_type(const Type block_type) { return block_type == Type::OTHER_BLOCK || block_type == Type::LINKED_BLOCK; }

private:
  struct IterElement final
  {
  public:
    IterElement()
      : value_ptr_(nullptr),
        count_(0),
        block_type_(Type::MAX)
    {
    }
    IterElement(
      const MacroBlockId *value_ptr,
      const int64_t count,
      const Type block_type)
      : value_ptr_(value_ptr),
        count_(count),
        block_type_(block_type)
    {
    }
    OB_INLINE bool is_valid() const
    {
      return nullptr != value_ptr_
             && count_ > 0
             && is_block_type_valid(block_type_);
    }
    TO_STRING_KV(KP_(value_ptr), K_(count), K_(block_type));

  public:
    const MacroBlockId *value_ptr_;
    int64_t count_;
    Type block_type_;
  };

private:
  int get_next_element_(/*out*/const IterElement *&element_ptr);

private:
  Type target_type_;
  ObSEArray<IterElement, Type::MAX> iter_elements_;
  int64_t element_idx_;
  int64_t element_pos_;
  int64_t iter_cnt_;
  int64_t total_count_;
  bool is_inited_;
  common::ObArenaAllocator allocator_;
};

class ObSSTableMacroInfo final
{
public:
  ObSSTableMacroInfo();
  ~ObSSTableMacroInfo();
  int init_macro_info(
      common::ObArenaAllocator &allocator,
      const storage::ObTabletCreateSSTableParam &param);
  int load_root_block_data(
      common::ObArenaAllocator &allocator,
      const ObMicroBlockDesMeta &des_meta);
  bool is_valid() const;
  void reset();
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObArenaAllocator &allocator,
      const ObMicroBlockDesMeta &des_meta,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;
  OB_INLINE const storage::ObMetaDiskAddr &get_macro_meta_addr() const
  {
    return macro_meta_info_.get_addr();
  }
  OB_INLINE const ObMicroBlockData &get_macro_meta_data() const
  {
    return macro_meta_info_.get_block_data();
  }
  OB_INLINE int get_data_block_count(/*out*/int64_t &data_blk_cnt) const
  {
    int64_t other_blk_cnt = -1, linked_blk_cnt = -1;
    return get_block_count(data_blk_cnt, other_blk_cnt, linked_blk_cnt);
  }
  OB_INLINE int get_other_block_count(/*out*/int64_t &other_blk_cnt) const
  {
    int64_t data_blk_cnt = -1, linked_blk_cnt = -1;
    return get_block_count(data_blk_cnt, other_blk_cnt, linked_blk_cnt);
  }
  OB_INLINE int get_linked_block_count(/*out*/int64_t &linked_blk_cnt) const
  {
    int64_t data_blk_cnt = -1, other_blk_cnt = -1;
    return get_block_count(data_blk_cnt, other_blk_cnt, linked_blk_cnt);
  }
  int get_block_count(
    /*out*/int64_t &data_blk_cnt,
    /*out*/int64_t &other_blk_cnt,
    /*out*/int64_t &linked_blk_cnt) const;
  int get_data_block_iter(ObMacroIdIterator &iterator) const;
  int get_other_block_iter(ObMacroIdIterator &iterator) const;
  int get_linked_block_iter(ObMacroIdIterator &iterator) const;
  /// @brief: data blocks + other blocks + linked blocks
  int get_all_block_iter(ObMacroIdIterator &iterator) const;
  OB_INLINE bool is_meta_root() const
  {
    return is_meta_root_;
  }
  OB_INLINE int64_t get_variable_size() const
  {
    int64_t blk_ids_cnt = 0;
    if (OB_NOT_NULL(data_block_ids_)) {
      blk_ids_cnt += data_block_count_;
    }
    if (OB_NOT_NULL(other_block_ids_)) {
      blk_ids_cnt += other_block_count_;
    }
    if (OB_NOT_NULL(linked_block_ids_)) {
      blk_ids_cnt += linked_block_count_;
    }
    return macro_meta_info_.get_variable_size() + sizeof(MacroBlockId) * blk_ids_cnt;
  }
  int deep_copy(
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      ObSSTableMacroInfo &dest) const;
  OB_INLINE int64_t get_nested_offset() const
  {
    return nested_offset_;
  }
  OB_INLINE int64_t get_nested_size() const
  {
    return nested_size_;
  }
  static int read_block_ids(
      const MacroBlockId &entry_id,
      common::ObArenaAllocator &allocator,
      MacroBlockId *&data_block_ids,
      int64_t &data_block_count,
      MacroBlockId *&other_block_ids,
      int64_t &other_block_count,
      MacroBlockId *&linked_blk_ids,
      int64_t &linked_blk_cnt);
  OB_INLINE const ObRootBlockInfo &get_macro_meta_info() const
  {
    return macro_meta_info_;
  }
  int expand_block_ids(common::ObIAllocator &allocator);
  DECLARE_TO_STRING;
private:
  int serialize_(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize_(
      common::ObArenaAllocator &allocator,
      const ObMicroBlockDesMeta &des_meta,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size_() const;
  static int read_block_ids(
      common::ObArenaAllocator &allocator,
      storage::ObLinkedMacroBlockItemReader &reader,
      MacroBlockId *&data_block_ids,
      int64_t &data_block_count,
      MacroBlockId *&other_block_ids,
      int64_t &other_block_count,
      MacroBlockId *&linked_block_ids,
      int64_t &linked_block_count);
  int persist_block_ids(
      common::ObArenaAllocator &allocator,
      const ObLinkedMacroInfoWriteParam &param,
      int64_t &macro_start_seq,
      ObSharedObjectsWriteCtx &linked_block_write_ctx);
  int write_block_ids(
      const ObLinkedMacroInfoWriteParam &param,
      storage::ObLinkedMacroBlockItemWriter &writer,
      MacroBlockId &entry_id,
      int64_t &macro_start_seq) const;
  static int flush_ids(
      const MacroBlockId *blk_ids,
      const int64_t blk_cnt,
      storage::ObLinkedMacroBlockItemWriter &writer);
  static int save_linked_block_list_(
      const common::ObIArray<MacroBlockId> &list,
      common::ObArenaAllocator &allocator,
      /*out*/MacroBlockId *&linked_block_ids,
      /*out*/int64_t &linked_block_count);
  int save_linked_block_list(
      const common::ObIArray<MacroBlockId> &list,
      common::ObArenaAllocator &allocator);
  void dec_linked_block_ref_cnt();
  static int deserialize_block_ids(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos,
      MacroBlockId *&blk_ids,
      int64_t &blk_cnt);
  static int64_t serialize_size_of_block_ids(
      const MacroBlockId *blk_ids,
      const int64_t blk_cnt)
  {
    int64_t len = 0;
    OB_UNIS_ADD_LEN_ARRAY(blk_ids, blk_cnt);
    return len;
  }
  static int load_members_by_entry_block_(
    const MacroBlockId &entry_block_id,
    /*out*/int64_t &data_blk_cnt,
    /*out*/int64_t &other_blk_cnt,
    /*out*/int64_t &linked_blk_cnt);

private:
  friend class ObSSTable;
  friend class ObSSTableMeta;
  static const int64_t MACRO_INFO_VERSION = 1;
  static const int64_t BLOCK_CNT_THRESHOLD = 15000; // 15000 ids, represents 30G data + metadata
private:
  ObRootBlockInfo macro_meta_info_;
  MacroBlockId *data_block_ids_; // always nullptr if entry_id_ != EMPTY_BLOCK_LIST
  MacroBlockId *other_block_ids_; // always nullptr if entry_id_ != EMPTY_BLOCK_LIST
  MacroBlockId *linked_block_ids_;  // always nullptr if entry_id_ != EMPTY_BLOCK_LIST
  int64_t data_block_count_; // call load_members_by_entry_block_ if entry_id != EMPTY_BLOCK_LIST
  int64_t other_block_count_; // call load_members_by_entry_block_ if entry_id != EMPTY_BLOCK_LIST
  int64_t linked_block_count_; // call load_members_by_entry_block_ if entry_id != EMPTY_BLOCK_LIST
  MacroBlockId entry_id_;
  bool is_meta_root_;
  int64_t nested_offset_;
  int64_t nested_size_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableMacroInfo);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SSTABLE_META_INFO_H_ */
