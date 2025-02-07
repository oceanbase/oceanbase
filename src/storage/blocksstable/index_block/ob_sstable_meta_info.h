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
      bool need_deserialize_header = true) const;

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
    MAX = 4,
  };
  ObMacroIdIterator();
  ~ObMacroIdIterator() { reset(); }
  bool is_valid() const { return is_inited_ && count_ >= 0; }
  int init(const Type type, const MacroBlockId &entry_id, const int64_t pos = 0);
  int init(MacroBlockId *ptr, const int64_t count, const int64_t pos = 0);
  void reset();
  int get_next_macro_id(MacroBlockId &macro_id);
  TO_STRING_KV(K_(value_ptr), K_(pos), K_(count), K_(is_inited));
private:
  MacroBlockId *value_ptr_;
  int64_t pos_;
  int64_t count_;
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
  OB_INLINE int64_t get_data_block_count() const { return data_block_count_; }
  OB_INLINE int64_t get_other_block_count() const { return other_block_count_; }
  OB_INLINE int64_t get_linked_block_count() const { return linked_block_count_; }
  int get_data_block_iter(ObMacroIdIterator &iterator) const;
  int get_other_block_iter(ObMacroIdIterator &iterator) const;
  OB_INLINE int get_linked_block_iter(ObMacroIdIterator &iterator) const
  {
    return iterator.init(linked_block_ids_, linked_block_count_);
  }
  OB_INLINE bool is_meta_root() const
  {
    return is_meta_root_;
  }
  OB_INLINE int64_t get_total_block_cnt() const
  {
    return data_block_count_ + other_block_count_ + linked_block_count_;
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
      int64_t &other_block_count);
  OB_INLINE const ObRootBlockInfo &get_macro_meta_info() const
  {
    return macro_meta_info_;
  }
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
      int64_t &other_block_count);
  int persist_block_ids(
      const ObTabletID &tablet_id,
      const int64_t tablet_transfer_seq,
      const int64_t snapshot_version,
      common::ObArenaAllocator &allocator,
      storage::ObSSTableLinkBlockWriteInfo * const link_write_info,
      ObSharedObjectsWriteCtx &linked_block_write_ctx);
  int write_block_ids(
      const ObTabletID &tablet_id,
      const int64_t tablet_transfer_seq,
      const int64_t snapshot_version,
      storage::ObLinkedMacroBlockItemWriter &writer,
      MacroBlockId &entry_id,
      storage::ObSSTableLinkBlockWriteInfo * const link_write_info) const;
  static int flush_ids(
      const MacroBlockId *blk_ids,
      const int64_t blk_cnt,
      storage::ObLinkedMacroBlockItemWriter &writer);
  int save_linked_block_list(
      const common::ObIArray<MacroBlockId> &list,
      common::ObArenaAllocator &allocator);
  int inc_linked_block_ref_cnt(common::ObArenaAllocator &allocator);
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

private:
  friend class ObSSTable;
  friend class ObSSTableMeta;
  static const int64_t MACRO_INFO_VERSION = 1;
  static const int64_t BLOCK_CNT_THRESHOLD = 15000; // 15000 ids, represents 30G data + metadata
private:
  ObRootBlockInfo macro_meta_info_;
  MacroBlockId *data_block_ids_;
  MacroBlockId *other_block_ids_;
  MacroBlockId *linked_block_ids_;
  int64_t data_block_count_;
  int64_t other_block_count_;
  int64_t linked_block_count_;
  MacroBlockId entry_id_;
  bool is_meta_root_;
  int64_t nested_offset_;
  int64_t nested_size_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableMacroInfo);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SSTABLE_META_INFO_H_ */
