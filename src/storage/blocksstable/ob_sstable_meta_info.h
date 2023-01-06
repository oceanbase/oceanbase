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
      common::ObIAllocator *allocator,
      const ObMicroBlockDesMeta &des_meta,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;
  int init_root_block_info(
      common::ObIAllocator *allocator,
      const storage::ObMetaDiskAddr &addr,
      const ObMicroBlockData &block_data);
  int load_root_block_data(const ObMicroBlockDesMeta &des_meta);
  int transform_root_block_data(const ObTableReadInfo &read_info);
  OB_INLINE const storage::ObMetaDiskAddr &get_addr() const { return addr_; }
  OB_INLINE const ObMicroBlockData &get_block_data() const { return block_data_; }
  TO_STRING_KV(K_(addr), K_(block_data), KP_(block_data_allocator));
private:
  static const int64_t ROOT_BLOCK_INFO_VERSION = 1;
  static int read_block_data(
      const storage::ObMetaDiskAddr &addr,
      char *buf,
      const int64_t buf_len);
  int serialize_(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize_(
      common::ObIAllocator *allocator,
      const ObMicroBlockDesMeta &des_meta,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size_() const;

protected:
  storage::ObMetaDiskAddr addr_;
  ObMicroBlockData block_data_;
private:
  common::ObIAllocator *block_data_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObRootBlockInfo);
};

class ObSSTableMacroInfo final
{
public:
  typedef common::ObFixedArray<MacroBlockId, common::ObIAllocator> MacroIdFixedList;
public:
  ObSSTableMacroInfo();
  ~ObSSTableMacroInfo();
  int init_macro_info(
      common::ObIAllocator *allocator,
      const storage::ObTabletCreateSSTableParam &param);
  int load_root_block_data(const ObMicroBlockDesMeta &des_meta);
  bool is_valid() const;
  void reset();
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObIAllocator *allocator,
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
  OB_INLINE const common::ObIArray<MacroBlockId> &get_data_block_ids() const
  {
    return data_block_ids_;
  }
  OB_INLINE const common::ObIArray<MacroBlockId> &get_other_block_ids() const
  {
    return other_block_ids_;
  }
  OB_INLINE const common::ObIArray<MacroBlockId> &get_linked_block_ids() const
  {
    return linked_block_ids_;
  }
  OB_INLINE bool is_meta_root() const
  {
    return is_meta_root_;
  }
  OB_INLINE int64_t get_total_block_cnt() const
  {
    return data_block_ids_.count() + other_block_ids_.count() + linked_block_ids_.count();
  }
  OB_INLINE int64_t get_nested_offset() const
  {
    return nested_offset_;
  }
  OB_INLINE int64_t get_nested_size() const
  {
    return nested_size_;
  }
  DECLARE_TO_STRING;
private:
  int serialize_(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize_(
      common::ObIAllocator *allocator,
      const ObMicroBlockDesMeta &des_meta,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size_() const;
  int read_block_ids(storage::ObLinkedMacroBlockItemReader &reader);
  int write_block_ids(
      storage::ObLinkedMacroBlockItemWriter &writer,
      MacroBlockId &entry_id) const;
  int flush_ids(
      const MacroIdFixedList &blk_ids,
      storage::ObLinkedMacroBlockItemWriter &writer) const;
  int save_linked_block_list(
      const common::ObIArray<MacroBlockId> &list,
      common::ObIArray<MacroBlockId> &linked_list) const;

private:
  friend class ObSSTable;
  friend class ObSSTableMeta;
  static const int64_t MACRO_INFO_VERSION = 1;
  static const int64_t BLOCK_CNT_THRESHOLD = 15000; // 15000 ids, represents 30G data + metadata
private:
  ObRootBlockInfo macro_meta_info_;
  MacroIdFixedList data_block_ids_;
  MacroIdFixedList other_block_ids_;
  MacroIdFixedList linked_block_ids_;
  MacroBlockId entry_id_;
  bool is_meta_root_;
  int64_t nested_offset_;
  int64_t nested_size_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableMacroInfo);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SSTABLE_META_INFO_H_ */
