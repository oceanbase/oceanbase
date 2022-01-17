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

#ifndef OB_PG_SSTABLE_META_BLOCK_WRITER_H_
#define OB_PG_SSTABLE_META_BLOCK_WRITER_H_

#include "blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_pg_meta_block_writer.h"
#include "storage/ob_i_table.h"

namespace oceanbase {
namespace storage {

struct ObPGMacroBlockMetaCheckpointEntry final {
public:
  static const int64_t PG_MACRO_META_ENTRY_VERSION = 1;
  ObPGMacroBlockMetaCheckpointEntry(blocksstable::ObMacroBlockMetaV2& meta)
      : disk_no_(0), macro_block_id_(), table_key_(), meta_(meta)
  {}
  ObPGMacroBlockMetaCheckpointEntry(const int64_t disk_no, const blocksstable::MacroBlockId& macro_block_id,
      const ObITable::TableKey& table_key, blocksstable::ObMacroBlockMetaV2& meta)
      : disk_no_(disk_no), macro_block_id_(macro_block_id), table_key_(table_key), meta_(meta)
  {}
  ~ObPGMacroBlockMetaCheckpointEntry() = default;
  TO_STRING_KV(K_(table_key), K_(macro_block_id), K_(table_key), K_(meta));
  OB_UNIS_VERSION_V(PG_MACRO_META_ENTRY_VERSION);

public:
  int64_t disk_no_;
  blocksstable::MacroBlockId macro_block_id_;
  ObITable::TableKey table_key_;
  blocksstable::ObMacroBlockMetaV2& meta_;
};

class ObPGMacroMeta : public ObIPGMetaItem {
public:
  ObPGMacroMeta();
  virtual ~ObPGMacroMeta() = default;
  void set_meta_entry(ObPGMacroBlockMetaCheckpointEntry& entry);
  virtual int serialize(const char*& buf, int64_t& buf_size) override;
  virtual int16_t get_item_type() const override
  {
    return PG_MACRO_META;
  }

private:
  int extend_buf(const int64_t request_size);

private:
  common::ObArenaAllocator allocator_;
  char* buf_;
  int64_t buf_size_;
  ObPGMacroBlockMetaCheckpointEntry* meta_entry_;
};

class ObPGMacroMetaIterator : public ObIPGMetaItemIterator {
public:
  ObPGMacroMetaIterator();
  virtual ~ObPGMacroMetaIterator() = default;
  int init(ObTablesHandle& tables_handle);
  virtual int get_next_item(ObIPGMetaItem*& item) override;
  void reset();

private:
  bool is_inited_;
  ObTablesHandle tables_handle_;
  ObArray<blocksstable::ObMacroBlockInfoPair> block_infos_;
  int64_t table_idx_;
  int64_t macro_idx_;
  ObPGMacroMeta item_;
  common::ObArenaAllocator allocator_;
  char* buf_;
};

class ObPGMacroMetaCheckpointWriter final {
public:
  ObPGMacroMetaCheckpointWriter();
  ~ObPGMacroMetaCheckpointWriter() = default;
  int init(ObTablesHandle& tables_handle, ObPGMetaItemWriter& writer);
  int write_checkpoint();
  void reset();

private:
  bool is_inited_;
  ObPGMetaItemWriter* writer_;
  ObPGMacroMetaIterator iter_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_PG_SSTABLE_META_BLOCK_WRITER_H_
