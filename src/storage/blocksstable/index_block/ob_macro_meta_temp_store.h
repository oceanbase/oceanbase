/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MACRO_META_TEMP_STORE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MACRO_META_TEMP_STORE_H_

#include "storage/blocksstable/ob_macro_block_meta.h"
#include "storage/blocksstable/ob_micro_block_reader_helper.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"
#include "storage/blocksstable/ob_macro_block_reader.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"

namespace oceanbase
{
namespace blocksstable
{

class ObMacroMetaTempStore
{
public:
  friend class ObMacroMetaTempStoreIter;
  ObMacroMetaTempStore();
  virtual ~ObMacroMetaTempStore() { reset(); }

  int init(const int64_t dir_id);
  int append(const char *block_buf, const int64_t block_size, const MacroBlockId &macro_id);
  int append(const ObDataMacroBlockMeta &macro_meta, const ObMicroBlockData *leaf_index_block);
  int wait();
  void reset();
  bool is_valid() const;
  bool is_empty() const;
  TO_STRING_KV(K(io_), K(is_inited_), K(is_empty_));

private:
  struct StoreItem
  {
  public:
    static const int64_t STORE_ITEM_VERSION = 1;

  public:
    struct StoreItemHeader
    {
    public:
      StoreItemHeader() : version_(StoreItem::STORE_ITEM_VERSION), total_length_(0), checksum_(0) {}

    public:
      int32_t version_;
      int32_t total_length_; // total length of StoreItem ( header + data ).
      int64_t checksum_;
    };

  public:
    StoreItem();
    virtual ~StoreItem() {}
    bool is_valid() const;
    void reset();
    int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
    int deserialize(const char *buf, const int64_t data_len, int64_t& pos);
    int64_t get_serialize_size() const;

  public:
    StoreItemHeader header_;
    const char *index_block_buf_;
    int64_t index_block_buf_size_;
    const char *macro_meta_block_buf_;
    int64_t macro_meta_block_size_;
  };

private:
  static int get_macro_block_header(const char *buf, const int64_t buf_size, ObSSTableMacroBlockHeader &macro_header);
  int inner_append(const ObDataMacroBlockMeta &macro_meta, const ObMicroBlockData *leaf_index_block);
  int get_macro_meta_from_block_buf(const ObSSTableMacroBlockHeader &macro_header,
                                    const MacroBlockId &macro_id,
                                    const char *buf,
                                    const int64_t buf_size,
                                    ObDataMacroBlockMeta &macro_meta);

private:
  int64_t dir_id_;
  tmp_file::ObTmpFileIOInfo io_;
  tmp_file::ObTmpFileIOHandle io_handle_;
  ObSelfBufferWriter buffer_;
  ObArenaAllocator allocator_; // micro block reader, lifecycle in get_macro_meta_from_block_buf
  ObArenaAllocator datum_allocator_; // io buffer to temp file, lifecycle in inner_append
  ObArenaAllocator macro_meta_allocator_; // macro meta buffer, lifecycle according to datum_row_
  ObMacroBlockReader macro_reader_;
  ObDatumRow datum_row_;
  bool is_inited_;
  bool is_empty_;
};

class ObMacroMetaTempStoreIter
{
public:
  ObMacroMetaTempStoreIter();
  virtual ~ObMacroMetaTempStoreIter() {}

  void reset();
  int init(ObMacroMetaTempStore &temp_meta_store);
  int get_next(ObDataMacroBlockMeta &macro_meta, ObMicroBlockData &leaf_index_block);

private:
  int try_submit_io();

private:
  typedef ObMacroMetaTempStore::StoreItem StoreItem;
  typedef StoreItem::StoreItemHeader StoreItemHeader;
  static const int64_t META_TEMP_STORE_INITIAL_READ_SIZE = 64 * 1024;

private:
  tmp_file::ObTmpFileIOInfo io_info_;
  int64_t meta_store_file_length_;
  ObArenaAllocator io_allocator_;
  ObMacroMetaTempStore::StoreItem curr_read_item_;
  int64_t submit_io_size_;
  int64_t meta_store_read_offset_;
  int64_t fragment_offset_;
  int64_t fragment_size_;
  char *meta_store_fragment_;
  bool is_iter_end_;
};


} // blocksstable
} // oceanbase

#endif
