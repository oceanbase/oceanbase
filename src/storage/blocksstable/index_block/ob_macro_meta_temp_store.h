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
  int append(
      const char *block_buf,
      const int64_t block_size,
      const MacroBlockId &macro_id);
  int wait();
  void reset();
  TO_STRING_KV(K(is_inited_), K(count_));
private:
  struct StoreItem
  {
    OB_UNIS_VERSION(1);
  public:
    StoreItem();
    virtual ~StoreItem() {}
    bool is_valid() const;
    void reset();

  public:
    MacroBlockId macro_id_;
    ObSSTableMacroBlockHeader macro_header_;
    const char *index_block_buf_;
    int64_t index_block_buf_size_;
    const char *macro_meta_block_buf_;
    int64_t macro_meta_block_size_;
  };
private:
  static int get_macro_block_header(const char *buf, const int64_t buf_size, ObSSTableMacroBlockHeader &macro_header);
private:
  int64_t count_;
  int64_t dir_id_;
  tmp_file::ObTmpFileIOInfo io_;
  tmp_file::ObTmpFileIOHandle io_handle_;
  ObSelfBufferWriter buffer_;
  // write aggregated StoreItems as a self-explained block if memory consumption of this array turned out to be a bottle-neck
  ObArray<int64_t> item_size_arr_;
  bool is_inited_;
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
  int read_next_item();
  int get_macro_meta_from_block_buf(ObDataMacroBlockMeta &macro_meta);
private:
  ObMacroMetaTempStore *meta_store_;
  ObArenaAllocator io_allocator_;
  ObArenaAllocator allocator_;
  ObMicroBlockReaderHelper micro_reader_helper_;
  ObMacroBlockReader macro_reader_;
  ObMacroMetaTempStore::StoreItem curr_read_item_;
  ObDatumRow datum_row_;
  int64_t curr_read_file_pos_;
  int64_t read_item_cnt_;
};


} // blocksstable
} // oceanbase

#endif
