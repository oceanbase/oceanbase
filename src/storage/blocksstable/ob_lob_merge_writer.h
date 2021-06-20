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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOB_MERGE_READER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOB_MERGE_READER_H_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "common/rowkey/ob_rowkey.h"
#include "storage/blocksstable/ob_lob_data_writer.h"

namespace oceanbase {
namespace storage {
class ObSSTable;
}
namespace blocksstable {

class ObLobMergeWriter {
public:
  ObLobMergeWriter();
  virtual ~ObLobMergeWriter();
  void reset();
  int init(const ObMacroDataSeq& macro_start_seq, const ObDataStoreDesc& data_store_desc,
      const common::ObIArray<blocksstable::ObMacroBlockInfoPair>* lob_macro_info_array);
  int overflow_lob_objs(const storage::ObStoreRow& row, const storage::ObStoreRow*& result_row);
  int reuse_lob_macro_blocks(const common::ObStoreRowkey& end_key);
  int skip_lob_macro_blocks(const common::ObStoreRowkey& end_key);
  inline ObMacroBlocksWriteCtx& get_macro_block_write_ctx()
  {
    return block_write_ctx_;
  }
  TO_STRING_KV(K_(orig_lob_macro_blocks), K_(block_write_ctx), K_(macro_start_seq), K_(use_old_macro_block_count));

private:
  static const int64_t DEFAULT_MACRO_BLOCK_NUM = 256;
  typedef common::ObSEArray<MacroBlockId, DEFAULT_MACRO_BLOCK_NUM> MacroBlockArray;
  typedef common::ObSEArray<ObMacroBlockMeta*, DEFAULT_MACRO_BLOCK_NUM> MacroBlockMetaArray;
  typedef common::ObSEArray<ObMacroBlockInfoPair, DEFAULT_MACRO_BLOCK_NUM> MacroBlockInfoArray;
  struct LobCompare {
    LobCompare(const storage::ObStoreRow& row) : row_(row)
    {}
    ~LobCompare()
    {}
    bool operator()(const int64_t left, const int64_t right)
    {
      return row_.row_val_.get_cell(left).get_data_length() > row_.row_val_.get_cell(right).get_data_length();
    }
    const storage::ObStoreRow& row_;
  };

private:
  bool is_valid() const;
  int find_cand_lob_cols(const storage::ObStoreRow& row, ObIArray<int64_t>& lob_col_idxs, int64_t& row_size);
  int write_lob_obj(const common::ObStoreRowkey& rowkey, const int64_t column_id, const ObObj& src_obj, ObObj& dst_obj);
  int slide_lob_macro_blocks(const common::ObStoreRowkey& end_key, const bool reuse);
  int check_lob_macro_block(const MacroBlockId& macro_id, const ObFullMacroBlockMeta& full_meta,
      const common::ObStoreRowkey& rowkey, const int64_t column_id, bool& check_ret);
  int copy_row_(const storage::ObStoreRow& row);

private:
  MacroBlockInfoArray orig_lob_macro_blocks_;
  ObMacroBlocksWriteCtx block_write_ctx_;  // TODO(): fix lob for ofs
  ObLobDataWriter lob_writer_;
  MacroBlockInfoArray::iterator lob_macro_block_iter_;
  const ObDataStoreDesc* data_store_desc_;
  int64_t macro_start_seq_;
  int64_t use_old_macro_block_count_;
  bool is_inited_;
  storage::ObStoreRow result_row_;
  char buffer_[sizeof(common::ObObj) * common::OB_ROW_MAX_COLUMNS_COUNT];
};

}  // end namespace blocksstable
}  // end namespace oceanbase
#endif  // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOB_MERGE_READER_H_
