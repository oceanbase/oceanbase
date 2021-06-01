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

#define USING_LOG_PREFIX STORAGE
#include "ob_lob_merge_writer.h"
#include "lib/container/ob_array_iterator.h"
#include "storage/ob_sstable.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
using namespace storage;

namespace blocksstable {

ObLobMergeWriter::ObLobMergeWriter()
    : orig_lob_macro_blocks_(),
      block_write_ctx_(),
      lob_writer_(),
      lob_macro_block_iter_(),
      data_store_desc_(NULL),
      macro_start_seq_(-1),
      use_old_macro_block_count_(0),
      is_inited_(false)
{}

ObLobMergeWriter::~ObLobMergeWriter()
{}

void ObLobMergeWriter::reset()
{
  orig_lob_macro_blocks_.reset();
  block_write_ctx_.reset();
  lob_writer_.reset();
  data_store_desc_ = NULL;
  macro_start_seq_ = -1;
  use_old_macro_block_count_ = 0;
  result_row_.reset();
  is_inited_ = false;
}

int ObLobMergeWriter::init(const ObMacroDataSeq& macro_start_seq, const ObDataStoreDesc& data_store_desc,
    const ObIArray<ObMacroBlockInfoPair>* org_lob_macro_info_array)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObLobMergeWriter init twice", K(ret));
  } else if (OB_UNLIKELY(!macro_start_seq.is_valid() || !data_store_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments to init ObLobMergeWriter", K(macro_start_seq), K(data_store_desc), K(ret));
  } else if (OB_NOT_NULL(org_lob_macro_info_array) && org_lob_macro_info_array->count() > 0) {
    if (OB_FAIL(orig_lob_macro_blocks_.assign(*org_lob_macro_info_array))) {
      STORAGE_LOG(WARN, "Failed to get old lob macro blocks from sstable", K(ret));
    } else {
      lob_macro_block_iter_ = orig_lob_macro_blocks_.begin();
    }
  }
  if (OB_SUCC(ret)) {
    ObMacroDataSeq lob_data_seq = macro_start_seq;
    ObStorageFileHandle file_handle;
    lob_data_seq.set_lob_block();
    if (OB_FAIL(lob_writer_.init(data_store_desc, lob_data_seq.get_data_seq()))) {
      STORAGE_LOG(WARN, "Failed to init lob data writer", K(ret));
    } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, block_write_ctx_.file_ctx_))) {
      LOG_WARN("Failed to init file ctx", K(ret));
    } else if (OB_FAIL(file_handle.assign(data_store_desc.file_handle_))) {
      STORAGE_LOG(WARN, "Failed to assign file handle", K(ret));
    } else if (!block_write_ctx_.file_handle_.is_valid() &&
               OB_FAIL(block_write_ctx_.file_handle_.assign(data_store_desc.file_handle_))) {
      STORAGE_LOG(WARN, "Failed to assign file handle", K(ret));
    } else {
      macro_start_seq_ = lob_data_seq.get_data_seq();
      use_old_macro_block_count_ = 0;
      data_store_desc_ = &data_store_desc;
      is_inited_ = true;
    }
  }

  return ret;
}

bool ObLobMergeWriter::is_valid() const
{
  bool ret = true;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = false;
    STORAGE_LOG(WARN, "ObLobMergeWriter is not inited", K(ret));
  } else if (OB_ISNULL(data_store_desc_) || macro_start_seq_ < 0) {
    ret = false;
    STORAGE_LOG(WARN, "Unexpected inner status in ObLobMergeWriter", KP_(data_store_desc), K_(macro_start_seq), K(ret));
  }

  return ret;
}

int ObLobMergeWriter::find_cand_lob_cols(const ObStoreRow& row, ObIArray<int64_t>& lob_col_idxs, int64_t& row_size)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> sort_lob_col_idxs;

  if (!row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments to overflow lob objects", K(row), K(ret));
  } else {
    row_size = 0;
    lob_col_idxs.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < row.row_val_.get_count(); i++) {
      const ObObj& obj = row.row_val_.get_cell(i);
      row_size += obj.get_data_length();
      if (!ob_is_large_text(obj.get_type())) {
        // tinytext should always inline
      } else if (obj.is_lob_outrow()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "[LOB] Unexcepeted outrow lob object, temporarily skip it", K(obj), K(ret));
      } else if (obj.get_data_length() < OB_MAX_LOB_HANDLE_LENGTH) {
        // bypass the unlikey overflow lob obj
      } else if (OB_FAIL(sort_lob_col_idxs.push_back(i))) {
        STORAGE_LOG(WARN, "Failed to push back lob column", K(i), K(obj), K(ret));
      } else {
        STORAGE_LOG(DEBUG,
            "[LOB] lob column candidate to process outrow",
            "obj_scale",
            obj.get_scale(),
            "obj_meta",
            obj.get_meta(),
            K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (row_size > OB_MAX_USER_ROW_LENGTH) {
      STORAGE_LOG(DEBUG, "[LOB] candidate lob column to reduce row size", K(sort_lob_col_idxs), K(ret));
      int64_t overflow_size = row_size - OB_MAX_USER_ROW_LENGTH;
      int64_t overflow_idx = 0;
      LobCompare lob_cmp(row);
      std::sort(sort_lob_col_idxs.begin(), sort_lob_col_idxs.end(), lob_cmp);
      for (; OB_SUCC(ret) && overflow_size > 0 && overflow_idx < sort_lob_col_idxs.count(); overflow_idx++) {
        int64_t idx = sort_lob_col_idxs.at(overflow_idx);
        overflow_size -= row.row_val_.get_cell(idx).get_data_length() - OB_MAX_LOB_HANDLE_LENGTH;
        STORAGE_LOG(DEBUG,
            "[LOB] add candidate lob column to bit set",
            K(idx),
            K(overflow_size),
            "lob_obj",
            row.row_val_.get_cell(idx),
            K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (overflow_size > 0) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN,
            "Unexpected row which has no more lob column to reduce row size",
            K(row_size),
            K(overflow_size),
            K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < overflow_idx; i++) {
          if (OB_FAIL(lob_col_idxs.push_back(sort_lob_col_idxs.at(i)))) {
            STORAGE_LOG(WARN, "Failed to push back lob column", K(i), "lob_col_idx", sort_lob_col_idxs.at(i), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObLobMergeWriter::overflow_lob_objs(const ObStoreRow& row, const ObStoreRow*& result_row)
{
  int ret = OB_SUCCESS;

  result_row = nullptr;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Invalid status of ObLobMergeWriter", K(ret));
  } else if (!row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments to overflow lob objects", K(row), K(ret));
  } else if (!row.is_sparse_row_ && row.row_val_.get_count() != data_store_desc_->row_column_count_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "Unexpected row column count",
        "row_column_count",
        row.row_val_.get_count(),
        "schema_column_count",
        data_store_desc_->row_column_count_,
        K(ret));
  } else {
    ObArray<int64_t> lob_col_idxs;
    int64_t store_row_length = 0;
    ObStoreRowkey rowkey;
    rowkey.assign(row.row_val_.cells_, data_store_desc_->rowkey_column_count_);
    if (ObActionFlag::OP_DEL_ROW == row.flag_) {
      // skip delete row
    } else if (OB_FAIL(slide_lob_macro_blocks(rowkey, false))) {
      STORAGE_LOG(WARN, "Failed to delete all lob objects in row", K(row), K(ret));
    } else if (OB_FAIL(find_cand_lob_cols(row, lob_col_idxs, store_row_length))) {
      STORAGE_LOG(WARN, "Failed to find candidate lob columns to reduce row lenght", K(store_row_length), K(ret));
    } else if (store_row_length <= OB_MAX_USER_ROW_LENGTH) {

    } else if (lob_col_idxs.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected row which has nomore lob column to reduce row size", K(store_row_length), K(ret));
    } else if (OB_FAIL(copy_row_(row))) {
      LOG_WARN("failed to copy row", K(ret), K(row));
    } else {
      std::sort(lob_col_idxs.begin(), lob_col_idxs.end());
      lob_writer_.reuse();
      int64_t idx = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < lob_col_idxs.count(); i++) {
        idx = lob_col_idxs.at(i);
        int64_t column_id = row.is_sparse_row_ ? row.column_ids_[idx] : data_store_desc_->column_ids_[idx];
        const ObObj& src_obj = row.row_val_.cells_[idx];
        ObObj& lob_obj = result_row_.row_val_.cells_[idx];
        int64_t orig_obj_length = src_obj.get_data_length();
        if (OB_FAIL(write_lob_obj(rowkey, column_id, src_obj, lob_obj))) {
          STORAGE_LOG(WARN, "Failed to write lob obj", K(rowkey), "column_idx", idx, K(column_id), K(ret));
        } else if (lob_obj.get_data_length() > orig_obj_length) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected lob objects overflow result", K(orig_obj_length), K(lob_obj), K(ret));
        } else {
          store_row_length -= (orig_obj_length - lob_obj.get_data_length());
          STORAGE_LOG(DEBUG, "[LOB] lob objects overflow success", K(orig_obj_length), K(lob_obj), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (store_row_length > OB_MAX_USER_ROW_LENGTH) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected lob overflow processing result", K(store_row_length), K(ret));
        } else {
          result_row = &result_row_;
          STORAGE_LOG(DEBUG, "[LOB] lob objects overflow success", K(store_row_length), K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(result_row)) {
      result_row = &row;
    }
  }

  return ret;
}

int ObLobMergeWriter::reuse_lob_macro_blocks(const ObStoreRowkey& end_key)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Invalid status of ObLobMergeWriter", K(ret));
  } else {
    ret = slide_lob_macro_blocks(end_key, true);
  }

  return ret;
}

int ObLobMergeWriter::skip_lob_macro_blocks(const ObStoreRowkey& end_key)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Invalid status of ObLobMergeWriter", K(ret));
  } else {
    ret = slide_lob_macro_blocks(end_key, false);
  }

  return ret;
}

int ObLobMergeWriter::slide_lob_macro_blocks(const ObStoreRowkey& end_key, const bool reuse)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!end_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to reuse lob macro blocks", K(end_key), K(ret));
  } else if (orig_lob_macro_blocks_.count() == 0 || lob_macro_block_iter_ == orig_lob_macro_blocks_.end()) {
    // skip empty lob blocks
  } else {
    for (; OB_SUCC(ret) && lob_macro_block_iter_ < orig_lob_macro_blocks_.end(); lob_macro_block_iter_++) {
      const MacroBlockId& block_id = lob_macro_block_iter_->block_id_;
      const ObFullMacroBlockMeta& meta = lob_macro_block_iter_->meta_;
      const ObMacroBlockMetaV2* macro_meta = meta.meta_;
      if (OB_ISNULL(macro_meta)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null macro meta", K(block_id), K(ret));
      } else if (!meta.is_valid() || !macro_meta->is_lob_data_block()) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "Invalid lob macro block meta", K(meta), K(block_id), K(ret));
      } else {
        ObStoreRowkey meta_rowkey(macro_meta->endkey_, macro_meta->rowkey_column_number_);
        int32_t cmp_ret = meta_rowkey.compare(end_key);
        if (cmp_ret > 0) {
          STORAGE_LOG(DEBUG, "[LOB] finish slide lob macro blocks", K(block_id), K(meta), K(end_key));
          break;
        } else if (reuse || cmp_ret < 0) {
          if (OB_FAIL(block_write_ctx_.add_macro_block(block_id, meta))) {
            STORAGE_LOG(WARN, "Failed to reuse old lob macro block", K(block_id), K(ret));
          } else {
            use_old_macro_block_count_++;
            if (OB_NOT_NULL(data_store_desc_) && OB_NOT_NULL(data_store_desc_->merge_info_)) {
              data_store_desc_->merge_info_->use_old_macro_block_count_++;
              data_store_desc_->merge_info_->macro_block_count_++;
              data_store_desc_->merge_info_->occupy_size_ += macro_meta->occupy_size_;
            }
            STORAGE_LOG(INFO, "[LOB] succ to reuse orig lob macro blocks", K(block_id), K(end_key), K(ret));
          }
        } else {
          STORAGE_LOG(INFO, "[LOB] succ to retire orig lob macro blocks", K(block_id), K(meta), K(end_key), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObLobMergeWriter::check_lob_macro_block(const MacroBlockId& macro_id, const ObFullMacroBlockMeta& meta,
    const ObStoreRowkey& rowkey, const int64_t column_id, bool& check_ret)
{
  int ret = OB_SUCCESS;
  ObMacroBlockMetaHandle meta_handle;
  const ObMacroBlockMetaV2* macro_meta = meta.meta_;

  check_ret = false;
  if (!macro_id.is_valid() || !meta.is_valid() || !rowkey.is_valid() || column_id < 0 ||
      column_id > OB_ROW_MAX_COLUMNS_COUNT) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(
        WARN, "Invalid arguments to find lob macro block", K(macro_id), K(meta), K(rowkey), K(column_id), K(ret));
  } else if (!macro_meta->is_valid() || !macro_meta->is_lob_data_block()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "Invalid lob macro block meta", K(*macro_meta), K(macro_id), K(ret));
  } else if (macro_meta->rowkey_column_number_ >= macro_meta->column_number_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN,
        "Invalid macro block column member",
        "rowkey_col_num",
        macro_meta->rowkey_column_number_,
        "column_num",
        macro_meta->column_number_,
        K(ret));
  } else {
    // TODO check lob macro meta other info
    ObStoreRowkey meta_rowkey(macro_meta->endkey_, macro_meta->rowkey_column_number_);
    check_ret = meta_rowkey == rowkey;
    if (check_ret && OB_NOT_NULL(data_store_desc_) && OB_NOT_NULL(data_store_desc_->merge_info_)) {
      data_store_desc_->merge_info_->macro_block_count_++;
      data_store_desc_->merge_info_->occupy_size_ += macro_meta->occupy_size_;
    }
  }
  return ret;
}

int ObLobMergeWriter::write_lob_obj(
    const ObStoreRowkey& rowkey, const int64_t column_id, const ObObj& src_obj, ObObj& dst_obj)
{
  int ret = OB_SUCCESS;

  if (!rowkey.is_valid() || column_id < 0 || column_id > OB_ROW_MAX_COLUMNS_COUNT ||
      !ob_is_text_tc(src_obj.get_type())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments to write lob obj", K(rowkey), K(column_id), K(src_obj), K(ret));
  } else if (src_obj.is_lob_outrow() || src_obj.get_data_length() <= OB_MAX_LOB_HANDLE_LENGTH) {
    src_obj.copy_value_or_obj(dst_obj, true);
  } else {
    ObArray<ObMacroBlockInfoPair> macro_blocks;
    if (OB_FAIL(lob_writer_.write_lob_data(rowkey, static_cast<uint16_t>(column_id), src_obj, dst_obj, macro_blocks))) {
      STORAGE_LOG(WARN, "Failed to write lob object to macro block", K(src_obj), K(ret));
    } else {
      bool check_ret = false;
      STORAGE_LOG(DEBUG, "[LOB], new lob object macro blocks", K(dst_obj), K(macro_blocks), K(ret));
      for (int64_t i = 0; OB_SUCC(ret) && i < macro_blocks.count(); i++) {
        if (OB_FAIL(check_lob_macro_block(
                macro_blocks.at(i).block_id_, macro_blocks.at(i).meta_, rowkey, column_id, check_ret))) {
          STORAGE_LOG(WARN, "Failed to check lob macro block", "new_lob_macro_block", macro_blocks.at(i), K(ret));
        } else if (!check_ret) {
          ret = OB_ERR_SYS;
          STORAGE_LOG(ERROR,
              "Lob macro block meta does not match write info",
              "new_lob_macro_block",
              macro_blocks.at(i),
              K(rowkey),
              K(column_id),
              K(ret));
        } else if (OB_FAIL(block_write_ctx_.add_macro_block(macro_blocks.at(i).block_id_, macro_blocks.at(i).meta_))) {
          STORAGE_LOG(
              WARN, "Failed to push back new lob macro block", K(ret), "new_lob_macro_block", macro_blocks.at(i));
        }
      }
    }
  }

  return ret;
}

int ObLobMergeWriter::copy_row_(const ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  result_row_ = row;
  result_row_.row_val_.cells_ = reinterpret_cast<ObObj*>(buffer_);
  result_row_.row_val_.count_ = row.row_val_.count_;
  MEMCPY(buffer_, row.row_val_.cells_, sizeof(ObObj) * row.row_val_.count_);
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
