/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/backup/v_1_4/ob_table_load_backup_macro_block_scanner_v_1_4.h"

namespace oceanbase
{
namespace observer
{
int ObTableLoadBackupMacroBlockScanner_V_1_4::init(const char *buf, int64_t buf_size, const ObIArray<int64_t> *column_ids)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(buf == nullptr || buf_size == 0 || column_ids == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(buf_size), KP(column_ids));
  } else if (OB_FAIL(macro_reader_.init(buf, buf_size))) {
    LOG_WARN("fail to init macro_reader_", KR(ret));
  } else if (OB_FAIL(init_column_map(column_ids))) {
    LOG_WARN("fail to init_column_map", KR(ret));
  } else if (!row_alloced_) {
    if (OB_FAIL(ob_create_row(allocator_, column_ids->count(), row_))) {
      LOG_WARN("fail to init row_", KR(ret), K(column_ids->count()));
    } else {
      row_alloced_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    for (int32_t i = 0; i < column_ids->count(); i++) {
      row_.cells_[i].set_nop_value();
    }
    is_inited_ = true;
  }

  return ret;
}

void ObTableLoadBackupMacroBlockScanner_V_1_4::reset()
{
  macro_reader_.reset();
  column_map_ids_.reset();
  micro_scanner_.reset();
  block_idx_ = -1;
  is_inited_ = false;
}

int ObTableLoadBackupMacroBlockScanner_V_1_4::get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else if (macro_reader_.get_macro_block_meta()->micro_block_count_ == 0) {
    ret = OB_ITER_END;
  } else if (block_idx_ == -1) {
    block_idx_++;
    if (OB_FAIL(init_micro_block_scanner())) {
      LOG_WARN("fail to init_micro_block_scanner", KR(ret), K(block_idx_));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(micro_scanner_.get_next_row(row_))) {
      if (ret == OB_ITER_END) {
        block_idx_++;
        if (block_idx_ < macro_reader_.get_macro_block_meta()->micro_block_count_) {
          if (OB_FAIL(init_micro_block_scanner())) {
            LOG_WARN("fail to init_micro_block_scanner", KR(ret), K(block_idx_));
          } else {
            ret = micro_scanner_.get_next_row(row_);
          }
        }
      } else {
        LOG_WARN("fail to get next row", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &row_;
  }

  return ret;
}

int ObTableLoadBackupMacroBlockScanner_V_1_4::init_column_map(const ObIArray<int64_t> *column_ids)
{
  int ret = OB_SUCCESS;
  const ObTableLoadBackupMacroBlockMeta_V_1_4 *meta = macro_reader_.get_macro_block_meta();
  for (int16_t i = 0; OB_SUCC(ret) && i < meta->column_number_; i++) {
    if (OB_FAIL(column_map_ids_.push_back(-1))) {
      LOG_WARN("fail to push back", KR(ret), K(i));
    } else {
      for (int32_t j = 0; j < column_ids->count(); j++) {
        if (meta->column_id_array_[i] == column_ids->at(j)) {
          column_map_ids_[i] = j;
          break;
        }
      }
    }
  }

  return ret;
}

int ObTableLoadBackupMacroBlockScanner_V_1_4::init_micro_block_scanner()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else {
    micro_scanner_.reset();
    if (OB_FAIL(macro_reader_.decompress_data(block_idx_))) {
      LOG_WARN("fail to decompress data", KR(ret), K(block_idx_));
    } else if (OB_FAIL(micro_scanner_.init(macro_reader_.get_uncomp_buf(),
                                           &column_map_ids_,
                                           macro_reader_.get_column_map()))) {
      LOG_WARN("fail to init micro_scanner_", KR(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
