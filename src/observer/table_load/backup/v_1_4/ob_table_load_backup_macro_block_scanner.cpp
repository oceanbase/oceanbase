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
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_macro_block_scanner.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_1_4
{
int ObTableLoadBackupMacroBlockScanner::init(
    const char *buf,
    int64_t buf_size,
    const ObSchemaInfo *schema_info,
    const ObIArray<int64_t> *column_ids)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(buf == nullptr || buf_size == 0 || schema_info == nullptr || column_ids == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(buf_size), KP(schema_info), KP(column_ids));
  } else if (OB_FAIL(macro_reader_.init(buf, buf_size))) {
    LOG_WARN("fail to init macro_reader_", KR(ret));
  } else if (OB_FAIL(init_column_map_ids(schema_info, column_ids))) {
    LOG_WARN("fail to init column map ids", KR(ret));
  } else if (OB_FAIL(init_row())) {
    LOG_WARN("fail to init row", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObTableLoadBackupMacroBlockScanner::reset()
{
  macro_reader_.reset();
  column_map_ids_.reset();
  micro_scanner_.reset();
  block_idx_ = -1;
  is_inited_ = false;
}

int ObTableLoadBackupMacroBlockScanner::get_next_row(common::ObNewRow *&row)
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
    if (OB_FAIL(micro_scanner_.get_next_row(row))) {
      if (ret == OB_ITER_END) {
        block_idx_++;
        if (block_idx_ < macro_reader_.get_macro_block_meta()->micro_block_count_) {
          if (OB_FAIL(init_micro_block_scanner())) {
            LOG_WARN("fail to init_micro_block_scanner", KR(ret), K(block_idx_));
          } else {
            ret = micro_scanner_.get_next_row(row);
          }
        }
      } else {
        LOG_WARN("fail to get next row", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(adjust_column_idx(row))) {
      LOG_WARN("fail to adjust column idx", KR(ret));
    } else {
      row = &row_;
    }
  }
  return ret;
}

int ObTableLoadBackupMacroBlockScanner::init_column_map_ids(
    const ObSchemaInfo *schema_info,
    const ObIArray<int64_t> *column_ids)
{
  int ret = OB_SUCCESS;
  const ObTableLoadBackupMacroBlockMeta *meta = macro_reader_.get_macro_block_meta();
  int64_t offset = schema_info->is_heap_table_ ? 1 : 0;
  // 对于堆表，schema_info由一个自增列+创建表的所有列组成，column_ids由14x逻辑备份里创建表的所有列组成，meta->column_id_array_表示备份宏块的列id顺序，由自增列+分区列(固定是2列)+创建表的剩余列组成
  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids->count(); i++) {
    bool has_match = false;
    int64_t match_idx = -1;
    for (int64_t j = 0; OB_SUCC(ret) && !has_match && j < meta->column_number_; j++) {
      if (column_ids->at(i) == meta->column_id_array_[j]) {
        has_match = true;
        match_idx = j;
        if (schema_info->column_desc_[i + offset].col_type_ != meta->column_type_array_[j]) {
          if ((schema_info->column_desc_[i + offset].col_type_.is_mysql_date() && meta->column_type_array_[j].is_date()) ||
              (schema_info->column_desc_[i + offset].col_type_.is_mysql_datetime() && meta->column_type_array_[j].is_datetime())) {
            // do nothing
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("direct load from 1.4x backup data, column type not match is not supported", KR(ret),
                K(i), K(j), K(schema_info->column_desc_[i + offset].col_type_), K(meta->column_type_array_[j]),
                K(schema_info->column_desc_), K(ObArrayWrap<ObObjMeta>(meta->column_type_array_, meta->column_number_)));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "direct load from backup data, column type not match is");
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(column_map_ids_.push_back(match_idx))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadBackupMacroBlockScanner::init_row()
{
  int ret = OB_SUCCESS;
  int64_t column_count = column_map_ids_.count();
  if (row_.count_ <= 0) {
    if (OB_FAIL(ob_create_row(allocator_, column_count, row_))) {
      LOG_WARN("fail to init out_row_", KR(ret));
    }
  } else if (OB_UNLIKELY(row_.count_ != column_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count", KR(ret), K(row_.count_), K(column_count));
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < column_count; i++) {
    row_.cells_[i].set_nop_value();
  }
  return ret;
}

int ObTableLoadBackupMacroBlockScanner::init_micro_block_scanner()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else {
    micro_scanner_.reuse();
    if (OB_FAIL(macro_reader_.decompress_data(block_idx_))) {
      LOG_WARN("fail to decompress data", KR(ret), K(block_idx_));
    } else if (OB_FAIL(micro_scanner_.init(macro_reader_.get_uncomp_buf(),
                                           macro_reader_.get_column_map()))) {
      LOG_WARN("fail to init micro_scanner_", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadBackupMacroBlockScanner::adjust_column_idx(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  for (int32_t i = 0; OB_SUCC(ret) && i < column_map_ids_.count(); i++) {
    int64_t idx = column_map_ids_[i];
    if (idx != -1) {
      if (OB_UNLIKELY(idx >= row->count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected idx", KR(ret), K(idx), K(row->count_));
      } else {
        row_.cells_[i] = row->cells_[idx];
      }
    }
  }
  return ret;
}

} // table_load_backup_v_1_4
} // namespace observer
} // namespace oceanbase
