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
#include "observer/table_load/backup/ob_table_load_backup_macro_block_scanner.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
using namespace common;
using namespace share;

ObTableLoadBackupMacroBlockScanner::ObTableLoadBackupMacroBlockScanner()
  : allocator_("TLD_BMaBS"),
    backup_version_(ObTableLoadBackupVersion::INVALID),
    macro_block_reader_(nullptr),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  column_map_ids_.set_tenant_id(MTL_ID());
}

int ObTableLoadBackupMacroBlockScanner::init(
    const char *buf,
    int64_t buf_size,
    const ObTableLoadBackupVersion &backup_version,
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
  } else {
    backup_version_ = backup_version;
    if (OB_FAIL(init_macro_block_reader(buf, buf_size))) {
      LOG_WARN("fail to init macro block reader", KR(ret));
    } else if (OB_FAIL(init_column_map_ids(schema_info, column_ids))) {
      LOG_WARN("fail to init column map ids", KR(ret));
    } else if (OB_FAIL(init_row())) {
      LOG_WARN("fail to init row_", KR(ret));
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

void ObTableLoadBackupMacroBlockScanner::reset()
{
  backup_version_ = ObTableLoadBackupVersion::INVALID;
  column_map_ids_.reset();
  micro_block_scanner_.reset();
  row_.reset();
  if (nullptr != macro_block_reader_) {
    macro_block_reader_->~ObTableLoadBackupIMacroBlockReader();
    allocator_.free(macro_block_reader_);
    macro_block_reader_ = nullptr;
  }
  allocator_.reset();
  is_inited_ = false;
}

int ObTableLoadBackupMacroBlockScanner::get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else if (!micro_block_scanner_.is_valid()) {
    if (OB_FAIL(switch_next_micro_block())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to switch next micro block", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(micro_block_scanner_.get_next_row(row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      } else {
        ret = OB_SUCCESS;
        if (OB_FAIL(switch_next_micro_block())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to switch next micro block", KR(ret));
          }
        } else {
          ret = micro_block_scanner_.get_next_row(row);
        }
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

int ObTableLoadBackupMacroBlockScanner::init_macro_block_reader(
  const char *buf,
  int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (backup_version_ == ObTableLoadBackupVersion::V_1_4) {
    ObTableLoadBackupMacroBlockReader *macro_block_reader = nullptr;
    if (OB_ISNULL(macro_block_reader_ = macro_block_reader = OB_NEWx(ObTableLoadBackupMacroBlockReader, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadBackupMacroBlockReader", KR(ret));
    } else if (OB_FAIL(macro_block_reader->init(buf, buf_size, backup_version_))) {
      LOG_WARN("fail to init macro block reader", KR(ret));
    }
  } else {
    ObTableLoadBackupSSTableBlockReader *macro_block_reader = nullptr;
    if (OB_ISNULL(macro_block_reader_ = macro_block_reader = OB_NEWx(ObTableLoadBackupSSTableBlockReader, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadBackupSSTableBlockReader", KR(ret));
    } else if (OB_FAIL(macro_block_reader->init(buf, buf_size, backup_version_))) {
      LOG_WARN("fail to init sstable block reader", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadBackupMacroBlockScanner::init_column_map_ids(
    const ObSchemaInfo *schema_info,
    const ObIArray<int64_t> *column_ids)
{
  int ret = OB_SUCCESS;
  if (is_lob_block()) {
    // do nothing
  } else if (is_logical_backup_version(backup_version_)) {
    int16_t column_number = 0;
    const ObObjMeta *column_type_array = nullptr;
    const uint16_t *column_id_array = nullptr;
    if (backup_version_ == ObTableLoadBackupVersion::V_1_4) {
      const ObMacroBlockMeta *meta = static_cast<ObTableLoadBackupMacroBlockReader*>(macro_block_reader_)->get_macro_block_meta();
      column_number = meta->column_number_;
      column_type_array = meta->column_type_array_;
      column_id_array = meta->column_id_array_;
    } else {
      const ObMacroBlockMetaV2 *meta = static_cast<ObTableLoadBackupSSTableBlockReader*>(macro_block_reader_)->get_macro_block_meta();
      column_number = meta->column_number_;
      column_type_array = meta->column_type_array_;
      column_id_array = meta->column_id_array_;
    }
    int64_t start_idx = schema_info->is_heap_table_ ? 1 : 0;
    // 对于堆表，schema_info由一个自增列+创建表的所有列组成，column_ids由逻辑备份里创建表的所有列组成，meta->column_id_array_表示备份宏块的列id顺序，由自增列+分区列(固定是2列)+创建表的剩余列组成
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids->count(); i++) {
      bool has_match = false;
      int64_t match_idx = -1;
      for (int64_t j = 0; OB_SUCC(ret) && !has_match && j < column_number; j++) {
        if (column_ids->at(i) == column_id_array[j]) {
          has_match = true;
          match_idx = j;
          if (schema_info->column_desc_[i + start_idx].col_type_ != column_type_array[j]) {
            if ((schema_info->column_desc_[i + start_idx].col_type_.is_mysql_date() && column_type_array[j].is_date()) ||
                (schema_info->column_desc_[i + start_idx].col_type_.is_mysql_datetime() && column_type_array[j].is_datetime()) ||
                (schema_info->column_desc_[i + start_idx].col_type_.is_decimal_int() && column_type_array[j].is_number())) {
              // do nothing
            } else {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("direct load from backup data, column type not match is not supported", KR(ret),
                  K(i), K(j), K(backup_version_), K((int64_t)schema_info->column_desc_[i + start_idx].col_type_.get_type()),
                  K((int64_t)column_type_array[j].get_type()), K(schema_info->column_desc_[i + start_idx].col_type_),
                  K(column_type_array[j]), K(schema_info->column_desc_), K(ObArrayWrap<ObObjMeta>(column_type_array, column_number)));
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
  } else {
    const common::ObArray<share::schema::ObColDesc> &columns = static_cast<ObTableLoadBackupSSTableBlockReader*>(macro_block_reader_)->get_columns();
    // 对于堆表，schema_info由一个自增列+创建表的所有列组成，columns表示备份宏块的列顺序，由分区键+自增列+创建表的剩余列组成
    if (OB_UNLIKELY(schema_info->column_desc_.count() != columns.count())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct load from backup data, column count not match is not supported", KR(ret), K(schema_info->column_desc_.count()), K(columns.count()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "direct load from backup data, column count not match is");
    } else {
      int64_t start_idx = schema_info->is_heap_table_ ? 1 : 0; // 堆表从1开始，有主键表从0开始
      int64_t remain_partkey_count = schema_info->partkey_count_;
      for (int64_t i = start_idx; OB_SUCC(ret) && i < schema_info->column_desc_.count(); i++) {
        int64_t schema_col_idx = 0;
        int64_t backup_col_idx = 0;
        if (schema_info->is_heap_table_) {
          if (schema_info->column_info_[i].is_partkey_) {
            backup_col_idx = schema_info->column_info_[i].partkey_idx_;
            remain_partkey_count--;
          } else {
            backup_col_idx = i + remain_partkey_count;
          }
        } else {
          backup_col_idx = i;
        }
        if (OB_UNLIKELY(schema_info->column_desc_[i].col_type_ != columns[backup_col_idx].col_type_)) {
          if ((schema_info->column_desc_[i].col_type_.is_mysql_date() && columns[backup_col_idx].col_type_.is_date()) ||
              (schema_info->column_desc_[i].col_type_.is_mysql_datetime() && columns[backup_col_idx].col_type_.is_datetime()) ||
              (schema_info->column_desc_[i].col_type_.is_decimal_int() && columns[backup_col_idx].col_type_.is_number())) {
            // do nothing
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("direct load from backup data, column type not match is not supported", KR(ret),
                K(i), K(backup_col_idx), K((int64_t)schema_info->column_desc_[i].col_type_.get_type()), K((int64_t)columns[backup_col_idx].col_type_.get_type()),
                  K(schema_info->column_desc_[i]), K(columns[backup_col_idx]), K(schema_info->column_desc_), K(columns));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "direct load from backup data, column type not match is");
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(column_map_ids_.push_back(backup_col_idx))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLoadBackupMacroBlockScanner::init_row()
{
  int ret = OB_SUCCESS;
  if (is_lob_block()) {
    // do nothing
  } else {
    int64_t column_count = column_map_ids_.count();
    if (row_.count_ <= 0) {
      if (OB_FAIL(ob_create_row(allocator_, column_count, row_))) {
        LOG_WARN("fail to init row_", KR(ret));
      }
    } else if (OB_UNLIKELY(row_.count_ != column_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected count", KR(ret), K(row_.count_), K(column_count));
    }
    for (int32_t i = 0; OB_SUCC(ret) && i < column_count; i++) {
      row_.cells_[i].set_nop_value();
    }
  }
  return ret;
}

int ObTableLoadBackupMacroBlockScanner::switch_next_micro_block()
{
  int ret = OB_SUCCESS;
  micro_block_scanner_.reuse();
  const ObMicroBlockData *micro_block_data = nullptr;
  if (OB_FAIL(macro_block_reader_->get_next_micro_block(micro_block_data))) {
    if (OB_UNLIKELY(ret != OB_ITER_END)) {
      LOG_WARN("fail to get next micro block", KR(ret));
    }
  } else if (OB_FAIL(micro_block_scanner_.init(micro_block_data, backup_version_, macro_block_reader_->get_column_map()))) {
    LOG_WARN("fail to init micro block scanner", KR(ret));
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

} // table_load_backup
} // namespace observer
} // namespace oceanbase
