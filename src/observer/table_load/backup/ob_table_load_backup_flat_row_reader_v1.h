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

#pragma once
#include "observer/table_load/backup/ob_table_load_backup_irow_reader.h"
#include "common/rowkey/ob_rowkey.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

// 用于14x和2x的非encoding宏块读
class ObFlatRowReaderV1 : public ObIRowReader
{
public:
  ObFlatRowReaderV1()
    : backup_version_(ObTableLoadBackupVersion::INVALID),
      pos_(0),
      start_pos_(0),
      row_end_pos_(0),
      buf_(nullptr),
      store_column_indexs_(nullptr),
      column_index_bytes_(0),
      row_header_v1_(nullptr),
      row_header_v2_(nullptr),
      is_first_row_(true)
  {
  }
  virtual ~ObFlatRowReaderV1() = default;
  int read_compact_rowkey(const common::ObObjMeta *column_types,
                          const int64_t column_count,
                          const ObTableLoadBackupVersion &backup_version,
                          ObIAllocator &allocator,
                          const char *buf,
                          const int64_t row_end_pos,
                          int64_t &pos,
                          common::ObNewRow &row);
  int read_row(const char *row_buf,
               const int64_t row_end_pos,
               int64_t pos,
               const ObTableLoadBackupVersion &backup_version,
               const ObIColumnMap *column_map,
               ObIAllocator &allocator,
               common::ObNewRow &row) override;

private:
  void force_read_meta() { is_first_row_ = true; }
  template<class T>
  const T *read();
  int setup_row(const char *buf,
                const int64_t row_end_pos,
                const int64_t pos,
                const ObTableLoadBackupVersion &backup_version,
                const int64_t column_index_count);
  int read_text_store(const ObStoreMeta &store_meta, common::ObIAllocator &allocator, common::ObObj &obj);
  int read_column_no_meta(const common::ObObjMeta &src_meta, ObIAllocator &allocator, common::ObObj &obj);
  int read_sequence_columns(const ObIColumnMap *column_map,
                            const int64_t column_count,
                            ObIAllocator &allocator,
                            common::ObNewRow &row);
  int read_column(const ObObjMeta &src_meta, ObIAllocator &allocator, ObObj &obj);
  int read_columns(const ObIColumnMap *column_map,
                   ObIAllocator &allocator,
                   common::ObNewRow &row);
  int read_columns(const int64_t start_column_index,
                   const bool check_null_value,
                   const bool read_no_meta,
                   const ObIColumnMap *column_map,
                   ObIAllocator &allocator,
                   common::ObNewRow &row,
                   bool &has_null_value);
private:
  ObTableLoadBackupVersion backup_version_;
  int64_t pos_;
  int64_t start_pos_;
  int64_t row_end_pos_;
  const char *buf_;
  const void *store_column_indexs_;
  int8_t column_index_bytes_;
  const ObRowHeaderV1 *row_header_v1_;
  const ObRowHeaderV2 *row_header_v2_;
  bool is_first_row_;
};

} // table_load_backup
} // namespace observer
} // namespace oceanbase
