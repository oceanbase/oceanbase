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

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

// 用于3x的非encoding宏块读
class ObFlatRowReaderV2 : public ObIRowReader
{
public:
  ObFlatRowReaderV2();
  virtual ~ObFlatRowReaderV2() = default;
  int read_row(const char *row_buf,
               const int64_t row_end_pos,
               int64_t pos,
               const ObTableLoadBackupVersion &backup_version,
               const ObIColumnMap *column_map,
               ObIAllocator &allocator,
               common::ObNewRow &row) override;
private:
  int setup_row(const char *buf,
                const int64_t row_end_pos,
                const int64_t pos,
                const int64_t column_index_count = INT_MAX);
  OB_INLINE int analyze_row_header(const int64_t column_cnt);
  int read_flat_row_from_flat_storage(const ObIColumnMap *column_map,
                                      ObIAllocator &allocator,
                                      common::ObNewRow &row);
  int sequence_read_flat_column(const ObIColumnMap *column_map,
                                ObIAllocator &allocator,
                                common::ObNewRow &row);
  int read_obj(const ObObjMeta &src_meta,
               ObIAllocator &allocator,
               ObObj &obj);
  int read_text_store(const ObStoreMeta &store_meta,
                      common::ObIAllocator &allocator,
                      common::ObObj &obj);
  int read_json_store(const ObStoreMeta &store_meta,
                      common::ObIAllocator &allocator,
                      common::ObObj &obj);
  template<class T>
  static const T *read(const char *row_buf, int64_t &pos);
private:
  const char *buf_;
  int64_t row_end_pos_;
  int64_t start_pos_;
  int64_t pos_;
  const ObRowHeaderV2 *row_header_;
  const void *store_column_indexs_;
  const uint16_t *column_ids_;
  bool is_setuped_;
};

template<class T>
inline const T *ObFlatRowReaderV2::read(const char *row_buf, int64_t &pos)
{
  const T *ptr = reinterpret_cast<const T*>(row_buf + pos);
  pos += sizeof(T);
  return ptr;
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
