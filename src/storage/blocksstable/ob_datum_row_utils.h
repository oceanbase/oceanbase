/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef DEV_SRC_STORAGE_BLOCKSSTABLE_OB_DATUM_ROW_UTILS_H_
#define DEV_SRC_STORAGE_BLOCKSSTABLE_OB_DATUM_ROW_UTILS_H_
#include "share/ob_define.h"
#include "ob_datum_row.h"
namespace oceanbase
{
namespace blocksstable
{
class ObDatumRowUtils
{
  typedef common::ObIArray<share::schema::ObColDesc> ObColDescIArray;
public:
  static int ob_create_row(ObIAllocator &allocator, int64_t col_count, ObDatumRow *&datum_row);
  static int ob_create_rows(ObIAllocator &allocator, int64_t row_count, int64_t col_count, ObDatumRow *&datum_rows);
  // TODO@xuanxi: rewrite it when store rowkey is no longer needed
  static int prepare_rowkey(
    const ObDatumRow &datum_row,
    const int key_datum_cnt,
    const ObColDescIArray &col_descs,
    common::ObIAllocator &allocator,
    ObDatumRowkey &rowkey);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_STORAGE_BLOCKSSTABLE_OB_DATUM_ROW_UTILS_H_ */
