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

#ifndef OCEANBASE_STORAGE_OB_SSTABLE_DUMP_ERROR_INFO_H_
#define OCEANBASE_STORAGE_OB_SSTABLE_DUMP_ERROR_INFO_H_

#include "storage/ob_sstable.h"

namespace oceanbase {

using namespace share::schema;

namespace storage {
class ObSSTableDumpErrorInfo {
public:
  ObSSTableDumpErrorInfo()
      : param_(),
        context_(),
        store_ctx_(),
        allocator_("dump_error_info"),
        column_ids_(COL_ARRAY_LEN, allocator_),
        ext_rowkey_()
  {}
  ~ObSSTableDumpErrorInfo()
  {
    destory();
  }

  int main_and_index_row_count_error(ObSSTable& main_table, const ObTableSchema& main_table_schema,
      ObSSTable& index_table, const ObTableSchema& index_table_schema);

  void reset();
  void destory();

private:
  static const int64_t COL_ARRAY_LEN = 128;

private:
  int find_extra_row(
      ObSSTable& sstable1, const ObTableSchema& schema1, ObSSTable& sstable2, const ObTableSchema& schema2);
  OB_INLINE int transform_rowkey(
      const ObStoreRow& row, const int64_t rowkey_cnt, ObIArray<int64_t>& projector, ObStoreRowkey& rowkey);
  int get_sstable_scan_iter(ObSSTable& sstable, const ObTableSchema& schema, ObStoreRowIterator*& scanner);
  int simple_get_sstable_rowkey_get_iter(
      ObSSTable& sstable, const common::ObStoreRowkey& rowkey, ObStoreRowIterator*& getter);
  int generate_projecter(const ObTableSchema& schema1, const ObTableSchema& schema2, ObIArray<int64_t>& projector);
  int get_row_with_rowkey_and_check(const ObStoreRow* input_row, ObStoreRowIterator* getter,
      common::ObSEArray<int64_t, COL_ARRAY_LEN>& projector, int64_t& found_row_cnt);
  int prepare_sstable_query_param(ObSSTable& sstable, const ObTableSchema& schema);

private:
  ObObj rowkey_obj_[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObTableIterParam param_;
  ObTableAccessContext context_;
  ObStoreCtx store_ctx_;
  ObArenaAllocator allocator_;
  ObArray<ObColDesc, ObIAllocator&> column_ids_;
  ObExtStoreRowkey ext_rowkey_;
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_SSTABLE_DUMP_ERROR_INFO_H_
