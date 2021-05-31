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

#include "storage/ob_sstable_dump_error_info.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
using namespace share::schema;
using namespace blocksstable;

namespace storage {

int ObSSTableDumpErrorInfo::get_sstable_scan_iter(
    ObSSTable& sstable, const ObTableSchema& schema, ObStoreRowIterator*& scanner)
{
  int ret = OB_SUCCESS;
  scanner = NULL;
  ObExtStoreRange range;
  range.get_range().set_whole_range();
  if (OB_FAIL(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_))) {
    STORAGE_LOG(WARN, "failed to transform range to collation free and range cutoff", K(range), K(ret));
  } else if (OB_FAIL(prepare_sstable_query_param(sstable, schema))) {
    STORAGE_LOG(WARN, "Fail to preapare scan param", K(ret));
  } else if (OB_FAIL(sstable.scan(param_, context_, range, scanner))) {
    STORAGE_LOG(WARN, "Fail to scan param", K(ret));
  }
  return ret;
}

int ObSSTableDumpErrorInfo::prepare_sstable_query_param(ObSSTable& sstable, const ObTableSchema& schema)
{
  int ret = OB_SUCCESS;
  reset();
  const uint64_t tenant_id = extract_tenant_id(schema.get_table_id());
  if (OB_FAIL(schema.get_column_ids(column_ids_, true))) {
    STORAGE_LOG(WARN, "Fail to get column ids. ", K(ret));
  } else if (FALSE_IT(param_.out_cols_ = &column_ids_)) {
    STORAGE_LOG(ERROR, "row getter", K(ret), K(column_ids_));
  } else {
    ObQueryFlag query_flag(ObQueryFlag::Forward,
        true, /*is daily merge scan*/
        true, /*is read multiple macro block*/
        true, /*sys task scan, read one macro block in single io*/
        false /*is full row scan?*/,
        false,
        false);
    store_ctx_.cur_pkey_ = sstable.get_partition_key();
    param_.table_id_ = schema.get_table_id();
    param_.rowkey_cnt_ = schema.get_rowkey_column_num();
    param_.schema_version_ = schema.get_schema_version();
    context_.query_flag_ = query_flag;
    context_.store_ctx_ = &store_ctx_;
    context_.allocator_ = &allocator_;
    context_.stmt_allocator_ = &allocator_;
    context_.trans_version_range_ = sstable.get_key().trans_version_range_;
    context_.is_inited_ = true;  // just used for dump
  }
  return ret;
}

int ObSSTableDumpErrorInfo::simple_get_sstable_rowkey_get_iter(
    ObSSTable& sstable, const common::ObStoreRowkey& rowkey, ObStoreRowIterator*& getter)
{
  int ret = OB_SUCCESS;
  getter = NULL;

  ext_rowkey_.get_store_rowkey() = rowkey;
  if (OB_FAIL(ext_rowkey_.to_collation_free_on_demand_and_cutoff_range(allocator_))) {
    STORAGE_LOG(WARN, "Fail to transfer rowkey", K(ret), K(ext_rowkey_));
  } else if (OB_FAIL(sstable.get(param_, context_, ext_rowkey_, getter))) {
    STORAGE_LOG(WARN, "Fail to get param", K(ret), K(column_ids_));
  }
  return ret;
}

int ObSSTableDumpErrorInfo::generate_projecter(
    const ObTableSchema& schema1, const ObTableSchema& schema2, ObIArray<int64_t>& projector)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColDesc, COL_ARRAY_LEN> column_ids_1;
  ObSEArray<ObColDesc, COL_ARRAY_LEN> column_ids_2;
  common::hash::ObHashMap<int64_t, int64_t> col_map_1;
  if (OB_FAIL(schema1.get_column_ids(column_ids_1))) {
    STORAGE_LOG(WARN, "failed to get rowkey column ids", K(ret), K(schema1), K(schema2));
  } else if (OB_FAIL(col_map_1.create(column_ids_1.count(), "dump_error_info"))) {
    STORAGE_LOG(WARN, "failed to create dap map", K(ret), K(column_ids_1.count()));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < column_ids_1.count(); ++i) {
      if (OB_FAIL(col_map_1.set_refactored(column_ids_1.at(i).col_id_, i))) {
        STORAGE_LOG(WARN, "failed to add into map", K(ret), K(i), K(column_ids_1.at(i)));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema2.get_column_ids(column_ids_2))) {
        STORAGE_LOG(WARN, "failed to get column ids", K(ret), K(schema1));
      } else {
        int64_t dest_pos = -1;
        for (int i = 0; OB_SUCC(ret) && i < column_ids_2.count(); ++i) {
          if (OB_FAIL(col_map_1.get_refactored(column_ids_2.at(i).col_id_, dest_pos))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
              dest_pos = -1;
            } else {
              STORAGE_LOG(WARN, "failed to get from hash map", K(ret), K(i), K(column_ids_2.at(i)));
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(projector.push_back(dest_pos))) {
            STORAGE_LOG(WARN, "success to get from hash map", K(ret), K(i), K(column_ids_2.at(i)), K(dest_pos));
          }
        }  // end for
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (projector.count() != column_ids_2.count()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "failed to generate rowkey projector", K(ret), K(schema1), K(schema2), K(projector));
    }
  }
  return ret;
}

int ObSSTableDumpErrorInfo::transform_rowkey(
    const ObStoreRow& row, const int64_t rowkey_cnt, common::ObIArray<int64_t>& projector, ObStoreRowkey& rowkey)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < rowkey_cnt; ++i) {
    rowkey_obj_[i] = row.row_val_.cells_[projector.at(i)];
  }
  rowkey.assign(rowkey_obj_, rowkey_cnt);
  return ret;
}

int ObSSTableDumpErrorInfo::get_row_with_rowkey_and_check(const ObStoreRow* input_row, ObStoreRowIterator* getter,
    common::ObSEArray<int64_t, COL_ARRAY_LEN>& projector, int64_t& found_row_cnt)
{
  int ret = OB_SUCCESS;
  const ObStoreRow* ret_row = NULL;
  if (OB_ISNULL(getter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "getter is NULL", K(ret), KPC(getter));
  } else if (OB_FAIL(getter->get_next_row(ret_row))) {  // get row from sstable1
    STORAGE_LOG(WARN, "failed to get row", K(ret), KPC(input_row));
  } else if (OB_ISNULL(ret_row) || !ret_row->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "row is invalid", K(ret), KPC(ret_row));
  } else if (ObActionFlag::OP_ROW_EXIST != ret_row->flag_) {
    ++found_row_cnt;
    STORAGE_LOG(ERROR, "found, row is not exist", KPC(input_row));
  } else {  // check row cells
    for (int i = 0; i < projector.count(); ++i) {
      if (projector.at(i) < 0 || projector.at(i) > input_row->row_val_.count_) {
        // not exist
      } else if (ret_row->row_val_.cells_[i] != input_row->row_val_.cells_[projector.at(i)]) {
        ++found_row_cnt;
        STORAGE_LOG(ERROR, "found, column is diffenrent", K(i), KPC(ret_row), KPC(input_row));
        break;
      }
    }  // end of for
  }

  return ret;
}

// make sure sstable1.get_meta().row_count_ > sstable2.get_meta().row_count_
int ObSSTableDumpErrorInfo::find_extra_row(
    ObSSTable& sstable1, const ObTableSchema& schema1, ObSSTable& sstable2, const ObTableSchema& schema2)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator* scanner = NULL;
  ObSEArray<int64_t, COL_ARRAY_LEN> projector;
  common::ObSEArray<share::schema::ObColDesc, COL_ARRAY_LEN> col_descs;
  int64_t found_row_cnt = 0;
  int64_t iter_row_cnt = 0;
  if (OB_FAIL(get_sstable_scan_iter(sstable1, schema1, scanner)) || OB_ISNULL(scanner)) {
    STORAGE_LOG(WARN, "failed to get sstable scan iter", K(ret), K(sstable1), K(schema1));
  } else if (OB_FAIL(generate_projecter(schema1, schema2, projector))) {
    STORAGE_LOG(WARN, "failed to generate rowkey projector", K(ret));
  } else if (OB_FAIL(schema2.get_column_ids(col_descs))) {
    STORAGE_LOG(WARN, "failed to get column id array", K(ret), K(schema2));
  } else if (OB_FAIL(prepare_sstable_query_param(sstable2, schema2))) {
    STORAGE_LOG(WARN, "Fail to preapare scan param", K(ret));
  } else {
    const ObStoreRow* row_in_table1 = NULL;
    ObStoreRowIterator* getter = NULL;
    common::ObStoreRowkey rowkey;
    const int64_t rowkey_cnt = schema2.get_rowkey_column_num();
    param_.table_id_ = schema2.get_table_id();
    param_.rowkey_cnt_ = rowkey_cnt;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(scanner->get_next_row(row_in_table1))) {  // get row from sstable1
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "failed to get next row", K(ret));
        } else {
          STORAGE_LOG(WARN, "meet iter end", K(ret), KPC(scanner));
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(row_in_table1) || !row_in_table1->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "row is invalid", K(ret), KPC(row_in_table1));
      } else if (OB_FAIL(transform_rowkey(
                     *row_in_table1, rowkey_cnt, projector, rowkey))) {  // project into rowkey of sstable2
        STORAGE_LOG(WARN, "failed to transfor rowkey", K(ret));
      } else if (OB_FAIL(simple_get_sstable_rowkey_get_iter(sstable2, rowkey, getter))) {  // get row in sstable2
        STORAGE_LOG(WARN, "failed to get table getter", K(ret), KPC(getter));
      } else if (OB_FAIL(get_row_with_rowkey_and_check(row_in_table1, getter, projector, found_row_cnt))) {
        STORAGE_LOG(WARN, "failed to check row", K(ret));
      }
      ++iter_row_cnt;
    }  // end of while
  }
  if (OB_SUCC(ret)) {
    if (found_row_cnt + sstable2.get_meta().row_count_ != sstable1.get_meta().row_count_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "have not found all extra rows", K(ret), K(found_row_cnt), K(iter_row_cnt));
    } else {
      STORAGE_LOG(ERROR,
          "success to get all extra rows",
          K(ret),
          K(sstable1),
          K(schema1),
          K(sstable2),
          K(schema2),
          K(found_row_cnt));
    }
  }
  return ret;
}

int ObSSTableDumpErrorInfo::main_and_index_row_count_error(ObSSTable& main_table,
    const ObTableSchema& main_table_schema, ObSSTable& index_table, const ObTableSchema& index_table_schema)
{
  int ret = OB_SUCCESS;
  if (main_table.get_meta().row_count_ > index_table.get_meta().row_count_) {
    if (OB_FAIL(find_extra_row(main_table, main_table_schema, index_table, index_table_schema))) {
      STORAGE_LOG(WARN, "failed to find extra rows", K(ret));
    }
  } else {  // main_table.get_meta().row_count_ < index_table.get_meta().row_count_
    if (OB_FAIL(find_extra_row(index_table, index_table_schema, main_table, main_table_schema))) {
      STORAGE_LOG(WARN, "failed to find extra rows", K(ret));
    }
  }
  return ret;
}

void ObSSTableDumpErrorInfo::reset()
{
  param_.reset();
  context_.reset();
  store_ctx_.reset();
  ext_rowkey_.reset();
}

void ObSSTableDumpErrorInfo::destory()
{
  reset();
  allocator_.reset();
}

}  // namespace storage
}  // namespace oceanbase
