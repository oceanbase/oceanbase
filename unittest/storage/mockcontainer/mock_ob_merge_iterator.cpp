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
#include <sstream>
#define private public
#define protected public
#include "mock_ob_merge_iterator.h"

namespace oceanbase
{
namespace common
{
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;

int ObMockScanMergeIterator::init(const ObVectorStore *vector_store,
                                  common::ObIAllocator &alloc,
                                  const ObITableReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vector_store)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null vector store", K(ret));
  } else if (OB_FAIL(row_.init(alloc, read_info.get_request_count()))) {
    STORAGE_LOG(WARN, "Failed to init row", K(ret));
  } else {
    int64_t column_cnt = read_info.get_request_count();
    vector_store_ = vector_store;
    datum_infos_ = &(vector_store_->datum_infos_);
    read_info_ = &read_info;
    sstable_row_.capacity_ = read_info.get_request_count();
    sstable_row_.row_val_.cells_ = reinterpret_cast<common::ObObj *>(alloc.alloc(sizeof(ObObj) * column_cnt));
    sstable_row_.row_val_.count_ = column_cnt;
    const ObIArray<ObColDesc> &cols_desc = read_info.get_columns_desc();
    for (int64_t i = 0; i < cols_desc.count(); i++) {
      sstable_row_.row_val_.cells_[i].set_type(cols_desc.at(i).col_type_.get_type());
    }
  }

  return ret;
}

int ObMockScanMergeIterator::get_next_row(const storage::ObStoreRow *&row)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<ObColDesc> &cols_desc = read_info_->get_columns_desc();
  if (end_of_block()) {
    ret = OB_ITER_END;
  } else {
    row_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < row_.count_; i++) {
      const ObObjType obj_type = cols_desc.at(i).col_type_.get_type();
      const ObObjDatumMapType map_type = ObDatum::get_obj_datum_map_type(obj_type);
      if (OB_FAIL(row_.storage_datums_[i].from_storage_datum(
                  datum_infos_->at(i).datum_ptr_[current_],
                  map_type))) {
        STORAGE_LOG(WARN, "Failed to convert storage datum", K(ret), K(datum_infos_->at(i).datum_ptr_[current_]),
                    K(obj_type), K(map_type));
      }
    }
  }

  if (OB_SUCC(ret)) {
    current_++;
    row = &sstable_row_;
    sstable_row_.flag_ = ObDmlFlag::DF_INSERT;
    int64_t count = row_.get_column_count();
    for (int64_t i = 0; i < count; i++) {
      if (row_.storage_datums_[i].is_nop()) {
        sstable_row_.row_val_.cells_[i].set_nop_value();
      } else if (row_.storage_datums_[i].is_null()) {
        sstable_row_.row_val_.cells_[i].set_null();
      } else {
        row_.storage_datums_[i].to_obj(sstable_row_.row_val_.cells_[i], cols_desc.at(i).col_type_);
      }
    }
  }
  return ret;
}

} // namespace unitest
} // namespace oceanbase
