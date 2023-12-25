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

#ifndef OCEANBASE_STORAGE_MOCK_OB_TABLE_READ_INFO_H_
#define OCEANBASE_STORAGE_MOCK_OB_TABLE_READ_INFO_H_

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <gmock/gmock.h>
#include "src/storage/access/ob_table_read_info.h"
#include "src/storage/ob_i_store.h"

namespace oceanbase
{
using namespace common;
namespace storage
{

// only for unittest
class MockObTableReadInfo final : public ObTableReadInfo
{
public:
  MockObTableReadInfo() = default;
  ~MockObTableReadInfo() = default;
  int init(
    common::ObIAllocator &allocator,
    const int64_t schema_column_count,
    const int64_t schema_rowkey_cnt,
    const bool is_oracle_mode,
    const common::ObIArray<ObColDesc> &cols_desc,
    const common::ObIArray<int32_t> *storage_cols_index = nullptr,
    const common::ObIArray<ObColumnParam *> *cols_param = nullptr);
};

int MockObTableReadInfo::init(common::ObIAllocator &allocator,
    const int64_t schema_column_count,
    const int64_t schema_rowkey_cnt,
    const bool is_oracle_mode,
    const common::ObIArray<ObColDesc> &cols_desc,
    const common::ObIArray<int32_t> *storage_cols_index,
    const common::ObIArray<ObColumnParam *> *cols_param)
{
  int ret = OB_SUCCESS;
  const int64_t out_cols_cnt = cols_desc.count();

  const int64_t extra_rowkey_col_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const bool is_cg_sstable = (schema_rowkey_cnt == 0 && schema_column_count == 1);
  init_basic_info(schema_column_count, schema_rowkey_cnt, is_oracle_mode, is_cg_sstable); // init basic info
  if (OB_FAIL(prepare_arrays(allocator, cols_desc, out_cols_cnt))) {
    STORAGE_LOG(WARN, "failed to prepare arrays", K(ret), K(out_cols_cnt));
  } else if (nullptr != cols_param && OB_FAIL(cols_param_.init_and_assign(*cols_param, allocator))) {
    STORAGE_LOG(WARN, "Fail to assign cols_param", K(ret));
  } else if (OB_UNLIKELY(cols_index_.rowkey_mode_ || memtable_cols_index_.rowkey_mode_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "cols index is unexpected rowkey_mode", K(ret), K(cols_index_), K(memtable_cols_index_));
  } else {
    int32_t col_index = OB_INVALID_INDEX;
    for (int64_t i = 0; i < out_cols_cnt; i++) {
      col_index = (nullptr == storage_cols_index) ? i : storage_cols_index->at(i);
      cols_index_.array_[i] = col_index;
      memtable_cols_index_.array_[i] = col_index;
    }
    if (FAILEDx(init_datum_utils(allocator, false/*is_cg_sstable*/))) {
      STORAGE_LOG(WARN, "failed to init sequence read info & datum utils", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

}
}

#endif
