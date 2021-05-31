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

#include "ob_multi_version_col_desc_generate.h"
#include "memtable/ob_memtable.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace storage {

/**
 * ---------------------------------------------------------ObMultiVersionColDesc-------------------------------------------------------------
 */
ObMultiVersionRowInfo::ObMultiVersionRowInfo()
    : rowkey_column_cnt_(0), multi_version_rowkey_column_cnt_(0), column_cnt_(0), trans_version_index_(0)
{}

void ObMultiVersionRowInfo::reset()
{
  rowkey_column_cnt_ = 0;
  multi_version_rowkey_column_cnt_ = 0;
  column_cnt_ = 0;
  trans_version_index_ = 0;
}

/**
 * ---------------------------------------------------------ObMultiVersionManager-------------------------------------------------------------
 */

ObMultiVersionColDescGenerate::ObMultiVersionColDescGenerate()
    : is_inited_(false), schema_(NULL), row_info_(), out_cols_project_()
{}

ObMultiVersionColDescGenerate::~ObMultiVersionColDescGenerate()
{}

void ObMultiVersionColDescGenerate::reset()
{
  is_inited_ = false;
  schema_ = NULL;
  row_info_.reset();
  out_cols_project_.reset();
}

int ObMultiVersionColDescGenerate::init(const ObTableSchema* schema)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    reset();
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "multi version manager init twice", K(ret));
  } else if (OB_ISNULL(schema) || !schema->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "multi version manager get invalid argument", K(ret), KP(schema));
  } else {
    schema_ = schema;
    is_inited_ = true;
  }
  return ret;
}

// add trans_version & hop_value:
// |--------------------------------------------------------------------- |
// |rowkey: |cell1|cell2|.....celln| trasn_version| sql_no(A)             |
// |----------------------------------------------------------------------|
// trans_version is a rowkey column
int ObMultiVersionColDescGenerate::generate_column_ids(ObIArray<ObColDesc>& column_ids)
{
  int ret = OB_SUCCESS;
  int32_t index = 0;
  const bool need_build_store_projector = false;
  ObArray<ObColDesc> ori_column_ids;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multi version col desc generate do not init", K(ret));
  } else if (OB_FAIL(schema_->get_store_column_ids(ori_column_ids))) {
    STORAGE_LOG(WARN, "fail to get column_ids", K(ret));
  } else {
    const int64_t rowkey_cnt = schema_->get_rowkey_column_num();
    const int64_t extra_multi_version_rowkey_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    for (int64_t i = 0; OB_SUCC(ret) && i < ori_column_ids.count(); ++i) {
      index = i;
      if (OB_FAIL(column_ids.push_back(ori_column_ids.at(i)))) {
        STORAGE_LOG(WARN, "failed to push back column ids", K(ret), K(i));
      } else if (rowkey_cnt - 1 == i &&
                 OB_FAIL(ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(
                     column_ids, index, need_build_store_projector, extra_multi_version_rowkey_cnt))) {
        STORAGE_LOG(WARN, "failed to add extra rowkey cols", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(generate_out_cols_project(column_ids))) {
        STORAGE_LOG(WARN, "fail to generate out cols project", K(ret));
      }
    }
  }
  return ret;
}

int ObMultiVersionColDescGenerate::generate_multi_version_row_info(const ObMultiVersionRowInfo*& multi_version_row_info)
{
  int ret = OB_SUCCESS;
  multi_version_row_info = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multi version col desc generate do not init", K(ret));
  } else if (OB_FAIL(schema_->get_store_column_count(row_info_.column_cnt_))) {
    STORAGE_LOG(WARN, "failed to get store column count", K(ret));
  } else {
    int32_t extra_rowkey_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    row_info_.rowkey_column_cnt_ = schema_->get_rowkey_column_num();
    row_info_.multi_version_rowkey_column_cnt_ = row_info_.rowkey_column_cnt_ + extra_rowkey_col_cnt;
    row_info_.column_cnt_ += extra_rowkey_col_cnt;
    row_info_.trans_version_index_ = row_info_.rowkey_column_cnt_;
    multi_version_row_info = &row_info_;
  }
  return ret;
}

// multi_version row has more columns than normal row
// Need a mapping relationship to fill the multi-version column in the correct position
// Assuming that the non-multi-version row has 5 columns, the subscripts are 0, 1, 2, 3, 4,
// where the first two columns are rowkey columns
// The mapping relationship is as follows
// non-multi-version-row : 0    1         2    3    4
// Mapping relationship  : 0    1   -1    2    3    4
// where -1 is the value added to the trans_version column
int ObMultiVersionColDescGenerate::generate_out_cols_project(const ObIArray<ObColDesc>& column_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multi version col desc generate do not init", K(ret));
  } else if (0 == column_ids.count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "column ids should not be empty", K(ret), K(column_ids.count()));
  } else {
    int32_t inc_index = 0;
    int32_t column_index = 0;
    for (int64_t i = 0; i < column_ids.count() && OB_SUCC(ret); ++i) {
      if (OB_UNLIKELY(OB_HIDDEN_TRANS_VERSION_COLUMN_ID == column_ids.at(i).col_id_) ||
          OB_UNLIKELY(OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID == column_ids.at(i).col_id_)) {
        column_index = -1;
      } else {
        column_index = inc_index;
        ++inc_index;
      }
      if (OB_FAIL(out_cols_project_.push_back(column_index))) {
        STORAGE_LOG(WARN, "Fail to push column index to out cols project", K(ret));
      }
    }
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
