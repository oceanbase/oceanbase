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

#include "ob_icolumn_encoder.h"
#include "ob_encoding_util.h"
#include "lib/container/ob_array_iterator.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;

ObIColumnEncoder::ObIColumnEncoder() : is_inited_(false),
    ctx_(NULL), column_index_(0), extend_value_bit_(0), rows_(NULL)
{
}

ObIColumnEncoder::~ObIColumnEncoder()
{
}

int ObIColumnEncoder::init(const ObColumnEncodingCtx &ctx,
    const int64_t column_index, const ObConstDatumRowArray &rows)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (!ctx.encoding_ctx_->is_valid() || column_index < 0
      || column_index >= ctx.encoding_ctx_->column_cnt_ || rows.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ctx), K(column_index), "row count", rows.count());
  } else {
    ctx_ = &ctx;
    column_index_ = column_index;
    rows_ = &rows;
    const ObObjMeta& col_type = ctx_->encoding_ctx_->col_descs_->at(column_index).col_type_;
    column_type_ = col_type;
    is_inited_ = true;
  }
  return ret;
}

void ObIColumnEncoder::reuse()
{
  column_type_.reset();
  ctx_ = NULL;
  column_index_ = 0;
  extend_value_bit_ = 0;
  rows_ = NULL;
  desc_.reuse();
  column_header_.reuse();
  is_inited_ = false;
}

int ObIColumnEncoder::get_row_checksum(int64_t &checksum) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    checksum = 0;
    FOREACH(r, *rows_) {
      checksum += r->get_datum(column_index_).checksum(0);
    }
    /*
    const ObObjTypeStoreClass sc = get_store_class_map()[ob_obj_type_class(column_type_.get_type())];
    if (ObNumberSC == sc || ObStringSC == sc) {
      for (int64_t i = 0; i < rows_->count(); ++i) {
        checksum += rows_->at(i).row_val_.cells_[column_index_].checksum(0);
      }
    } else {
      for (int64_t i = 0; i < ctx_->col_values_->count(); ++i) {
        checksum += ctx_->col_values_->at(i).checksum(0);
      }
    }
    */
  }
  return ret;
}

const int64_t ObSpanColumnEncoder::MAX_EXC_CNT = 100;
const int64_t ObSpanColumnEncoder::EXC_THRESHOLD_PCT = 10;


} // end namespace blocksstable
} // end namespace oceanbase
