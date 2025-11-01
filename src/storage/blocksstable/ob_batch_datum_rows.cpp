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

#include "storage/blocksstable/ob_batch_datum_rows.h"

namespace oceanbase
{
namespace blocksstable
{

void ObBatchDatumRows::reset()
{
  row_flag_.reset();
  mvcc_row_flag_.reset();
  trans_id_.reset();
  vectors_.reset();
  row_count_ = 0;
}

int ObBatchDatumRows::to_datum_row(int64_t idx, ObDatumRow &datum_row) const {
  int ret = OB_SUCCESS;

  if ((idx < 0 || idx >= row_count_) || datum_row.count_ != vectors_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(idx), K(datum_row.count_), K(vectors_.count()), KR(ret));
  }

  if (OB_SUCC(ret)) {
    datum_row.row_flag_ = row_flag_;
    datum_row.mvcc_row_flag_ = mvcc_row_flag_;
    datum_row.trans_id_ = trans_id_;
  }

  const char *pay_load = nullptr;
  bool is_null = false;
  ObLength length = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < vectors_.count(); i ++) {
    common::ObIVector *vec = vectors_.at(i);
    if (vec == nullptr) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("vec should not be null", KR(ret));
    } else {
      vec->get_payload(idx, is_null, pay_load, length);
      if (is_null) {
        datum_row.storage_datums_[i].set_null();
      } else {
        datum_row.storage_datums_[i].shallow_copy_from_datum(ObDatum(pay_load, length, is_null));
      }
    }
  }

  return ret;
}

int ObBatchDatumRows::shadow_copy(const ObBatchDatumRows &other)
{
  int ret = OB_SUCCESS;
  row_flag_ = other.row_flag_;
  mvcc_row_flag_ = other.mvcc_row_flag_;
  trans_id_ = other.trans_id_;
  row_count_ = other.row_count_;
  vectors_.reuse();
  if (OB_FAIL(vectors_.prepare_allocate(other.vectors_.count()))) {
    LOG_WARN("failed to prepare allocate", KR(ret));
  } else {
    for (int64_t i = 0; i < other.vectors_.count(); i++) {
      vectors_.at(i) = other.vectors_.at(i);
    }
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
