
/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_column_datum_iter.h"
namespace oceanbase
{
namespace blocksstable
{

int ObColumnDatumIter::get_next(const ObDatum *&datum)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(col_datums_.count() == idx_)) {
    ret = OB_ITER_END;
  } else {
    datum = &col_datums_.at(idx_);
    idx_++;
  }

  return ret;
}

int ObDictDatumIter::get_next(const ObDatum *&datum)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(iter_ == ht_.end())) {
    ret = OB_ITER_END;
  } else {
    datum = &iter_->datum_;
    iter_++;
  }

  return ret;
}

int ObEncodingHashTableDatumIter::get_next(const ObDatum *&datum)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(iter_ == ht_.end())) {
    ret = OB_ITER_END;
  } else {
    datum = iter_->header_->datum_;
    iter_++;
  }

  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase
