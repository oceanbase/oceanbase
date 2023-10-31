
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
    datum = iter_->header_->datum_;
    iter_++;
  }

  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase
