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

#include "ob_encoding_bitset.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

int ObBitMapMetaBaseWriter::init(
    const common::ObIArray<int64_t> *exc_row_ids,
    const ObColDatums *col_datums,
    const common::ObObjMeta type)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(exc_row_ids) || OB_ISNULL(col_datums) || OB_UNLIKELY(exc_row_ids->count() <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(exc_row_ids), KP(col_datums));
  } else {
    exc_row_ids_ = exc_row_ids;
    col_datums_ = col_datums;
    type_ = type;
    is_inited_ = true;
  }
  return ret;
}

int64_t ObBitMapMetaBaseWriter::size() const
{
  return sizeof(ObBitMapMetaHeader) + meta_.data_offset_ + exc_total_size_;
}

} // end namespace blocksstable
} // end namespace oceanbase

