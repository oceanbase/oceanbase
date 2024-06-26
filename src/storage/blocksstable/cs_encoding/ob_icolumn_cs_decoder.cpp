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

#include "ob_icolumn_cs_decoder.h"
#include "ob_cs_micro_block_transformer.h"
#include "ob_cs_encoding_util.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

int ObIColumnCSDecoder::get_null_count(
    const ObColumnCSDecoderCtx &ctx,
    const int32_t *row_ids,
    const int64_t row_cap,
    int64_t &null_count) const
{
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

} // end of namespace oceanbase
} // end of namespace oceanbase
