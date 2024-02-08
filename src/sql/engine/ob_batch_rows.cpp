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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/ob_batch_rows.h"

namespace oceanbase
{
namespace sql
{

DEF_TO_STRING(ObBatchRows)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(size), K_(end), KP(skip_),
       "skip_bit_vec", ObLogPrintHex(reinterpret_cast<char *>(skip_),
                                     NULL == skip_ ? 0 : ObBitVector::memory_size(size_)));
  J_OBJ_END();
  return pos;
}

} // end namespace sql
} // end namespace oceanbase
