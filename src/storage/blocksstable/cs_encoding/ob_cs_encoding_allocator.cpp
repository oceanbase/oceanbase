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

#include "ob_cs_encoding_allocator.h"

namespace oceanbase
{
namespace blocksstable
{

#define CS_DEF_SIZE_ARRAY(Item, size_array)  \
int64_t size_array [] = {                    \
  sizeof(ObInteger##Item),                   \
  sizeof(ObString##Item),                    \
  sizeof(ObIntDict##Item),                   \
  sizeof(ObStrDict##Item),                   \
  sizeof(ObSemiStruct##Item),                \
}                                            \

CS_DEF_SIZE_ARRAY(ColumnEncoder, cs_encoder_sizes);
CS_DEF_SIZE_ARRAY(ColumnDecoder, cs_decoder_sizes);

}//end namespace blocksstable
}//end namespace oceanbase

