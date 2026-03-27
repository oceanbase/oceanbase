/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
