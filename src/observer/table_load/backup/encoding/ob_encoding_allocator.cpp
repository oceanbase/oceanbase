/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER
#include "ob_encoding_allocator.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

#define DEF_SIZE_ARRAY(Item, size_array) \
int64_t size_array [] = {                \
  sizeof(ObRaw##Item),                   \
  sizeof(ObDict##Item),                  \
  sizeof(ObRLE##Item),                   \
  sizeof(ObConst##Item),                 \
  sizeof(ObIntegerBaseDiff##Item),       \
  sizeof(ObStringDiff##Item),            \
  sizeof(ObHexString##Item),             \
  sizeof(ObStringPrefix##Item),          \
  sizeof(ObColumnEqual##Item),           \
  sizeof(ObInterColSubStr##Item),        \
}                                        \

DEF_SIZE_ARRAY(Decoder, decoder_sizes);

} // table_load_backup
} // namespace observer
} // namespace oceanbase
