/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
