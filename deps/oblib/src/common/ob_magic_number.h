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

#ifndef OCEANBASE_COMMON_MAGIC_NUMBER_H_
#define OCEANBASE_COMMON_MAGIC_NUMBER_H_

#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{
//block sstable
const int64_t BLOCKSSTABLE_SUPER_BLOCK_MAGIC = 1000;
const int64_t BLOCKSSTABLE_COMMON_HEADER_MAGIC = 1001;
const int64_t BLOCKSSTABLE_MACRO_BLOCK_HEADER_MAGIC = 1002;
const int64_t BLOCKSSTABLE_SCHEMA_INDEX_HEADER_MAGIC = 1003;
const int64_t BLOCKSSTABLE_TABLET_IMAGE_HEADER_MAGIC = 1004;
const int64_t BLOCKSSTABLE_MICRO_BLOCK_DATA_MAGIC = 1005;
const int64_t BLOCKSSTABLE_COMPRESSOR_NAME_HEADER_MAGIC = 1006;

}//end namespace common
}//end namespace oceanbase

#endif
