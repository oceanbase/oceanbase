/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
