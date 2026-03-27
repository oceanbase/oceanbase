/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_bit_stream.h"


namespace oceanbase
{
namespace blocksstable
{

using namespace common;

const ObBitStream::BS_WORD ObBitStream::bit_mask_table_[] = {
  0x0, 0x1, 0x3, 0x7, 0xf, 0x1f, 0x3f, 0x7f, 0xff };


} // end namespace blocksstable
} // end namespace oceanbase
