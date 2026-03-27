/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_STORAGE_FORMAT_H_
#define OB_STORAGE_FORMAT_H_

#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{

enum ObStorageFormatVersion
{
  OB_STORAGE_FORMAT_VERSION_INVALID = 0,
  OB_STORAGE_FORMAT_VERSION_V1 = 1, // supports micro block compaction
  OB_STORAGE_FORMAT_VERSION_V2 = 2, // supports encoding, not used any more
  OB_STORAGE_FORMAT_VERSION_V3 = 3, // supports micro block compaction optimization
  OB_STORAGE_FORMAT_VERSION_V4 = 4, // supports optimize ObNumber integer store
  OB_STORAGE_FORMAT_VERSION_MAX = 5, // update MAX each time add new version
};

}  // end namespace common
}  // end namespace oceanbase

#endif  // OB_STORAGE_INTERNAL_FORMAT_H_
