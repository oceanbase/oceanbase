/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_MICRO_BLOCK_FORMAT_VERSION_HELPER_H_
#define OB_MICRO_BLOCK_FORMAT_VERSION_HELPER_H_

#include "common/ob_store_format.h"
#include "common/ob_version_def.h"

namespace oceanbase
{
namespace storage
{
class ObMicroBlockFormatVersionHelper
{
public:
  static constexpr int64_t DEFAULT_VERSION = 1;
};
} // namespace storage
} // namespace oceanbase

#endif