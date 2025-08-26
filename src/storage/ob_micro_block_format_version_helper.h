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
  // micro_block_format_version compact_version
  //          1                    >=0
  //          2                   >=4.4.1
  static constexpr int64_t VALID_VERSION_LIST[] = {INT64_MAX, 0, DATA_VERSION_4_4_1_0};

  static constexpr int64_t DEFAULT_VERSION = 1;

  static constexpr int64_t LATEST_VERSION = 2;

  static constexpr uint64_t MIN_SUPPORTED_VERSION = DATA_VERSION_4_4_1_0;

  static OB_INLINE ObRowStoreType decide_flat_format(int64_t version)
  {
    return version >= 2 ? FLAT_OPT_ROW_STORE : FLAT_ROW_STORE;
  }

  static OB_INLINE bool check_version_valid(const int64_t micro_block_format_version,
                                            const int64_t compat_version)
  {
    return micro_block_format_version > 0 && micro_block_format_version <= LATEST_VERSION
           && compat_version >= VALID_VERSION_LIST[micro_block_format_version];
  }
};
} // namespace storage
} // namespace oceanbase

#endif