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

#include "common/ob_version_def.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{

bool VersionUtil::check_version_valid(const uint64_t version)
{
  bool bret = true;
  const uint32_t major = OB_VSN_MAJOR(version);
  const uint16_t minor = OB_VSN_MINOR(version);
  const uint8_t major_patch = OB_VSN_MAJOR_PATCH(version);
  const uint8_t minor_patch = OB_VSN_MINOR_PATCH(version);
  if (major < 3 || (3 == major && minor < 2)) {
    // cluster_version is less than "3.2":
    // - should be "a.b.0.c";
    bret = (0 == major_patch);
  } else if (3 == major && 2 == minor) {
    // cluster_version's prefix is "3.2":
    // - cluster_version == 3.2.0.0/1/2
    // - cluster_version >= 3.2.3.x
    bret = (0 == major_patch && minor_patch <= 2) || (major_patch >= 3);
  } else {
    // cluster_version is greator than "3.2"
    bret = true;
  }
  return bret;
}

int64_t VersionUtil::print_version_str(char *buf, const int64_t buf_len, uint64_t version)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const uint32_t major = OB_VSN_MAJOR(version);
  const uint16_t minor = OB_VSN_MINOR(version);
  const uint8_t major_patch = OB_VSN_MAJOR_PATCH(version);
  const uint8_t minor_patch = OB_VSN_MINOR_PATCH(version);
  if (OB_UNLIKELY(!check_version_valid(version))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "invalid cluster version", K(version), K(lbt()));
  } else if (major < 3
             || (3 == major && minor < 2)
             || (3 == major && 2 == minor && 0 == major_patch && minor_patch < 3)) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%u.%u.%u",
                major, minor, minor_patch))) {
      COMMON_LOG(WARN, "fail to print version str", K(ret), K(version));
    }
  } else {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%u.%u.%u.%u",
                major, minor, major_patch, minor_patch))) {
      COMMON_LOG(WARN, "fail to print version str", K(ret), K(version));
    }
  }
  if (OB_FAIL(ret)) {
    pos = OB_INVALID_INDEX;
  }
  return pos;
}

} // namespace common
} // namespace oceanbase