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

#define USING_LOG_PREFIX SHARE
#include "share/backup/ob_archive_compatible.h"

namespace oceanbase
{
namespace share {

OB_SERIALIZE_MEMBER(ObArchiveCompatible, version_);

bool ObArchiveCompatible::is_valid() const
{
  return Compatible::NONE < version_ && Compatible::MAX_COMPATIBLE > version_;
}

int ObArchiveCompatible::set_version(int64_t compatible)
{
  int ret = OB_SUCCESS;
  if (!ObArchiveCompatible::is_valid(compatible)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid compatible", K(ret), K(compatible));
  } else {
    version_ = static_cast<Compatible>(compatible);
  }

  return ret;
}

bool ObArchiveCompatible::is_valid(int64_t compatible)
{
  return static_cast<int64_t>(Compatible::NONE) < compatible && static_cast<int64_t>(Compatible::MAX_COMPATIBLE) > compatible;
}

}
}