/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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