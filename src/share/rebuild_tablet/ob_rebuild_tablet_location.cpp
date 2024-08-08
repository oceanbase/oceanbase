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
#include "share/rebuild_tablet/ob_rebuild_tablet_location.h"

using namespace oceanbase;
using namespace share;

static const char *location_type_strs[] = {
    "",
    "server_addr",
    "region",
    "server_id",
};

const char *ObRebuildTabletLocationType::get_str(const TYPE &type)
{
  const char *str = nullptr;

  if (type < 0 || type >= MAX) {
    str = "UNKNOWN";
  } else {
    str = location_type_strs[type];
  }
  return str;
}

ObRebuildTabletLocationType::TYPE ObRebuildTabletLocationType::get_type(const char *type_str)
{
  ObRebuildTabletLocationType::TYPE type = ObRebuildTabletLocationType::MAX;

  const int64_t count = ARRAYSIZEOF(location_type_strs);
  STATIC_ASSERT(static_cast<int64_t>(ObRebuildTabletLocationType::MAX) == count, "status count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == strcmp(type_str, location_type_strs[i])) {
      type = static_cast<ObRebuildTabletLocationType::TYPE>(i);
      break;
    }
  }
  return type;
}

ObRebuildTabletLocation::ObRebuildTabletLocation()
  : type_(ObRebuildTabletLocationType::NONE)
{
  MEMSET(location_, '\0', sizeof(location_));
}

void ObRebuildTabletLocation::reset()
{
  type_ = ObRebuildTabletLocationType::NONE;
  MEMSET(location_, '\0', sizeof(location_));
}

bool ObRebuildTabletLocation::is_valid() const
{
  bool b_ret = false;
  int ret = OB_SUCCESS;
  if (!ObRebuildTabletLocationType::is_valid(type_)) {
    b_ret = false;
  } else {
    switch (type_) {
    case ObRebuildTabletLocationType::NONE: {
      b_ret = true;
      break;
    }
    case ObRebuildTabletLocationType::SERVER_ADDR : {
      ObAddr addr;
      if (OB_FAIL(addr.parse_from_cstring(location_))) {
        LOG_WARN("failed to parse from cstring", K(ret), K(location_));
      } else if (addr.is_valid()) {
        b_ret = true;
      }
      break;
    }
    case ObRebuildTabletLocationType::REGION :
    case ObRebuildTabletLocationType::SERVER_ID : {
      b_ret = false;
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("rebuild tablet location type do not support", K(ret), K(type_));
      break;
    }
    default: {
      b_ret = false;
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid type", K(ret), K(type_));
    }
    }
  }
  return b_ret;
}

int ObRebuildTabletLocation::get_location_addr(
    common::ObAddr &addr) const
{
  int ret = OB_SUCCESS;
  addr.reset();
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild tablet src is invalid", K(ret), KPC(this));
  } else {
    switch (type_) {
    case ObRebuildTabletLocationType::NONE: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get src addr type is unexpected", K(ret), K(type_));
      break;
    }
    case ObRebuildTabletLocationType::SERVER_ADDR : {
      if (OB_FAIL(addr.parse_from_cstring(location_))) {
        LOG_WARN("failed to parse from cstring", K(ret));
      }
      break;
    }
    case ObRebuildTabletLocationType::REGION :
    case ObRebuildTabletLocationType::SERVER_ID : {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("rebuild tablet src type do not support", K(ret), K(type_));
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid type", K(ret), K(type_));
    }
    }
  }
  return ret;
}

int ObRebuildTabletLocation::resolve_location(const ObString &location)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  int64_t pos = 0;
  ObString location_str_without_blank;
  int64_t src_type_length = 0;

  if (location.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("resolve src get invalid argument", K(ret), K(location));
  } else if (OB_FAIL(ob_strip_space(allocator, location, location_str_without_blank))) {
    LOG_WARN("strip location space failed", K(location), K(ret));
  } else {
    for (int64_t i = 0; i < location_str_without_blank.length(); ++i) {
      if (':' == location_str_without_blank[i]) {
        pos = i + 1; //skip ':'
        src_type_length = i;
        break;
      }
    }
    const int64_t left_size = location_str_without_blank.length() - pos;
    if (left_size >= MAX_REGION_LENGTH || left_size <= 0 || src_type_length <= 0 || src_type_length >= MAX_REGION_LENGTH) {
      ret = OB_INVALID_ERROR;
      LOG_ERROR("src string is too long or is empty, cannot work", K(ret), K(left_size), K(location_str_without_blank),
          K(pos), K(src_type_length));
    } else {
      ObString src_type_str(src_type_length, location_str_without_blank.ptr());
      if (OB_FAIL(get_type_from_src_(src_type_str))) {
        LOG_WARN("failed to get type from location", K(ret), K(location_str_without_blank));
      } else {
        MEMCPY(location_, location_str_without_blank.ptr() + pos, left_size);
        location_[left_size] ='\0';
        if (!is_valid()) {
          ret = OB_INVALID_ERROR;
          LOG_WARN("rebuild src is invalid", K(ret), K(location_str_without_blank));
        }
      }
    }
  }
  return ret;
}

int ObRebuildTabletLocation::get_type_from_src_(const ObString &src)
{
  int ret = OB_SUCCESS;
  type_ = ObRebuildTabletLocationType::MAX;
  const int64_t count = ARRAYSIZEOF(location_type_strs);
  bool found = false;

  for (int64_t i = 1; OB_SUCC(ret) && i < count && !found; ++i) {
    if (0 == src.compare(location_type_strs[i])) {
      type_ = ObRebuildTabletLocationType::get_type(location_type_strs[i]);
      found = true;
    }
  }

  if (OB_SUCC(ret) && !found) {
    ret = OB_INVALID_ERROR;
    LOG_WARN("invalid src type", K(ret), K(src));
  }
  return ret;
}

int64_t ObRebuildTabletLocation::to_string(char *buffer, const int64_t size) const
{
  int64_t pos = 0;
  pos = snprintf(buffer, size, "[type = %d], [address = %s]", type_, location_);
  if (pos < 0) {
    pos = 0;
  }
  return pos;
}

OB_SERIALIZE_MEMBER(ObRebuildTabletLocation,
                    type_,
                    location_);
