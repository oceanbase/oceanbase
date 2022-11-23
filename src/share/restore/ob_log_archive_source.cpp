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
#include "ob_log_archive_source.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_define.h"

using namespace oceanbase::share;
static const char* LogArchiveSourceTypeArray[static_cast<int64_t>(ObLogArchiveSourceType::MAX)] = {
  "INVALID",
  "SERVICE",
  "LOCATION",
  "RAWPATH",
};

bool ObLogArchiveSourceItem::is_valid() const
{
  return id_ > 0 && is_valid_log_source_type(type_) && !value_.empty() && until_ts_ > 0;
}

ObLogArchiveSourceType ObLogArchiveSourceItem::get_source_type(const ObString &type_str)
{
  ObLogArchiveSourceType type = ObLogArchiveSourceType::INVALID;
  for (int64_t i = 0; i < static_cast<int64_t>(ObLogArchiveSourceType::MAX); i++) {
    if (OB_NOT_NULL(LogArchiveSourceTypeArray[i])
        && 0 == type_str.case_compare(LogArchiveSourceTypeArray[i])) {
      type = static_cast<ObLogArchiveSourceType>(i);
      break;
    }
  }
  return type;
}

const char *ObLogArchiveSourceItem::get_source_type_str(const ObLogArchiveSourceType &type)
{
  const char* str = NULL;
  if (type >= ObLogArchiveSourceType::INVALID && type < ObLogArchiveSourceType::MAX) {
    str = LogArchiveSourceTypeArray[static_cast<int64_t>(type)];
  }
  return str;
}

int ObLogArchiveSourceItem::deep_copy(ObLogArchiveSourceItem &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  id_ = other.id_;
  type_ = other.type_;
  until_ts_ = other.until_ts_;
  OZ (ob_write_string(allocator_, other.value_, value_));
  return ret;
}
