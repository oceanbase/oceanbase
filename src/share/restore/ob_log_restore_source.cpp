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
#include "ob_log_restore_source.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_define.h"

using namespace oceanbase::share;
static const char* LogRestoreSourceTypeArray[static_cast<int64_t>(ObLogRestoreSourceType::MAX)] = {
  "INVALID",
  "SERVICE",
  "LOCATION",
  "RAWPATH",
};

bool ObLogRestoreSourceItem::is_valid() const
{
  return id_ > 0 && is_valid_log_source_type(type_) && !value_.empty() && until_scn_.is_valid();
}

ObLogRestoreSourceType ObLogRestoreSourceItem::get_source_type(const ObString &type_str)
{
  ObLogRestoreSourceType type = ObLogRestoreSourceType::INVALID;
  for (int64_t i = 0; i < static_cast<int64_t>(ObLogRestoreSourceType::MAX); i++) {
    if (OB_NOT_NULL(LogRestoreSourceTypeArray[i])
        && 0 == type_str.case_compare(LogRestoreSourceTypeArray[i])) {
      type = static_cast<ObLogRestoreSourceType>(i);
      break;
    }
  }
  return type;
}

const char *ObLogRestoreSourceItem::get_source_type_str(const ObLogRestoreSourceType &type)
{
  const char* str = NULL;
  if (type >= ObLogRestoreSourceType::INVALID && type < ObLogRestoreSourceType::MAX) {
    str = LogRestoreSourceTypeArray[static_cast<int64_t>(type)];
  }
  return str;
}

int ObLogRestoreSourceItem::deep_copy(ObLogRestoreSourceItem &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  id_ = other.id_;
  type_ = other.type_;
  until_scn_ = other.until_scn_;
  OZ (ob_write_string(allocator_, other.value_, value_));
  return ret;
}
