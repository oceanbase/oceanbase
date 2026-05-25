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
#include "share/ob_define.h"
#include "share/config/ob_config_helper.h"  // ObConfigTimeParser
#include "share/ob_cluster_version.h"       // GET_MIN_DATA_VERSION, DATA_VERSION_4_4_2_2
#include "deps/oblib/src/lib/literals/ob_literals.h"

using namespace oceanbase::share;
static const char* LogRestoreSourceTypeArray[static_cast<int64_t>(ObLogRestoreSourceType::MAX)] = {
  "INVALID",
  "SERVICE",
  "LOCATION",
  "RAWPATH",
};

bool ObLogRestoreSourceItem::is_valid() const
{
  return id_ > 0 && is_valid_log_source_type(type_) && !value_.empty() && until_scn_.is_valid()
      && recover_delay_us_ >= 0;
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
  recover_delay_us_ = other.recover_delay_us_;
  OZ (ob_write_string(allocator_, other.value_, value_));
  return ret;
}

int ObLogRestoreSourceItem::parse_delay_value(const char *str, int64_t &delay_us)
{
  int ret = OB_SUCCESS;
  delay_us = 0;
  if (OB_ISNULL(str) || 0 == STRLEN(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid delay value", KR(ret), KP(str));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "DELAY, e.g. DELAY=30s, DELAY=1h");
  } else {
    bool valid = false;
    const int64_t val = ObConfigTimeParser::get(str, valid);
    if (!valid || val < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid delay value", KR(ret), K(str));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "DELAY, e.g. DELAY=30s, DELAY=1h");
    } else {
      delay_us = val;
    }
  }
  return ret;
}


int ObLogRestoreSourceItem::check_delay_data_version(
    const uint64_t tenant_id, bool &enabled)
{
  int ret = OB_SUCCESS;
  enabled = false;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(tenant_id), data_version))) {
    LOG_WARN("failed to get data version", KR(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_4_2_2) {
    enabled = false;
  } else {
    enabled = true;
  }
  return ret;
}
