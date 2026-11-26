/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_CONFIG_LOG_UTIL_H_
#define OCEANBASE_SHARE_OB_CONFIG_LOG_UTIL_H_

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace share
{
// Hide the entire value, including malformed credentials and lists of storage URIs.
// This is only a logging view; callers must serialize the original configuration.
inline common::ObString get_config_value_for_log(const common::ObString &name, const common::ObString &value)
{
  common::ObString result = value;
  const char *const sensitive_names[] = {
    "data_backup_dest", "backup_archive_dest", "log_restore_source", "log_archive_dest",
    "log_archive_dest_1", "log_archive_dest_2", "log_archive_dest_3", "log_archive_dest_4",
    "log_archive_dest_5", "log_archive_dest_6", "log_archive_dest_7", "log_archive_dest_8",
    "access_info", "attribute", "path", "location", "audit_log_path",
    "sts_credential", "external_kms_info", "ssl_external_kms_info"
  };
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(sensitive_names) / sizeof(sensitive_names[0])); ++i) {
    if (0 == name.case_compare(sensitive_names[i])) {
      result = common::ObString::make_string("<redacted>");
      break;
    }
  }
  return result;
}
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_CONFIG_LOG_UTIL_H_
