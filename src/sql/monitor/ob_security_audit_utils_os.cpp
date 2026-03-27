/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OB_BUILD_AUDIT_SECURITY
#define USING_LOG_PREFIX SQL_MONITOR


namespace oceanbase
{
namespace sql
{

ObAuditTrailType get_audit_trail_type_from_string(const common::ObString &string)
{
  ObAuditTrailType ret_type = ObAuditTrailType::INVALID;
  return ret_type;
}

int ObSecurityAuditUtils::check_allow_audit(ObSQLSessionInfo &session, ObAuditTrailType &at_type)
{
  int ret = OB_SUCCESS;
  at_type = ObAuditTrailType::NONE;
  return ret;
}

int ObSecurityAuditUtils::get_audit_file_name(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  time_t t = 0;
  time(&t);
  struct tm tm;
  ::localtime_r(&t, &tm);
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, OB_LOGGER.SECURITY_AUDIT_FILE_NAME_FORMAT,
                              getpid(), tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                              tm.tm_hour, tm.tm_min, tm.tm_sec))) {
    SERVER_LOG(WARN, "databuff_printf failed", K(ret), K(pos), K(buf_len));
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase

#endif