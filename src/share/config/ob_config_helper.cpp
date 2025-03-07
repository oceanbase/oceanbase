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

#include "ob_config_helper.h"
#include "share/ob_resource_limit.h"
#include "src/observer/ob_server.h"
#include "share/config/ob_config_mode_name_def.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "share/backup/ob_tenant_archive_mgr.h"
#include "plugin/sys/ob_plugin_load_param.h"
#include "share/table/ob_table_config_util.h"

namespace oceanbase
{
using namespace share;
using namespace obrpc;
using namespace plugin;

namespace common
{

bool ObConfigIpChecker::check(const ObConfigItem &t) const
{
  struct sockaddr_in sa;
  int result = inet_pton(AF_INET, t.str(), &(sa.sin_addr));
  return result != 0;
}

ObConfigConsChecker:: ~ObConfigConsChecker()
{
  if (NULL != left_) {
    ObConfigChecker *left = const_cast<ObConfigChecker*>(left_);
    OB_DELETE(ObConfigChecker, "unused", left);
  }
  if (NULL != right_) {
    ObConfigChecker *right = const_cast<ObConfigChecker*>(right_);
    OB_DELETE(ObConfigChecker, "unused", right);
  }
}
bool ObConfigConsChecker::check(const ObConfigItem &t) const
{
  return (NULL == left_ ? true : left_->check(t))
         && (NULL == right_ ? true : right_->check(t));
}

bool ObConfigEvenIntChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  int64_t value = ObConfigIntParser::get(t.str(), is_valid);
  if (is_valid) {
    is_valid = value % 2 == 0;
  }
  return is_valid;
}

bool ObConfigFreezeTriggerIntChecker::check(const uint64_t tenant_id,
                                            const ObAdminSetConfigItem &t)
{
  bool is_valid = false;
  int64_t value = ObConfigIntParser::get(t.value_.ptr(), is_valid);
  int64_t write_throttle_trigger = get_write_throttle_trigger_percentage_(tenant_id);
  if (is_valid) {
    is_valid = value > 0 && value < 100;
  }
  if (is_valid) {
    is_valid = write_throttle_trigger != 0;
  }
  if (is_valid) {
    is_valid = value < write_throttle_trigger;
  }
  return is_valid;
}

int64_t ObConfigFreezeTriggerIntChecker::get_write_throttle_trigger_percentage_(const uint64_t tenant_id)
{
  int64_t percent = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    percent = tenant_config->writing_throttling_trigger_percentage;
  }
  return percent;
}

bool ObConfigTxShareMemoryLimitChecker::check(const uint64_t tenant_id, const ObAdminSetConfigItem &t)
{
  bool is_valid = false;
  int64_t value = ObConfigIntParser::get(t.value_.ptr(), is_valid);
  int64_t cluster_memstore_limit = GCONF.memstore_limit_percentage;
  int64_t memstore_limit = 0;
  int64_t tx_data_limit = 0;
  int64_t mds_limit = 0;

  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    memstore_limit = tenant_config->_memstore_limit_percentage;
    tx_data_limit = tenant_config->_tx_data_memory_limit_percentage;
    mds_limit = tenant_config->_mds_memory_limit_percentage;
  } else {
    is_valid = false;
    OB_LOG_RET(ERROR, OB_INVALID_CONFIG, "tenant config is invalid", K(tenant_id));
  }

  if (0 == memstore_limit) {
    memstore_limit = cluster_memstore_limit;
  }
  if (!is_valid) {
  } else if (0 == memstore_limit) {
    // both 0 means adjust the percentage automatically.
    is_valid = true;
  } else if (0 == value) {
    // 0 is default value, which means (_tx_share_memory_limit_percentage = memstore_limit_percentage + 10)
    is_valid = true;
  } else if ((value > 0 && value < 100) && (memstore_limit <= value) && (tx_data_limit <= value) &&
             (mds_limit <= value)) {
    is_valid = true;
  } else {
    is_valid = false;
  }

  if (!is_valid) {
    OB_LOG_RET(WARN, OB_INVALID_CONFIG,
       "update _tx_share_memory_limit_percentage failed",
       "_tx_share_memory_limit_percentage",   value,
       "memstore_limit_percentage",           memstore_limit,
       "_tx_data_memory_limit_percentage",    tx_data_limit,
       "_mds_memory_limit_percentage",        mds_limit);
  }

  return is_valid;
}

bool less_or_equal_tx_share_limit(const uint64_t tenant_id, const int64_t value)
{
  bool bool_ret = true;
  int64_t tx_share_limit = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    tx_share_limit = tenant_config->_tx_share_memory_limit_percentage;
    if (0 == value) {
      // 0 is default value, which means memstore limit percentage will adjust itself.
      bool_ret = true;
    } else if (0 == tx_share_limit) {
      // 0 is default value, which means (_tx_share_memory_limit_percentage = memstore_limit_percentage + 10)
      bool_ret = true;
    } else if (value > 0 && value < 100 && value <= tx_share_limit) {
      bool_ret = true;
    } else {
      bool_ret = false;
    }
  } else {
    bool_ret = false;
    OB_LOG_RET(ERROR, OB_INVALID_CONFIG, "tenant config is invalid", K(tenant_id));
  }
  return bool_ret;
}

bool check_vector_memory_limit(const uint64_t tenant_id, const int64_t value)
{
  bool bool_ret = false;
  int64_t vector_memory_limit = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    vector_memory_limit = tenant_config->ob_vector_memory_limit_percentage;
    if (0 == vector_memory_limit) {
      // 0 is default value, which means vector index is disabled,do not need to check
      bool_ret = true;
    } else if (value + 15 + vector_memory_limit >= 100) {
      bool_ret = false;
    } else {
      bool_ret = true;
    }
  } else {
    bool_ret = false;
    OB_LOG_RET(ERROR, OB_INVALID_CONFIG, "tenant config check_vector_memory_limit is invalid",K(value), K(vector_memory_limit), K(tenant_id));
  }
  return bool_ret;
}

bool ObConfigMemstoreLimitChecker::check(const uint64_t tenant_id, const obrpc::ObAdminSetConfigItem &t)
{
  bool is_valid = false;
  int64_t value = ObConfigIntParser::get(t.value_.ptr(), is_valid);
  if (less_or_equal_tx_share_limit(tenant_id, value) &&
      check_vector_memory_limit(tenant_id, value)) {
    is_valid = true;
  } else {
    is_valid = false;
  }
  return is_valid;
}

bool ObConfigTxDataLimitChecker::check(const uint64_t tenant_id, const obrpc::ObAdminSetConfigItem &t)
{
  bool is_valid = false;
  int64_t value = ObConfigIntParser::get(t.value_.ptr(), is_valid);
  if (less_or_equal_tx_share_limit(tenant_id, value)) {
    is_valid = true;
  } else {
    is_valid = false;
  }
  return is_valid;
}

bool ObConfigMdsLimitChecker::check(const uint64_t tenant_id, const obrpc::ObAdminSetConfigItem &t)
{
  bool is_valid = false;
  int64_t value = ObConfigIntParser::get(t.value_.ptr(), is_valid);
  if (less_or_equal_tx_share_limit(tenant_id, value)) {
    is_valid = true;
  } else {
    is_valid = false;
  }
  return is_valid;
}

bool ObConfigWriteThrottleTriggerIntChecker::check(const uint64_t tenant_id,
                                                   const ObAdminSetConfigItem &t)
{
  bool is_valid = false;
  int64_t value = ObConfigIntParser::get(t.value_.ptr(), is_valid);
  int64_t freeze_trigger = get_freeze_trigger_percentage_(tenant_id);
  if (is_valid) {
    is_valid = value > 0 && value <= 100;
  }
  if (is_valid) {
    is_valid = freeze_trigger != 0;
  }
  if (is_valid) {
    is_valid = value > freeze_trigger;
  }
  return is_valid;
}

int64_t ObConfigWriteThrottleTriggerIntChecker::get_freeze_trigger_percentage_(const uint64_t tenant_id)
{
  int64_t percent = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    percent = tenant_config->freeze_trigger_percentage;
  }
  return percent;
}

bool ObConfigLogDiskLimitThresholdIntChecker::check(const uint64_t tenant_id,
                                                               const ObAdminSetConfigItem &t)
{
  bool is_valid = false;
  const int64_t value = ObConfigIntParser::get(t.value_.ptr(), is_valid);
  const int64_t throttling_percentage = get_log_disk_throttling_percentage_(tenant_id);
  if (is_valid) {
    is_valid = (throttling_percentage != 0);
  }
  if (is_valid) {
    is_valid = (throttling_percentage == 100) || (value > throttling_percentage);
  }
  return is_valid;
}

int64_t ObConfigLogDiskLimitThresholdIntChecker::get_log_disk_throttling_percentage_(const uint64_t tenant_id)
{
  int64_t percent = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    percent = tenant_config->log_disk_throttling_percentage;
  }
  return percent;
}

bool ObConfigLogDiskThrottlingPercentageIntChecker::check(const uint64_t tenant_id, const obrpc::ObAdminSetConfigItem &t)
{
  bool is_valid = false;
  const int64_t value = ObConfigIntParser::get(t.value_.ptr(), is_valid);
  const int64_t limit_threshold = get_log_disk_utilization_limit_threshold_(tenant_id);
  if (is_valid) {
    is_valid = (limit_threshold != 0);
  }
  if (is_valid) {
    is_valid = (value == 100) || (value < limit_threshold);
  }
  return is_valid;
}

int64_t ObConfigLogDiskThrottlingPercentageIntChecker::get_log_disk_utilization_limit_threshold_(const uint64_t tenant_id)
{
  int64_t threshold = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    threshold = tenant_config->log_disk_utilization_limit_threshold;
  }
  return threshold;
}

bool ObConfigTabletSizeChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  const int64_t mask = (1 << 21) - 1;
  int64_t value = ObConfigCapacityParser::get(t.str(), is_valid, false);
  if (is_valid) {
    // value has to be a multiple of 2M
    is_valid = (value >= 0) && !(value & mask);
  }
  return is_valid;
}

bool ObConfigStaleTimeChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  int64_t stale_time = ObConfigTimeParser::get(t.str(), is_valid);
  if (is_valid) {
    is_valid = (stale_time >= GCONF.weak_read_version_refresh_interval);
    if (!is_valid) {
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "max_stale_time_for_weak_consistency violate"
          " weak_read_version_refresh_interval,");
    }
  }
  return is_valid;
}

bool ObConfigCompressFuncChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  for (int i = 0; i < ARRAYSIZEOF(common::compress_funcs); ++i) {
    if (0 == ObString::make_string(compress_funcs[i]).case_compare(t.str())) {
      if (i != DISABLED_ZLIB_1_COMPRESS_IDX) {
        is_valid = true;
      }
      break;
    }
  }
  return is_valid;
}

bool ObConfigPerfCompressFuncChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  for (int i = 0; i < ARRAYSIZEOF(common::perf_compress_funcs) && !is_valid; ++i) {
    if (0 == ObString::make_string(perf_compress_funcs[i]).case_compare(t.str())) {
      is_valid = true;
    }
  }
  return is_valid;
}

bool ObConfigResourceLimitSpecChecker::check(const ObConfigItem &t) const
{
  ObResourceLimit rl;
  int ret = rl.load_config(t.str());
  return OB_SUCCESS == ret;
}

bool ObConfigTempStoreFormatChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  for (int i = 0; i < ARRAYSIZEOF(share::temp_store_format_options) && !is_valid; ++i) {
    if (0 == ObString::make_string(temp_store_format_options[i]).case_compare(t.str())) {
      is_valid = true;
    }
  }
  return is_valid;
}

bool ObConfigPxBFGroupSizeChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  ObString str("auto");
  if (0 == str.case_compare(t.str())) {
    is_valid = true;
  // max_number: 2^64 - 1
  } else if (strlen(t.str()) <= 20 && strlen(t.str()) > 0) {
    is_valid = true;
    for (int i = 0; i < strlen(t.str()); ++i) {
      if (0 == i && (t.str()[i] <= '0' || t.str()[i] > '9')) {
        is_valid = false;
        break;
      } else if (t.str()[i] < '0' || t.str()[i] > '9') {
        is_valid = false;
        break;
      }
    }
  }
  return is_valid;
}

bool ObConfigRowFormatChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  ObStoreFormatType type = OB_STORE_FORMAT_INVALID;
  if (OB_ISNULL(t.str()) || strlen(t.str()) == 0) {
  } else if (OB_SUCCESS != ObStoreFormat::find_store_format_type_mysql(ObString::make_string(t.str()), type)) {
  } else if (ObStoreFormat::is_store_format_mysql(type)) {
    is_valid = true;
  }
  return is_valid;
}

bool ObConfigCompressOptionChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  ObStoreFormatType type = OB_STORE_FORMAT_INVALID;
  if (OB_ISNULL(t.str()) || strlen(t.str()) == 0) {
  } else if (OB_SUCCESS != ObStoreFormat::find_store_format_type_oracle(ObString::make_string(t.str()), type)) {
  } else if (ObStoreFormat::is_store_format_oracle(type)) {
    is_valid = true;
  }
  return is_valid;
}

bool ObConfigMaxSyslogFileCountChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  int64_t max_count = ObConfigIntParser::get(t.str(), is_valid);
  if (is_valid) {
    int64_t uncompressed_count = GCONF.syslog_file_uncompressed_count;
    if (max_count == 0 || max_count >= uncompressed_count) {
      is_valid = true;
    } else {
      is_valid = false;
    }
  }
  return is_valid;
}

bool ObConfigSyslogCompressFuncChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  for (int i = 0; i < ARRAYSIZEOF(common::syslog_compress_funcs) && !is_valid; ++i) {
    if (0 == ObString::make_string(syslog_compress_funcs[i]).case_compare(t.str())) {
      is_valid = true;
    }
  }
  return is_valid;
}

bool ObConfigSyslogFileUncompressedCountChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  int64_t uncompressed_count = ObConfigIntParser::get(t.str(), is_valid);
  if (is_valid) {
    int64_t max_count = GCONF.max_syslog_file_count;
    if (uncompressed_count >= 0 && (max_count == 0 || uncompressed_count <= max_count)) {
      is_valid = true;
    } else {
      is_valid = false;
    }
  }
  return is_valid;
}

bool ObConfigLogLevelChecker::check(const ObConfigItem &t) const
{
  const ObString tmp_str(t.str());
  return ((0 == tmp_str.case_compare(ObLogger::PERF_LEVEL))
      || OB_SUCCESS == OB_LOGGER.parse_check(tmp_str.ptr(), tmp_str.length()));
}

bool ObConfigAlertLogLevelChecker::check(const ObConfigItem &t) const
{
  const ObString tmp_str(t.str());
  return OB_SUCCESS == OB_LOGGER.parse_check_alert(tmp_str.ptr(), tmp_str.length());
}

bool ObConfigAuditTrailChecker::check(const ObConfigItem &t) const
{
  common::ObString tmp_string(t.str());
  return sql::get_audit_trail_type_from_string(tmp_string) != sql::ObAuditTrailType::INVALID ;
}

bool ObConfigAuditLogCompressionChecker::check(const ObConfigItem &t) const
{
  common::ObString tmp_string(t.str());
  return 0 == tmp_string.case_compare("NONE")
      || 0 == tmp_string.case_compare("ZSTD");
}

bool ObConfigAuditLogPathChecker::check(const ObConfigItem &t) const
{
  int ret = OB_SUCCESS;
  common::ObString tmp_string(t.str());
  ObBackupDest dest;
  if (tmp_string.empty()) {
    // do nothing
  } else if (OB_FAIL(dest.set(tmp_string))) {
    OB_LOG(WARN, "failed to set backup dest", K(ret));
  }
  return OB_SUCCESS == ret;
}

bool ObConfigAuditLogFormatChecker::check(const ObConfigItem &t) const
{
  common::ObString tmp_string(t.str());
  return 0 == tmp_string.case_compare("CSV");
}

bool ObConfigAuditLogQuerySQLChecker::check(const ObConfigItem &t) const
{
  common::ObString tmp_string(t.str());
  return 0 == tmp_string.case_compare("ALL")
      || 0 == tmp_string.case_compare("NONE");
}

bool ObConfigAuditLogStrategyChecker::check(const ObConfigItem &t) const
{
  common::ObString tmp_string(t.str());
  return 0 == tmp_string.case_compare("ASYNCHRONOUS")
      || 0 == tmp_string.case_compare("PERFORMANCE")
      || 0 == tmp_string.case_compare("SYNCHRONOUS");
}

bool ObConfigWorkAreaPolicyChecker::check(const ObConfigItem &t) const
{
  const ObString tmp_str(t.str());
  return ((0 == tmp_str.case_compare(MANUAL)) || (0 == tmp_str.case_compare(AUTO)));
}

bool ObConfigLogArchiveOptionsChecker::check(const ObConfigItem &t) const
{
  bool bret = true;
  int ret = OB_SUCCESS;
  SMART_VAR(char[OB_MAX_CONFIG_VALUE_LEN], tmp_str) {
    const size_t str_len = STRLEN(t.str());
    MEMCPY(tmp_str, t.str(), str_len);
    tmp_str[str_len] = 0;
    const int64_t FORMAT_BUF_LEN = str_len * 3;// '=' will be replaced with ' = '
    char format_str_buf[FORMAT_BUF_LEN];
    int ret = OB_SUCCESS;
    //first replace '=' with ' = '
    if (OB_FAIL(ObConfigLogArchiveOptionsItem::format_option_str(tmp_str,
                                                                 str_len,
                                                                 format_str_buf,
                                                                 FORMAT_BUF_LEN))) {
      bret = false;
      OB_LOG(WARN, "failed to format_option_str", KR(bret), K(tmp_str));
    } else {
      char *saveptr = NULL;
      char *s = STRTOK_R(format_str_buf, " ", &saveptr);
      bool is_equal_sign_demanded = false;
      int64_t key_idx = -1;
      if (OB_LIKELY(NULL != s)) {
        do {
          if (is_equal_sign_demanded) {
            if (0 == ObString::make_string("=").case_compare(s)) {
              is_equal_sign_demanded = false;
            } else {
              OB_LOG(WARN, " '=' is expected", K(s));
              bret = false;
            }
          } else if (key_idx < 0) {
            int64_t idx = ObConfigLogArchiveOptionsItem::get_keywords_idx(s, is_equal_sign_demanded);
            if (idx < 0) {
              bret = false;
              OB_LOG(WARN, " not expected isolate option", K(s));
            } else if (is_equal_sign_demanded) {
              key_idx = idx;
            } else {
              key_idx = -1;
              bret = ObConfigLogArchiveOptionsItem::is_valid_isolate_option(idx);
            }
          } else if (LOG_ARCHIVE_COMPRESSION_IDX == key_idx) {
            if (-1 == ObConfigLogArchiveOptionsItem::get_compression_option_idx(s)) {
              OB_LOG(WARN, "failed to get_compression_option_idx", K(key_idx), K(s));
              bret = false;
            }
            key_idx = -1;
          } else if (LOG_ARCHIVE_ENCRYPTION_MODE_IDX == key_idx) {
            ObBackupEncryptionMode::EncryptionMode mode = ObBackupEncryptionMode::parse_str(s);
            if (!ObBackupEncryptionMode::is_valid_for_log_archive(mode)) {
              OB_LOG(WARN, "invalid encrytion mode", K(mode));
              bret = false;
            }
            key_idx = -1;
          } else if (LOG_ARCHIVE_ENCRYPTION_ALGORITHM_IDX == key_idx) {
            share::ObCipherOpMode encryption_algorithm;
            if (OB_FAIL(ObEncryptionUtil::parse_encryption_algorithm(s, encryption_algorithm))) {
              bret = false;
              OB_LOG(WARN, "invalid encrytion algorithm", K(s));
            }
            key_idx = -1;
          } else {
            OB_LOG(WARN, "invalid key_idx", K(key_idx), K(s));
            bret = false;
          }
        } while (OB_LIKELY(NULL != (s = STRTOK_R(NULL, " ", &saveptr))) && bret);

        if (key_idx >= 0) {
          bret = false;
          OB_LOG(WARN, "kv option is not compelte", K(tmp_str));
        }
      } else {
        bret = false;
        OB_LOG(WARN, "invalid config value", K(tmp_str));
      }
    }
  }
  return bret;
}

bool ObConfigRpcChecksumChecker::check(const ObConfigItem &t) const
{
  common::ObString tmp_string(t.str());
  return obrpc::get_rpc_checksum_check_level_from_string(tmp_string) != obrpc::ObRpcCheckSumCheckLevel::INVALID ;
}

bool ObTTLDutyDurationChecker::check(const ObConfigItem& t) const
{
  common::ObTTLDutyDuration duty_duration;
  return OB_SUCCESS == common::ObTTLUtil::parse(t.str(), duty_duration) && duty_duration.is_valid();
}

bool ObMySQLVersionLengthChecker::check(const ObConfigItem& t) const
{
  return STRLEN(t.str()) < 16; // length of MySQL version is less then 16
}

bool ObConfigPublishSchemaModeChecker::check(const ObConfigItem& t) const
{
  return 0 == t.case_compare(PUBLISH_SCHEMA_MODE_BEST_EFFORT)
         || 0 == t.case_compare(PUBLISH_SCHEMA_MODE_ASYNC);
}

bool ObConfigMemoryLimitChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  int64_t value = ObConfigCapacityParser::get(t.str(), is_valid, false);
  if (is_valid) {
    int64_t min_memory_size = OBSERVER.is_arbitration_mode() ? lib::ObRunningModeConfig::instance().MIN_MEM :
                                                               lib::ObRunningModeConfig::instance().MINI_MEM_LOWER;
    is_valid = 0 == value || value >= min_memory_size;
  }
  return is_valid;
}

bool ObConfigTenantMemoryChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  int64_t value = ObConfigCapacityParser::get(t.str(), is_valid);
  if (is_valid) {
    is_valid = 0 == value || (value >= ObUnitResource::UNIT_MIN_MEMORY);
  }
  return is_valid;
}

bool ObConfigTenantDataDiskChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  int64_t value = ObConfigCapacityParser::get(t.str(), is_valid);
  if (is_valid) {
    is_valid = ((0 == value) || (value >= ObUnitResource::HIDDEN_SYS_TENANT_MIN_DATA_DISK_SIZE));
  }
  return is_valid;
}

bool ObConfigVectorMemoryChecker::check(const uint64_t tenant_id, const obrpc::ObAdminSetConfigItem &t)
{
  bool is_valid = false;
  int64_t value = ObConfigIntParser::get(t.value_.ptr(), is_valid);
  int64_t cur_value = 0;
  int64_t upper_limit = 0;
  int ret = OB_SUCCESS;
  if (is_valid) {
    if (value == 0) {
      is_valid = true;
    } else if (OB_FAIL(ObPluginVectorIndexHelper::get_vector_memory_value_and_limit(tenant_id, cur_value, upper_limit))) {
      OB_LOG_RET(ERROR, OB_INVALID_CONFIG, "fail to get_vector_memory_value_and_limit", K(tenant_id));
    } else if (0 < value && value < upper_limit) {
      is_valid = true;
    } else {
      is_valid = false;
    }
    int64_t memory_size = 0;
    ObPluginVectorIndexHelper::get_vector_memory_limit_size(tenant_id, memory_size);
  }
  return is_valid;
}

bool ObConfigQueryRateLimitChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  int64_t value = ObConfigIntParser::get(t.str(), is_valid);
  if (is_valid) {
    is_valid = (-1 == value ||
                (value >= MIN_QUERY_RATE_LIMIT &&
                 value <= MAX_QUERY_RATE_LIMIT));
  }
  return is_valid;
}

const char *ObConfigPartitionBalanceStrategyFuncChecker::balance_strategy[
      ObConfigPartitionBalanceStrategyFuncChecker::PARTITION_BALANCE_STRATEGY_MAX] = {
  "auto",
  "standard",
  "disk_utilization_only",
};

bool ObConfigPartitionBalanceStrategyFuncChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  for (int64_t i = 0; i < ARRAYSIZEOF(balance_strategy) && !is_valid; ++i) {
    if (0 == ObString::make_string(balance_strategy[i]).case_compare(t.str())) {
      is_valid = true;
    }
  }
  return is_valid;
}

bool ObDataStorageErrorToleranceTimeChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  int64_t value = ObConfigTimeParser::get(t.str(), is_valid);
  if (is_valid) {
    const int64_t warning_value = GCONF.data_storage_warning_tolerance_time;
    is_valid = value >= warning_value;
  }
  return is_valid;
}

bool ObConfigOfsBlockVerifyIntervalChecker::check(const ObConfigItem &t) const
{
  bool is_valid = true;
  int64_t value = ObConfigTimeParser::get(t.str(), is_valid);
  if (is_valid) {
    is_valid = (0 == value) || (value >= MIN_VALID_INTVL && value <= MAX_VALID_INTVL);
  }
  return is_valid;
}

bool ObLogDiskUsagePercentageChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  int64_t value = ObConfigIntParser::get(t.str(), is_valid);
  if (is_valid) {
    // TODO by runlun: 租户级配置项检查
    const int64_t log_disk_utilization_threshold = 100;
    if (value < log_disk_utilization_threshold) {
      is_valid = false;
      LOG_USER_ERROR(OB_INVALID_CONFIG,
          "log_disk_utilization_limit_threshold "
          "should not be less than log_disk_utilization_threshold");
    }
  }
  return is_valid;
}

bool ObConfigEnableDefensiveChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  int64_t value = ObConfigIntParser::get(t.str(), is_valid);
  if (is_valid) {
    if (value > 2 || value < 0) {
      is_valid = false;
    }
  }
  return is_valid;
}

int64_t ObConfigIntParser::get(const char *str, bool &valid)
{
  char *p_end = NULL;
  int64_t value = 0;

  if (OB_ISNULL(str) || '\0' == str[0]) {
    valid = false;
  } else {
    valid = true;
    value = strtol(str, &p_end, 0);
    if ('\0' == *p_end) {
      valid = true;
    } else {
      valid = false;
      OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "set int error", K(str), K(valid));
    }
  }
  return value;
}

int64_t ObConfigCapacityParser::get(const char *str, bool &valid,
                                    bool check_unit /* = true */,
                                    bool use_byte /* = false*/)
{
  return parse_config_capacity(str, valid, check_unit, use_byte);
}

int64_t ObConfigReadableIntParser::get(const char *str, bool &valid)
{
  char *p_unit = NULL;
  int64_t value = 0;

  if (OB_ISNULL(str) || '\0' == str[0]) {
    valid = false;
  } else {
    valid = true;
    value = strtol(str, &p_unit, 0);

    if (OB_ISNULL(p_unit)) {
      valid = false;
    } else if (value < 0) {
      valid = false;
    } else if ('\0' == *p_unit) {
      //
      // without any unit, do nothing
    } else if (0 == STRCASECMP("k", p_unit)) {
      value *= UNIT_K;
    } else if (0 == STRCASECMP("m", p_unit)) {
      value *= UNIT_M;
    } else {
      valid = false;
      OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "set readable int error", K(str), K(p_unit));
    }
  }

  return value;
}

int64_t ObConfigTimeParser::get(const char *str, bool &valid)
{
  char *p_unit = NULL;
  int64_t value = 0;

  if (OB_ISNULL(str) || '\0' == str[0]) {
    valid = false;
  } else {
    valid = true;
    value = strtol(str, &p_unit, 0);

    if (OB_ISNULL(p_unit)) {
      valid = false;
    } else if (value < 0) {
      valid = false;
    } else if (0 == STRCASECMP("us", p_unit)) {
      value = value * TIME_MICROSECOND;
    } else if (0 == STRCASECMP("ms", p_unit)) {
      value = value * TIME_MILLISECOND;
    } else if ('\0' == *p_unit || 0 == STRCASECMP("s", p_unit)) {
      value = value * TIME_SECOND;
    } else if (0 == STRCASECMP("m", p_unit)) {
      value = value * TIME_MINUTE;
    } else if (0 == STRCASECMP("h", p_unit)) {
      value = value * TIME_HOUR;
    } else if (0 == STRCASECMP("d", p_unit)) {
      value = value * TIME_DAY;
    } else {
      valid = false;
      OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "set time error", K(str), K(p_unit));
    }
  }

  return value;
}

bool ObConfigUpgradeStageChecker::check(const ObConfigItem &t) const
{
  obrpc::ObUpgradeStage stage = obrpc::get_upgrade_stage(t.str());
  return obrpc::OB_UPGRADE_STAGE_INVALID < stage
         && obrpc::OB_UPGRADE_STAGE_MAX > stage;
}

bool ObConfigPlanCacheGCChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  for (int i = 0; i < ARRAYSIZEOF(sql::plan_cache_gc_confs) && !is_valid; i++) {
    if (0 == ObString::make_string(sql::plan_cache_gc_confs[i]).case_compare(t.str())) {
      is_valid = true;
    }
  }
  return is_valid;
}

bool ObConfigSTScredentialChecker::check(const ObConfigItem &t) const
{
  int ret = OB_SUCCESS;
  bool flag = true;
  const char *tmp_credential = t.str();
  ObStsCredential key;
  if (OB_ISNULL(tmp_credential) || OB_UNLIKELY(strlen(tmp_credential) <= 0
      || strlen(tmp_credential) > OB_MAX_STS_CREDENTIAL_LENGTH)) {
    flag = false;
    OB_LOG(WARN, "invalid sts credential", KP(tmp_credential));
  } else if (OB_FAIL(check_sts_credential_format(tmp_credential, key))) {
    flag = false;
    OB_LOG(WARN, "fail to check sts credential format", K(ret), K(key), KP(tmp_credential));
  }
  return flag;
}

bool ObConfigUseLargePagesChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  for (int i = 0; i < ARRAYSIZEOF(lib::use_large_pages_confs) && !is_valid; i++) {
    if (0 == ObString::make_string(lib::use_large_pages_confs[i]).case_compare(t.str())) {
      is_valid = true;
    }
  }
  return is_valid;
}

bool ObConfigAuditModeChecker::check(const ObConfigItem &t) const
{
  ObString v_str(t.str());
  return 0 == v_str.case_compare("NONE") ||
         0 == v_str.case_compare("ORACLE") ||
         0 == v_str.case_compare("MYSQL");
}

bool ObConfigBoolParser::get(const char *str, bool &valid)
{
  bool value = true;
  valid = false;

  if (OB_ISNULL(str)) {
    valid = false;
    OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "Get bool config item fail, str is NULL!");
  } else if (0 == STRCASECMP(str, "false")) {
    valid = true;
    value = false;
  } else if (0 == STRCASECMP(str, "true")) {
    valid = true;
    value = true;
  } else if (0 == STRCASECMP(str, "off")) {
    valid = true;
    value = false;
  } else if (0 == STRCASECMP(str, "on")) {
    valid = true;
    value = true;
  } else if (0 == STRCASECMP(str, "no")) {
    valid = true;
    value = false;
  } else if (0 == STRCASECMP(str, "yes")) {
    valid = true;
    value = true;
  } else if (0 == STRCASECMP(str, "f")) {
    valid = true;
    value = false;
  } else if (0 == STRCASECMP(str, "t")) {
    valid = true;
    value = true;
  } else if (0 == STRCASECMP(str, "1")) {
    valid = true;
    value = true;
  } else if (0 == STRCASECMP(str, "0")) {
    valid = true;
    value = false;
  } else {
    OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "Get bool config item fail", K(str));
    valid = false;
  }
  return value;
}

bool ObCtxMemoryLimitChecker::check(const ObConfigItem &t) const
{
  uint64_t ctx_id = 0;
  int64_t limit = 0;
  return check(t.str(), ctx_id, limit);
}

bool ObCtxMemoryLimitChecker::check(const char* str, uint64_t& ctx_id, int64_t& limit) const
{
  bool is_valid = false;
  ctx_id = 0;
  limit = 0;
  if ('\0' == str[0]) {
    is_valid = true;
  } else {
    auto len = STRLEN(str);
    for (int64_t i = 0; i + 1 < len && !is_valid; ++i) {
      if (':' == str[i]) {
        limit = ObConfigCapacityParser::get(str + i + 1, is_valid, false);
        if (is_valid) {
          int ret = OB_SUCCESS;
          SMART_VAR(char[OB_MAX_CONFIG_VALUE_LEN], tmp_str) {
            strncpy(tmp_str, str, i);
            tmp_str[i] = '\0';
            is_valid = get_global_ctx_info().is_valid_ctx_name(tmp_str, ctx_id);
          }
        }
      }
    }
  }
  return is_valid && limit >= 0;
}

bool ObAutoIncrementModeChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  ObString order("order");
  ObString noorder("noorder");
  if (0 == order.case_compare(t.str()) || 0 == noorder.case_compare(t.str())) {
    is_valid = true;
  }
  return is_valid;
}

bool ObRpcClientAuthMethodChecker::check(const ObConfigItem &t) const
{
  ObString v_str(t.str());
  return 0 == v_str.case_compare("NONE") ||
         0 == v_str.case_compare("SSL_NO_ENCRYPT") ||
         0 == v_str.case_compare("SSL_IO");
}

bool ObRpcServerAuthMethodChecker::is_valid_server_auth_method(const ObString &str) const
{
  return 0 == str.case_compare("NONE") ||
         0 == str.case_compare("SSL_NO_ENCRYPT") ||
         0 == str.case_compare("SSL_IO") ||
         0 == str.case_compare("ALL");
}

bool ObRpcServerAuthMethodChecker::check(const ObConfigItem &t) const
{
  bool bret = true;
  int MAX_METHOD_LENGTH = 256;
  char tmp_str[MAX_METHOD_LENGTH];
  size_t str_len = STRLEN(t.str());
  if (str_len >= MAX_METHOD_LENGTH) {
    bret = false;
  } else {
    MEMCPY(tmp_str, t.str(), str_len);
    tmp_str[str_len] = 0;
    ObString str(str_len, reinterpret_cast<const char *>(tmp_str));
    if (NULL == str.find(',')) {
      bret = is_valid_server_auth_method(str);
    } else {
      //split by comma
      char *token = NULL;
      char *save = NULL;
      char *str_token = tmp_str;
      int hint = 0;
      do {
        token = strtok_r(str_token, ",", &save);
        str_token = NULL;
        if (token) {
          hint = 1;
          ObString tmp(STRLEN(token), reinterpret_cast<const char *>(token));
          ObString tmp_to_check = tmp.trim();
          if (is_valid_server_auth_method(tmp_to_check)) {
          } else {
            bret = false;
            break;
          }
        } else {
          if (!hint) {
            bret = false;
          }
          break;
        }
      } while(true);
    }
  }
  return bret;
}

int64_t ObConfigRuntimeFilterChecker::get_runtime_filter_type(const char *str, int64_t len)
{
  int64_t rf_type = -1;
  int64_t l = 0, r = len;
  if (0 == len) {
    rf_type = 0;
  } else {
    int64_t l = 0, r = len;
    bool is_valid = true;
    int flag[3] = {0, 0, 0};
    auto fill_flag = [&] (ObString &p_str) {
      bool valid = true;
      ObString trim_str = p_str.trim();
      if (0 == trim_str.case_compare("bloom_filter")) {
        flag[0]++;
      } else if (0 == trim_str.case_compare("range")) {
        flag[1]++;
      } else if (0 == trim_str.case_compare("in")) {
        flag[2]++;
      } else {
        valid = false;
      }
      if (valid) {
        if (flag[0] > 1 || flag[1] > 1 || flag[2] > 1) {
          valid = false;
        }
      }
      return valid;
    };
    for (int i = 0; i < len && is_valid; ++i) {
      if (str[i] == ',') {
        r = i;
        ObString p_str(r - l, str + l);
        is_valid = fill_flag(p_str);
        l = i + 1;
        continue;
      }
    }
    if (is_valid) {
      ObString p_str(len - l, str + l);
      is_valid = fill_flag(p_str);
    }
    if (is_valid) {
      rf_type = flag[0] << 1 |
                flag[1] << 2 |
                flag[2] << 3;
    } else {
      rf_type = -1;
    }

  }
  return rf_type;
}

bool ObConfigRuntimeFilterChecker::check(const ObConfigItem &t) const
{
  int64_t len = strlen(t.str());
  const char *p = t.str();
  int64_t rf_type = get_runtime_filter_type(t.str(), len);
  return rf_type >= 0;
}

bool ObConfigSQLTlsVersionChecker::check(const ObConfigItem &t) const
{
  const ObString tmp_str(t.str());
  return 0 == tmp_str.case_compare("NONE")    ||
         0 == tmp_str.case_compare("TLSV1")   ||
         0 == tmp_str.case_compare("TLSV1.1") ||
         0 == tmp_str.case_compare("TLSV1.2") ||
         0 == tmp_str.case_compare("TLSV1.3");
}

bool ObSqlPlanManagementModeChecker::check(const ObConfigItem &t) const
{
  const ObString tmp_str(t.str());
  return  get_spm_mode_by_string(tmp_str) != -1;
}

/**
 * return spm mode
 * -1 represent invalid mode
 * 0  represent disable spm
 * 1  represent online evolve mode
*/
int64_t ObSqlPlanManagementModeChecker::get_spm_mode_by_string(const common::ObString &string)
{
  int64_t spm_mode = -1;
  if (string.empty()) {
    spm_mode = -1;
  } else if (0 == string.case_compare("Disable")) {
    spm_mode = 0;
  } else if (0 == string.case_compare("OnlineEvolve")) {
    spm_mode = 1;
  } else if (0 == string.case_compare("BaselineFirst")) {
    uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();
    if (cluster_version >= CLUSTER_VERSION_4_3_5_0 ||
        (cluster_version >= MOCK_CLUSTER_VERSION_4_2_5_0 && cluster_version < CLUSTER_VERSION_4_3_0_0)) {
      spm_mode = 2;
    } else {
      spm_mode = -1;
    }
  }
  return spm_mode;
}

bool ObDefaultLoadModeChecker::check(const ObConfigItem &t) const
{
  const ObString tmp_str(t.str());
  bool result = false;
  if (0 == tmp_str.case_compare("DISABLED")) {
    result = true;
  } else if (0 == tmp_str.case_compare("FULL_DIRECT_WRITE")) {
    result = true;
  } else if (0 == tmp_str.case_compare("INC_DIRECT_WRITE")) {
    result = true;
  } else if (0 == tmp_str.case_compare("INC_REPLACE_DIRECT_WRITE")) {
    result = true;
  }
  return result;
}

int ObModeConfigParserUitl::parse_item_to_kv(char *item, ObString &key, ObString &value, const char* delim)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(item)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "item is NULL", K(ret));
  } else {
    // key
    char *save_ptr = NULL;
    char *key_ptr = STRTOK_R(item, delim, &save_ptr);
    ObString tmp_key(key_ptr);
    key = tmp_key.trim();
    // value
    ObString tmp_value(save_ptr);
    value = tmp_value.trim();
    if (value.case_compare("on") != 0 && value.case_compare("off") != 0) {
      ret = OB_INVALID_CONFIG;
      OB_LOG(WARN, "item value is invalid", K(ret), K(value));
    }
  }
  return ret;
}

int ObModeConfigParserUitl::format_mode_str(const char *src, int64_t src_len, char *dst, int64_t dst_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src) || OB_UNLIKELY(src_len <=0)
      || OB_ISNULL(dst) || dst_len < (3 * src_len)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", KR(ret), KP(src), KP(dst), K(src_len), K(dst_len));
  } else {
    const char *source_str = src;
    const char *locate_str = NULL;
    int64_t source_left_len = src_len;
    int32_t locate = -1;
    int64_t pos = 0;
    while (OB_SUCC(ret) && (source_left_len > 0)
           && (NULL != (locate_str = STRCHR(source_str, ',')))) {
      locate = static_cast<int32_t>(locate_str - source_str);
      if (OB_FAIL(databuff_printf(dst, dst_len, pos, "%.*s , ", locate, source_str))) {
        OB_LOG(WARN, "failed to databuff_print", K(ret), K(dst), K(locate), K(source_str));
      } else {
        source_str = locate_str + 1;
        source_left_len -= (locate + 1);
      }
    }

    if (OB_SUCC(ret) && source_left_len > 0) {
      if (OB_FAIL(databuff_printf(dst, dst_len, pos, "%s", source_str))) {
        OB_LOG(WARN, "failed to databuff_print", KR(ret), K(dst), K(pos));
      }
    }
    OB_LOG(DEBUG, "format_option_str", K(ret), K(src), K(dst));
  }
  return ret;
}

int ObModeConfigParserUitl::get_kv_list(char *str, ObIArray<std::pair<ObString, ObString>> &kv_list, const char* delim)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "item is NULL", K(ret));
  } else {
    ObString key;
    ObString value;
    char *save_ptr = NULL;
    char *token = STRTOK_R(str, ",", &save_ptr);
    while (OB_SUCC(ret) && OB_NOT_NULL(token)) {
      // trim left space
      while (*token == ' ') token++;
      // trim right space
      uint64_t len = strlen(token);
      while (len > 0 && token[len - 1] == ' ') token[--len] = '\0';
      // check and set mode
      if (OB_FAIL(parse_item_to_kv(token, key, value, delim))) {
        OB_LOG(WARN, "fail to check config item", K(ret));
      } else if (OB_FAIL(kv_list.push_back(std::make_pair(key, value)))) {
        OB_LOG(WARN, "fail to push back key and value pair", K(ret), K(key), K(value));
      } else {
        token = STRTOK_R(NULL, ",", &save_ptr);
      }
    }
  }
  return ret;
}

bool ObKvFeatureModeParser::parse(const char *str, uint8_t *arr, int64_t len)
{
  bool bret = true;
  if (str ==  NULL || arr == NULL) {
    bret = false;
    OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "Get mode config item fail, str or value arr is NULL!");
  } else if (strlen(str) == 0) {
    bret = true;
    OB_LOG_RET(DEBUG, OB_SUCCESS, "strlen is 0");
  } else {
    int tmp_ret = OB_SUCCESS;
    ObSEArray<std::pair<ObString, ObString>, 8> kv_list;
    int64_t str_len = strlen(str);
    const int64_t buf_len = 3 * str_len; // need replace ',' to ' , '
    char buf[buf_len];
    MEMSET(buf, 0, sizeof(buf));
    MEMCPY(buf, str, str_len);
    if (OB_SUCCESS != (tmp_ret = ObModeConfigParserUitl::format_mode_str(str, str_len, buf, buf_len))) {
      bret = false;
      OB_LOG_RET(WARN, tmp_ret, "fail to format mode str", K(str));
    } else if (OB_SUCCESS != (tmp_ret = ObModeConfigParserUitl::get_kv_list(buf, kv_list))) {
      bret = false;
      OB_LOG_RET(WARN, tmp_ret, "fail to get kv list", K(str));
    } else {
      ObKVFeatureMode kv_mode;
      for (int64_t i = 0; i < kv_list.count() && bret; i++) {
        uint8_t mode = MODE_DEFAULT;
        if (kv_list.at(i).second.case_compare(MODE_VAL_ON) == 0) {
          mode = MODE_ON;
        } else if (kv_list.at(i).second.case_compare(MODE_VAL_OFF) == 0) {
          mode = MODE_OFF;
        } else {
          bret = false;
          OB_LOG_RET(WARN, OB_INVALID_CONFIG, "unknown mode type", K(kv_list.at(i).second));
        }
        if (!bret) {
        } else if (kv_list.at(i).first.case_compare(MODE_NAME_TTL) == 0) {
          kv_mode.set_ttl_mode(mode);
        } else if (kv_list.at(i).first.case_compare(MODE_NAME_REROUTING) == 0) {
          kv_mode.set_rerouting_mode(mode);
        } else if (kv_list.at(i).first.case_compare(MODE_NAME_HOTKEY) == 0) {
          kv_mode.set_hotkey_mode(mode);
        } else {
          bret = false;
          OB_LOG_RET(WARN, OB_INVALID_CONFIG, "unknown mode name", K(kv_list.at(i).first));
        }
      } // end for
      if (bret) {
        arr[0] = kv_mode.get_value();
      }
    }
  }
  return bret;
}

bool ObConfigIndexStatsModeChecker::check(const ObConfigItem &t) const {
  const ObString tmp_str(t.str());
  return 0 == tmp_str.case_compare("SAMPLED") || 0 == tmp_str.case_compare("ALL");
}

bool ObConfigTableStoreFormatChecker::check(const ObConfigItem &t) const {
  bool bret = true;
  const ObString tmp_str(t.str());
  // Note: Shared-Storage mode does not support column store in default. if want to test
  // column store under shared-storage mode, then need to set tracepoint.
  bool is_column_store_supported = true;
  if (GCTX.is_shared_storage_mode()) {
    int tmp_ret = OB_E(EventTable::EN_ENABLE_SHARED_STORAGE_COLUMN_GROUP) OB_SUCCESS;
    is_column_store_supported = (tmp_ret != OB_SUCCESS);
  }
  if (is_column_store_supported) {
    bret = ((0 == tmp_str.case_compare("ROW")) ||
            (0 == tmp_str.case_compare("COLUMN")) ||
            (0 == tmp_str.case_compare("COMPOUND")));
  } else {
    bret = (0 == tmp_str.case_compare("ROW"));
  }
  return bret;
}

bool ObConfigDDLNoLoggingChecker::check(const uint64_t tenant_id, const obrpc::ObAdminSetConfigItem &t) {
  int ret = OB_SUCCESS;
  bool is_valid = true;
  uint64_t data_version = 0;
  const bool value = ObConfigBoolParser::get(t.value_.ptr(), is_valid);

  if (!is_valid) {
  } else if (!GCTX.is_shared_storage_mode()) {
    is_valid = false;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "it's not allowded to set no logging in shared nothing mode");
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    is_valid = false;
    OB_LOG(WARN, "failed to get mini data version", K(ret));
  } else if (data_version < DATA_VERSION_4_3_5_0) {
    is_valid = false;
    ret = OB_NOT_SUPPORTED;
    OB_LOG(WARN, "it's not allowded to set no logging during cluster updating process", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "it's not allowded to set no logging during cluster updating process");
  }
  if (!is_valid) {
  } else {
    if (OB_SYS_TENANT_ID == tenant_id) {
      /* sys tenant not no allow archive */
    } else {
      ObArchivePersistHelper archive_op;
      ObArchiveMode archive_mode;
      common::ObMySQLProxy *sql_proxy = nullptr;
      if (OB_ISNULL(sql_proxy = GCTX.sql_proxy_))  {
        is_valid = false;
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "invalid sql proxy", K(ret), KP(sql_proxy));
      } else if (OB_FAIL(archive_op.init(tenant_id))) {
        is_valid = false;
        OB_LOG(WARN, "failed to init archive op", K(ret), K(tenant_id));
      } else if (OB_FAIL(archive_op.get_archive_mode(*sql_proxy, archive_mode))) {
        is_valid = false;
        OB_LOG(WARN, "failed to get archive mode", K(ret));
      } else if (value && archive_mode.is_archivelog()) {
        is_valid = false;
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "it's no allowded to set no logging during archive");
      }
    }
  }

  if (OB_FAIL(ret)) {
    is_valid = false;
  }
  return is_valid;
}

bool ObConfigMigrationChooseSourceChecker::check(const ObConfigItem &t) const
{
  ObString v_str(t.str());
  return 0 == v_str.case_compare("idc")
      || 0 == v_str.case_compare("region");
}

bool ObConfigArchiveLagTargetChecker::check(const uint64_t tenant_id, const ObAdminSetConfigItem &t)
{
  bool is_valid = false;
  int ret = OB_SUCCESS;
  int64_t value = ObConfigTimeParser::get(t.value_.ptr(), is_valid);
  ObArchivePersistHelper archive_op;
  ObBackupPathString archive_dest_str;
  ObBackupDest archive_dest;
  ObStorageType device_type;
  const int64_t dest_no = 0;
  const bool lock = false;
  if (is_valid) {
    if (OB_FAIL(archive_op.init(tenant_id))) {
      OB_LOG(WARN, "fail to init archive persist helper", K(ret), K(tenant_id));
    } else if (OB_FAIL(archive_op.get_archive_dest(*GCTX.sql_proxy_, lock, dest_no, archive_dest_str))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        OB_LOG(WARN, "failed to get archive dest", K(ret), K(tenant_id));
      } else { // no dest exist, set archive_lag_target is disallowed
        is_valid =  false;
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "log_archive_dest has not been set, set archive_lag_target is");
      }
    } else if (OB_FAIL(archive_dest.set(archive_dest_str))) {
      OB_LOG(WARN, "fail to set archive dest", K(ret), K(archive_dest_str));
    } else if (archive_dest.is_storage_type_s3()) {
      is_valid = MIN_LAG_TARGET_FOR_S3 <= value;
      if (!is_valid) {
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set archive_lag_target smaller than 60s when log_archive_dest is S3 is");
      }
    } else {
      is_valid = true;
    }
  }
  return is_valid;
}

bool ObConfigSQLSpillCompressionCodecChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  for (int i = 0; i < ARRAYSIZEOF(common::sql_temp_store_compress_funcs) && !is_valid; ++i) {
    if (0 == ObString::make_string(sql_temp_store_compress_funcs[i]).case_compare(t.str())) {
      is_valid = true;
    }
  }
  return is_valid;
}

bool ObParallelDDLControlParser::parse(const char *str, uint8_t *arr, int64_t len)
{
  bool bret = true;
  ObParallelDDLControlMode ddl_mode;
  if (OB_ISNULL(str) || OB_ISNULL(arr)) {
    bret = false;
    OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "Get config item failed", KP(str), KP(arr));
  } else if (strlen(str) == 0) {
    // do nothing
  } else {
    int tmp_ret = OB_SUCCESS;
    ObSEArray<std::pair<ObString, ObString>, 1> kv_list;
    int64_t str_len = strlen(str);
    const int64_t buf_len = 3 * str_len; // need replace ',' to ' , '
    char buf[buf_len];
    MEMSET(buf, 0, sizeof(buf));
    MEMCPY(buf, str, str_len);
    if (OB_TMP_FAIL(ObModeConfigParserUitl::format_mode_str(str, str_len, buf, buf_len))) {
      bret = false;
      OB_LOG_RET(WARN, tmp_ret, "fail to format mode str", K(str));
    } else if (OB_TMP_FAIL(ObModeConfigParserUitl::get_kv_list(buf, kv_list, ":"))) {
      bret = false;
      OB_LOG_RET(WARN, tmp_ret, "fail to get kv list", K(str));
    } else {
      for (int64_t i = 0; bret && i < kv_list.count(); ++i) {
        uint8_t mode = MODE_DEFAULT;
        if (kv_list.at(i).second.case_compare("on") == 0) {
          mode = MODE_ON;
        } else if (kv_list.at(i).second.case_compare("off") == 0) {
          mode = MODE_OFF;
        } else {
          bret = false;
          OB_LOG_RET(WARN, OB_INVALID_CONFIG, "unknown mode type", K(kv_list.at(i).second));
        }
        ObParallelDDLControlMode::ObParallelDDLType ddl_type = ObParallelDDLControlMode::MAX_TYPE;
        if (!bret) {
          // do nothing
        } else if (OB_TMP_FAIL(ObParallelDDLControlMode::string_to_ddl_type(kv_list.at(i).first, ddl_type))) {
          bret = false;
          OB_LOG_RET(WARN, tmp_ret, "fail to trans string ddl_type", K(kv_list.at(i).first));
        } else if (OB_TMP_FAIL(ddl_mode.set_parallel_ddl_mode(ddl_type, mode))) {
          bret = false;
          OB_LOG_RET(WARN, tmp_ret, "fail to set parallel ddl mode", K(ddl_type), K(mode));
        }
      }
    }
  }
  if (bret) {
    for (uint64_t i = 0; i < 8; ++i) {
      arr[i] = static_cast<uint8_t>((ddl_mode.get_value() >> (i * 8)) & 0xFF);
    }
  }
  return bret;
}

bool ObConfigKvGroupCommitRWModeChecker::check(const ObConfigItem &t) const
{
  ObString v_str(t.str());
  return 0 == v_str.case_compare("all")
    || 0 == v_str.case_compare("read")
    || 0 == v_str.case_compare("write");
}

bool ObConfigDegradationPolicyChecker::check(const ObConfigItem &t) const
{
  common::ObString tmp_str(t.str());
  return 0 == tmp_str.case_compare("LS_POLICY") || 0 == tmp_str.case_compare("CLUSTER_POLICY");
}

bool ObConfigRegexpEngineChecker::check(const ObConfigItem &t) const
{
  bool valid = false;
  if (0 == ObString::make_string("Hyperscan").case_compare(t.str())) {
#if defined(__x86_64__)
    valid = true;
#else
    valid = false;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "using hyperscan as regex engine in platforms other than x86");
#endif
  } else {
    valid = (0 == ObString::make_string("ICU").case_compare(t.str()));
  }
  return valid;
}

bool ObConfigReplicaParallelMigrationChecker::check(const ObConfigItem &t) const
{
  ObString v_str(t.str());
  return 0 == v_str.case_compare("auto")
      || 0 == v_str.case_compare("on")
      || 0 == v_str.case_compare("off");
}

bool ObConfigS3URLEncodeTypeChecker::check(const ObConfigItem &t) const
{
  // When compliantRfc3986Encoding is set to true:
  // - Adhere to RFC 3986 by supporting the encoding of reserved characters
  //   such as '-', '_', '.', '$', '@', etc.
  // - This approach mitigates inconsistencies in server behavior when accessing
  //   COS using the S3 SDK.
  // Otherwise, the reserved characters will not be encoded,
  // following the default behavior of the S3 SDK.
  bool bret = false;
  common::ObString tmp_str(t.str());
  if (0 == tmp_str.case_compare("default")) {
    bret = true;
    Aws::Http::SetCompliantRfc3986Encoding(false);
  } else if (0 == tmp_str.case_compare("compliantRfc3986Encoding")) {
    bret = true;
    Aws::Http::SetCompliantRfc3986Encoding(true);
  } else {
    bret = false;
  }
  return bret;
}

bool ObConfigDefaultTableOrganizationChecker::check(const obrpc::ObAdminSetConfigItem &t)
{
  const ObString tmp_str(t.value_.size(), t.value_.ptr());
  return 0 == tmp_str.case_compare("INDEX")
      || 0 == tmp_str.case_compare("HEAP");
}


bool ObConfigEnableHashRollupChecker::check(const ObConfigItem &t) const
{
  int bret = false;
  common::ObString tmp_str(t.str());
  bret = (0 == tmp_str.case_compare("auto")
          || 0 == tmp_str.case_compare("forced")
          || 0 == tmp_str.case_compare("disabled"));
  return bret;
}

bool ObConfigPxNodePolicyChecker::check(const ObConfigItem &t) const
{
  int bret = false;
  common::ObString tmp_str(t.str());
  bret = (0 == tmp_str.case_compare("data")
          || 0 == tmp_str.case_compare("zone")
          || 0 == tmp_str.case_compare("cluster"));
  return bret;
}

bool ObConfigPluginsLoadChecker::check(const ObConfigItem& t) const
{
  bool bret = false;
  ObString plugins_load(t.str());
  ObArray<ObPluginLoadParam> plugin_load_params;
  ObMemAttr mem_attr(OB_SYS_TENANT_ID, "Config");
  plugin_load_params.set_attr(mem_attr);
  int ret = ObPluginLoadParamParser::parse(plugins_load, plugin_load_params);
  if (OB_FAIL(ret)) {
    OB_LOG_RET(WARN, OB_INVALID_CONFIG, "failed to parse plugins load config", K(plugins_load));
    bret = false;
  } else {
    bret = true;
  }
  return bret;
}

bool ObConfigNonStdCmpLevelChecker::check(const ObConfigItem &t) const
{
  int bret = false;
  common::ObString tmp_str(t.str());
  bret = (0 == tmp_str.case_compare("none")
          || 0 == tmp_str.case_compare("equal")
          || 0 == tmp_str.case_compare("range"));
  return bret;
}

bool ObConfigJavaParamsChecker::check(const ObConfigItem &t) const
{
  bool bret = false;
  // Only the ob_enable_java_env is true, then can pass to continue
  if (GCONF.ob_enable_java_env) {
    bret = true;
  }
  return bret;
}

bool ObConfigEnableAutoSplitChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  bool enable_auto_split = ObConfigBoolParser::get(t.str(), is_valid);
  return is_valid && !(GCTX.is_shared_storage_mode() && enable_auto_split);
}

bool ObConfigAutoSplitTabletSizeChecker::check(const ObConfigItem &t) const
{
  bool is_valid = false;
  int64_t value = ObConfigCapacityParser::get(t.str(), is_valid);
  return is_valid && !GCTX.is_shared_storage_mode();
}

} // end of namepace common
} // end of namespace oceanbase
