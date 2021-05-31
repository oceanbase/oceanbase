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

#include "share/config/ob_config_helper.h"
#include "share/backup/ob_backup_info_mgr.h"
#include "share/config/ob_config.h"
#include "lib/ob_running_mode.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/resource/achunk_mgr.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "common/ob_store_format.h"
#include "common/ob_smart_var.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
#include "share/ob_rpc_struct.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "share/ob_encryption_util.h"

namespace oceanbase {
using namespace share;
namespace common {

bool ObConfigIpChecker::check(const ObConfigItem& t) const
{
  struct sockaddr_in sa;
  int result = inet_pton(AF_INET, t.str(), &(sa.sin_addr));
  return result != 0;
}

ObConfigConsChecker::~ObConfigConsChecker()
{
  if (NULL != left_) {
    delete left_;
  }
  if (NULL != right_) {
    delete right_;
  }
}
bool ObConfigConsChecker::check(const ObConfigItem& t) const
{
  return (NULL == left_ ? true : left_->check(t)) && (NULL == right_ ? true : right_->check(t));
}

bool ObConfigGreaterThan::check(const ObConfigItem& t) const
{
  return t > val_;
}

bool ObConfigGreaterEqual::check(const ObConfigItem& t) const
{
  return t >= val_;
}

bool ObConfigLessThan::check(const ObConfigItem& t) const
{
  return t < val_;
}

bool ObConfigLessEqual::check(const ObConfigItem& t) const
{
  return t <= val_;
}

bool ObConfigEvenIntChecker::check(const ObConfigItem& t) const
{
  bool is_valid = false;
  int64_t value = ObConfigIntParser::get(t.str(), is_valid);
  if (is_valid) {
    is_valid = value % 2 == 0;
  }
  return is_valid;
}

bool ObConfigTabletSizeChecker::check(const ObConfigItem& t) const
{
  bool is_valid = false;
  const int64_t mask = (1 << 21) - 1;
  int64_t value = ObConfigCapacityParser::get(t.str(), is_valid);
  if (is_valid) {
    // value has to be a multiple of 2M
    is_valid = (value >= 0) && !(value & mask);
  }
  return is_valid;
}

bool ObConfigStaleTimeChecker::check(const ObConfigItem& t) const
{
  bool is_valid = false;
  int64_t stale_time = ObConfigTimeParser::get(t.str(), is_valid);
  if (is_valid) {
    is_valid = (stale_time >= GCONF.weak_read_version_refresh_interval);
    if (!is_valid) {
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
          "max_stale_time_for_weak_consistency violate"
          " weak_read_version_refresh_interval,");
    }
  }
  return is_valid;
}

bool ObConfigCompressFuncChecker::check(const ObConfigItem& t) const
{
  bool is_valid = false;
  for (int i = 0; i < ARRAYSIZEOF(common::compress_funcs) && !is_valid; ++i) {
    if (0 == ObString::make_string(compress_funcs[i]).case_compare(t.str())) {
      is_valid = true;
    }
  }
  return is_valid;
}

bool ObConfigPerfCompressFuncChecker::check(const ObConfigItem& t) const
{
  bool is_valid = false;
  for (int i = 0; i < ARRAYSIZEOF(common::perf_compress_funcs) && !is_valid; ++i) {
    if (0 == ObString::make_string(perf_compress_funcs[i]).case_compare(t.str())) {
      is_valid = true;
    }
  }
  return is_valid;
}

bool ObConfigBatchRpcCompressFuncChecker::check(const ObConfigItem& t) const
{
  bool is_valid = false;
  for (int i = 0; i < ARRAYSIZEOF(common::batch_rpc_compress_funcs) && !is_valid; ++i) {
    if (0 == ObString::make_string(batch_rpc_compress_funcs[i]).case_compare(t.str())) {
      is_valid = true;
    }
  }
  return is_valid;
}

bool ObConfigRowFormatChecker::check(const ObConfigItem& t) const
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

bool ObConfigCompressOptionChecker::check(const ObConfigItem& t) const
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

bool ObConfigLogLevelChecker::check(const ObConfigItem& t) const
{
  const ObString tmp_str(t.str());
  return ((0 == tmp_str.case_compare(ObLogger::PERF_LEVEL)) ||
          OB_SUCCESS == OB_LOGGER.parse_check(tmp_str.ptr(), tmp_str.length()));
}

bool ObConfigWorkAreaPolicyChecker::check(const ObConfigItem& t) const
{
  const ObString tmp_str(t.str());
  return ((0 == tmp_str.case_compare(MANUAL)) || (0 == tmp_str.case_compare(AUTO)));
}

bool ObConfigLogArchiveOptionsChecker::check(const ObConfigItem& t) const
{
  bool bret = true;
  int ret = OB_SUCCESS;
  SMART_VAR(char[OB_MAX_CONFIG_VALUE_LEN], tmp_str)
  {
    const size_t str_len = STRLEN(t.str());
    MEMCPY(tmp_str, t.str(), str_len);
    tmp_str[str_len] = 0;
    const int64_t FORMAT_BUF_LEN = str_len * 3;  // '=' will be replaced with ' = '
    char format_str_buf[FORMAT_BUF_LEN];
    int ret = OB_SUCCESS;
    // first replace '=' with ' = '
    if (OB_FAIL(ObConfigLogArchiveOptionsItem::format_option_str(tmp_str, str_len, format_str_buf, FORMAT_BUF_LEN))) {
      bret = false;
      OB_LOG(WARN, "failed to format_option_str", KR(bret), K(tmp_str));
    } else {
      char* saveptr = NULL;
      char* s = STRTOK_R(format_str_buf, " ", &saveptr);
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
            share::ObAesOpMode encryption_algorithm;
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

bool ObConfigRpcChecksumChecker::check(const ObConfigItem& t) const
{
  common::ObString tmp_string(t.str());
  return obrpc::get_rpc_checksum_check_level_from_string(tmp_string) != obrpc::ObRpcCheckSumCheckLevel::INVALID;
}

bool ObConfigMemoryLimitChecker::check(const ObConfigItem& t) const
{
  bool is_valid = false;
  int64_t value = ObConfigCapacityParser::get(t.str(), is_valid);
  if (is_valid) {
    is_valid = value >= lib::ObRunningModeConfig::instance().MINI_MEM_LOWER;
  }
  return is_valid;
}

bool ObConfigQueryRateLimitChecker::check(const ObConfigItem& t) const
{
  bool is_valid = false;
  int64_t value = ObConfigIntParser::get(t.str(), is_valid);
  if (is_valid) {
    is_valid = (-1 == value || (value >= MIN_QUERY_RATE_LIMIT && value <= MAX_QUERY_RATE_LIMIT));
  }
  return is_valid;
}

const char* ObConfigPartitionBalanceStrategyFuncChecker::balance_strategy
    [ObConfigPartitionBalanceStrategyFuncChecker::PARTITION_BALANCE_STRATEGY_MAX] = {
        "auto",
        "standard",
        "disk_utilization_only",
};

bool ObConfigPartitionBalanceStrategyFuncChecker::check(const ObConfigItem& t) const
{
  bool is_valid = false;
  for (int64_t i = 0; i < ARRAYSIZEOF(balance_strategy) && !is_valid; ++i) {
    if (0 == ObString::make_string(balance_strategy[i]).case_compare(t.str())) {
      is_valid = true;
    }
  }
  return is_valid;
}

int64_t ObConfigIntParser::get(const char* str, bool& valid)
{
  char* p_end = NULL;
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
      OB_LOG(ERROR, "set int error", K(str), K(valid));
    }
  }
  return value;
}

int64_t ObConfigCapacityParser::get(const char* str, bool& valid)
{
  char* p_unit = NULL;
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
      value <<= CAP_MB;  // default
    } else if (0 == STRCASECMP("b", p_unit) || 0 == STRCASECMP("byte", p_unit)) {
      // do nothing
    } else if (0 == STRCASECMP("kb", p_unit) || 0 == STRCASECMP("k", p_unit)) {
      value <<= CAP_KB;
    } else if (0 == STRCASECMP("mb", p_unit) || 0 == STRCASECMP("m", p_unit)) {
      value <<= CAP_MB;
    } else if (0 == STRCASECMP("gb", p_unit) || 0 == STRCASECMP("g", p_unit)) {
      value <<= CAP_GB;
    } else if (0 == STRCASECMP("tb", p_unit) || 0 == STRCASECMP("t", p_unit)) {
      value <<= CAP_TB;
    } else if (0 == STRCASECMP("pb", p_unit) || 0 == STRCASECMP("p", p_unit)) {
      value <<= CAP_PB;
    } else {
      valid = false;
      OB_LOG(ERROR, "set capacity error", K(str), K(p_unit));
    }
  }

  return value;
}

int64_t ObConfigReadableIntParser::get(const char* str, bool& valid)
{
  char* p_unit = NULL;
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
      // without any unit, do nothing
    } else if (0 == STRCASECMP("k", p_unit)) {
      value *= UNIT_K;
    } else if (0 == STRCASECMP("m", p_unit)) {
      value *= UNIT_M;
    } else {
      valid = false;
      OB_LOG(ERROR, "set readable int error", K(str), K(p_unit));
    }
  }

  return value;
}

int64_t ObConfigTimeParser::get(const char* str, bool& valid)
{
  char* p_unit = NULL;
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
      OB_LOG(ERROR, "set time error", K(str), K(p_unit));
    }
  }

  return value;
}

bool ObConfigUpgradeStageChecker::check(const ObConfigItem& t) const
{
  obrpc::ObUpgradeStage stage = obrpc::get_upgrade_stage(t.str());
  return obrpc::OB_UPGRADE_STAGE_INVALID < stage && obrpc::OB_UPGRADE_STAGE_MAX > stage;
}

bool ObConfigPlanCacheGCChecker::check(const ObConfigItem& t) const
{
  bool is_valid = false;
  for (int i = 0; i < ARRAYSIZEOF(sql::plan_cache_gc_confs) && !is_valid; i++) {
    if (0 == ObString::make_string(sql::plan_cache_gc_confs[i]).case_compare(t.str())) {
      is_valid = true;
    }
  }
  return is_valid;
}

bool ObConfigUseLargePagesChecker::check(const ObConfigItem& t) const
{
  bool is_valid = false;
  for (int i = 0; i < ARRAYSIZEOF(lib::use_large_pages_confs) && !is_valid; i++) {
    if (0 == ObString::make_string(lib::use_large_pages_confs[i]).case_compare(t.str())) {
      is_valid = true;
    }
  }
  return is_valid;
}

}  // namespace common
}  // end of namespace oceanbase
