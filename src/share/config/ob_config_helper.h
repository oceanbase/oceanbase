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

#ifndef OCEANBASE_SHARE_CONFIG_OB_CONFIG_HELPER_H_
#define OCEANBASE_SHARE_CONFIG_OB_CONFIG_HELPER_H_

#include <arpa/inet.h>
#include "lib/hash/ob_hashmap.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/hash/ob_hashutils.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace obrpc
{
struct ObAdminSetConfigItem;
}

namespace common
{
class ObConfigItem;
class ObConfigIntegralItem;
class ObConfigAlwaysTrue;

class ObConfigChecker
{
public:
  ObConfigChecker() {}
  virtual ~ObConfigChecker() {}
  virtual bool check(const ObConfigItem &t) const = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigChecker);
};

class ObConfigAlwaysTrue
  : public ObConfigChecker
{
public:
  ObConfigAlwaysTrue() {}
  virtual ~ObConfigAlwaysTrue() {}
  bool check(const ObConfigItem &t) const { UNUSED(t); return true; }

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigAlwaysTrue);
};

class ObConfigIpChecker
  : public ObConfigChecker
{
public:
  ObConfigIpChecker() {}
  virtual ~ObConfigIpChecker() {}
  bool check(const ObConfigItem &t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigIpChecker);
};

class ObConfigConsChecker
  : public ObConfigChecker
{
public:
  ObConfigConsChecker(const ObConfigChecker *left, const ObConfigChecker *right)
      : left_(left), right_(right)
  {}
  virtual ~ObConfigConsChecker();
  bool check(const ObConfigItem &t) const;

private:
  const ObConfigChecker *left_;
  const ObConfigChecker *right_;
  DISALLOW_COPY_AND_ASSIGN(ObConfigConsChecker);
};

class ObConfigEvenIntChecker
  : public ObConfigChecker
{
public:
  ObConfigEvenIntChecker() {}
  virtual ~ObConfigEvenIntChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigEvenIntChecker);
};

class ObConfigFreezeTriggerIntChecker
{
public:
  static bool check(const uint64_t tenant_id,
                    const obrpc::ObAdminSetConfigItem &t);
private:
  static int64_t get_write_throttle_trigger_percentage_(const uint64_t tenant_id);
  DISALLOW_COPY_AND_ASSIGN(ObConfigFreezeTriggerIntChecker);
};
class ObConfigTxShareMemoryLimitChecker
{
public:
  static bool check(const uint64_t tenant_id, const obrpc::ObAdminSetConfigItem &t);

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigTxShareMemoryLimitChecker);
};
class ObConfigMemstoreLimitChecker
{
public:
  static bool check(const uint64_t tenant_id, const obrpc::ObAdminSetConfigItem &t);

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigMemstoreLimitChecker);
};

class ObConfigTxDataLimitChecker
{
public:
  static bool check(const uint64_t tenant_id, const obrpc::ObAdminSetConfigItem &t);

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigTxDataLimitChecker);
};
class ObConfigMdsLimitChecker
{
public:
  static bool check(const uint64_t tenant_id, const obrpc::ObAdminSetConfigItem &t);

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigMdsLimitChecker);
};

class ObConfigWriteThrottleTriggerIntChecker
{
public:
  static bool check(const uint64_t tenant_id,
                    const obrpc::ObAdminSetConfigItem &t);
private:
  static int64_t get_freeze_trigger_percentage_(const uint64_t tenant_id);
  DISALLOW_COPY_AND_ASSIGN(ObConfigWriteThrottleTriggerIntChecker);
};

//only used for RS checking
class ObConfigLogDiskLimitThresholdIntChecker
{
public:
  static bool check(const uint64_t tenant_id,
                    const obrpc::ObAdminSetConfigItem &t);
private:
  static int64_t get_log_disk_throttling_percentage_(const uint64_t tenant_id);
  DISALLOW_COPY_AND_ASSIGN(ObConfigLogDiskLimitThresholdIntChecker);
};

//only used for RS checking
class ObConfigLogDiskThrottlingPercentageIntChecker
{
public:
  static bool check(const uint64_t tenant_id,
                    const obrpc::ObAdminSetConfigItem &t);
private:
  static int64_t get_log_disk_utilization_limit_threshold_(const uint64_t tenant_id);
  DISALLOW_COPY_AND_ASSIGN(ObConfigLogDiskThrottlingPercentageIntChecker);
};

class ObConfigTabletSizeChecker
  : public ObConfigChecker
{
public:
  ObConfigTabletSizeChecker() {}
  virtual ~ObConfigTabletSizeChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigTabletSizeChecker);
};

class ObConfigStaleTimeChecker
  : public ObConfigChecker
{
public:
  ObConfigStaleTimeChecker() {}
  virtual ~ObConfigStaleTimeChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigStaleTimeChecker);
};

class ObConfigCompressFuncChecker
  : public ObConfigChecker
{
public:
  ObConfigCompressFuncChecker() {}
  virtual ~ObConfigCompressFuncChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigCompressFuncChecker);
};

class ObConfigPerfCompressFuncChecker
  : public ObConfigChecker
{
public:
  ObConfigPerfCompressFuncChecker() {}
  virtual ~ObConfigPerfCompressFuncChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigPerfCompressFuncChecker);
};

class ObConfigResourceLimitSpecChecker
  : public ObConfigChecker
{
public:
  ObConfigResourceLimitSpecChecker() {}
  virtual ~ObConfigResourceLimitSpecChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigResourceLimitSpecChecker);
};

class ObConfigTempStoreFormatChecker
  : public ObConfigChecker
{
public:
  ObConfigTempStoreFormatChecker() {}
  virtual ~ObConfigTempStoreFormatChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigTempStoreFormatChecker);
};

class ObConfigPxBFGroupSizeChecker
  : public ObConfigChecker
{
public:
  ObConfigPxBFGroupSizeChecker() {}
  virtual ~ObConfigPxBFGroupSizeChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigPxBFGroupSizeChecker);
};

class ObConfigRowFormatChecker
  : public ObConfigChecker
{
public:
  ObConfigRowFormatChecker() {}
  virtual ~ObConfigRowFormatChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigRowFormatChecker);
};

class ObConfigCompressOptionChecker
  : public ObConfigChecker
{
public:
  ObConfigCompressOptionChecker() {}
  virtual ~ObConfigCompressOptionChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigCompressOptionChecker);
};

class ObConfigMaxSyslogFileCountChecker
  : public ObConfigChecker
{
public:
  ObConfigMaxSyslogFileCountChecker() {}
  virtual ~ObConfigMaxSyslogFileCountChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigMaxSyslogFileCountChecker);
};

class ObConfigSyslogCompressFuncChecker
  : public ObConfigChecker
{
public:
  ObConfigSyslogCompressFuncChecker() {}
  virtual ~ObConfigSyslogCompressFuncChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigSyslogCompressFuncChecker);
};

class ObConfigSyslogFileUncompressedCountChecker
  : public ObConfigChecker
{
public:
  ObConfigSyslogFileUncompressedCountChecker() {}
  virtual ~ObConfigSyslogFileUncompressedCountChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigSyslogFileUncompressedCountChecker);
};

// Used to check the format of STS credential
class ObConfigSTScredentialChecker
  : public ObConfigChecker
{
public:
  ObConfigSTScredentialChecker() {}
  virtual ~ObConfigSTScredentialChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigSTScredentialChecker);
};

class ObConfigUseLargePagesChecker
  : public ObConfigChecker
{
public:
  ObConfigUseLargePagesChecker() {}
  virtual ~ObConfigUseLargePagesChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigUseLargePagesChecker);
};

class ObConfigLogLevelChecker
  : public ObConfigChecker
{
public:
  ObConfigLogLevelChecker() {}
  virtual ~ObConfigLogLevelChecker() {};
  bool check(const ObConfigItem &t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigLogLevelChecker);
};

class ObConfigAlertLogLevelChecker
  : public ObConfigChecker
{
public:
  ObConfigAlertLogLevelChecker() {}
  virtual ~ObConfigAlertLogLevelChecker() {};
  bool check(const ObConfigItem &t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigAlertLogLevelChecker);
};

class ObConfigAuditTrailChecker
  : public ObConfigChecker
{
public:
  ObConfigAuditTrailChecker() {}
  virtual ~ObConfigAuditTrailChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigAuditTrailChecker);
};

class ObConfigAuditLogCompressionChecker
  : public ObConfigChecker
{
public:
  ObConfigAuditLogCompressionChecker() {}
  virtual ~ObConfigAuditLogCompressionChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigAuditLogCompressionChecker);
};

class ObConfigAuditLogPathChecker
  : public ObConfigChecker
{
public:
  ObConfigAuditLogPathChecker() {}
  virtual ~ObConfigAuditLogPathChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigAuditLogPathChecker);
};

class ObConfigAuditLogFormatChecker
  : public ObConfigChecker
{
public:
  ObConfigAuditLogFormatChecker() {}
  virtual ~ObConfigAuditLogFormatChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigAuditLogFormatChecker);
};

class ObConfigAuditLogQuerySQLChecker
  : public ObConfigChecker
{
public:
  ObConfigAuditLogQuerySQLChecker() {}
  virtual ~ObConfigAuditLogQuerySQLChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigAuditLogQuerySQLChecker);
};

class ObConfigAuditLogStrategyChecker
  : public ObConfigChecker
{
public:
  ObConfigAuditLogStrategyChecker() {}
  virtual ~ObConfigAuditLogStrategyChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigAuditLogStrategyChecker);
};

class ObConfigWorkAreaPolicyChecker
  : public ObConfigChecker
{
public:
  ObConfigWorkAreaPolicyChecker() {}
  virtual ~ObConfigWorkAreaPolicyChecker() {};
  bool check(const ObConfigItem &t) const;

private:
  static constexpr const char *MANUAL = "MANUAL";
  static constexpr const char *AUTO = "AUTO";

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigWorkAreaPolicyChecker);
};

class ObConfigLogArchiveOptionsChecker
  : public ObConfigChecker
{
public:
  ObConfigLogArchiveOptionsChecker() {}
  virtual ~ObConfigLogArchiveOptionsChecker() {}
  //TODO(yaoying.yyy): backup fix it
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigLogArchiveOptionsChecker);
};

class ObConfigRpcChecksumChecker
  : public ObConfigChecker
{
public:
  ObConfigRpcChecksumChecker() {}
  virtual ~ObConfigRpcChecksumChecker() {};
  bool check(const ObConfigItem &t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigRpcChecksumChecker);
};

class ObConfigMemoryLimitChecker
  : public ObConfigChecker
{
public:
  ObConfigMemoryLimitChecker() {}
  virtual ~ObConfigMemoryLimitChecker() {};
  bool check(const ObConfigItem &t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigMemoryLimitChecker);
};

class ObConfigTenantMemoryChecker
  : public ObConfigChecker
{
public:
  ObConfigTenantMemoryChecker() {}
  virtual ~ObConfigTenantMemoryChecker() {};
  bool check(const ObConfigItem &t) const;
};

class ObConfigTenantDataDiskChecker
  : public ObConfigChecker
{
public:
  ObConfigTenantDataDiskChecker() {}
  virtual ~ObConfigTenantDataDiskChecker() {};
  bool check(const ObConfigItem &t) const;
};

class ObConfigUpgradeStageChecker : public ObConfigChecker
{
public:
  ObConfigUpgradeStageChecker() {}
  virtual ~ObConfigUpgradeStageChecker() {}

  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigUpgradeStageChecker);
};

class ObConfigPlanCacheGCChecker
  : public ObConfigChecker
{
public:
  ObConfigPlanCacheGCChecker() {}
  virtual ~ObConfigPlanCacheGCChecker() {}

  bool check(const ObConfigItem &t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigPlanCacheGCChecker);
};

class ObConfigQueryRateLimitChecker
  : public ObConfigChecker
{
public:
  ObConfigQueryRateLimitChecker() {}
  virtual ~ObConfigQueryRateLimitChecker() {};
  bool check(const ObConfigItem &t) const;

  static constexpr int64_t MAX_QUERY_RATE_LIMIT = 200000;
  static constexpr int64_t MIN_QUERY_RATE_LIMIT = 10;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigQueryRateLimitChecker);
};

class ObConfigPartitionBalanceStrategyFuncChecker
  : public ObConfigChecker
{
public:
  enum PartitionBalanceStrategy
  {
    AUTO = 0,
    STANDARD,
    DISK_UTILIZATION_ONLY,
    PARTITION_BALANCE_STRATEGY_MAX,
  };
  static const char *balance_strategy[PARTITION_BALANCE_STRATEGY_MAX];
public:
  ObConfigPartitionBalanceStrategyFuncChecker() {}
  virtual ~ObConfigPartitionBalanceStrategyFuncChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigPartitionBalanceStrategyFuncChecker);
};

class ObDataStorageErrorToleranceTimeChecker
  : public ObConfigChecker
{
public:
  ObDataStorageErrorToleranceTimeChecker() {}
  virtual ~ObDataStorageErrorToleranceTimeChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISABLE_COPY_ASSIGN(ObDataStorageErrorToleranceTimeChecker);
};

class ObConfigAuditModeChecker
  : public ObConfigChecker
{
public:
  ObConfigAuditModeChecker() {}
  virtual ~ObConfigAuditModeChecker() {}

  bool check(const ObConfigItem &t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigAuditModeChecker);
};

class ObConfigOfsBlockVerifyIntervalChecker
  : public ObConfigChecker
{
public:
  ObConfigOfsBlockVerifyIntervalChecker() {}
  virtual ~ObConfigOfsBlockVerifyIntervalChecker() {}
  bool check(const ObConfigItem &t) const;
  static constexpr int64_t MIN_VALID_INTVL = 60 * 60 * 1000 * 1000UL; // 1 hour
  static constexpr int64_t MAX_VALID_INTVL = 30 * 24 * MIN_VALID_INTVL; // 30 days
private:
  DISABLE_COPY_ASSIGN(ObConfigOfsBlockVerifyIntervalChecker);
};

class ObLogDiskUsagePercentageChecker
  : public ObConfigChecker
{
public:
  ObLogDiskUsagePercentageChecker() {}
  virtual ~ObLogDiskUsagePercentageChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogDiskUsagePercentageChecker);
};

class ObCtxMemoryLimitChecker
  : public ObConfigChecker
{
public:
  ObCtxMemoryLimitChecker() {}
  virtual ~ObCtxMemoryLimitChecker() {};
  bool check(const ObConfigItem &t) const;
  bool check(const char* str, uint64_t& ctx_id, int64_t& limit) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCtxMemoryLimitChecker);
};

class ObAutoIncrementModeChecker : public ObConfigChecker
{
public:
  ObAutoIncrementModeChecker() {}
  virtual ~ObAutoIncrementModeChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAutoIncrementModeChecker);

};

class ObConfigEnableDefensiveChecker
  : public ObConfigChecker
{
public:
  ObConfigEnableDefensiveChecker() {}
  virtual ~ObConfigEnableDefensiveChecker() {};
  bool check(const ObConfigItem &t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigEnableDefensiveChecker);
};

class ObConfigRuntimeFilterChecker
  : public ObConfigChecker
{
public:
  ObConfigRuntimeFilterChecker() {}
  virtual ~ObConfigRuntimeFilterChecker() {}
  bool check(const ObConfigItem &t) const;
  static int64_t get_runtime_filter_type(const char *str, int64_t len);
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigRuntimeFilterChecker);
};

class ObTTLDutyDurationChecker : public ObConfigChecker {
public:
  ObTTLDutyDurationChecker()
  {}
  virtual ~ObTTLDutyDurationChecker(){};
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTTLDutyDurationChecker);
};

class ObMySQLVersionLengthChecker : public ObConfigChecker {
public:
  ObMySQLVersionLengthChecker()
  {}
  virtual ~ObMySQLVersionLengthChecker(){};
  bool check(const ObConfigItem& t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMySQLVersionLengthChecker);
};

class ObConfigPublishSchemaModeChecker
  : public ObConfigChecker
{
public:
  ObConfigPublishSchemaModeChecker() {}
  virtual ~ObConfigPublishSchemaModeChecker() {}
  bool check(const ObConfigItem& t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigPublishSchemaModeChecker);
};

// config item container
class ObConfigStringKey
{
public:
  ObConfigStringKey() { MEMSET(str_, 0, sizeof(str_)); }
  explicit ObConfigStringKey(const char *str);
  explicit ObConfigStringKey(const ObString &string);
  virtual ~ObConfigStringKey() {}
  uint64_t hash() const;
  inline int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }

  // case unsensitive
  bool operator == (const ObConfigStringKey &str) const
  {
    return 0 == STRCASECMP(str.str_, this->str_);
  }

  const char *str() const { return str_; }

private:
  char str_[OB_MAX_CONFIG_NAME_LEN];
  // ObConfigContainer 容器中使用了对象的拷贝构造函数,不能禁止
  //DISALLOW_COPY_AND_ASSIGN(ObConfigStringKey);
};
inline ObConfigStringKey::ObConfigStringKey(const char *str)
{
  int64_t pos = 0;
  (void) databuff_printf(str_, sizeof(str_), pos, "%s", str);
}

inline ObConfigStringKey::ObConfigStringKey(const ObString &string)
{
  int64_t pos = 0;
  (void) databuff_printf(str_, sizeof(str_), pos, "%.*s", string.length(), string.ptr());
}
inline uint64_t ObConfigStringKey::hash() const
{
  return 0; // murmurhash(str_, (int32_t)STRLEN(str_), 0); // murmurhash is case sensitive
}

template <class Key, class Value, int num>
class __ObConfigContainer
  : public hash::ObHashMap<Key, Value *, hash::NoPthreadDefendMode>
{
public:
  __ObConfigContainer()
  {
    this->create(num,
                 oceanbase::common::ObModIds::OB_HASH_BUCKET_CONF_CONTAINER,
                 oceanbase::common::ObModIds::OB_HASH_NODE_CONF_CONTAINER);
  }
 virtual ~__ObConfigContainer() {}

private:
  DISALLOW_COPY_AND_ASSIGN(__ObConfigContainer);
};

class ObConfigIntParser
{
public:
  ObConfigIntParser() {}
  virtual ~ObConfigIntParser() {}
  static int64_t get(const char *str, bool &valid);
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigIntParser);
};

class ObConfigCapacityParser
{
public:
  ObConfigCapacityParser() {}
  virtual ~ObConfigCapacityParser() {}
  static int64_t get(const char *str, bool &valid, bool check_unit = true, bool use_byte = false);
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigCapacityParser);
};

class ObConfigReadableIntParser
{
public:
  ObConfigReadableIntParser() {}
  virtual ~ObConfigReadableIntParser() {}
  static int64_t get(const char *str, bool &valid);

private:
  enum INT_UNIT
  {
    // 通常对于一个数字，可以写成 1k, 1m, 分别表示
    // 1000(kilo), 1000000(million)
    // billion 不支持，避免和 capacity 字节的 1b 混淆
    UNIT_K = 1000,
    UNIT_M = 1000000,
  };
  DISALLOW_COPY_AND_ASSIGN(ObConfigReadableIntParser);
};

class ObConfigTimeParser
{
public:
  ObConfigTimeParser() {}
  ~ObConfigTimeParser() {}
  static int64_t get(const char *str, bool &valid);
private:
  enum TIME_UNIT
  {
    TIME_MICROSECOND = 1UL,
    TIME_MILLISECOND = 1000UL,
    TIME_SECOND = 1000 * 1000UL,
    TIME_MINUTE = 60 * 1000 * 1000UL,
    TIME_HOUR = 60 * 60 * 1000 * 1000UL,
    TIME_DAY = 24 * 60 * 60 * 1000 * 1000UL,
  };
  DISALLOW_COPY_AND_ASSIGN(ObConfigTimeParser);
};

struct ObConfigBoolParser
{
  static bool get(const char *str, bool &valid);
};

class ObRpcClientAuthMethodChecker
  : public ObConfigChecker
{
public:
  ObRpcClientAuthMethodChecker() {}
  virtual ~ObRpcClientAuthMethodChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcClientAuthMethodChecker);
};

class ObRpcServerAuthMethodChecker
  : public ObConfigChecker
{
public:
  ObRpcServerAuthMethodChecker() {}
  virtual ~ObRpcServerAuthMethodChecker() {}
  bool check(const ObConfigItem &t) const;
  bool is_valid_server_auth_method(const ObString &str) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcServerAuthMethodChecker);
};

class ObConfigSQLTlsVersionChecker
  : public ObConfigChecker
{
public:
  ObConfigSQLTlsVersionChecker() {}
  virtual ~ObConfigSQLTlsVersionChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigSQLTlsVersionChecker);
};

class ObConfigSQLSpillCompressionCodecChecker
  : public ObConfigChecker
{
public:
  ObConfigSQLSpillCompressionCodecChecker() {}
  virtual ~ObConfigSQLSpillCompressionCodecChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigSQLSpillCompressionCodecChecker);
};

class ObSqlPlanManagementModeChecker
  : public ObConfigChecker
{
public:
  ObSqlPlanManagementModeChecker() {}
  virtual ~ObSqlPlanManagementModeChecker() {}
  bool check(const ObConfigItem &t) const;
  static int64_t get_spm_mode_by_string(const common::ObString &string);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlPlanManagementModeChecker);
};

class ObDefaultLoadModeChecker
  : public ObConfigChecker
{
public:
  ObDefaultLoadModeChecker() {}
  virtual ~ObDefaultLoadModeChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDefaultLoadModeChecker);
};

class ObModeConfigParserUitl
{
public:
  // parse config item like: "xxx=yyy", "xxx:yyy"
  static int parse_item_to_kv(char *item, ObString &key, ObString &value, const char* delim = "=");
  static int get_kv_list(char *str, ObIArray<std::pair<ObString, ObString>> &kv_list, const char* delim = "=");
  // format str for split config item
  static int format_mode_str(const char *src, int64_t src_len, char *dst, int64_t dst_len);
};

class ObConfigParser
{
public:
  ObConfigParser() {}
  virtual ~ObConfigParser() {}
  virtual bool parse(const char *str, uint8_t *arr, int64_t len) = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigParser);
};

class ObKvFeatureModeParser : public ObConfigParser
{
public:
  ObKvFeatureModeParser() {}
  virtual ~ObKvFeatureModeParser() {}
  virtual bool parse(const char *str, uint8_t *arr, int64_t len) override;
public:
  static const int8_t MODE_DEFAULT = 0b00;
  static const int8_t MODE_ON = 0b01;
  static const int8_t MODE_OFF = 0b10;
  DISALLOW_COPY_AND_ASSIGN(ObKvFeatureModeParser);
};

class ObConfigIndexStatsModeChecker : public ObConfigChecker {
public:
  ObConfigIndexStatsModeChecker(){}
  virtual ~ObConfigIndexStatsModeChecker(){}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigIndexStatsModeChecker);
};

class ObConfigTableStoreFormatChecker: public ObConfigChecker {
public:
  ObConfigTableStoreFormatChecker(){}
  virtual ~ObConfigTableStoreFormatChecker(){}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigTableStoreFormatChecker);
};

class ObConfigDDLNoLoggingChecker: public ObConfigChecker {
  public:
    static bool check(const uint64_t tenant_id, const obrpc::ObAdminSetConfigItem &t);
  private:
    DISALLOW_COPY_AND_ASSIGN(ObConfigDDLNoLoggingChecker);
};

class ObConfigArchiveLagTargetChecker {
public:
  ObConfigArchiveLagTargetChecker(){}
  virtual ~ObConfigArchiveLagTargetChecker(){}
  static bool check(const uint64_t tenant_id, const obrpc::ObAdminSetConfigItem &t);
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigArchiveLagTargetChecker);
};

class ObConfigMigrationChooseSourceChecker
  : public ObConfigChecker
{
public:
  ObConfigMigrationChooseSourceChecker() {}
  virtual ~ObConfigMigrationChooseSourceChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigMigrationChooseSourceChecker);
};


class ObParallelDDLControlParser : public ObConfigParser
{
public:
  ObParallelDDLControlParser() {}
  virtual ~ObParallelDDLControlParser() {}
  virtual bool parse(const char *str, uint8_t *arr, int64_t len) override;
public:
  static const uint8_t MODE_DEFAULT = 0b00;
  static const uint8_t MODE_OFF = 0b01;
  static const uint8_t MODE_ON = 0b10;
private:
  DISALLOW_COPY_AND_ASSIGN(ObParallelDDLControlParser);
};

class ObConfigKvGroupCommitRWModeChecker
  : public ObConfigChecker
{
public:
  ObConfigKvGroupCommitRWModeChecker() {}
  virtual ~ObConfigKvGroupCommitRWModeChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigKvGroupCommitRWModeChecker);
};

class ObConfigRegexpEngineChecker
  : public ObConfigChecker
{
public:
  ObConfigRegexpEngineChecker(){}
  virtual ~ObConfigRegexpEngineChecker(){}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigRegexpEngineChecker);
};

class ObConfigS3URLEncodeTypeChecker : public ObConfigChecker
{
public:
  ObConfigS3URLEncodeTypeChecker() {}
  virtual ~ObConfigS3URLEncodeTypeChecker() {}
  virtual bool check(const ObConfigItem &t) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigS3URLEncodeTypeChecker);
};

class ObConfigReplicaParallelMigrationChecker : public ObConfigChecker
{
public:
  ObConfigReplicaParallelMigrationChecker() {}
  virtual ~ObConfigReplicaParallelMigrationChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigReplicaParallelMigrationChecker);
};

class ObConfigDegradationPolicyChecker : public ObConfigChecker
{
public:
  ObConfigDegradationPolicyChecker() {}
  virtual ~ObConfigDegradationPolicyChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigDegradationPolicyChecker);
};

typedef __ObConfigContainer<ObConfigStringKey,
                            ObConfigItem, OB_MAX_CONFIG_NUMBER> ObConfigContainer;

class ObConfigVectorMemoryChecker
{
public:
  static bool check(const uint64_t tenant_id, const obrpc::ObAdminSetConfigItem &t);

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigVectorMemoryChecker);
};

class ObConfigEnableHashRollupChecker: public ObConfigChecker
{
public:
  ObConfigEnableHashRollupChecker()
  {}
  virtual ~ObConfigEnableHashRollupChecker()
  {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigEnableHashRollupChecker);
};

class ObConfigJavaParamsChecker
  : public ObConfigChecker
{
public:
  ObConfigJavaParamsChecker() {}
  virtual ~ObConfigJavaParamsChecker() {}
  bool check(const ObConfigItem& t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigJavaParamsChecker);
};

class ObConfigDefaultOrganizationChecker : public ObConfigChecker
{
public:
  ObConfigDefaultOrganizationChecker() {}
  virtual ~ObConfigDefaultOrganizationChecker() {}
  bool check(const ObConfigItem &t) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigDefaultOrganizationChecker);
};
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CONFIG_OB_CONFIG_HELPER_H_
