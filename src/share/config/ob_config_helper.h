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
#include "share/ob_define.h"

namespace oceanbase {
namespace common {
class ObConfigItem;
class ObConfigIntegralItem;
class ObConfigAlwaysTrue;

class ObConfigChecker {
public:
  ObConfigChecker()
  {}
  virtual ~ObConfigChecker()
  {}
  virtual bool check(const ObConfigItem& t) const = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigChecker);
};

class ObConfigAlwaysTrue : public ObConfigChecker {
public:
  ObConfigAlwaysTrue()
  {}
  virtual ~ObConfigAlwaysTrue()
  {}
  bool check(const ObConfigItem& t) const
  {
    UNUSED(t);
    return true;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigAlwaysTrue);
};

class ObConfigIpChecker : public ObConfigChecker {
public:
  ObConfigIpChecker()
  {}
  virtual ~ObConfigIpChecker()
  {}
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigIpChecker);
};

class ObConfigConsChecker : public ObConfigChecker {
public:
  ObConfigConsChecker(const ObConfigChecker* left, const ObConfigChecker* right) : left_(left), right_(right)
  {}
  virtual ~ObConfigConsChecker();
  bool check(const ObConfigItem& t) const;

private:
  const ObConfigChecker* left_;
  const ObConfigChecker* right_;
  DISALLOW_COPY_AND_ASSIGN(ObConfigConsChecker);
};

class ObConfigBinaryChecker : public ObConfigChecker {
public:
  explicit ObConfigBinaryChecker(const char* str);
  virtual ~ObConfigBinaryChecker()
  {}
  const char* value() const
  {
    return val_;
  }

protected:
  char val_[OB_MAX_CONFIG_VALUE_LEN];

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigBinaryChecker);
};
inline ObConfigBinaryChecker::ObConfigBinaryChecker(const char* str)
{
  int64_t pos = 0;
  (void)databuff_printf(val_, sizeof(val_), pos, "%s", str);
}

class ObConfigGreaterThan : public ObConfigBinaryChecker {
public:
  explicit ObConfigGreaterThan(const char* str) : ObConfigBinaryChecker(str)
  {}
  virtual ~ObConfigGreaterThan()
  {}
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigGreaterThan);
};

class ObConfigGreaterEqual : public ObConfigBinaryChecker {
public:
  explicit ObConfigGreaterEqual(const char* str) : ObConfigBinaryChecker(str)
  {}
  virtual ~ObConfigGreaterEqual()
  {}
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigGreaterEqual);
};

class ObConfigLessThan : public ObConfigBinaryChecker {
public:
  explicit ObConfigLessThan(const char* str) : ObConfigBinaryChecker(str)
  {}
  virtual ~ObConfigLessThan()
  {}
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigLessThan);
};

class ObConfigLessEqual : public ObConfigBinaryChecker {
public:
  explicit ObConfigLessEqual(const char* str) : ObConfigBinaryChecker(str)
  {}
  virtual ~ObConfigLessEqual()
  {}
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigLessEqual);
};

class ObConfigEvenIntChecker : public ObConfigChecker {
public:
  ObConfigEvenIntChecker()
  {}
  virtual ~ObConfigEvenIntChecker()
  {}
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigEvenIntChecker);
};

class ObConfigTabletSizeChecker : public ObConfigChecker {
public:
  ObConfigTabletSizeChecker()
  {}
  virtual ~ObConfigTabletSizeChecker()
  {}
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigTabletSizeChecker);
};

class ObConfigStaleTimeChecker : public ObConfigChecker {
public:
  ObConfigStaleTimeChecker()
  {}
  virtual ~ObConfigStaleTimeChecker()
  {}
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigStaleTimeChecker);
};

class ObConfigPerfCompressFuncChecker : public ObConfigChecker {
public:
  ObConfigPerfCompressFuncChecker()
  {}
  virtual ~ObConfigPerfCompressFuncChecker()
  {}
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigPerfCompressFuncChecker);
};

class ObConfigCompressFuncChecker : public ObConfigChecker {
public:
  ObConfigCompressFuncChecker()
  {}
  virtual ~ObConfigCompressFuncChecker()
  {}
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigCompressFuncChecker);
};

class ObConfigBatchRpcCompressFuncChecker : public ObConfigChecker {
public:
  ObConfigBatchRpcCompressFuncChecker()
  {}
  virtual ~ObConfigBatchRpcCompressFuncChecker()
  {}
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigBatchRpcCompressFuncChecker);
};

class ObConfigRowFormatChecker : public ObConfigChecker {
public:
  ObConfigRowFormatChecker()
  {}
  virtual ~ObConfigRowFormatChecker()
  {}
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigRowFormatChecker);
};

class ObConfigCompressOptionChecker : public ObConfigChecker {
public:
  ObConfigCompressOptionChecker()
  {}
  virtual ~ObConfigCompressOptionChecker()
  {}
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigCompressOptionChecker);
};

class ObConfigUseLargePagesChecker : public ObConfigChecker {
public:
  ObConfigUseLargePagesChecker()
  {}
  virtual ~ObConfigUseLargePagesChecker()
  {}
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigUseLargePagesChecker);
};

class ObConfigLogLevelChecker : public ObConfigChecker {
public:
  ObConfigLogLevelChecker()
  {}
  virtual ~ObConfigLogLevelChecker(){};
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigLogLevelChecker);
};

class ObConfigWorkAreaPolicyChecker : public ObConfigChecker {
public:
  ObConfigWorkAreaPolicyChecker()
  {}
  virtual ~ObConfigWorkAreaPolicyChecker(){};
  bool check(const ObConfigItem& t) const;

private:
  static constexpr const char* MANUAL = "MANUAL";
  static constexpr const char* AUTO = "AUTO";

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigWorkAreaPolicyChecker);
};

class ObConfigBackupRegionChecker : public ObConfigChecker {
public:
  ObConfigBackupRegionChecker()
  {}
  virtual ~ObConfigBackupRegionChecker()
  {}
  // TODO: backup fix it
  bool check(const ObConfigItem& t) const
  {
    UNUSED(t);
    return true;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigBackupRegionChecker);
};

class ObConfigLogArchiveOptionsChecker : public ObConfigChecker {
public:
  ObConfigLogArchiveOptionsChecker()
  {}
  virtual ~ObConfigLogArchiveOptionsChecker()
  {}
  // TODO: backup fix it
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigLogArchiveOptionsChecker);
};

class ObConfigRpcChecksumChecker : public ObConfigChecker {
public:
  ObConfigRpcChecksumChecker()
  {}
  virtual ~ObConfigRpcChecksumChecker(){};
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigRpcChecksumChecker);
};

class ObConfigMemoryLimitChecker : public ObConfigChecker {
public:
  ObConfigMemoryLimitChecker()
  {}
  virtual ~ObConfigMemoryLimitChecker(){};
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigMemoryLimitChecker);
};

class ObConfigUpgradeStageChecker : public ObConfigChecker {
public:
  ObConfigUpgradeStageChecker()
  {}
  virtual ~ObConfigUpgradeStageChecker()
  {}

  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigUpgradeStageChecker);
};

class ObConfigPlanCacheGCChecker : public ObConfigChecker {
public:
  ObConfigPlanCacheGCChecker()
  {}
  virtual ~ObConfigPlanCacheGCChecker()
  {}

  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigPlanCacheGCChecker);
};

class ObConfigQueryRateLimitChecker : public ObConfigChecker {
public:
  ObConfigQueryRateLimitChecker()
  {}
  virtual ~ObConfigQueryRateLimitChecker(){};
  bool check(const ObConfigItem& t) const;

  static constexpr int64_t MAX_QUERY_RATE_LIMIT = 200000;
  static constexpr int64_t MIN_QUERY_RATE_LIMIT = 10;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigQueryRateLimitChecker);
};

class ObConfigPartitionBalanceStrategyFuncChecker : public ObConfigChecker {
public:
  enum PartitionBalanceStrategy {
    AUTO = 0,
    STANDARD,
    DISK_UTILIZATION_ONLY,
    PARTITION_BALANCE_STRATEGY_MAX,
  };
  static const char* balance_strategy[PARTITION_BALANCE_STRATEGY_MAX];

public:
  ObConfigPartitionBalanceStrategyFuncChecker()
  {}
  virtual ~ObConfigPartitionBalanceStrategyFuncChecker()
  {}
  bool check(const ObConfigItem& t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigPartitionBalanceStrategyFuncChecker);
};

// config item container
class ObConfigStringKey {
public:
  ObConfigStringKey()
  {
    MEMSET(str_, 0, sizeof(str_));
  }
  explicit ObConfigStringKey(const char* str);
  explicit ObConfigStringKey(const ObString& string);
  virtual ~ObConfigStringKey()
  {}
  uint64_t hash() const;

  // case unsensitive
  bool operator==(const ObConfigStringKey& str) const
  {
    return 0 == STRCASECMP(str.str_, this->str_);
  }

  const char* str() const
  {
    return str_;
  }

private:
  char str_[OB_MAX_CONFIG_NAME_LEN];
  // DISALLOW_COPY_AND_ASSIGN(ObConfigStringKey);
};
inline ObConfigStringKey::ObConfigStringKey(const char* str)
{
  int64_t pos = 0;
  (void)databuff_printf(str_, sizeof(str_), pos, "%s", str);
}

inline ObConfigStringKey::ObConfigStringKey(const ObString& string)
{
  int64_t pos = 0;
  (void)databuff_printf(str_, sizeof(str_), pos, "%.*s", string.length(), string.ptr());
}
inline uint64_t ObConfigStringKey::hash() const
{
  // if str_ is null, hash value will be 0
  uint64_t h = 0;
  if (OB_LIKELY(NULL != str_)) {
    murmurhash(str_, (int32_t)STRLEN(str_), 0);
  }
  return h;
}

template <class Key, class Value, int num>
class __ObConfigContainer : public hash::ObHashMap<Key, Value*> {
public:
  __ObConfigContainer()
  {
    this->create(num,
        oceanbase::common::ObModIds::OB_HASH_BUCKET_CONF_CONTAINER,
        oceanbase::common::ObModIds::OB_HASH_NODE_CONF_CONTAINER);
  }
  virtual ~__ObConfigContainer()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(__ObConfigContainer);
};

class ObConfigIntParser {
public:
  ObConfigIntParser()
  {}
  virtual ~ObConfigIntParser()
  {}
  static int64_t get(const char* str, bool& valid);

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigIntParser);
};

class ObConfigCapacityParser {
public:
  ObConfigCapacityParser()
  {}
  virtual ~ObConfigCapacityParser()
  {}
  static int64_t get(const char* str, bool& valid);

private:
  enum CAP_UNIT {
    // shift bits between unit of byte and that
    CAP_B = 0,
    CAP_KB = 10,
    CAP_MB = 20,
    CAP_GB = 30,
    CAP_TB = 40,
    CAP_PB = 50,
  };
  DISALLOW_COPY_AND_ASSIGN(ObConfigCapacityParser);
};

class ObConfigReadableIntParser {
public:
  ObConfigReadableIntParser()
  {}
  virtual ~ObConfigReadableIntParser()
  {}
  static int64_t get(const char* str, bool& valid);

private:
  enum INT_UNIT {
    UNIT_K = 1000,
    UNIT_M = 1000000,
  };
  DISALLOW_COPY_AND_ASSIGN(ObConfigReadableIntParser);
};

class ObConfigTimeParser {
public:
  ObConfigTimeParser()
  {}
  ~ObConfigTimeParser()
  {}
  static int64_t get(const char* str, bool& valid);

private:
  enum TIME_UNIT {
    TIME_MICROSECOND = 1UL,
    TIME_MILLISECOND = 1000UL,
    TIME_SECOND = 1000 * 1000UL,
    TIME_MINUTE = 60 * 1000 * 1000UL,
    TIME_HOUR = 60 * 60 * 1000 * 1000UL,
    TIME_DAY = 24 * 60 * 60 * 1000 * 1000UL,
  };
  DISALLOW_COPY_AND_ASSIGN(ObConfigTimeParser);
};

typedef __ObConfigContainer<ObConfigStringKey, ObConfigItem, OB_MAX_CONFIG_NUMBER> ObConfigContainer;
}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_CONFIG_OB_CONFIG_HELPER_H_
