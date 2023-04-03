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

#ifndef OCEANBASE_SHARE_CONFIG_OB_SYSTEM_CONFIG_H_
#define OCEANBASE_SHARE_CONFIG_OB_SYSTEM_CONFIG_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_array.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/config/ob_system_config_key.h"
#include "share/config/ob_system_config_value.h"

namespace oceanbase
{
namespace common
{
class ObConfigItem;

class ObSystemConfig
{
public:
  static const int64_t INIT_VERSION = 2;
  typedef hash::ObHashMap<ObSystemConfigKey, ObSystemConfigValue*> hashmap;
public:
  ObSystemConfig() : allocator_("SystemConfig", common::OB_MALLOC_MIDDLE_BLOCK_SIZE),
                     map_(), version_(0) {};
  virtual ~ObSystemConfig() {};

  int clear();
  int init();
  int update(ObMySQLProxy::MySQLResult &result);

  int find_newest(const ObSystemConfigKey &key,
                  const ObSystemConfigValue *&pvalue,
                  int64_t &max_version) const;
  int find(const ObSystemConfigKey &key, const ObSystemConfigValue *&pvalue) const;
  int read_int32(const ObSystemConfigKey &key, int32_t &value, const int32_t &def) const;
  int read_int64(const ObSystemConfigKey &key, int64_t &value, const int64_t &def) const;
  int read_int(const ObSystemConfigKey &key, int64_t &value, const int64_t &def) const;
  int read_str(const ObSystemConfigKey &key, char buf[], int64_t len, const char *def) const;
  int read_config(const ObSystemConfigKey &key, ObConfigItem &item) const;
  int64_t to_string(char *buf, const int64_t len) const;
  int reload(FILE *fp);
  int dump2file(const char *path) const;
  int64_t get_version() const { return version_; }

private:
  int update_value(const ObSystemConfigKey &key, const ObSystemConfigValue &value);
private:
  static const int64_t MAP_SIZE = 512;
  ObArenaAllocator allocator_;
  hashmap map_;
  int64_t version_;
  DISALLOW_COPY_AND_ASSIGN(ObSystemConfig);
};

inline int ObSystemConfig::clear()
{
  int ret = map_.clear();
  allocator_.reset();
  return ret;
}

inline int ObSystemConfig::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(map_.create(MAP_SIZE, ObModIds::OB_HASH_BUCKET_SYS_CONF))) {
    OB_LOG(WARN, "create params_map_ fail", K(ret));
  }
  return ret;
}

inline int ObSystemConfig::read_int(const ObSystemConfigKey &key,
                                    int64_t &value,
                                    const int64_t &def) const
{
  return read_int64(key, value, def);
}
} // end of namespace common
} // end of namespace oceanbase

#endif // OCEANBASE_SHARE_CONFIG_OB_SYSTEM_CONFIG_H_
