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

#ifndef OCEANBASE_SHARE_UNIT_OB_UNIT_CONFIG_H_
#define OCEANBASE_SHARE_UNIT_OB_UNIT_CONFIG_H_

#include "lib/string/ob_string.h"                 // ObString
#include "lib/utility/ob_unify_serialize.h"       // OB_UNIS_VERSION
#include "lib/string/ob_fixed_length_string.h"    // ObFixedLengthString
#include "share/unit/ob_unit_resource.h"          // ObUnitResource

namespace oceanbase
{
namespace share
{

typedef common::ObFixedLengthString<common::MAX_UNIT_CONFIG_LENGTH> ObUnitConfigName;
struct ObUnitConfig
{
  OB_UNIS_VERSION(1);

public:
  static const uint64_t SYS_UNIT_CONFIG_ID = 1;
  static const uint64_t HIDDEN_SYS_UNIT_CONFIG_ID = 1000;
  static const uint64_t VIRTUAL_TENANT_UNIT_CONFIG_ID = 1000;
  static const char *SYS_UNIT_CONFIG_NAME;
  static const char *HIDDEN_SYS_UNIT_CONFIG_NAME;
  static const char *VIRTUAL_TENANT_UNIT_CONFIG_NAME;

public:
  ObUnitConfig();
  ObUnitConfig(const ObUnitConfig &unit);
  ObUnitConfig(
      const uint64_t unit_config_id,
      const ObUnitConfigName &name,
      const ObUnitResource &resource) :
      unit_config_id_(unit_config_id),
      name_(name),
      resource_(resource)
  {}
  ~ObUnitConfig() {}
  bool is_valid() const;

  //////////////// init funcs ///////////////////
  void reset();
  int init(const uint64_t unit_config_id,
           const ObUnitConfigName &name,
           const ObUnitResource &resource);

  // set and do not check valid
  //
  // not all properties of unit_resource are valid.
  //    * valid properties means: user specified
  //    * invalid properties means: user not specified
  int set(const uint64_t unit_config_id,
          const common::ObString &name,
          const ObUnitResource &resource);

  ////////////////  assign funcs /////////////////
  int assign(const ObUnitConfig &other);

  /////////////////  get func /////////////////
  uint64_t unit_config_id() const { return unit_config_id_; }
  const ObUnitConfigName &name() const { return name_; }
  const ObUnitResource &unit_resource() const { return resource_; }
  double max_cpu() const { return resource_.max_cpu(); }
  double min_cpu() const { return resource_.min_cpu(); }
  int64_t memory_size() const { return resource_.memory_size(); }
  int64_t log_disk_size() const { return resource_.log_disk_size(); }
  int64_t max_iops() const { return resource_.max_iops(); }
  int64_t min_iops() const { return resource_.min_iops(); }
  int64_t iops_weight() const { return resource_.iops_weight(); }

  ///////////////// update func ///////////////////
  int update_unit_resource(ObUnitResource &ur);

  ///////////////// other func ////////////////////
  int gen_sys_tenant_unit_config(const bool is_hidden_sys);
  int gen_virtual_tenant_unit_config(
      const double max_cpu,
      const double min_cpu,
      const int64_t mem_limit);

  ///////////////////////////////////  compute func //////////////////////////////////////
  ObUnitConfig operator+(const ObUnitConfig &config) const
  {
    return ObUnitConfig(unit_config_id_, name_, resource_ + config.resource_);
  }
  ObUnitConfig operator-(const ObUnitConfig &config) const
  {
    return ObUnitConfig(unit_config_id_, name_, resource_ - config.resource_);
  }
  ObUnitConfig &operator+=(const ObUnitConfig &config)
  {
    resource_ += config.resource_;
    return *this;
  }
  ObUnitConfig &operator-=(const ObUnitConfig &config)
  {
    resource_ -= config.resource_;
    return *this;
  }
  ObUnitConfig operator*(const int64_t count) const
  {
    return ObUnitConfig(unit_config_id_, name_, resource_ * count);
  }
  bool operator==(const ObUnitConfig &config) const
  {
    return unit_config_id_ == config.unit_config_id_ &&
           name_ == config.name_ &&
           resource_ == config.resource_;
  }


public:
  TO_STRING_KV(K_(unit_config_id), K_(name), K_(resource));

private:
  uint64_t          unit_config_id_;
  ObUnitConfigName  name_;
  ObUnitResource    resource_;
};

}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_UNIT_OB_UNIT_CONFIG_H_
