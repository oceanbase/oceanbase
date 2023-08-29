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

#define USING_LOG_PREFIX  SHARE

#include "ob_unit_config.h"

#include "lib/oblog/ob_log_module.h"        // *_LOG
#include "share/config/ob_server_config.h"  // GCONF

namespace oceanbase
{
using namespace common;
namespace share
{
const char *ObUnitConfig::SYS_UNIT_CONFIG_NAME = "sys_unit_config";
const char *ObUnitConfig::HIDDEN_SYS_UNIT_CONFIG_NAME = "hidden_sys_unit";
const char *ObUnitConfig::VIRTUAL_TENANT_UNIT_CONFIG_NAME = "virtual_tenant_unit";

ObUnitConfig::ObUnitConfig() :
    unit_config_id_(OB_INVALID_ID),
    name_(),
    resource_()
{
}

ObUnitConfig::ObUnitConfig(const ObUnitConfig &unit) :
    unit_config_id_(unit.unit_config_id()),
    name_(unit.name()),
    resource_(unit.unit_resource())
{
}

void ObUnitConfig::reset()
{
  unit_config_id_ = OB_INVALID_ID;
  name_.reset();
  resource_.reset();
}

bool ObUnitConfig::is_valid() const
{
  return unit_config_id_ != OB_INVALID_ID && !name_.is_empty() && resource_.is_valid();
}

OB_SERIALIZE_MEMBER(ObUnitConfig,
                    unit_config_id_,
                    name_,
                    resource_);

int ObUnitConfig::assign(const ObUnitConfig &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else if (OB_FAIL(name_.assign(other.name_))) {
    LOG_WARN("fail to assign config name", KR(ret), K(other));
  } else {
    unit_config_id_ = other.unit_config_id_;
    resource_ = other.resource_;
  }
  return ret;
}

int ObUnitConfig::set(
    const uint64_t unit_config_id,
    const ObString &name,
    const ObUnitResource &resource)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(name_.assign(name))) {
    LOG_WARN("assign name fail", KR(ret), K(name));
  } else {
    unit_config_id_ = unit_config_id;
    resource_ = resource;
  }
  return ret;
}

int ObUnitConfig::init(
    const uint64_t unit_config_id,
    const ObUnitConfigName &name,
    const ObUnitResource &resource)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == unit_config_id ||
                  name.is_empty() ||
                  ! resource.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(unit_config_id), K(name), K(resource));
  } else if (OB_FAIL(name_.assign(name))) {
    LOG_WARN("fail to assign config name", KR(ret), K(name));
  } else {
    unit_config_id_ = unit_config_id;
    resource_ = resource;
  }
  return ret;
}

int ObUnitConfig::update_unit_resource(ObUnitResource &ur)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("unit config not valid, can not update unit resource", KR(ret), KPC(this), K(ur));
  } else if (OB_UNLIKELY(! ur.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unit resource, can not update unit resource", KR(ret), K(ur));
  } else {
    resource_ = ur;
  }
  return ret;
}

int ObUnitConfig::gen_sys_tenant_unit_config(const bool is_hidden_sys)
{
  int ret = OB_SUCCESS;
  ObUnitResource ur;
  const char *name = is_hidden_sys ? HIDDEN_SYS_UNIT_CONFIG_NAME : SYS_UNIT_CONFIG_NAME;
  const uint64_t unit_config_id = is_hidden_sys ? HIDDEN_SYS_UNIT_CONFIG_ID : SYS_UNIT_CONFIG_ID;

  if (OB_FAIL(ur.gen_sys_tenant_default_unit_resource(is_hidden_sys))) {
    LOG_WARN("generate sys tenant default unit resource fail", KR(ret), K(ur));
  } else if (OB_FAIL(init(unit_config_id, name, ur))) {
    LOG_WARN("init unit config fail", KR(ret), K(unit_config_id), K(name), K(ur));
  }
  return ret;
}


int ObUnitConfig::gen_virtual_tenant_unit_config(
    const double max_cpu,
    const double min_cpu,
    const int64_t mem_limit)
{
  int ret = OB_SUCCESS;
  const int64_t log_disk_size = ObUnitResource::UNIT_MIN_LOG_DISK_SIZE;
  const int64_t min_iops = 10000;
  const int64_t max_iops = 50000;
  const int64_t iops_weight = 0;
  const char *name = VIRTUAL_TENANT_UNIT_CONFIG_NAME;
  const uint64_t unit_config_id = VIRTUAL_TENANT_UNIT_CONFIG_ID;

  const ObUnitResource ur(
      max_cpu,
      min_cpu,
      mem_limit,
      log_disk_size,
      max_iops,
      min_iops,
      iops_weight);

  if (OB_FAIL(init(unit_config_id, name, ur))) {
    LOG_WARN("init unit config for virtual tenant fail", KR(ret), K(min_cpu), K(max_cpu),
        K(mem_limit), K(unit_config_id), K(name), K(ur));
  }

  return ret;
}

}//end namespace share
}//end namespace oceanbase
