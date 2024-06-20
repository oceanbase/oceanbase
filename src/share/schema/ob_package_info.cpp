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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "share/schema/ob_package_info.h"
#include "share/schema/ob_trigger_info.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

int ObPackageInfo::assign(const ObPackageInfo &package_info)
{
  int ret = OB_SUCCESS;
  if (this != &package_info) {
    reset();
    set_tenant_id(package_info.get_tenant_id());
    database_id_ = package_info.database_id_;
    owner_id_ = package_info.owner_id_;
    package_id_ = package_info.package_id_;
    schema_version_ = package_info.schema_version_;
    type_ = package_info.type_;
    flag_ = package_info.flag_;
    comp_flag_ = package_info.comp_flag_;

    if (OB_FAIL(deep_copy_str(package_info.get_package_name(), package_name_))) {
      LOG_WARN("deep copy package name failed", "package_name", package_info.package_name_, K(ret));
    } else if (OB_FAIL(deep_copy_str(package_info.get_source(), source_))) {
      LOG_WARN("deep copy src failed", "source", package_info.source_, K(ret));
    } else if (OB_FAIL(deep_copy_str(package_info.exec_env_, exec_env_))) {
      LOG_WARN("deep copy exec env failed", K(ret), "exec_env", package_info.exec_env_);
    } else if (OB_FAIL(deep_copy_str(package_info.comment_, comment_))) {
      LOG_WARN("deep copy comment failed", K(ret), "comment", package_info.comment_);
    } else if (OB_FAIL(deep_copy_str(package_info.route_sql_, route_sql_))) {
      LOG_WARN("deep copy comment failed", K(ret), "route_sql", package_info.route_sql_);
    }else {
      // do nothing
    }
    this->error_ret_ = ret;
  }
  return ret;
}

bool ObPackageInfo::is_valid() const
{
  bool bret = false;
  bret = ObSchema::is_valid();
  if (bret) {
    bret = tenant_id_ != OB_INVALID_ID
        && !package_name_.empty()
        && type_ != INVALID_PACKAGE_TYPE
        && !source_.empty();
  }
  return bret;
}

void ObPackageInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  owner_id_ = OB_INVALID_ID;
  package_id_ = OB_INVALID_ID;
  reset_string(package_name_);
  schema_version_ = common::OB_INVALID_VERSION;
  type_ = INVALID_PACKAGE_TYPE;
  flag_ = 0;
  comp_flag_ = 0;
  reset_string(exec_env_);
  reset_string(source_);
  reset_string(comment_);
  reset_string(route_sql_);
  ObSchema::reset();
}

int64_t ObPackageInfo::get_convert_size() const
{
  int64_t len = 0;

  len += static_cast<int64_t>(sizeof(ObPackageInfo));
  len += package_name_.length() + 1;
  len += exec_env_.length() + 1;
  len += source_.length() + 1;
  len += comment_.length() + 1;
  len += route_sql_.length() + 1;

  return len;
}

bool ObPackageInfo::is_for_trigger() const
{
  return ObTriggerInfo::is_trigger_package_id(package_id_);
}

OB_DEF_SERIALIZE(ObPackageInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              database_id_,
              owner_id_,
              package_id_,
              package_name_,
              type_,
              flag_,
              comp_flag_,
              exec_env_,
              source_,
              comment_,
              route_sql_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPackageInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              database_id_,
              owner_id_,
              package_id_,
              package_name_,
              type_,
              flag_,
              comp_flag_,
              exec_env_,
              source_,
              comment_,
              route_sql_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPackageInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_,
              database_id_,
              owner_id_,
              package_id_,
              package_name_,
              type_,
              flag_,
              comp_flag_,
              exec_env_,
              source_,
              comment_,
              route_sql_);
  return len;
}
}  // namespace schema
}  // namespace share
}  // namespace oceanbase




