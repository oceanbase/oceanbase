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
#include "share/schema/ob_location_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

ObLocationSchema::ObLocationSchema()
  : ObSchema()
{
  reset();
}

ObLocationSchema::ObLocationSchema(common::ObIAllocator *allocator)
  : ObSchema(allocator),
    tenant_id_(common::OB_INVALID_TENANT_ID),
    location_id_(OB_INVALID_ID),
    schema_version_(common::OB_INVALID_VERSION),
    location_name_(),
    location_url_(),
    location_access_info_()
{
  name_case_mode_ = OB_NAME_CASE_INVALID;
}

ObLocationSchema::~ObLocationSchema()
{
}

ObLocationSchema::ObLocationSchema(const ObLocationSchema &other)
  : ObSchema()
{
  *this = other;
}

ObLocationSchema &ObLocationSchema::operator=(const ObLocationSchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    location_id_ = other.location_id_;
    schema_version_ = other.schema_version_;
    set_name_case_mode(other.name_case_mode_);

    if (OB_FAIL(set_location_name(other.location_name_))) {
      LOG_WARN("Fail to deep copy directory name", K(ret));
    } else if (OB_FAIL(set_location_url(other.location_url_))) {
      LOG_WARN("Fail to deep copy directory path", K(ret));
    } else if (OB_FAIL(set_location_access_info(other.location_access_info_))) {
      LOG_WARN("Fail to deep copy location access info", K(ret));
    }

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

int ObLocationSchema::assign(const ObLocationSchema &other)
{
  int ret = OB_SUCCESS;
  *this = other;
  ret = get_err_ret();
  return ret;
}

bool ObLocationSchema::is_valid() const
{
  bool ret = true;
  if (!ObSchema::is_valid()
      || !is_valid_tenant_id(tenant_id_)
      || !is_valid_id(location_id_)
      || schema_version_ < 0
      || location_name_.empty()
      || location_url_.empty()) {
    ret = false;
  }
  return ret;
}

void ObLocationSchema::reset()
{
  tenant_id_ = common::OB_INVALID_TENANT_ID;
  location_id_ = OB_INVALID_ID;
  schema_version_ = common::OB_INVALID_VERSION;
  location_name_.reset();
  location_url_.reset();
  location_access_info_.reset();
  name_case_mode_ = OB_NAME_CASE_INVALID;
}

int64_t ObLocationSchema::get_convert_size() const
{
  return sizeof(ObLocationSchema)
        + location_name_.length() + 1
        + location_url_.length() + 1
        + location_access_info_.length() + 1;
}

OB_SERIALIZE_MEMBER(ObLocationSchema,
                    tenant_id_,
                    location_id_,
                    schema_version_,
                    location_name_,
                    location_url_,
                    location_access_info_,
                    name_case_mode_);
}
}
}