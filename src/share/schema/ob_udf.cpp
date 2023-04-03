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
#include "ob_udf.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{

using namespace std;
using namespace common;

namespace share
{
namespace schema
{

ObUDF::ObUDF(common::ObIAllocator *allocator)
    : ObSchema(allocator), tenant_id_(common::OB_INVALID_ID), udf_id_(common::OB_INVALID_ID), name_(), ret_(UDFRetType::UDF_RET_UNINITIAL),
      dl_(), type_(UDFType::UDF_TYPE_UNINITIAL), schema_version_(common::OB_INVALID_VERSION)
{
  reset();
}

ObUDF::ObUDF(const ObUDF &src_schema)
    : ObSchema(), tenant_id_(common::OB_INVALID_ID), udf_id_(common::OB_INVALID_ID), name_(), ret_(UDFRetType::UDF_RET_UNINITIAL),
      dl_(), type_(UDFType::UDF_TYPE_UNINITIAL), schema_version_(common::OB_INVALID_VERSION)
{
  reset();
  *this = src_schema;
}

ObUDF::~ObUDF()
{
}

ObUDF &ObUDF::operator = (const ObUDF &src_schema)
{
  if (this != &src_schema) {
    reset();
    error_ret_ = src_schema.error_ret_;
    tenant_id_ = src_schema.tenant_id_;
    ret_ = src_schema.ret_;
    type_ = src_schema.type_;

    int ret = OB_SUCCESS;
    if (OB_FAIL(deep_copy_str(src_schema.name_, name_))) {
      LOG_WARN("Fail to deep copy udf name, ", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_schema.dl_, dl_))) {
      LOG_WARN("Fail to deep copy udf dl, ", K(ret));
    } else {/*do nothing*/}

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
    LOG_DEBUG("operator =", K(src_schema), K(*this));
  }
  return *this;
}

bool ObUDF::operator==(const ObUDF &r) const
{
  return (tenant_id_ == r.tenant_id_ && name_ == r.name_);
}

bool ObUDF::operator !=(const ObUDF &r) const
{
  return !(*this == r);
}

int64_t ObUDF::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += name_.length() + 1;
  convert_size += dl_.length() + 1;
  return convert_size;
}

void ObUDF::reset()
{
  tenant_id_ = OB_INVALID_ID;
  name_.reset();
  ret_ = STRING;
  dl_.reset();
  type_ = FUNCTION;
  ObSchema::reset();
}

OB_SERIALIZE_MEMBER(ObUDF,
										tenant_id_,
										name_,
										ret_,
										dl_,
										type_);



OB_SERIALIZE_MEMBER(ObUDFMeta,
                    tenant_id_,
                    name_,
                    ret_,
                    dl_,
                    type_);

}
}
}
