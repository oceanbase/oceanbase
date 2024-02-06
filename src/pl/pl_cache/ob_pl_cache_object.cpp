/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX PL_CACHE
#include "ob_pl_cache_object.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace pl
{

OB_SERIALIZE_MEMBER(ObPlParamInfo,
                    flag_,
                    scale_,
                    type_,
                    ext_real_type_,
                    is_oracle_empty_string_,
                    col_type_,
                    pl_type_,
                    udt_id_);

void ObPLCacheObject::reset()
{
  ObILibCacheObject::reset();
  tenant_schema_version_ = OB_INVALID_VERSION;
  sys_schema_version_ = OB_INVALID_VERSION;
  params_info_.reset();
  sql_expression_factory_.destroy();
  expr_operator_factory_.destroy();
  expressions_.reset();
}

int ObPLCacheObject::set_params_info(const ParamStore &params)
{
  int ret = OB_SUCCESS;
  int64_t N = params.count();
  ObPlParamInfo param_info;
  if (N > 0 && OB_FAIL(params_info_.reserve(N))) {
    OB_LOG(WARN, "fail to reserve params info", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    param_info.flag_ = params.at(i).get_param_flag();
    param_info.type_ = params.at(i).get_param_meta().get_type();
    param_info.col_type_ = params.at(i).get_collation_type();
    if (sql::ObSQLUtils::is_oracle_empty_string(params.at(i))) {
      param_info.is_oracle_empty_string_ = true;
    }
    if (params.at(i).get_param_meta().get_type() != params.at(i).get_type()) {
      LOG_TRACE("differ in set_params_info",
                K(params.at(i).get_param_meta().get_type()),
                K(params.at(i).get_type()),
                K(common::lbt()));
    }
    if (params.at(i).is_pl_extend()) {
      ObDataType data_type;
      param_info.pl_type_ = params.at(i).get_meta().get_extend_type();
      if (param_info.pl_type_ == pl::PL_NESTED_TABLE_TYPE ||
          param_info.pl_type_ == pl::PL_ASSOCIATIVE_ARRAY_TYPE ||
          param_info.pl_type_ == pl::PL_VARRAY_TYPE ||
          param_info.pl_type_ == pl::PL_RECORD_TYPE) {
        const pl::ObPLComposite *composite =
                reinterpret_cast<const pl::ObPLComposite*>(params.at(i).get_ext());
        if (OB_ISNULL(composite)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("nested table is null", K(ret));
        } else {
          param_info.udt_id_ = composite->get_id();
        }
      } else {
        if (OB_FAIL(sql::ObSQLUtils::get_ext_obj_data_type(params.at(i), data_type))) {
          LOG_WARN("fail to get ext obj data type", K(ret));
        } else {
          param_info.ext_real_type_ = data_type.get_obj_type();
          param_info.scale_ = data_type.get_scale();
        }
      }
      LOG_DEBUG("ext params info", K(data_type), K(param_info), K(params.at(i)));
    } else {
      param_info.scale_ = params.at(i).get_scale();
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(params_info_.push_back(param_info))) {
        LOG_WARN("failed to push back param info", K(ret));
      }
    }
    param_info.reset();
  }
  return ret;
}

}
}