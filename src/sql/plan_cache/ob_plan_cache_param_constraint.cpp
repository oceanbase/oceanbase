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

#define USING_LOG_PREFIX SQL_PC

#include "ob_plan_cache_param_constraint.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/allocator/ob_mod_define.h"
#include "share/object/ob_obj_cast.h"
namespace oceanbase
{
namespace sql
{
int ObPCUnixTimestampParamConstraint::build(const common::ParamStore &params, void *ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  if (OB_FAIL(ObPCUnixTimestampParamConstraint::deduce_result_type(params, type_, precision_, scale_))) {
    LOG_WARN("failed to deduce result type for uinx_timestamp expr", K(ret), K(param_idx_), K(precision_), K(scale_));
  }
  return ret;
}

int ObPCUnixTimestampParamConstraint::deep_copy(ObIAllocator &allocator, ObPCParamConstraint *&to)
{ 
  int ret = OB_SUCCESS;
  void *buf = allocator.alloc(sizeof(ObPCUnixTimestampParamConstraint));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(sizeof(ObPCUnixTimestampParamConstraint)));
  } else {
    to = new(buf)ObPCUnixTimestampParamConstraint(param_idx_, type_, precision_, scale_);
  }
  return ret; 
}

int ObPCUnixTimestampParamConstraint::deduce_result_type(const common::ParamStore &params,
                                                     uint8_t &type,
                                                     int16_t &precision,
                                                     int16_t &scale)
{
  int ret = OB_SUCCESS;
  if (param_idx_ >= params.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param idx out of range", K(ret), K(param_idx_), K(params.count()));
  } else {
    const common::ObObjParam &date_obj = params.at(param_idx_);
    bool is_number_res_type = false;
    int64_t utz_value = 0;
    if (date_obj.is_string_type()) {
      scale = DEFAULT_SCALE_FOR_INTEGER;
      ObTimeConvertCtx cvrt_ctx(NULL, false);
      if (OB_FAIL(ObTimeConverter::str_to_datetime(
                  date_obj.get_string(), cvrt_ctx, utz_value, &scale, 0))) {
        LOG_WARN("failed to cast str to datetime", K(ret));
        ret = OB_SUCCESS;
        is_number_res_type = true;
        scale = MAX_SCALE_FOR_TEMPORAL;
      }
    } else {
      ObArenaAllocator oballocator(ObModIds::BLOCK_ALLOC);
      ObCastCtx cast_ctx(&oballocator,
                        NULL,
                        0,
                        CM_NONE,
                        CS_TYPE_INVALID,
                        NULL);
      EXPR_GET_DATETIME_V2(date_obj, utz_value);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to cast date to datetime", K(ret), K(date_obj));
        utz_value = 0;
        ret = OB_SUCCESS;
      }
    }
    if (is_number_res_type || utz_value % 1000000) {
      type = static_cast<uint8_t>(ObNumberType);
      if (!date_obj.is_string_type()) {
        scale = date_obj.get_scale();
      }
      precision = static_cast<int16_t>(TIMESTAMP_VALUE_LENGTH + easy_max(0, scale));

    } else {
      type = static_cast<uint8_t>(ObIntType);
      precision = static_cast<int16_t>(TIMESTAMP_VALUE_LENGTH);
      scale = DEFAULT_SCALE_FOR_INTEGER;
    }
  }
  return ret;
}

int ObPCUnixTimestampParamConstraint::match(const common::ParamStore &params, void *ctx, bool &is_match)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  uint8_t type = ObMaxType;
  int16_t precision = -1;
  int16_t scale = -1;
  is_match = false;
  if (OB_FAIL(deduce_result_type(params, type, precision, scale))) {
    LOG_WARN("failed to deduce result type for uinx_timestamp expr", K(ret), K(type), K(precision), K(scale));
  } else {
    is_match = type_ == type && precision_ == precision && scale == scale_;
  }
  LOG_TRACE("after match", K(ret), K(is_match), K(type), K(precision), K(scale), K(type_), K(precision_), K(scale_));
  return ret;
}
}
}
