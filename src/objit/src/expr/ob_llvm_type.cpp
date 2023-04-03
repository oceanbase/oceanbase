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

#define USING_LOG_PREFIX JIT

#include "core/jit_context.h"
#include "lib/oblog/ob_log.h"
#include "ob_llvm_type.h"
namespace oceanbase {
namespace jit {

using namespace ::oceanbase::common;

void ObIRObj::reset() {
  type_ = nullptr;
  cs_level_ = nullptr;
  cs_type_ = nullptr;
  scale_ = nullptr;
  val_len_ = nullptr;
  v_ = nullptr;
}

//const value set
int ObIRObj::set_obj(core::JitContext &jc, const ObObj &obj) {
  int ret = OB_SUCCESS;
  reset();
  switch (obj.get_type()) {
    case ObTinyIntType:
    case ObIntType: {
      v_ = get_const(jc.get_context(), 64, obj.get_int());
    } break;
    case ObVarcharType: {
      //v_ = get_const(ctx, 64, obj.get_int());
      v_ = jc.get_builder().CreateGlobalStringPtr(::llvm::StringRef(obj.get_string().ptr(),
                                               obj.get_string().length()));
      val_len_ = get_const(jc.get_context(), 32, obj.get_val_len());
    } break;
    default: {
      ret = OB_NOT_SUPPORTED;
    }
  }

  if (OB_SUCCESS == ret) {
    set_type(jc.get_context(), obj.get_type());
    // set cs_level and cs_type
    switch (obj.get_type()) {
    case ObTinyIntType:
    case ObIntType: {
      set_cs_level(jc.get_context(), CS_LEVEL_NUMERIC);
      set_cs_type(jc.get_context(), CS_TYPE_BINARY);
      break;
    }
    case ObVarcharType: {
      // do nothing
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
    }
    }
  } else {
    reset();
  }

  return ret;
}

int ObIRObj::set_obj(core::JitContext &jc, const ObIRValuePtr obj, ObObjType type)
{
  int ret = OB_SUCCESS;
  reset();
  switch (type) {
    case ObTinyIntType:
    case ObIntType: {
      //do nothing
    } break;
    case ObVarcharType: {
      ObIRValuePtr len_indices[] = {get_const(jc.get_context(), 32, 0),
                                    get_const(jc.get_context(), 32, OBJ_IDX_VAL_LEN)};
      ObIRValuePtr str_len_p = jc.get_builder().CreateGEP(obj, makeArrayRef(len_indices));
      val_len_ = jc.get_builder().CreateLoad(str_len_p);
      //ptr
    } break;
    default: {
      ret = OB_NOT_SUPPORTED;
    }
  }

  if (common::OB_SUCCESS == ret) {
    v_ = get_ir_value_element(jc, obj, OBJ_IDX_VAL);
    type_ = get_ir_value_element(jc, obj, OBJ_IDX_TYPE);
    cs_level_ = get_ir_value_element(jc, obj, OBJ_IDX_CS_LEVEL);
    cs_type_ = get_ir_value_element(jc, obj, OBJ_IDX_CS_TYPE);
    if (NULL == v_ || NULL == type_ || NULL == cs_level_ || NULL == cs_type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get value from ir obj failed", K(ret));
    }
  } else {
    reset();
  }

  return ret;
}

ObIRValuePtr ObIRObj::get_ir_value_element(core::JitContext &jc,
                                           const ObIRValuePtr obj,
                                           int64_t idx)
{
  ObIRValuePtr ret = NULL;
  ObIRValuePtr indices[] = {get_const(jc.get_context(), 32, 0),
                            get_const(jc.get_context(), 32, idx)};
  ObIRValuePtr value_p = jc.get_builder().CreateGEP(obj,
                                               makeArrayRef(indices),
                                               "obj_elem_p");
  ret = jc.get_builder().CreateLoad(value_p, "value_elem");

  return ret;
}

ObIRValuePtr ObIRObj::is_null(core::JitContext &jc)
{
  ObIRValuePtr ret = NULL;
  ret = jc.get_builder().CreateICmpEQ(type_,
                                 get_const(jc.get_context(), 8, ObNullType),
                                 "is_null_int8");
  ret = jc.get_builder().CreateZExt(ret, ObIRType::getInt64Ty(jc.get_context()), "is_null_int64");

  return ret;
}

ObIRValuePtr ObIRObj::is_not_null(core::JitContext &jc)
{
  ObIRValuePtr ret = NULL;
  ret = jc.get_builder().CreateICmpNE(type_,
                                 get_const(jc.get_context(), 8, ObNullType),
                                 "is_null_int8");
  ret = jc.get_builder().CreateZExt(ret, ObIRType::getInt64Ty(jc.get_context()), "is_null_int64");

  return ret;
}

} //jit end
} //oceanbase end
