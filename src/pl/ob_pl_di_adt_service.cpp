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

#define USING_LOG_PREFIX PL

#include "ob_pl_di_adt_service.h"
#include "ob_pl_type.h"
#include "ob_pl_stmt.h"
#include "ob_pl_exception_handling.h"
#include "ob_pl_user_type.h"
#include "ob_pl.h"

#define CREATE_MEMBER_TYPE(name, offset, line, type) \
  if (OB_FAIL(ret)) {                                                                   \
  } else if (OB_FAIL(helper_.create_member_type(name, offset, 0, type, member_type))) { \
    LOG_WARN("failed to create member type", K(ret));                                   \
  } else if (OB_FAIL(member_types.push_back(member_type))) {                            \
    LOG_WARN("failed to push back element", K(ret));                                    \
  } else {                                                                              \
  }

namespace oceanbase
{
using namespace common;
using namespace jit;
namespace pl
{

int ObPLDIADTService::init(const char *func_name)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(helper_.create_compile_unit(func_name))) {
    LOG_WARN("failed to create di compile unit", K(ret), K(func_name));
  } else if (OB_FAIL(helper_.create_file())) {
    LOG_WARN("failed to create di file", K(ret), K(func_name));
  }
  return ret;
}

int ObPLDIADTService::get_obj_type(ObObjType obj_type, ObLLVMDIType &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(obj_types_[obj_type].get_v())) {
    if (OB_FAIL(helper_.create_basic_type(obj_type, obj_types_[obj_type]))) {
      LOG_WARN("failed to create di basic type", K(obj_type), K(ret));
    } else if (OB_ISNULL(obj_types_[obj_type].get_v())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create di obj type", K(obj_type), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    type = obj_types_[obj_type];
  }
  return ret;
}

int ObPLDIADTService::get_obj_meta(ObLLVMDIType &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(obj_meta_.get_v())) {
    ObLLVMDIType uint8_type;
    ObLLVMDIType int8_type;
    ObLLVMDIType member_type;
    ObSEArray<ObLLVMDIType, 8> member_types;
    // TODO: need a file and line ?
    if (OB_FAIL(get_obj_type(ObUTinyIntType, uint8_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(get_obj_type(ObTinyIntType, int8_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("type_", ObObjMeta::type_offset_bits(),
                                                  0, uint8_type, member_type))) {
      LOG_WARN("failed to create di member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("cs_level_", ObObjMeta::cs_level_offset_bits(),
                                                  0, uint8_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("cs_type_", ObObjMeta::cs_type_offset_bits(),
                                                  0, uint8_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("scale_", ObObjMeta::scale_offset_bits(),
                                                  0, int8_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_struct_type("obj_meta", 0/*line*/,
                                                  sizeof(ObObjMeta) * 8, 32,
                                                  member_types, obj_meta_))) {
      LOG_WARN("failed to create di struct type", K(ret));
    } else if (OB_ISNULL(obj_meta_.get_v())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create di obj meta", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    type = obj_meta_;
  }
  return ret;
}

int ObPLDIADTService::get_obj_datum(jit::ObLLVMDIType &type)
{
  int ret = OB_SUCCESS;
  if (NULL == obj_datum_.get_v()) {
    ObLLVMDIType int32_type;
    ObLLVMDIType int64_type;
    ObLLVMDIType member_type;
    ObSEArray<ObLLVMDIType, 8> member_types;
    // TODO: need a file and line ?
    if (OB_FAIL(get_obj_type(ObInt32Type, int32_type))) {
      LOG_WARN("failed to get obj type", K(ret));
    } else if (OB_FAIL(get_obj_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get obj type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("len_nmb_", 0, 0, int32_type, member_type))) {
      LOG_WARN("failed to create di member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("v_", 4 * 8, 0, int64_type, member_type))) {
      LOG_WARN("failed to create di member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_struct_type("obj_datum", 0/*line*/,
                                                  (sizeof(ObObj) - sizeof(ObObjMeta)) * 8, 32,
                                                  member_types, obj_datum_))) {
      LOG_WARN("failed to create struct type", K(ret));
    } else if (OB_ISNULL(obj_datum_.get_v())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create di obj datum", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    type = obj_datum_;
  }
  return ret;
}

int ObPLDIADTService::get_obj(ObLLVMDIType &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(obj_.get_v())) {
    ObLLVMDIType objmeta_type;
    ObLLVMDIType int32_type;
    ObLLVMDIType int64_type;
    ObLLVMDIType member_type;
    ObSEArray<ObLLVMDIType, 8> member_types;
    // TODO: need a file and line ?
    if (OB_FAIL(get_obj_meta(objmeta_type))) {
      LOG_WARN("failed to get di obj meta", K(ret));
    } else if (OB_FAIL(get_obj_type(ObInt32Type, int32_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(get_obj_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("meta_", ObObj::meta_offset_bits(),
                                                  0, objmeta_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("val_len_", ObObj::val_len_offset_bits(),
                                                  0, int32_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
//    } else if (OB_FAIL(helper_.create_di_member_type("nmb_desc_",
//                                                     ObObj::offset_nmb_desc(),
//                                                     int32_type, member_type))) {
//      LOG_WARN("failed to create member type", K(ret));
//    } else if (OB_FAIL(member_types.push_back(member_type))) {
//      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("v_", ObObj::v_offset_bits(),
                                                  0, int64_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_struct_type("obj", 0/*line*/,
                                                  sizeof(ObObj) * 8, 32,
                                                  member_types, obj_))) {
      LOG_WARN("failed to create di struct type", K(ret));
    } else if (OB_ISNULL(obj_.get_v())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create di obj", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    type = obj_;
  }
  return ret;
}

int ObPLDIADTService::get_param_flag(jit::ObLLVMDIType &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_flag_.get_v())) {
    ObLLVMDIType int64_type;
    ObLLVMDIType uint8_type;
    ObLLVMDIType member_type;
    ObSEArray<ObLLVMDIType, 8> member_types;
    if (OB_FAIL(get_obj_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(get_obj_type(ObUTinyIntType, uint8_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("vtable_", 0, // offset of vtable is always 0.
                                                  0, int64_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("flag_", ParamFlag::flag_offset_bits(),
                                                  0, uint8_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_struct_type("ParamFlag", 0/*line*/,
                                                  sizeof(ParamFlag) * 8, 32,
                                                  member_types, param_flag_))) {
      LOG_WARN("failed to create di struct type", K(ret));
    } else if (OB_ISNULL(param_flag_.get_v())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create di param flag", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    type = param_flag_;
  }
  return ret;
}

int ObPLDIADTService::get_objparam(ObLLVMDIType &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(obj_param_.get_v())) {
    ObLLVMDIType obj_type;
    ObLLVMDIType param_flag_type;
    ObLLVMDIType int64_type;
    ObLLVMDIType uint32_type;
    ObLLVMDIType member_type;
    ObSEArray<ObLLVMDIType, 8> member_types;
    // TODO: need a file and line ?
    if (OB_FAIL(get_obj(obj_type))) {
      LOG_WARN("failed to get di obj", K(ret));
    } else if (OB_FAIL(get_param_flag(param_flag_type))) {
      LOG_WARN("failed to get di param flag", K(ret));
    } else if (OB_FAIL(get_obj_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(get_obj_type(ObUInt32Type, uint32_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("obj", 0/*offset*/,
                                                  0, obj_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("accuracy_", ObObjParam::accuracy_offset_bits(),
                                                  0, int64_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("res_flags_", ObObjParam::res_flags_offset_bits(),
                                                  0, uint32_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("flag_", ObObjParam::flag_offset_bits(),
                                                  0, param_flag_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_struct_type("objparam", 0/*line*/,
                                                  sizeof(ObObjParam) * 8, 32,
                                                  member_types, obj_param_))) {
      LOG_WARN("failed to create di struct type", K(ret));
    } else if (OB_ISNULL(obj_param_.get_v())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create di obj param", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    type = obj_param_;
  }
  return ret;
}

int ObPLDIADTService::get_param_store(ObLLVMDIType &type)
{
#define ARRAY_TY Ob2DArray<ObObjParam, OB_MALLOC_BIG_BLOCK_SIZE, ObWrapperAllocator, false>
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_store_.get_v())) {
    ObLLVMDIType int64_type;
    ObLLVMDIType int32_type;
    ObLLVMDIType int8_type;
    ObLLVMDIType block_alloc;
    ObLLVMDIType blocks;
    ObLLVMDIType member_type;
    ObSEArray<ObLLVMDIType, 8> member_types;
    // TODO: need a file and line ?
    if (OB_FAIL(get_obj_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(get_obj_type(ObInt32Type, int32_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(get_obj_type(ObTinyIntType, int8_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(get_wrapper_allocator(block_alloc))) {
      LOG_WARN("failed to get block_alloc type", K(ret));
    } else if (OB_FAIL(get_seg_pointer_array(blocks))) {
      LOG_WARN("failed to get blocks type", K(ret));
    } else {
      // do nothing
    }
    CREATE_MEMBER_TYPE("vtable_", 0, 0, int64_type);
    CREATE_MEMBER_TYPE("magic_", ARRAY_TY::magic_offset_bits(), 0, int32_type);
    CREATE_MEMBER_TYPE("block_alloc_", ARRAY_TY::block_alloc_offset_bits(), 0, block_alloc);
    CREATE_MEMBER_TYPE("blocks_", ARRAY_TY::blocks_offset_bits(), 0, blocks);
    CREATE_MEMBER_TYPE("count_", ARRAY_TY::count_offset_bits(), 0, int64_type);
    CREATE_MEMBER_TYPE("capacity_", ARRAY_TY::capacity_offset_bits(), 0, int64_type);
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(helper_.create_struct_type("param_store", 0/*line*/,
                                                  sizeof(ARRAY_TY) * 8, 32,
                                                  member_types, param_store_))) {
      LOG_WARN("failed to create di struct type", K(ret));
    } else if (OB_ISNULL(param_store_.get_v())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create di param store", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    type = param_store_;
  }
#undef ARRAY_TY
  return ret;
}

int ObPLDIADTService::get_objparam_array(jit::ObLLVMDIType &type)
{
  return get_param_store(type);
}

int ObPLDIADTService::get_exec_ctx(ObLLVMDIType &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pl_exec_ctx_.get_v())) {
    ObLLVMDIType int64_type;
    ObLLVMDIType param_store_type;
    ObLLVMDIType param_store_ptr_type;
    ObLLVMDIType obj_type;
    ObLLVMDIType obj_ptr_type;
    ObLLVMDIType member_type;
    ObSEArray<ObLLVMDIType, 8> member_types;
    // TODO: need a file and line ?
    if (OB_FAIL(get_obj_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(get_param_store(param_store_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(helper_.create_pointer_type(param_store_type, param_store_ptr_type))) {
      LOG_WARN("failed to create di pointer type", K(ret));
    } else if (OB_FAIL(get_obj(obj_type))) {
      LOG_WARN("failed to get di obj", K(ret));
    } else if (OB_FAIL(helper_.create_pointer_type(obj_type, obj_ptr_type))) {
      LOG_WARN("failed to create di pointer type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("allocator_", ObPLExecCtx::allocator_offset_bits(),
                                                  0, int64_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("exec_ctx_", ObPLExecCtx::exec_ctx_offset_bits(),
                                                  0, int64_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("params_", ObPLExecCtx::params_offset_bits(),
                                                  0, param_store_ptr_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("result_", ObPLExecCtx::result_offset_bits(),
                                                  0, obj_ptr_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_struct_type("pl_exec_context", 0/*line*/,
                                                  sizeof(ObPLExecCtx) * 8, 32,
                                                  member_types, pl_exec_ctx_))) {
      LOG_WARN("failed to create di struct type", K(ret));
    } else if (OB_ISNULL(pl_exec_ctx_.get_v())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create di pl exec ctx", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    type = pl_exec_ctx_;
  }
  return ret;
}

int ObPLDIADTService::get_argv(uint64_t argc, ObLLVMDIType &type)
{
  int ret = OB_SUCCESS;
  ObLLVMDIType di_argv;
  ObLLVMDIType int64_type;
  // TODO: need a file and line ?
  if (OB_FAIL(get_obj_type(ObIntType, int64_type))) {
    LOG_WARN("failed to get di objparam", K(ret));
  } else if (OB_FAIL(helper_.create_array_type(int64_type, argc, di_argv))) {
    LOG_WARN("failed to create di array type", K(ret));
  } else if (OB_ISNULL(di_argv.get_v())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create di argv", K(ret));
  }
  if (OB_SUCC(ret)) {
    type = di_argv;
  }
  return ret;
}

int ObPLDIADTService::get_pl_cond_value(ObLLVMDIType &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pl_cond_value_.get_v())) {
    ObLLVMDIType int64_type;
    ObLLVMDIType int8_type;
    ObLLVMDIType char_type;
    ObLLVMDIType member_type;
    ObSEArray<ObLLVMDIType, 8> member_types;
    // TODO: need a file and line ?
    if (OB_FAIL(get_obj_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(get_obj_type(ObTinyIntType, int8_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(get_obj_type(ObCharType, char_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("type_", ObPLConditionValue::type_offset_bits(),
                                                  0, int64_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("error_code_", ObPLConditionValue::error_code_offset_bits(),
                                                  0, int64_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("sql_state_", ObPLConditionValue::sql_state_offset_bits(),
                                                  0, char_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("str_len_", ObPLConditionValue::str_len_offset_bits(),
                                                  0, int64_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("stmt_id_", ObPLConditionValue::stmt_id_offset_bits(),
                                                  0, int64_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("signal_", ObPLConditionValue::signal_offset_bits(),
                                                  0, int8_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_struct_type("pl_condition_value", 0/*line*/,
                                                  sizeof(ObPLConditionValue) * 8, 32,
                                                  member_types, pl_cond_value_))) {
      LOG_WARN("failed to create di struct type", K(ret));
    } else if (OB_ISNULL(pl_cond_value_.get_v())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create di pl cond value", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    type = pl_cond_value_;
  }
  return ret;
}

int ObPLDIADTService::get_unwind_exception(jit::ObLLVMDIType &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(unwind_exception_.get_v())) {
    ObLLVMDIType uint64_type;
    ObLLVMDIType member_type;
    ObSEArray<ObLLVMDIType, 8> member_types;
    // TODO: need a file and line ?
    if (OB_FAIL(get_obj_type(ObUInt64Type, uint64_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("exception_class", 0,
                                                  0, uint64_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_struct_type("unwind_exception", 0/*line*/,
                                                  64, 32,
                                                  member_types, unwind_exception_))) {
      LOG_WARN("failed to create di struct type", K(ret));
    } else if (OB_ISNULL(unwind_exception_.get_v())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create di unwind exception", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    type = unwind_exception_;
  }
  return ret;
}

int ObPLDIADTService::get_pl_exception(jit::ObLLVMDIType &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pl_exception_.get_v())) {
    ObLLVMDIType pl_cond_value_type;
    ObLLVMDIType unwind_exception_type;
    ObLLVMDIType member_type;
    ObSEArray<ObLLVMDIType, 8> member_types;
    // TODO: need a file and line ?
    if (OB_FAIL(get_pl_cond_value(pl_cond_value_type))) {
      LOG_WARN("failed to get di pl cond value", K(ret));
    } else if (OB_FAIL(get_unwind_exception(unwind_exception_type))) {
      LOG_WARN("failed to get di unwind exception", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("type_", ObPLException::type_offset_bits(),
                                                  0, pl_cond_value_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("body_", ObPLException::body_offset_bits(),
                                                  0, unwind_exception_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_struct_type("pl_exception", 0/*line*/,
                                                  sizeof(ObPLException) * 8, 32,
                                                  member_types, pl_exception_))) {
      LOG_WARN("failed to create di struct type", K(ret));
    } else if (OB_ISNULL(pl_exception_.get_v())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create di pl exception", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    type = pl_exception_;
  }
  return ret;
}

//int ObPLDIADTService::get_di_landingpad(jit::ObLLVMDIType &type)
//{
//  int ret = OB_SUCCESS;
//  if (OB_ISNULL(di_landingpad_.get_v())) {
//    ObDIScope *scope = di_file_.get_v();
//    ObLLVMDIType pl_cond_value_type;
//    ObLLVMDIType unwind_exception_type;
//    ObLLVMDIType member_type;
//    ObSEArray<ObLLVMDIType, 8> member_types;
//    // TODO: need a file and line ?
//    if (OB_ISNULL(scope)) {
//      ret = OB_NOT_INIT;
//      LOG_WARN("compile unit is NULL", K(ret));
//    } else if (OB_FAIL(get_di_pl_cond_value(pl_cond_value_type))) {
//      LOG_WARN("failed to get di pl cond value", K(ret));
//    } else if (OB_FAIL(get_di_unwind_exception(unwind_exception_type))) {
//      LOG_WARN("failed to get di unwind exception", K(ret));
//    } else if (OB_FAIL(helper_.create_di_member_type("type_",
//                                                     ObPLException::type_offset_bits(),
//                                                     pl_cond_value_type, member_type))) {
//      LOG_WARN("failed to create member type", K(ret));
//    } else if (OB_FAIL(member_types.push_back(member_type))) {
//      LOG_WARN("failed to push back member type", K(ret));
//    } else if (OB_FAIL(helper_.create_di_member_type("body_",
//                                                     ObPLException::body_offset_bits(),
//                                                     unwind_exception_type, member_type))) {
//      LOG_WARN("failed to create member type", K(ret));
//    } else if (OB_FAIL(member_types.push_back(member_type))) {
//      LOG_WARN("failed to push back member type", K(ret));
//    } else if (OB_FAIL(helper_.create_di_struct_type("pl_exception", 0/*line*/,
//                                                     sizeof(ObPLException) * 8, 32,
//                                                     0, member_types, di_landingpad_))) {
//      LOG_WARN("failed to create di struct type", K(ret));
//    } else if (OB_ISNULL(di_landingpad_.get_v())) {
//      ret = OB_ERR_UNEXPECTED;
//      LOG_WARN("failed to create di pl exception", K(ret));
//    }
//  }
//  if (OB_SUCC(ret)) {
//    type = di_landingpad_;
//  }
//  return ret;
//}

int ObPLDIADTService::get_obstring(jit::ObLLVMDIType &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(obstring_.get_v())) {
    ObLLVMDIType int32_type;
    ObLLVMDIType char_type;
    ObLLVMDIType member_type;
    ObSEArray<ObLLVMDIType, 8> member_types;
    // TODO: need a file and line ?
    if (OB_FAIL(get_obj_type(ObInt32Type, int32_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(get_obj_type(ObCharType, char_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("buffer_size_", ObString::buffer_size_offset_bits(),
                                                  0, int32_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("data_length_", ObString::data_length_offset_bits(),
                                                  0, int32_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_member_type("ptr_", ObString::ptr_offset_bits(),
                                                  0, char_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else if (OB_FAIL(helper_.create_struct_type("obstring", 0/*line*/,
                                                  sizeof(ObString) * 8, 32,
                                                  member_types, obstring_))) {
      LOG_WARN("failed to create di struct type", K(ret));
    } else if (OB_ISNULL(obstring_.get_v())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create di obstring", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    type = obstring_;
  }
  return ret;
}

int ObPLDIADTService::get_table_type(const ObString &type_name, uint32_t line,
                                     ObLLVMDIType &element_type, ObLLVMDIType &table_type)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(type_name, line, element_type, table_type);
#else
  ObLLVMDIType element_array_type;
  ObLLVMDIType element_array_ptr_type;
  ObLLVMDIType member_type;
  ObSEArray<ObLLVMDIType, 8> member_types;
  if (OB_FAIL(get_collection(line, member_types))) {
    LOG_WARN("failed to get collation", K(ret));
  } else if (OB_FAIL(helper_.create_array_type(element_type, 0, element_array_type))) {
    LOG_WARN("failed to create array type", K(ret));
  } else if (OB_FAIL(helper_.create_pointer_type(element_array_type, element_array_ptr_type))) {
    LOG_WARN("failed to create pointer type", K(ret));
  } else if (OB_FAIL(helper_.create_member_type("data_", ObPLNestedTable::data_offset_bits(),
                                                line, element_array_ptr_type, member_type))) {
    LOG_WARN("failed to create member type", K(ret));
  } else if (OB_FAIL(member_types.push_back(member_type))) {
    LOG_WARN("failed to push back member type", K(ret));
  } else if (OB_FAIL(helper_.create_struct_type(type_name, line,
                                                sizeof(ObPLNestedTable) * 8, 32,
                                                member_types, table_type))) {
    LOG_WARN("failed to create di struct type", K(ret));
  } else if (OB_ISNULL(table_type.get_v())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create di table type", K(ret));
  }
#endif
  return ret;
}

int ObPLDIADTService::get_record_type(const ObString &type_name, uint32_t line,
                                      ObIArray<ObString> &cell_names,
                                      ObIArray<ObLLVMDIType> &cell_types,
                                      ObLLVMDIType &record_type)
{
  int ret = OB_SUCCESS;
  ObLLVMDIType member_type;
  ObSEArray<ObLLVMDIType, 8> member_types;
  uint64_t offset_bits = 0;
  if (cell_names.count() != cell_types.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cell names count and types array count are not equal",
             K(cell_names.count()), K(cell_types.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cell_names.count(); i++) {
    ObString &cell_name = cell_names.at(i);
    ObLLVMDIType &cell_type = cell_types.at(i);
    if (OB_FAIL(helper_.create_member_type(cell_name, offset_bits, line, cell_type, member_type))) {
      LOG_WARN("failed to create member type", K(ret));
    } else if (OB_FAIL(member_types.push_back(member_type))) {
      LOG_WARN("failed to push back member type", K(ret));
    } else {
      offset_bits += cell_type.get_size_bits();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(helper_.create_struct_type(type_name, line, offset_bits, 32, member_types, record_type))) {
      LOG_WARN("failed to create di struct type", K(ret));
    } else if (OB_ISNULL(record_type.get_v())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create di record type", K(ret));
    }
  }
  return ret;
}


int ObPLDIADTService::get_di_type(ObObjType obj_type, ObLLVMDIType &type)
{
  int ret = OB_SUCCESS;
  if (ob_is_string_tc(obj_type)
      || ob_is_number_tc(obj_type)
      || ob_is_text_tc(obj_type)
      || ob_is_otimestampe_tc(obj_type)
      || ob_is_raw_tc(obj_type)) {
    if (OB_FAIL(get_obj_datum(type))) {
      LOG_WARN("failed to get obj datum", K(obj_type), K(ret));
    }
  } else {
    if (OB_FAIL(get_obj_type(obj_type, type))) {
      LOG_WARN("failed to get obj type", K(obj_type), K(ret));
    }
  }
  return ret;
}

int ObPLDIADTService::get_datum_type(ObObjType obj_type, ObLLVMDIType &type)
{
  int ret = OB_SUCCESS;
  if (ob_is_string_tc(obj_type)
      || ob_is_number_tc(obj_type)
      || ob_is_text_tc(obj_type)
      || ob_is_otimestampe_tc(obj_type)
      || ob_is_raw_tc(obj_type)) {
    if (OB_FAIL(get_obj_datum(type))) {
      LOG_WARN("failed to get obj datum", K(obj_type), K(ret));
    }
  } else {
    if (OB_FAIL(get_obj_type(ObIntType, type))) {
      LOG_WARN("failed to get obj type", K(obj_type), K(ret));
    }
  }
  return ret;
}

int ObPLDIADTService::get_collection(uint32_t line, ObIArray<ObLLVMDIType> &member_types)
{
  int ret = OB_SUCCESS;
  ObLLVMDIType int64_type;
  ObLLVMDIType member_type;
  if (OB_FAIL(get_obj_type(ObIntType, int64_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(helper_.create_member_type("allocator_", ObPLCollection::allocator_offset_bits(),
                                                line, int64_type, member_type))) {
    LOG_WARN("failed to create member type", K(ret));
  } else if (OB_FAIL(member_types.push_back(member_type))) {
    LOG_WARN("failed to push back member type", K(ret));
  } else if (OB_FAIL(helper_.create_member_type("count_", ObPLCollection::count_offset_bits(),
                                                line, int64_type, member_type))) {
    LOG_WARN("failed to create member type", K(ret));
  } else if (OB_FAIL(member_types.push_back(member_type))) {
    LOG_WARN("failed to push back member type", K(ret));
  } else if (OB_FAIL(helper_.create_member_type("first_", ObPLCollection::first_offset_bits(),
                                                line, int64_type, member_type))) {
    LOG_WARN("failed to create member type", K(ret));
  } else if (OB_FAIL(helper_.create_member_type("last_", ObPLCollection::last_offset_bits(),
                                                  line, int64_type, member_type))) {
    LOG_WARN("failed to create member type", K(ret));
  } else if (OB_FAIL(member_types.push_back(member_type))) {
    LOG_WARN("failed to push back member type", K(ret));
  }
  return ret;
}

int ObPLDIADTService::get_wrapper_allocator(jit::ObLLVMDIType &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(wrapper_allocator_.get_v())) {
    ObLLVMDIType int64_type;
    ObLLVMDIType member_type;
    ObSEArray<ObLLVMDIType, 4> member_types;
    if (OB_FAIL(get_obj_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    }
    CREATE_MEMBER_TYPE("vtabel_", 0, 0, int64_type);
    CREATE_MEMBER_TYPE("allocator_",
                       ObWrapperAllocator::alloc_offset_bits(),
                       0,
                       int64_type);
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(helper_.create_struct_type("WrapperAllocator", 0,
                                                  sizeof(common::ObWrapperAllocator) * 8, 32,
                                                  member_types, wrapper_allocator_))) {
      LOG_WARN("failed to create struct type", K(ret));
    } else if (OB_ISNULL(wrapper_allocator_.get_v())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create di wrapper allocator", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    type = wrapper_allocator_;
  }
  return ret;
}

int ObPLDIADTService::get_seg_pointer_array(jit::ObLLVMDIType &type)
{
#define ARRAY_TY ObSEArray<char *,OB_BLOCK_POINTER_ARRAY_SIZE, ObWrapperAllocator, false>
  int ret = OB_SUCCESS;
  if (OB_ISNULL(seg_pointer_array_.get_v())) {
    ObLLVMDIType int64_type;
    ObLLVMDIType member_type;
    ObLLVMDIType wrapper_allocator;
    ObLLVMDIType block_buf_type;
    ObLLVMDIType block_ptr_type;
    ObLLVMDIType int32_type;
    ObLLVMDIType int8_type;
    ObSEArray<ObLLVMDIType, 16> member_types;
    if (OB_FAIL(get_obj_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(get_obj_type(ObInt32Type, int32_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(get_obj_type(ObTinyIntType, int8_type))) {
      LOG_WARN("failed to get di obj type", K(ret));
    } else if (OB_FAIL(helper_.create_array_type(int64_type,
                                                 common::OB_BLOCK_POINTER_ARRAY_SIZE,
                                                 block_buf_type))) {
      LOG_WARN("failed to create array type", K(ret));
    } else if (OB_FAIL(get_wrapper_allocator(wrapper_allocator))) {
      LOG_WARN("failed to get wrapper allocator type", K(ret));
    } else if (OB_FAIL(helper_.create_pointer_type(block_buf_type, block_ptr_type))) {
      LOG_WARN("failed to create pointer type", K(ret));
    }
    CREATE_MEMBER_TYPE("vtable_", 0, 0, int64_type);
    CREATE_MEMBER_TYPE("data_", ARRAY_TY::local_data_offset_bits(), 0, block_ptr_type);
    CREATE_MEMBER_TYPE("count_", ARRAY_TY::count_offset_bits(), 0, int64_type);
    CREATE_MEMBER_TYPE("local_data_buf_", ARRAY_TY::local_data_buf_offset_bits(),
                       0, block_buf_type);
    CREATE_MEMBER_TYPE("block_size_", ARRAY_TY::block_size_offset_bits(), 0, int64_type);
    CREATE_MEMBER_TYPE("capacity_", ARRAY_TY::capacity_offset_bits(), 0, int64_type);
    CREATE_MEMBER_TYPE("max_print_count_", ARRAY_TY::max_print_count_offset_bits(),
                       0, int64_type);
    CREATE_MEMBER_TYPE("error_", ARRAY_TY::error_offset_bits(), 0, int32_type);
    CREATE_MEMBER_TYPE("has_alloc_", ARRAY_TY::has_alloc_offset_bits(), 0, int8_type);
    CREATE_MEMBER_TYPE("mem_context_", ARRAY_TY::mem_context_offset_bits(), 0, int64_type);
    CREATE_MEMBER_TYPE("block_allocator_", ARRAY_TY::allocator_offset_bits(),
                       0, wrapper_allocator);
    CREATE_MEMBER_TYPE("has_alloc_", ARRAY_TY::has_alloc_offset_bits(), 0, int8_type);
    CREATE_MEMBER_TYPE("mem_context_", ARRAY_TY::mem_context_offset_bits(), 0, int64_type);

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(helper_.create_struct_type("SegPointerArray", 0,
                                                  sizeof(ARRAY_TY) * 8,
                                                  32, member_types, seg_pointer_array_))) {
      LOG_WARN("failed to create struct type", K(ret));
    } else if (OB_ISNULL(seg_pointer_array_.get_v())) {
      LOG_WARN("failed to create di seg pointer array", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    type = seg_pointer_array_;
  }
#undef ARRAY_TY
  return ret;
}

}
}

#undef CREATE_MEMBER_TYPE
