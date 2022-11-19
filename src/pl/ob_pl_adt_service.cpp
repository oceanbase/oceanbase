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

#include "ob_pl_adt_service.h"
#include "common/ob_field.h"
#include "ob_pl_type.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "lib/container/ob_2d_array.h"
#include "lib/utility/ob_template_utils.h"

namespace oceanbase
{
using namespace common;
using namespace jit;
namespace pl
{

int ObPLADTService::init(const int64_t arg_count, const int64_t param_count)
{
  int ret = OB_SUCCESS;
  arg_count_ = arg_count;
  param_count_ = param_count;
  return ret;
}

int ObPLADTService::get_objmeta(ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  if (NULL == obj_meta_.get_v()) {
    ObLLVMType struct_type;
    ObSEArray<ObLLVMType, 4> obj_meta;
    ObLLVMType int8_type;
    if (OB_FAIL(helper_.get_llvm_type(ObTinyIntType, int8_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else {
      /*
        uint8_t type_;
        uint8_t cs_level_;    // collation level
        uint8_t cs_type_;     // collation type
        int8_t scale_;        // scale, 当type_ 为ObBitType时，该字段存储bit的length
       */
      if (OB_FAIL(obj_meta.push_back(int8_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(obj_meta.push_back(int8_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(obj_meta.push_back(int8_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(obj_meta.push_back(int8_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(helper_.create_struct_type(ObString("obj_meta"), obj_meta, struct_type))) {
        LOG_WARN("failed to create struct type", K(ret));
      } else {
        obj_meta_.set_v(struct_type.get_v());
      }
    }
  }
  if (OB_SUCC(ret)) {
    type = obj_meta_;
  }
  return ret;
}

int ObPLADTService::get_datum(ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  if (NULL == datum_.get_v()) {
    ObLLVMType struct_type;
    ObSEArray<ObLLVMType, 2> datum;
    ObLLVMType int32_type;
    ObLLVMType int64_type;
    if (OB_FAIL(helper_.get_llvm_type(ObInt32Type, int32_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else {
      /*
        union
        {
        int32_t val_len_;
        number::ObNumber::Desc nmb_desc_;
        };  // sizeof = 4
        ObObjValue v_;  // sizeof = 8
       */
      if (OB_FAIL(datum.push_back(int32_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(datum.push_back(int64_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(helper_.create_struct_type(ObString("datum"), datum, struct_type))) {
        LOG_WARN("failed to create struct type", K(ret));
      } else {
        datum_.set_v(struct_type.get_v());
      }
    }
  }
  if (OB_SUCC(ret)) {
    type = datum_;
  }
  return ret;
}

int ObPLADTService::get_obj(ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  if (NULL == obj_.get_v()) {
    ObLLVMType struct_type;
    ObSEArray<ObLLVMType, 3> obj;
    ObLLVMType objmeta;
    ObLLVMType int32_type;
    ObLLVMType int64_type;
    if (OB_FAIL(helper_.get_llvm_type(ObInt32Type, int32_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(get_objmeta(objmeta))) {
      LOG_WARN("push_back error", K(ret));
    } else {
      /*
        ObObjMeta meta_;  // sizeof = 4
        union
        {
        int32_t val_len_;
        number::ObNumber::Desc nmb_desc_;
        };  // sizeof = 4
        ObObjValue v_;  // sizeof = 8
      */
      if (OB_FAIL(obj.push_back(objmeta))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(obj.push_back(int32_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(obj.push_back(int64_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(helper_.create_struct_type(ObString("obj"), obj, struct_type))) {
        LOG_WARN("failed to create struct type", K(ret));
      } else {
        obj_.set_v(struct_type.get_v());
      }
    }
  }
  if (OB_SUCC(ret)) {
    type = obj_;
  }
  return ret;
}

int ObPLADTService::get_objparam(ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  if (NULL == obj_param_.get_v()) {
    ObLLVMType struct_type;
    ObSEArray<ObLLVMType, 4> obj_param;
    ObLLVMType obj_type;
    ObLLVMType obj_meta_type;
    ObLLVMType int8_type;
    ObLLVMType int32_type;
    ObLLVMType int64_type;
    if (OB_FAIL(helper_.get_llvm_type(ObTinyIntType, int8_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObInt32Type, int32_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(get_obj(obj_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(get_objmeta(obj_meta_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else {
      ObLLVMType param_flag_type;
      ObSEArray<ObLLVMType, 2> param_flag;
      /*
        union
        {
          uint8_t flag_;
          struct {
            uint8_t need_to_check_type_: 1; //TRUE if the type need to be checked by plan cache, FALSE otherwise
            uint8_t need_to_check_bool_value_ : 1;//TRUE if the bool value need to be checked by plan cache, FALSE otherwise
            uint8_t expected_bool_value_ : 1;//bool value, effective only when need_to_check_bool_value_ is true
            uint8_t reserved_ : 5;
          };
        };
      */
      if (OB_FAIL(param_flag.push_back(int64_type))) { //VTable
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(param_flag.push_back(int8_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(helper_.create_struct_type(ObString("obj"), param_flag, param_flag_type))) {
        LOG_WARN("failed to create struct type", K(ret));
      } else { /*do nothing*/ }

      if (OB_SUCC(ret)) {
        /*
          ObAccuracy accuracy_;
          uint32_t res_flags_;  // BINARY, NUM, NOT_NULL, TIMESTAMP, etc
          // reference: src/lib/regex/include/mysql_com.h
          ParamFlag flag_;
         */
        if (OB_FAIL(obj_param.push_back(obj_type))) {
          LOG_WARN("push_back error", K(ret));
        } else if (OB_FAIL(obj_param.push_back(int64_type))) { //ObAccuracy
          LOG_WARN("push_back error", K(ret));
        } else if (OB_FAIL(obj_param.push_back(int32_type))) {
          LOG_WARN("push_back error", K(ret));
        } else if (OB_FAIL(obj_param.push_back(param_flag_type))) {
          LOG_WARN("push_back error", K(ret));
        } else if (OB_FAIL(obj_param.push_back(int32_type))) {  // raw_text_pos_
          LOG_WARN("push_back error", K(ret));
        } else if (OB_FAIL(obj_param.push_back(int32_type))) {  // raw_text_len_
          LOG_WARN("push_back error", K(ret));
        } else if (OB_FAIL(obj_param.push_back(obj_meta_type))) { //param_meta_
          LOG_WARN("push_back error", K(ret));
        } else if (OB_FAIL(helper_.create_struct_type(ObString("objparam"), obj_param, struct_type))) {
          LOG_WARN("failed to create struct type", K(ret));
        } else {
          obj_param_.set_v(struct_type.get_v());
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    type = obj_param_;
  }
  return ret;
}

int ObPLADTService::get_param_store(ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  if (NULL == seg_param_store_.get_v()) {
    ObLLVMType struct_type;
    ObLLVMType block_alloc;
    ObLLVMType blocks;
    ObLLVMType int8_type;
    ObLLVMType int32_type;
    ObLLVMType int64_type;
    if (OB_FAIL(helper_.get_llvm_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObInt32Type, int32_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObTinyIntType, int8_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(get_seg_pointer_array(blocks))) {
      LOG_WARN("failed to get_seg_pointer_array", K(ret));
    } else if (OB_FAIL(get_wrapper_allocator(block_alloc))) {
      LOG_WARN("failed to get_wrapper_allocator", K(ret));
    } else  {
      /*
        int32_t magic_;
        ObWrapperAllocator block_alloc_
        ObSEArray<char *, 64, ObWrapperAllocator, false> blocks_;
        int64_t count_;
        int64_t capacity_;
      */
      ObSEArray<ObLLVMType, 32> seg_param_store;
      if (OB_FAIL(seg_param_store.push_back(int64_type))) { // vtable
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(seg_param_store.push_back(int32_type))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(seg_param_store.push_back(block_alloc))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(seg_param_store.push_back(blocks))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(seg_param_store.push_back(int64_type))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(seg_param_store.push_back(int64_type))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(helper_.create_struct_type(ObString("seg_param_store"),
                                                    seg_param_store, struct_type))) {
        LOG_WARN("failed to create struct type", K(ret));
      } else {
        seg_param_store_.set_v(struct_type.get_v());
      }
    }
  }
  if (OB_SUCC(ret)) {
    type = seg_param_store_;
  }
  return ret;
}

int ObPLADTService::get_data_type(ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  if (NULL == data_type_.get_v()) {
    ObLLVMType struct_type;
    ObSEArray<ObLLVMType, 4> data_type;
    ObLLVMType obj_meta_type;
    ObLLVMType int8_type;
    ObLLVMType int32_type;
    ObLLVMType int64_type;
    if (OB_FAIL(helper_.get_llvm_type(ObTinyIntType, int8_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObInt32Type, int32_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(get_objmeta(obj_meta_type))) {
      LOG_WARN("push_back error", K(ret));
    } else {
      /*
        ObObjMeta meta_;
        ObAccuracy accuracy_;
        ObCharsetType charset_;
        bool is_binary_collation_
        bool is_zero_fill_;
       */
      if (OB_FAIL(data_type.push_back(obj_meta_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(data_type.push_back(int64_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(data_type.push_back(int32_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(data_type.push_back(int8_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(data_type.push_back(int8_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(helper_.create_struct_type(ObString("data_type"),
                                                    data_type,
                                                    struct_type))) {
        LOG_WARN("failed to create struct type", K(ret));
      } else {
        data_type_.set_v(struct_type.get_v());
      }
    }
  }
  if (OB_SUCC(ret)) {
    type = data_type_;
  }
  return ret;
}

int ObPLADTService::get_elem_desc(ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  if (NULL == elem_desc_.get_v()) {
    ObLLVMType struct_type;
    ObSEArray<ObLLVMType, 4> elem_desc;
    ObLLVMType obj_meta_type;
    ObLLVMType int8_type;
    ObLLVMType int32_type;
    ObLLVMType int64_type;
    if (OB_FAIL(helper_.get_llvm_type(ObTinyIntType, int8_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObInt32Type, int32_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(get_objmeta(obj_meta_type))) {
      LOG_WARN("push_back error", K(ret));
    } else {
      /*
        ObObjMeta meta_;
        ObAccuracy accuracy_;
        ObCharsetType charset_;
        bool is_binary_collation_
        bool is_zero_fill_;
        ObPLType type_;
        bool not_null_;
        int32_t field_cnt_;
       */
      if (OB_FAIL(elem_desc.push_back(obj_meta_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(elem_desc.push_back(int64_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(elem_desc.push_back(int32_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(elem_desc.push_back(int8_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(elem_desc.push_back(int8_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(elem_desc.push_back(int32_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(elem_desc.push_back(int8_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(elem_desc.push_back(int32_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(helper_.create_struct_type(ObString("elem_desc"),
                                                    elem_desc,
                                                    struct_type))) {
        LOG_WARN("failed to create struct type", K(ret));
      } else {
        elem_desc_.set_v(struct_type.get_v());
      }
    }
  }
  if (OB_SUCC(ret)) {
    type = elem_desc_;
  }
  return ret;
}

int ObPLADTService::get_pl_exec_context(ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  if (NULL == pl_exec_context_.get_v()) {
    ObLLVMType struct_type;
    ObSEArray<ObLLVMType, 5> pl_exec_context;
    ObLLVMType seg_param_store;
    ObLLVMType seg_param_store_pointer;
    ObLLVMType obj;
    ObLLVMType obj_pointer;
    ObLLVMType int8_type;
    ObLLVMType int32_type;
    ObLLVMType int32_pointer_type;
    ObLLVMType int64_type;
    if (OB_FAIL(helper_.get_llvm_type(ObTinyIntType, int8_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObInt32Type, int32_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(int32_type.get_pointer_to(int32_pointer_type))) {
      LOG_WARN("failed to get_pointer_to", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(get_param_store(seg_param_store))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(seg_param_store.get_pointer_to(seg_param_store_pointer))) {
      LOG_WARN("failed to get pointer to", K(ret));
    } else if (OB_FAIL(get_obj(obj))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(obj.get_pointer_to(obj_pointer))) {
      LOG_WARN("failed to get pointer to", K(ret));
    } else {
      /*
          common::ObIAllocator *allocator_;
          sql::ObExecContext *exec_ctx_;
          common::Ob2DArray<common::ObObjParam, ObWrapperAllocator, false> *params_;
          common::ObObj *result_;
          int *status_;
          ObPLException *pre_reserved_e_;
          ObPLFunction *func_;
          bool in_function_;
       */
      if (OB_FAIL(pl_exec_context.push_back(int64_type))) { //VTable
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(pl_exec_context.push_back(int64_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(pl_exec_context.push_back(int64_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(pl_exec_context.push_back(seg_param_store_pointer))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(pl_exec_context.push_back(obj_pointer))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(pl_exec_context.push_back(int32_pointer_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(pl_exec_context.push_back(int64_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(pl_exec_context.push_back(int8_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(pl_exec_context.push_back(int64_type))) { // plcontext
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(helper_.create_struct_type(ObString("pl_exec_context"), pl_exec_context, struct_type))) {
        LOG_WARN("failed to create struct type", K(ret));
      } else {
        pl_exec_context_.set_v(struct_type.get_v());
      }
    }
  }
  if (OB_SUCC(ret)) {
    type = pl_exec_context_;
  }
  return ret;
}


int ObPLADTService::get_argv(ObLLVMType &type)
{
  return get_argv(arg_count_, type);
}

int ObPLADTService::get_argv(int64_t argc, ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  ObLLVMType array_type;
  ObLLVMType int64_type;
  if (OB_FAIL(helper_.get_llvm_type(ObIntType, int64_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(ObLLVMArrayType::get(int64_type, argc, array_type))) {
    LOG_WARN("failed to get array type", K(ret));
  } else {
    type.set_v(array_type.get_v());
  }
  return ret;
}

int ObPLADTService::get_pl_condition_value(ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  if (NULL == pl_condition_value_.get_v()) {
    ObLLVMType struct_type;
    ObSEArray<ObLLVMType, 6> pl_condition_value;
    ObLLVMType int8_type;
    ObLLVMType int64_type;
    ObLLVMType char_type;
    if (OB_FAIL(helper_.get_llvm_type(ObTinyIntType, int8_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObCharType, char_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else {
      /*
        ObPLConditionType type_;
        int64_t error_code_;
        const char *sql_state_;
        int64_t str_len_;
        int64_t stmt_id_;
        bool signal_;
       */
      if (OB_FAIL(pl_condition_value.push_back(int64_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(pl_condition_value.push_back(int64_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(pl_condition_value.push_back(char_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(pl_condition_value.push_back(int64_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(pl_condition_value.push_back(int64_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(pl_condition_value.push_back(int8_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(helper_.create_struct_type(ObString("pl_condition_value"), pl_condition_value, struct_type))) {
        LOG_WARN("failed to create struct type", K(ret));
      } else {
        pl_condition_value_.set_v(struct_type.get_v());
      }
    }
  }
  if (OB_SUCC(ret)) {
    type = pl_condition_value_;
  }
  return ret;
}

int ObPLADTService::get_unwind_exception(ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  if (NULL == unwind_exception_.get_v()) {
    ObLLVMType struct_type;
    ObSEArray<ObLLVMType, 1> pl_unwind_exception;
    /*
      _Unwind_Exception_Class exception_class;
      _Unwind_Exception_Cleanup_Fn exception_cleanup;
      _Unwind_Word private_1;
      _Unwind_Word private_2;
     */
    //只映射第一项exception_class
    ObLLVMType int64_type;
    if (OB_FAIL(helper_.get_llvm_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(pl_unwind_exception.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(helper_.create_struct_type(ObString("unwind_exception"), pl_unwind_exception, struct_type))) {
      LOG_WARN("failed to create struct type", K(ret));
    } else {
      unwind_exception_.set_v(struct_type.get_v());
    }
  }
  if (OB_SUCC(ret)) {
    type = unwind_exception_;
  }
  return ret;
}

int ObPLADTService::get_pl_exception(ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  if (NULL == pl_exception_.get_v()) {
    ObLLVMType struct_type;
    ObSEArray<ObLLVMType, 2> pl_exception;
    ObLLVMType condition_value;
    ObLLVMType unwind_exception;
    if (OB_FAIL(get_pl_condition_value(condition_value))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(get_unwind_exception(unwind_exception))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(pl_exception.push_back(condition_value))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(pl_exception.push_back(unwind_exception))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(helper_.create_struct_type(ObString("pl_exception"), pl_exception, struct_type))) {
      LOG_WARN("failed to create struct type", K(ret));
    } else {
      pl_exception_.set_v(struct_type.get_v());
    }
  }
  if (OB_SUCC(ret)) {
    type = pl_exception_;
  }
  return ret;
}

int ObPLADTService::get_landingpad_result(ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  if (NULL == landingpad_result_.get_v()) {
    ObLLVMType struct_type;
    ObSEArray<ObLLVMType, 2> landingpad_result;
    ObLLVMType tinyint_pointer;
    ObLLVMType int8_type;
    ObLLVMType int32_type;
    if (OB_FAIL(helper_.get_llvm_type(ObTinyIntType, int8_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObInt32Type, int32_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(int8_type.get_pointer_to(tinyint_pointer))) {
      LOG_WARN("failed to get_pointer_to", K(ret));
    } else if (OB_FAIL(landingpad_result.push_back(tinyint_pointer))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(landingpad_result.push_back(int32_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(helper_.create_struct_type(ObString("landingpad_result"), landingpad_result, struct_type))) {
      LOG_WARN("failed to create struct type", K(ret));
    } else {
      landingpad_result_.set_v(struct_type.get_v());
    }
  }
  if (OB_SUCC(ret)) {
    type = landingpad_result_;
  }
  return ret;
}

int ObPLADTService::get_obstring(ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  if (NULL == obstring_.get_v()) {
    /**
      obstr_size_t buffer_size_;
      obstr_size_t data_length_;
      char *ptr_;
    */
    ObLLVMType struct_type;
    ObSEArray<ObLLVMType, 3> obstring;
    ObLLVMType int32_type;
    ObLLVMType char_type;
    if (OB_FAIL(helper_.get_llvm_type(ObIntType, int32_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObCharType, char_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(obstring.push_back(int32_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(obstring.push_back(int32_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(obstring.push_back(char_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(helper_.create_struct_type(ObString("obstring"), obstring, struct_type))) {
      LOG_WARN("failed to create struct type", K(ret));
    } else {
      obstring_.set_v(struct_type.get_v());
    }
  }
  if (OB_SUCC(ret)) {
    type = obstring_;
  }
  return ret;
}

int ObPLADTService::get_seg_pointer_array(jit::ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  if (NULL == seg_pointer_array_.get_v()) {
    ObLLVMType struct_type;
    ObLLVMType int32_type;
    ObLLVMType int64_type;
    ObLLVMType local_data_buf;
    ObLLVMType pointer_carray_pointer;
    ObLLVMType wrapper_allocator;
    ObLLVMType memory_context;
    ObLLVMType int8_type;
    if (OB_FAIL(helper_.get_llvm_type(ObInt32Type, int32_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObTinyIntType, int8_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(get_wrapper_allocator(wrapper_allocator))) {
      LOG_WARN("failed to get wrapper_allocator", K(ret));
    } else if (OB_FAIL(get_memory_context(memory_context))) {
      LOG_WARN("failed to get memory_context", K(ret));
    } else if (OB_FAIL(ObLLVMHelper::get_array_type(int64_type,
                                                    common::OB_BLOCK_POINTER_ARRAY_SIZE,
                                                    local_data_buf))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(local_data_buf.get_pointer_to(pointer_carray_pointer))) {
      LOG_WARN("failed to get_pointer_to", K(ret));
    } else {
      /*
        T *data_;
        int64_t count_;
        char local_data_buf_[LOCAL_ARRAY_SIZE * sizeof(T)];
        int64_t block_size_; // 申请内存的块单位
        int64_t capacity_; // 标记ObSEArray的内部可用空间
        int64_t max_print_count_; //表示最大需要打印的元素数量；
        int32_t error_;  //
        ObWrapperAllocator block_allocator_;
        bool has_alloc_;
        lib::MemoryContext mem_context_;
      */
      ObSEArray<ObLLVMType, 16> seg_pointer_array;
      if (OB_FAIL(seg_pointer_array.push_back(int64_type))) { // vtable
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(seg_pointer_array.push_back(pointer_carray_pointer))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(seg_pointer_array.push_back(int64_type))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(seg_pointer_array.push_back(local_data_buf))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(seg_pointer_array.push_back(int64_type))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(seg_pointer_array.push_back(int64_type))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(seg_pointer_array.push_back(int64_type))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(seg_pointer_array.push_back(int32_type))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(seg_pointer_array.push_back(wrapper_allocator))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(seg_pointer_array.push_back(int8_type))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(seg_pointer_array.push_back(memory_context))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(helper_.create_struct_type(ObString("seg_pointer_array"),
                                                    seg_pointer_array, struct_type))) {
        LOG_WARN("failed to create struct type", K(ret));
      } else {
        seg_pointer_array_.set_v(struct_type.get_v());
      }
    }
  }
  if (OB_SUCC(ret)) {
    type = seg_pointer_array_;
  }
  return ret;
}

int ObPLADTService::get_wrapper_allocator(ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  if (NULL == wrapper_allocator_.get_v()) {
    ObLLVMType struct_type;
    ObLLVMType int64_type;
    if (OB_FAIL(helper_.get_llvm_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else {
      /*
        ObIAllocator *alloc_;
       */
      ObSEArray<ObLLVMType, 2> wrapper_allocator;
      if (OB_FAIL(wrapper_allocator.push_back(int64_type))) { // vtable
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(wrapper_allocator.push_back(int64_type))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(helper_.create_struct_type(ObString("wrapper_allocator"),
                                                    wrapper_allocator, struct_type))) {
        LOG_WARN("failed to create struct type", K(ret));
      } else {
        wrapper_allocator_.set_v(struct_type.get_v());
      }
    }
  }
  if (OB_SUCC(ret)) {
    type = wrapper_allocator_;
  }
  return ret;
}

int ObPLADTService::get_memory_context(ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  if (NULL == memory_context_.get_v()) {
    ObLLVMType struct_type;
    ObLLVMType int64_type;
    if (OB_FAIL(helper_.get_llvm_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else {
      /*
        int64_t magic_code_;
        int64_t seq_id_;
        __MemoryContext__ *ref_context_;
      */
      STATIC_ASSERT(sizeof(lib::MemoryContext) == sizeof(int64_t) * 3, "check MemoryContext layout failed");
      ObSEArray<ObLLVMType, 3> memory_context;
      if (OB_FAIL(memory_context.push_back(int64_type))) { // magic_code_
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(memory_context.push_back(int64_type))) { // seq_id_
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(memory_context.push_back(int64_type))) { // ref_context_
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(helper_.create_struct_type(ObString("memory_context"),
                                                    memory_context, struct_type))) {
        LOG_WARN("failed to create struct type", K(ret));
      } else {
        memory_context_.set_v(struct_type.get_v());
      }
    }
  }
  if (OB_SUCC(ret)) {
    type = memory_context_;
  }
  return ret;
}

}
}
