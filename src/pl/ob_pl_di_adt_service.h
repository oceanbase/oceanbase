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

#ifndef OCEANBASE_SRC_PL_OB_PL_DI_ADT_SERVICE_H_
#define OCEANBASE_SRC_PL_OB_PL_DI_ADT_SERVICE_H_

#include "objit/ob_llvm_helper.h"
#include "objit/ob_llvm_di_helper.h"

namespace oceanbase
{
namespace pl
{

class ObPLDIADTService
{
public:
  ObPLDIADTService(jit::ObLLVMDIHelper &helper)
  : helper_(helper),
    obj_types_(),
    obj_meta_(),
    obj_datum_(),
    obj_(),
    param_flag_(),
    obj_param_(),
    param_store_(),
    pl_exec_ctx_(),
    pl_cond_value_(),
    unwind_exception_(),
    pl_exception_(),
    landingpad_(),
    obstring_(),
    seg_pointer_array_(),
    wrapper_allocator_()
  {
    for (int i = 0; i < common::ObMaxType; i++) {
      obj_types_[i].set_v(NULL);
    }
  }
  virtual ~ObPLDIADTService() {}

public:
  int init(const char *func_name);
  int get_obj_type(common::ObObjType obj_type, jit::ObLLVMDIType &type);
  int get_obj_meta(jit::ObLLVMDIType &type);
  int get_obj_datum(jit::ObLLVMDIType &type);
  int get_obj(jit::ObLLVMDIType &type);
  int get_param_flag(jit::ObLLVMDIType &type);
  int get_objparam(jit::ObLLVMDIType &type);
  int get_param_store(jit::ObLLVMDIType &type);
  int get_objparam_array(jit::ObLLVMDIType &type);
  int get_exec_ctx(jit::ObLLVMDIType &type);
  int get_argv(uint64_t argc, jit::ObLLVMDIType &type);
  int get_pl_cond_value(jit::ObLLVMDIType &type);
  int get_unwind_exception(jit::ObLLVMDIType &type);
  int get_pl_exception(jit::ObLLVMDIType &type);
  int get_obstring(jit::ObLLVMDIType &type);
  int get_table_type(const common::ObString &type_name, uint32_t line,
                     jit::ObLLVMDIType &element_type, jit::ObLLVMDIType &table_type);
  int get_record_type(const common::ObString &type_name, uint32_t line,
                      common::ObIArray<common::ObString> &cell_names,
                      common::ObIArray<jit::ObLLVMDIType> &cell_types,
                      jit::ObLLVMDIType &record_type);
  // see get_ir_type() and get_datum_type() in ob_pl_type.h,
  // then U can understand these strange names.
  int get_di_type(common::ObObjType obj_type, jit::ObLLVMDIType &type);
  int get_datum_type(common::ObObjType obj_type, jit::ObLLVMDIType &type);
  int get_seg_pointer_array(jit::ObLLVMDIType &type);
  int get_wrapper_allocator(jit::ObLLVMDIType &type);
private:
  int get_collection(uint32_t line, common::ObIArray<jit::ObLLVMDIType> &base_types);

private:
  jit::ObLLVMDIHelper &helper_;
  jit::ObLLVMDIType obj_types_[common::ObMaxType];
  jit::ObLLVMDIType obj_meta_;
  jit::ObLLVMDIType obj_datum_;
  jit::ObLLVMDIType obj_;
  jit::ObLLVMDIType param_flag_;
  jit::ObLLVMDIType obj_param_;
  jit::ObLLVMDIType param_store_;
  jit::ObLLVMDIType pl_exec_ctx_;
  jit::ObLLVMDIType pl_cond_value_;
  jit::ObLLVMDIType unwind_exception_;
  jit::ObLLVMDIType pl_exception_;
  jit::ObLLVMDIType landingpad_;
  jit::ObLLVMDIType obstring_;
  jit::ObLLVMDIType seg_pointer_array_;
  jit::ObLLVMDIType wrapper_allocator_;
};

}
}

#endif /* OCEANBASE_SRC_PL_OB_PL_DI_ADT_SERVICE_H_ */
