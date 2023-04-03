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

#ifndef OCEANBASE_SRC_PL_OB_PL_ADT_SERVICE_H_
#define OCEANBASE_SRC_PL_OB_PL_ADT_SERVICE_H_

#include "objit/ob_llvm_helper.h"

namespace oceanbase
{
namespace pl
{

class ObPLADTService
{
public:
  ObPLADTService(jit::ObLLVMHelper &helper) :
    helper_(helper),
    obj_meta_(),
    datum_(),
    obj_(),
    obj_param_(),
    param_store_(),
    seg_param_store_(),
    data_type_(),
    elem_desc_(),
    pl_exec_context_(),
    pl_condition_value_(),
    unwind_exception_(),
    pl_exception_(),
    landingpad_result_(),
    obstring_(),
    seg_pointer_array_(),
    arg_count_(0),
    param_count_(0) {}
  virtual ~ObPLADTService() {}

  int init(const int64_t arg_count, const int64_t param_count);

  int get_objmeta(jit::ObLLVMType &type);
  int get_datum(jit::ObLLVMType &type);
  int get_obj(jit::ObLLVMType &type);
  int get_objparam(jit::ObLLVMType &type);
  int get_param_store(jit::ObLLVMType &type);
  int get_data_type(jit::ObLLVMType &type);
  int get_elem_desc(jit::ObLLVMType &type);
  int get_pl_exec_context(jit::ObLLVMType &type);
  int get_argv(jit::ObLLVMType &type);
  int get_argv(int64_t argc, jit::ObLLVMType &type);
  int get_pl_condition_value(jit::ObLLVMType &type);
  int get_unwind_exception(jit::ObLLVMType &type);
  int get_pl_exception(jit::ObLLVMType &type);
  int get_landingpad_result(jit::ObLLVMType &type);
  int get_obstring(jit::ObLLVMType &type);
  int get_seg_pointer_array(jit::ObLLVMType &type);
  int get_wrapper_allocator(jit::ObLLVMType &type);
  int get_memory_context(jit::ObLLVMType &type);

private:
  jit::ObLLVMHelper &helper_;
  jit::ObLLVMType obj_meta_;
  jit::ObLLVMType datum_;
  jit::ObLLVMType obj_;
  jit::ObLLVMType obj_param_;
  jit::ObLLVMType param_store_;
  jit::ObLLVMType seg_param_store_;
  jit::ObLLVMType data_type_;
  jit::ObLLVMType elem_desc_;
  jit::ObLLVMType pl_exec_context_;
  jit::ObLLVMType pl_condition_value_;
  jit::ObLLVMType unwind_exception_;
  jit::ObLLVMType pl_exception_;
  jit::ObLLVMType landingpad_result_;
  jit::ObLLVMType obstring_;
  jit::ObLLVMType seg_pointer_array_;
  jit::ObLLVMType wrapper_allocator_;
  jit::ObLLVMType memory_context_;

private:
  int64_t arg_count_;
  int64_t param_count_;
};

}
}


#endif /* OCEANBASE_SRC_PL_OB_PL_ADT_SERVICE_H_ */
