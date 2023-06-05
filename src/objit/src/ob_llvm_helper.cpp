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

#include "core/jit_context.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Verifier.h"
#include "llvm/IR/GlobalVariable.h"
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/DynamicLibrary.h"

#include "share/ob_define.h"
#include "objit/ob_llvm_helper.h"
#include "objit/ob_llvm_di_helper.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/container/ob_se_array.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/alloc/malloc_hook.h"

using namespace llvm;

typedef llvm::Value ObIRValue;
typedef llvm::Function ObIRFunction;
typedef llvm::Type ObIRType;
typedef llvm::StructType ObIRStructType;
typedef llvm::ArrayType ObIRArrayType;
typedef llvm::Constant ObIRConstant;
typedef llvm::ConstantDataArray ObIRConstantDataArray;
typedef llvm::ConstantInt ObIRConstantInt;
typedef llvm::ConstantStruct ObIRConstantStruct;
typedef llvm::GlobalVariable ObIRGlobalVariable;
typedef llvm::FunctionType ObIRFunctionType;
typedef llvm::ArrayRef<llvm::Type*> TypeArray;
typedef llvm::StringRef ObStringRef;
typedef llvm::LLVMContext ObIRContext;
typedef llvm::IRBuilder<> ObIRBuilder;
typedef llvm::Module ObIRModule;
typedef llvm::ExecutionEngine ObLLVMExecEngine;

#define OB_LLVM_MALLOC_GUARD(mod) lib::ObMallocHookAttrGuard malloc_guard(ObMemAttr(MTL_ID() == OB_INVALID_TENANT_ID ? OB_SYS_TENANT_ID : MTL_ID(), mod))

#if !defined(__aarch64__)
namespace llvm {
  struct X86MemoryFoldTableEntry;
  extern const X86MemoryFoldTableEntry* lookupUnfoldTable(unsigned MemOp);
}
#endif

namespace oceanbase
{
using namespace common;
namespace jit
{

typedef ObIRType* (*ObGetIRType)(ObIRContext&, ...);
typedef ObIRValue* (*ObGetIRConst)(ObIRContext&, const common::ObObj&);
static ObGetIRType OB_IR_TYPE[common::ObMaxType + 1] =
{
  NULL,                                                   //0.ObNullType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt8Ty),     //1.ObTinyIntType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt16Ty),    //2.ObSmallIntType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt32Ty),    //3.ObMediumIntType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt32Ty),    //4.ObInt32Type
  reinterpret_cast<ObGetIRType>(ObIRType::getInt64Ty),    //5.ObIntType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt8Ty),     //6.ObUTinyIntType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt16Ty),    //7.ObUSmallIntType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt32Ty),    //8.ObUMediumIntType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt32Ty),    //9.ObUInt32Type
  reinterpret_cast<ObGetIRType>(ObIRType::getInt64Ty),    //10.ObUInt64Type
  reinterpret_cast<ObGetIRType>(ObIRType::getFloatTy),    //11.ObFloatType
  reinterpret_cast<ObGetIRType>(ObIRType::getDoubleTy),   //12.ObDoubleType
  reinterpret_cast<ObGetIRType>(ObIRType::getFloatTy),    //13.ObUFloatType
  reinterpret_cast<ObGetIRType>(ObIRType::getDoubleTy),   //14.ObUDoubleType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt32PtrTy), //15.ObNumberType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt32PtrTy), //16.ObUNumberType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt64Ty),    //17.ObDateTimeType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt64Ty),    //18.ObTimestampType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt64Ty),    //19.ObDateType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt64Ty),    //20.ObTimeType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt64Ty),    //21.ObYearType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt8PtrTy),  //22.ObVarcharType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt8PtrTy),  //23.ObCharType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt8PtrTy),  //24.ObHexStringType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt64Ty),    //25.ObExtendType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt64Ty),    //26.ObUnknownType
  NULL,                                                   //27.ObTinyTextType
  NULL,                                                   //28.ObTextType
  NULL,                                                   //29.ObMediumTextType
  NULL,                                                   //30.ObLongTextType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt64Ty),    //31.ObBitType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt64Ty),    //32.ObEnumType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt64Ty),    //33.ObSetType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt8PtrTy),  //34.ObEnumInnerType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt8PtrTy),  //35.ObSetInnerType
  NULL,   //36.ObTimestampTZType
  NULL,  //37.ObTimestampLTZType
  NULL,  //38.ObTimestampNanoType
  NULL,  //39.ObRawType
  NULL,  //40.ObIntervalYMType
  NULL,  //41.ObIntervalDSType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt32PtrTy),  //42.ObNumberFloatType
  reinterpret_cast<ObGetIRType>(ObIRType::getInt8PtrTy),  //43.ObNVarchar2Type
  reinterpret_cast<ObGetIRType>(ObIRType::getInt8PtrTy),  //44.ObNVarchar2Type
  reinterpret_cast<ObGetIRType>(ObIRType::getInt8PtrTy),  // 45. ObURowIDType
  NULL,                                                    //46.ObLobType
  NULL,                                                    //47.ObJsonType
  NULL,                                                    //48.ObGeometryType
  NULL,                                                    //49.ObUserDefinedSQLType
  NULL,                                                    //50.ObMaxType
};

template<typename T, int64_t N>
llvm::ArrayRef<T> make_array_ref(const common::ObFastArray<T, N> &array)
{
  size_t array_cnt = static_cast<size_t>(array.count());
  return llvm::ArrayRef<T>(array.head(), array_cnt);
}

inline llvm::StringRef make_string_ref(const common::ObString &str)
{
  return llvm::StringRef(str.ptr(), str.length());
}

int ObLLVMType::get_pointer_to(ObLLVMType &result)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(get_v())) {
    ret = common::OB_NOT_INIT;
  } else {
    result.set_v(get_v()->getPointerTo());
  }
  return ret;
}

int ObLLVMType::same_as(ObLLVMType &other, bool &same)
{
  int ret = common::OB_SUCCESS;
  same = true;
  if (OB_ISNULL(get_v()) || OB_ISNULL(other.get_v())) {
    ret = common::OB_NOT_INIT;
  } else {
    if (get_id() != other.get_id()) {
      same = false;
      LOG_WARN("type not match", K(get_id()), K(other.get_id()), K(ret));
    } else if (get_width() != other.get_width()) {
      same = false;
      LOG_WARN("type not match", K(get_width()), K(other.get_width()), K(ret));
    } else if (get_num_child() != other.get_num_child()) {
      same = false;
      LOG_WARN("type not match", K(get_id()), K(get_num_child()), K(other.get_id()), K(other.get_num_child()), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && same && i < get_num_child(); ++i) {
        ObLLVMType type = other.get_child(i);
        if (OB_FAIL(get_child(i).same_as(type, same))) {
          LOG_WARN("failed to check same", K(i), K(ret));
        } else if (!same) {
          LOG_WARN("type not match", K(i), K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int64_t ObLLVMType::get_id() const
{
  if (OB_ISNULL(get_v())) {
    return OB_INVALID_ID;
  } else {
    return get_v()->getTypeID();
  }
}

int64_t ObLLVMType::get_width() const
{
  if (OB_ISNULL(get_v())) {
    return OB_INVALID_SIZE;
  } else if (get_v()->isIntegerTy()) {
    return get_v()->getIntegerBitWidth();
  } else {
    return OB_INVALID_SIZE;
  }
}

int64_t ObLLVMType::get_num_child() const
{
  if (OB_ISNULL(get_v())) {
    return OB_INVALID_COUNT;
  } else {
    return get_v()->getNumContainedTypes();
  }
}

ObLLVMType ObLLVMType::get_child(int64_t i) const
{
  if (OB_ISNULL(get_v())) {
    return ObLLVMType();
  } else {
    return ObLLVMType(get_v()->getContainedType(i));
  }
}

int ObLLVMValue::get_type(ObLLVMType &result) const
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(get_v())) {
    ret = common::OB_NOT_INIT;
  } else {
    result.set_v(get_v()->getType());
  }
  return ret;
}

ObLLVMType ObLLVMValue::get_type() const
{
  return ObLLVMType(NULL == get_v() ? ObLLVMType() : get_v()->getType());
}

int64_t ObLLVMValue::get_type_id() const
{
  if (OB_ISNULL(get_v()) || OB_ISNULL(get_v()->getType())) {
    return OB_INVALID_ID;
  } else {
    return get_v()->getType()->getTypeID();
  }
}

int ObLLVMValue::set_name(const common::ObString &name)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(get_v())) {
    ret = common::OB_NOT_INIT;
  } else {
    get_v()->setName(llvm::StringRef(name.ptr(), name.length()));
  }
  return ret;
}

int ObLLVMArrayType::get(const ObLLVMType &elem_type, uint64_t size, ObLLVMType &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(elem_type.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("element type is NULL", K(elem_type), K(size), K(ret));
  } else {
    llvm::ArrayType *type = llvm::ArrayType::get(const_cast<llvm::Type*>(elem_type.get_v()), size);
    if (OB_ISNULL(type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create struct type", K(ret));
    } else {
      result.set_v(type);
    }
  }
  return ret;
}

int ObLLVMConstant::get_null_value(const ObLLVMType &type, ObLLVMConstant &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(type.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("type is NULL", K(type), K(ret));
  } else {
    llvm::Constant *value = llvm::Constant::getNullValue(const_cast<llvm::Type*>(type.get_v()));
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create struct type", K(ret));
    } else {
      result.set_v(value);
    }
  }
  return ret;
}

int ObLLVMConstantStruct::get(ObLLVMStructType &type, common::ObIArray<ObLLVMConstant> &elem_values,  ObLLVMConstant &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(type.get_v()) || elem_values.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("element types is empty", K(type), K(elem_values), K(ret));
  } else {
    ObArenaAllocator allocator;
    ObFastArray<llvm::Constant*, 64> array(allocator);
    for (int64_t i = 0; OB_SUCC(ret) && i < elem_values.count(); ++i) {
      if (OB_FAIL(array.push_back(static_cast<llvm::Constant*>(elem_values.at(i).get_v())))) {
        LOG_WARN("push_back error", K(i), K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      llvm::Constant *value = llvm::ConstantStruct::get(static_cast<llvm::StructType*>(type.get_v()), make_array_ref(array));
      if (OB_ISNULL(value)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to create struct type", K(ret));
      } else {
        result.set_v(value);
      }
    }
  }
  return ret;
}

int ObLLVMGlobalVariable::set_constant()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_v())) {
    ret = OB_NOT_INIT;
    LOG_WARN("v_ is NULL", K(ret));
  } else {
    static_cast<llvm::GlobalVariable*>(get_v())->setConstant(true);
  }
  return ret;
}

int ObLLVMGlobalVariable::set_initializer(ObLLVMConstant &value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_v())) {
    ret = OB_NOT_INIT;
    LOG_WARN("v_ is NULL", K(ret));
  } else if (OB_ISNULL(value.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(value), K(ret));
  } else {
    static_cast<llvm::GlobalVariable*>(get_v())->setInitializer(static_cast<llvm::Constant*>(value.get_v()));
  }
  return ret;
}

int ObLLVMFunctionType::get(const ObLLVMType &ret_type, common::ObIArray<ObLLVMType> &arg_types, ObLLVMFunctionType &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ret_type.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("element types is empty", K(ret_type), K(ret));
  } else {
    ObArenaAllocator allocator;
    ObFastArray<llvm::Type*, 64> array(allocator);
    for (int64_t i = 0; OB_SUCC(ret) && i < arg_types.count(); ++i) {
      if (OB_FAIL(array.push_back(arg_types.at(i).get_v()))) {
        LOG_WARN("push_back error", K(i), K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      llvm::FunctionType *type = llvm::FunctionType::get(const_cast<llvm::Type*>(ret_type.get_v()), make_array_ref(array), false);
      if (OB_ISNULL(type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to create struct type", K(ret));
      } else {
        result.set_v(type);
      }
    }
  }
  return ret;
}

int ObLLVMFunction::set_personality(ObLLVMFunction &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_v())) {
    ret = OB_NOT_INIT;
    LOG_WARN("v_ is NULL", K(ret));
  } else if (OB_ISNULL(func.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(ret));
  } else {
    get_v()->setPersonalityFn(func.get_v());
  }
  return ret;
}

int ObLLVMFunction::get_argument_size(int64_t &size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_v())) {
    ret = OB_NOT_INIT;
    LOG_WARN("v_ is NULL", K(ret));
  } else {
    size = get_v()->arg_size();
  }
  return ret;
}

int ObLLVMFunction::get_argument(int64_t idx, ObLLVMValue &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_v())) {
    ret = OB_NOT_INIT;
    LOG_WARN("v_ is NULL", K(ret));
  } else {
    llvm::Function::arg_iterator ai = get_v()->arg_begin();
    for (int64_t i = 0; i < idx; ++i) {
      ++ai;
    }
    arg.set_v(&(*ai));
  }
  return ret;
}

int ObLLVMFunction::set_subprogram(ObLLVMDISubprogram *di_subprogram)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_v()) || OB_ISNULL(di_subprogram) || OB_ISNULL(di_subprogram->get_v())) {
    ret = OB_NOT_INIT;
    LOG_WARN("function or subprogram is NULL", K(ret));
  } else {
    get_v()->setSubprogram(di_subprogram->get_v());
  }
  return ret;
}

bool ObLLVMBasicBlock::is_terminated()
{
  return NULL == v_ ? false : NULL != v_->getTerminator();
}

int ObLLVMLandingPad::set_cleanup()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_v())) {
    ret = OB_NOT_INIT;
    LOG_WARN("v_ is NULL", K(ret));
  } else {
    static_cast<llvm::LandingPadInst*>(get_v())->setCleanup(true);
  }
  return ret;
}

int ObLLVMLandingPad::add_clause(ObLLVMConstant &clause)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_v())) {
    ret = OB_NOT_INIT;
    LOG_WARN("v_ is NULL", K(ret));
  } else if (OB_ISNULL(clause.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(ret));
  } else {
    static_cast<llvm::LandingPadInst*>(get_v())->addClause(static_cast<llvm::Constant*>(clause.get_v()));
  }
  return ret;
}


int ObLLVMSwitch::add_case(ObLLVMConstantInt &value, ObLLVMBasicBlock &block)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_v())) {
    ret = OB_NOT_INIT;
    LOG_WARN("v_ is NULL", K(ret));
  } else if (OB_ISNULL(value.get_v()) || OB_ISNULL(block.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(ret));
  } else {
    get_v()->addCase(static_cast<llvm::ConstantInt*>(value.get_v()), block.get_v());
  }
  return ret;
}

int ObLLVMSwitch::add_case(const ObLLVMValue &value, ObLLVMBasicBlock &block)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_v())) {
    ret = OB_NOT_INIT;
    LOG_WARN("v_ is NULL", K(ret));
  } else if (OB_ISNULL(value.get_v()) || OB_ISNULL(block.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(ret));
  } else {
    get_v()->addCase(static_cast<llvm::ConstantInt*>(const_cast<llvm::Value*>(value.get_v())), block.get_v());
  }
  return ret;
}

ObLLVMHelper::~ObLLVMHelper()
{
  OB_LLVM_MALLOC_GUARD("PlJit");
  final();
  if (nullptr != jit_) {
    jit_->~ObOrcJit();
    jit_ = NULL;
  }
}

int ObLLVMHelper::init()
{
  int ret = OB_SUCCESS;
  OB_LLVM_MALLOC_GUARD("PlJit");

  if (nullptr == (jc_ = OB_NEWx(core::JitContext, (&allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for jit context", K(ret));
#ifndef ORC2
  } else if (nullptr == (jit_ = OB_NEWx(core::ObOrcJit, (&allocator_), allocator_))) {
#else
  } else if (nullptr == (jit_ = core::ObOrcJit::create(allocator_))) {
#endif
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for jit", K(ret));
  } else {
    jc_->InitializeModule(*jit_);
  }

  return ret;
}

void ObLLVMHelper::final()
{
  OB_LLVM_MALLOC_GUARD("PlJit");
  if (nullptr != jc_) {
    jc_->~JitContext();
    allocator_.free(jc_);
    jc_ = nullptr;
  }
}

int ObLLVMHelper::initialize()
{
  int ret = OB_SUCCESS;

  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();

#if !defined(__aarch64__)
  // initialize LLVM X86 unfold table
  llvm::lookupUnfoldTable(0);
#endif

  OZ (init_llvm());

  return ret;
}

int ObLLVMHelper::init_llvm() {
  int ret = OB_SUCCESS;

  OB_LLVM_MALLOC_GUARD("PlJit");
  ObArenaAllocator alloc("PlJit", OB_MALLOC_NORMAL_BLOCK_SIZE, OB_SYS_TENANT_ID);
  ObLLVMHelper helper(alloc);
  ObLLVMDIHelper di_helper(alloc);
  static char init_func_name[] = "pl_init_func";

  OZ (helper.init());
  OZ (di_helper.init(helper.get_jc()));

  ObSEArray<ObLLVMType, 8> arg_types;
  ObLLVMType int64_type;
  ObLLVMFunction init_func;
  ObLLVMFunctionType ft;
  ObLLVMBasicBlock block;
  ObLLVMValue magic;

  OZ (helper.get_llvm_type(ObIntType, int64_type));
  OZ (arg_types.push_back(int64_type));
  OZ (ObLLVMFunctionType::get(int64_type, arg_types, ft));
  OZ (helper.create_function(init_func_name, ft, init_func));
  OZ (helper.create_block("entry", init_func, block));
  OZ (helper.set_insert_point(block));
  OZ (helper.get_int64(OB_SUCCESS, magic));
  OZ (helper.create_ret(magic));

  OX (helper.compile_module());
  OX (helper.get_function_address(init_func_name));

  return ret;
}

void ObLLVMHelper::compile_module(bool optimization)
{
  if (optimization) {
    OB_LLVM_MALLOC_GUARD("PlCodeGen");
    jc_->optimize();
    LOG_INFO("================Optimized LLVM Module================");
    dump_module();
  }
  OB_LLVM_MALLOC_GUARD("PlJit");
  jc_->compile();
}

void ObLLVMHelper::dump_module()
{
  OB_LLVM_MALLOC_GUARD("PlCodeGen");
  if (OB_ISNULL(jc_)) {
    //do nothing
  } else {
    std::string o;
    llvm::raw_string_ostream s(o);
    jc_->TheModule->print(s, nullptr);
    LOG_INFO("Dump LLVM Compile Module!\n", K(s.str().c_str()));
  }
}

void ObLLVMHelper::dump_debuginfo()
{
  OB_LLVM_MALLOC_GUARD("PlCodeGen");
  if (OB_ISNULL(jit_) || jit_->get_debug_info_size() <= 0) {
    // do nothing ...
  } else {
    ObDWARFHelper::dump(jit_->get_debug_info_data(), jit_->get_debug_info_size());
  }
}

int ObLLVMHelper::verify_function(ObLLVMFunction &function)
{
  OB_LLVM_MALLOC_GUARD("PlCodeGen");
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(function.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(ret));
  } else if (verifyFunction(*function.get_v())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to verify function", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLLVMHelper::verify_module()
{
  OB_LLVM_MALLOC_GUARD("PlCodeGen");
  int ret = OB_SUCCESS;
  std::string verify_error;
  llvm::raw_string_ostream verify_raw_os(verify_error);
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (verifyModule((jc_->get_module()), &verify_raw_os)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to verify function", K(ret), K(verify_error.c_str()));
  } else { /*do nothing*/ }
  return ret;
}

uint64_t ObLLVMHelper::get_function_address(const ObString &name)
{
  OB_LLVM_MALLOC_GUARD("PlJit");
  return jc_->TheJIT->get_function_address(std::string(name.ptr(), name.length()));
}

void ObLLVMHelper::add_symbol(const ObString &name, void *value)
{
  llvm::sys::DynamicLibrary::AddSymbol(make_string_ref(name), value);
}

ObDIRawData ObLLVMHelper::get_debug_info() const
{
  ObDIRawData result;
  if (jit_ != NULL) {
    result.set_data(jit_->get_debug_info_data());
    result.set_size(jit_->get_debug_info_size());
  }
  return result;
}

#define CHECK_INSERT_POINT \
  do { \
    bool is_valid = false; \
    if (OB_FAIL(check_insert_point(is_valid))) { \
      LOG_WARN("failed to check insert point", K(ret)); \
    } else if (!is_valid) { \
      ret = OB_ERR_UNEXPECTED; \
      LOG_WARN("insert point is invalid, maybe current block has been terminated", K(ret)); \
    } else { /*do nothing*/ } \
  } while (0)

int ObLLVMHelper::create_br(const ObLLVMBasicBlock &dest)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(dest.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(dest), K(ret));
  } else {
    CHECK_INSERT_POINT;
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(jc_->get_builder().CreateBr(const_cast<llvm::BasicBlock*>(dest.get_v())))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to create br", K(ret));
      }
    }
  }
  return ret;
}

int ObLLVMHelper::create_cond_br(ObLLVMValue &value, ObLLVMBasicBlock &true_dest, ObLLVMBasicBlock &false_dest)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(value.get_v()) || OB_ISNULL(true_dest.get_v()) || OB_ISNULL(false_dest.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(value), K(true_dest), K(false_dest), K(ret));
  } else if (OB_ISNULL(jc_->get_builder().CreateCondBr(value.get_v(), true_dest.get_v(), false_dest.get_v()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create condbr", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLLVMHelper::create_call(const ObString &name, ObLLVMFunction &callee, common::ObIArray<ObLLVMValue> &args, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(callee.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("function is NULL", K(name), K(callee), K(args), K(ret));
  } else if (OB_ISNULL(callee.get_v()->getReturnType())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("return type is NULL", K(ret));
  } else {
    int64_t func_arg_count = 0;
    if (OB_FAIL(callee.get_argument_size(func_arg_count))) {
      LOG_WARN("failed to get argument size", K(ret));
    } else if (func_arg_count != args.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("argument not match", K(name), K(func_arg_count), K(args.count()), K(ret));
    } else {
      ObLLVMValue arg;
      ObLLVMType type1;
      ObLLVMType type2;
      bool same = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < func_arg_count; ++i) {
        if (OB_FAIL(callee.get_argument(i, arg))) {
          LOG_WARN("failed to get argument", K(i), K(ret));
        } else if (OB_FAIL(arg.get_type(type1))) {
          LOG_WARN("failed to get type", K(i), K(ret));
        } else if (OB_FAIL(args.at(i).get_type(type2))) {
          LOG_WARN("failed to get type", K(i), K(ret));
        } else if (OB_FAIL(type1.same_as(type2, same))) {
          LOG_WARN("failed to checkout same", K(name), K(i), K(type1.get_id()), K(type2.get_id()), K(ret));
        } else if (!same) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("argument not match", K(name), K(i), K(type1.get_id()), K(type2.get_id()), K(ret));
        } else { /*do nothing*/ }
      }
    }

    if (OB_SUCC(ret)) {
      ObFastArray<llvm::Value*, 64> array(allocator_);
      for (int64_t i = 0; OB_SUCC(ret) && i < args.count(); ++i) {
        if (OB_FAIL(array.push_back(args.at(i).get_v()))) {
          LOG_WARN("push_back error", K(i), K(ret));
        } else { /*do nothing*/ }
      }
      if (OB_SUCC(ret)) {
        llvm::Value *value = NULL;
        if (name.empty() || callee.get_v()->getReturnType()->isVoidTy()) {
          value = jc_->get_builder().CreateCall(callee.get_v(), make_array_ref(array));
        } else {
          value = jc_->get_builder().CreateCall(callee.get_v(), make_array_ref(array), make_string_ref(name));
        }
        if (OB_ISNULL(value)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to create call", K(ret));
        } else {
          result.set_v(value);
        }
      }
    }
  }
  return ret;
}

int ObLLVMHelper::create_call(const ObString &name, ObLLVMFunction &callee, common::ObIArray<ObLLVMValue> &args)
{
  int ret = OB_SUCCESS;
  ObLLVMValue result;
  if (OB_FAIL(create_call(name, callee, args, result))) {
    LOG_WARN("failed to create call", K(ret));
  }
  return ret;
}

int ObLLVMHelper::create_call(const ObString &name, ObLLVMFunction &callee, const ObLLVMValue &arg, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLLVMValue, 1> args;
  if (OB_FAIL(args.push_back(arg))) {
    LOG_WARN("push_back error", K(ret));
  } else if (OB_FAIL(create_call(name, callee, args, result))) {
    LOG_WARN("failed to create call", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLLVMHelper::create_call(const ObString &name, ObLLVMFunction &callee, const ObLLVMValue &arg)
{
  int ret = OB_SUCCESS;
  ObLLVMValue result;
  if (OB_FAIL(create_call(name, callee, arg, result))) {
    LOG_WARN("failed to create call", K(ret));
  }
  return ret;
}

int ObLLVMHelper::create_invoke(const ObString &name, ObLLVMFunction &callee, ObIArray<ObLLVMValue> &args, const ObLLVMBasicBlock &normal_dest, const ObLLVMBasicBlock &unwind_dest, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(callee.get_v()) || OB_ISNULL(normal_dest.get_v()) || OB_ISNULL(unwind_dest.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(name), K(callee), K(args), K(ret));
  } else {
    CHECK_INSERT_POINT;
    if (OB_SUCC(ret)) {
      int64_t func_arg_count = 0;
      if (OB_FAIL(callee.get_argument_size(func_arg_count))) {
        LOG_WARN("failed to get argument size", K(ret));
      } else if (func_arg_count != args.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("argument not match", K(name), K(func_arg_count), K(args.count()), K(ret));
      } else {
        ObLLVMValue arg;
        ObLLVMType type1;
        ObLLVMType type2;
        bool same = false;
        for (int64_t i = 0; OB_SUCC(ret) && i < func_arg_count; ++i) {
          if (OB_FAIL(callee.get_argument(i, arg))) {
            LOG_WARN("failed to get argument", K(i), K(ret));
          } else if (OB_FAIL(arg.get_type(type1))) {
            LOG_WARN("failed to get type", K(i), K(ret));
          } else if (OB_FAIL(args.at(i).get_type(type2))) {
            LOG_WARN("failed to get type", K(i), K(ret));
          } else if (OB_FAIL(type1.same_as(type2, same))) {
            LOG_WARN("failed to checkout same", K(name), K(i), K(type1.get_id()), K(type2.get_id()), K(ret));
          } else if (!same) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("argument not match", K(name), K(i), K(type1.get_id()), K(type2.get_id()), K(ret));
          } else { /*do nothing*/ }
        }
      }

      if (OB_SUCC(ret)) {
        ObFastArray<llvm::Value*, 64> array(allocator_);
        for (int64_t i = 0; OB_SUCC(ret) && i < args.count(); ++i) {
          if (OB_FAIL(array.push_back(args.at(i).get_v()))) {
            LOG_WARN("push_back error", K(i), K(ret));
          } else { /*do nothing*/ }
        }
        if (OB_SUCC(ret)) {
          llvm::Value *value = NULL;
          if (name.empty() || callee.get_v()->getReturnType()->isVoidTy()) {
            value = jc_->get_builder().CreateInvoke(const_cast<llvm::Function*>(callee.get_v()), const_cast<llvm::BasicBlock*>(normal_dest.get_v()), const_cast<llvm::BasicBlock*>(unwind_dest.get_v()), make_array_ref(array));
          } else {
            value = jc_->get_builder().CreateInvoke(const_cast<llvm::Function*>(callee.get_v()), const_cast<llvm::BasicBlock*>(normal_dest.get_v()), const_cast<llvm::BasicBlock*>(unwind_dest.get_v()), make_array_ref(array), make_string_ref(name));
          }
          if (OB_ISNULL(value)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to create invoke", K(ret));
          } else {
            result.set_v(value);
          }
        }
      }
    }
  }
  return ret;
}

int ObLLVMHelper::create_invoke(const ObString &name, ObLLVMFunction &callee, ObLLVMValue &arg, const ObLLVMBasicBlock &normal_dest, const ObLLVMBasicBlock &unwind_dest, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLLVMValue, 1> args;
  if (OB_FAIL(args.push_back(arg))) {
    LOG_WARN("push_back error", K(ret));
  } else if (OB_FAIL(create_invoke(name, callee, args, normal_dest, unwind_dest, result))) {
    LOG_WARN("failed to create call", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLLVMHelper::create_invoke(const ObString &name, ObLLVMFunction &callee, common::ObIArray<ObLLVMValue> &args, const ObLLVMBasicBlock &normal_dest, const ObLLVMBasicBlock &unwind_dest)
{
  int ret = OB_SUCCESS;
  ObLLVMValue result;
  if (OB_FAIL(create_invoke(name, callee, args, normal_dest, unwind_dest, result))) {
    LOG_WARN("failed to create incoke", K(ret));
  }
  return ret;
}

int ObLLVMHelper::create_invoke(const ObString &name, ObLLVMFunction &callee, ObLLVMValue &arg, const ObLLVMBasicBlock &normal_dest, const ObLLVMBasicBlock &unwind_dest)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLLVMValue, 1> args;
  if (OB_FAIL(args.push_back(arg))) {
    LOG_WARN("push_back error", K(ret));
  } else if (OB_FAIL(create_invoke(name, callee, args, normal_dest, unwind_dest))) {
    LOG_WARN("failed to create call", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLLVMHelper::create_alloca(const ObString &name, const ObLLVMType &type, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(type.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(name), K(type), K(ret));
  } else {
    llvm::Value *value = jc_->get_builder().CreateAlloca(const_cast<llvm::Type*>(type.get_v()), NULL, make_string_ref(name));
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create alloca", K(ret));
    } else {
      result.set_v(value);
    }
  }
  return ret;
}

int ObLLVMHelper::create_store(const ObLLVMValue &src, ObLLVMValue &dest)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(src.get_v()) || OB_ISNULL(dest.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(src), K(dest), K(ret));
  } else {
    ObLLVMType type1;
    ObLLVMType type2;
    bool same = false;
    if (OB_FAIL(src.get_type(type1))) {
      LOG_WARN("failed to get type", K(ret));
    } else if (OB_FAIL(dest.get_type(type2))) {
      LOG_WARN("failed to get type", K(ret));
    } else if (OB_FAIL(type1.get_pointer_to(type1))) {
      LOG_WARN("failed to get pointer to", K(ret));
    } else if (OB_FAIL(type1.same_as(type2, same))) {
      LOG_WARN("failed to checkout same", K(type1.get_id()), K(type2.get_id()), K(ret));
    } else if (!same) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("argument not match", K(type1.get_id()), K(type2.get_id()), K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(jc_->get_builder().CreateStore(const_cast<llvm::Value*>(src.get_v()), dest.get_v()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create store", K(ret));
    }
  }
  return ret;
}

int ObLLVMHelper::create_load(const ObString &name, ObLLVMValue &ptr, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(ptr.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(name), K(ptr), K(ret));
  } else {
    llvm::Value *value = jc_->get_builder().CreateLoad(ptr.get_v(), make_string_ref(name));
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create load", K(ret));
    } else {
      result.set_v(value);
    }
  }
  return ret;
}

int ObLLVMHelper::create_ialloca(const common::ObString &name, common::ObObjType obj_type, int64_t default_value, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMType type;
  if (obj_type < ObTinyIntType || obj_type > ObUInt64Type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted int type", K(obj_type), K(ret));
  } else if (OB_FAIL(get_llvm_type(obj_type, type))) {
    LOG_WARN("failed to get llvm type", K(obj_type), K(ret));
  } else if (OB_FAIL(create_alloca(ObString("int_alloca"), type, result))) {
    LOG_WARN("failed to create alloca", K(ret));
  } else if (OB_FAIL(create_istore(default_value, result))) {
    LOG_WARN("failed to create istore", K(default_value), K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLLVMHelper::create_istore(int64_t i, ObLLVMValue &dest)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(dest.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(i), K(dest), K(ret));
  } else if (llvm::Type::PointerTyID != dest.get_type_id()
      || 1 != dest.get_type().get_num_child()
      || llvm::Type::IntegerTyID != dest.get_type().get_child(0).get_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dest type is not integer pointer ",
             K(i),
             K(dest.get_type_id()),
             K(dest.get_type().get_num_child()),
             K(dest.get_type().get_child(0).get_id()),
             K(ret));
  } else {
    ObLLVMValue value;
    if (OB_FAIL(get_int_value(dest.get_type().get_child(0), i, value))) {
      LOG_WARN("failed to get_int64", K(ret));
    } else if (OB_FAIL(create_store(value, dest))) {
      LOG_WARN("failed to create_store", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLLVMHelper::create_icmp_eq(ObLLVMValue &value, int64_t i, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_icmp(value, i, ICMP_EQ, result))) {
    LOG_WARN("failed to get int64", K(i), K(ret));
  }
  return ret;
}

int ObLLVMHelper::create_icmp_slt(ObLLVMValue &value, int64_t i, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_icmp(value, i, ICMP_SLT, result))) {
    LOG_WARN("failed to get int64", K(i), K(ret));
  }
  return ret;
}

int ObLLVMHelper::create_icmp(ObLLVMValue &value, int64_t i, CMPTYPE type, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(i), K(value), K(ret));
  } else if (OB_ISNULL(value.get_v()->getType())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("type is NULL", K(i), K(ret));
  } else if (!value.get_v()->getType()->isIntegerTy()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not a integer", K(i), K(value.get_type_id()), K(ret));
  } else {
    ObLLVMValue i_value;
    switch (value.get_v()->getType()->getIntegerBitWidth()) {
    case 8: {
      if (OB_FAIL(get_int8(i, i_value))) {
        LOG_WARN("failed to get int8", K(i), K(ret));
      }
    }
      break;
    case 16: {
      if (OB_FAIL(get_int16(i, i_value))) {
        LOG_WARN("failed to get int16", K(i), K(ret));
      }
    }
      break;
    case 32: {
      if (OB_FAIL(get_int32(i, i_value))) {
        LOG_WARN("failed to get int32", K(i), K(ret));
      }
    }
      break;
    case 64: {
      if (OB_FAIL(get_int64(i, i_value))) {
        LOG_WARN("failed to get int64", K(i), K(ret));
      }
    }
      break;
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected integer", K(i), K(ret));
    }
      break;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(create_icmp(value, i_value, type, result))) {
        LOG_WARN("Failed to create_cmp", K(i), K(ret));
      }
    }
  }
  return ret;
}

int ObLLVMHelper::create_icmp(ObLLVMValue &value1, ObLLVMValue &value2, CMPTYPE type, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(value1.get_v()) || OB_ISNULL(value2.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(value1), K(value2), K(ret));
  } else {
    llvm::Value *is_equal = NULL;
    switch (type) {
    case ICMP_EQ: {
      is_equal = jc_->get_builder().CreateICmpEQ(value1.get_v(), value2.get_v());
    }
    break;
    case ICMP_NE: {
      is_equal = jc_->get_builder().CreateICmpNE(value1.get_v(), value2.get_v());
    }
    break;
    case ICMP_UGT: {
      is_equal = jc_->get_builder().CreateICmpUGT(value1.get_v(), value2.get_v());
    }
    break;
    case ICMP_UGE: {
      is_equal = jc_->get_builder().CreateICmpUGE(value1.get_v(), value2.get_v());
    }
    break;
    case ICMP_ULT: {
      is_equal = jc_->get_builder().CreateICmpULT(value1.get_v(), value2.get_v());
    }
    break;
    case ICMP_ULE: {
      is_equal = jc_->get_builder().CreateICmpULE(value1.get_v(), value2.get_v());
    }
    break;
    case ICMP_SGT: {
      is_equal = jc_->get_builder().CreateICmpSGT(value1.get_v(), value2.get_v());
    }
    break;
    case ICMP_SGE: {
      is_equal = jc_->get_builder().CreateICmpSGE(value1.get_v(), value2.get_v());
    }
    break;
    case ICMP_SLT: {
      is_equal = jc_->get_builder().CreateICmpSLT(value1.get_v(), value2.get_v());
    }
    break;
    case ICMP_SLE: {
      is_equal = jc_->get_builder().CreateICmpSLE(value1.get_v(), value2.get_v());
    }
    break;
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Invalid compare type", K(type), K(ret));
    }
    break;
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(is_equal)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to create icmpeq", K(ret));
      } else {
        result.set_v(is_equal);
      }
    }
  }
  return ret;
}

int ObLLVMHelper::get_int_value(const ObLLVMType &type, int64_t i, ObLLVMValue &i_value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(type.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("type is NULL", K(i), K(type), K(ret));
  } else {
    switch (type.get_width()) {
    case 8: {
      if (OB_FAIL(get_int8(i, i_value))) {
        LOG_WARN("failed to get int8", K(i), K(ret));
      }
    }
      break;
    case 16: {
      if (OB_FAIL(get_int16(i, i_value))) {
        LOG_WARN("failed to get int16", K(i), K(ret));
      }
    }
      break;
    case 32: {
      if (OB_FAIL(get_int32(i, i_value))) {
        LOG_WARN("failed to get int32", K(i), K(ret));
      }
    }
      break;
    case 64: {
      if (OB_FAIL(get_int64(i, i_value))) {
        LOG_WARN("failed to get int64", K(i), K(ret));
      }
    }
      break;
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Unexpected integer", K(type.get_id()), K(type.get_width()), K(i), K(ret));
    }
      break;
    }
  }
  return ret;
}

int ObLLVMHelper::create_inc(ObLLVMValue &value1, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  jit::ObLLVMValue one_value;
  if (OB_FAIL(get_int_value(value1.get_type(), 1, one_value))) {
    LOG_WARN("failed to get int value", K(ret));
  } else if (OB_FAIL(create_add(value1, one_value, result))) {
    LOG_WARN("failed to create add", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLLVMHelper::create_dec(ObLLVMValue &value1, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  jit::ObLLVMValue one_value;
  if (OB_FAIL(get_int_value(value1.get_type(), 1, one_value))) {
    LOG_WARN("failed to get int value", K(ret));
  } else if (OB_FAIL(create_sub(value1, one_value, result))) {
    LOG_WARN("failed to create add", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLLVMHelper::create_add(ObLLVMValue &value1, ObLLVMValue &value2, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(value1.get_v()) || OB_ISNULL(value2.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(value1), K(value2), K(ret));
  } else {
    llvm::Value *value = jc_->get_builder().CreateAdd(value1.get_v(), value2.get_v());
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create gep", K(ret));
    } else {
      result.set_v(value);
    }
  }
  return ret;
}

int ObLLVMHelper::create_sub(ObLLVMValue &value1, ObLLVMValue &value2, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(value1.get_v()) || OB_ISNULL(value2.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(value1), K(value2), K(ret));
  } else {
    llvm::Value *value = jc_->get_builder().CreateSub(value1.get_v(), value2.get_v());
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create gep", K(ret));
    } else {
      result.set_v(value);
    }
  }
  return ret;
}

#define DEFINE_CREATE_ARITH_INT(name) \
int ObLLVMHelper::create_##name(ObLLVMValue &value1, int64_t &value2, ObLLVMValue &result) \
{ \
  int ret = OB_SUCCESS; \
  jit::ObLLVMValue arith_value; \
  if (OB_FAIL(get_int_value(value1.get_type(), value2, arith_value))) { \
    LOG_WARN("failed to get int value", K(ret)); \
  } else if (OB_FAIL(create_##name(value1, arith_value, result))) { \
    LOG_WARN("failed to create add", K(ret)); \
  } else { /*do nothing*/ } \
  return ret; \
}

DEFINE_CREATE_ARITH_INT(add)
DEFINE_CREATE_ARITH_INT(sub)

int ObLLVMHelper::create_ret(ObLLVMValue &value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(value.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(value), K(ret));
  } else {
    CHECK_INSERT_POINT;
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(jc_->get_builder().CreateRet(value.get_v()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to create ret", K(ret));
      }
    }
  }
  return ret;
}

int ObLLVMHelper::create_gep(const ObString &name, ObLLVMValue &value, ObIArray<int64_t> &idxs, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLLVMValue, 8> array;
  for (int64_t i = 0; OB_SUCC(ret) && i < idxs.count(); ++i) {
    ObLLVMValue i_value;
    if (OB_FAIL(get_int32(idxs.at(i), i_value))) {
      LOG_WARN("failed to get int32", K(i), K(idxs.at(i)), K(ret));
    } else if (OB_FAIL(array.push_back(i_value))) {
      LOG_WARN("push_back error", K(i), K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(create_gep(name, value, array, result))) {
      LOG_WARN("failed to create gep", K(ret));
    }
  }
  return ret;
}

int ObLLVMHelper::create_gep(const ObString &name, ObLLVMValue &value, ObIArray<ObLLVMValue> &idxs, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(value.get_v()) || idxs.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(name), K(value), K(idxs), K(ret));
  } else {
    ObFastArray<llvm::Value*, 64> array(allocator_);
    for (int64_t i = 0; OB_SUCC(ret) && i < idxs.count(); ++i) {
      if (OB_FAIL(array.push_back(idxs.at(i).get_v()))) {
        LOG_WARN("push_back error", K(i), K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      llvm::Value *elem = jc_->get_builder().CreateGEP(value.get_v(), make_array_ref(array), make_string_ref(name));
      if (OB_ISNULL(elem)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to create gep", K(ret));
      } else {
        result.set_v(elem);
      }
    }
  }
  return ret;
}

int ObLLVMHelper::create_gep(const ObString &name, ObLLVMValue &value, int64_t idx, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 2> indices;
  if (OB_FAIL(indices.push_back(0)) || OB_FAIL(indices.push_back(idx))) {
    LOG_WARN("push_back error", K(ret));
  } else if (OB_FAIL(create_gep(ObString("extract_arg"), value, indices, result))) {
    LOG_WARN("failed to create gep", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLLVMHelper::create_gep(const ObString &name, ObLLVMValue &value, ObLLVMValue &idx, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(value.get_v()) || OB_ISNULL(idx.get_v()) ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(name), K(value), K(idx), K(ret));
  } else {
    ObFastArray<llvm::Value*, 64> array(allocator_);
    ObLLVMValue zero_value;
    if (OB_FAIL(get_int32(0, zero_value))) {
      LOG_WARN("failed to get int32", K(ret));
    } else if (OB_FAIL(array.push_back(zero_value.get_v()))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(array.push_back(idx.get_v()))) {
      LOG_WARN("push_back error", K(ret));
    } else {
      llvm::Value *elem = jc_->get_builder().CreateGEP(value.get_v(), make_array_ref(array), make_string_ref(name));
      if (OB_ISNULL(elem)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to create gep", K(ret));
      } else {
        result.set_v(elem);
      }
    }
  }
  return ret;
}

int ObLLVMHelper::create_const_gep1_64(const ObString &name, ObLLVMValue &value, uint64_t idx, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(value.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(name), K(value), K(idx), K(ret));
  } else {
    llvm::Value *elem = jc_->get_builder().CreateConstGEP1_64(value.get_v(), idx, make_string_ref(name));
    if (OB_ISNULL(elem)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create const gep164", K(ret));
    } else {
      result.set_v(elem);
    }
  }
  return ret;
}

int ObLLVMHelper::create_extract_value(const ObString &name, ObLLVMValue &value, uint64_t idx, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(value.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(name), K(value), K(idx), K(ret));
  } else {
    llvm::Value *elem = jc_->get_builder().CreateExtractValue(value.get_v(), idx, make_string_ref(name));
    if (OB_ISNULL(elem)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create extract value", K(ret));
    } else {
      result.set_v(elem);
    }
  }
  return ret;
}

#define DEFINE_CREATE_CAST(func_name, cast_name) \
int ObLLVMHelper::create_##func_name(const common::ObString &name, const ObLLVMValue &value, const ObLLVMType &type, ObLLVMValue &result) \
{                                                                                                                          \
  int ret = OB_SUCCESS;                                                                                                    \
  if (OB_ISNULL(jc_)) {                                                                                                    \
    ret = OB_NOT_INIT;                                                                                                     \
    LOG_WARN("jc is NULL", K(ret));                                                                                \
  } else if (OB_ISNULL(value.get_v()) || OB_ISNULL(type.get_v())) {                                                        \
    ret = OB_INVALID_ARGUMENT;                                                                                             \
    LOG_WARN("value is NULL", K(ret));                                                  \
  } else {                                                                                                                 \
    llvm::Value *dest = jc_->get_builder().Create##cast_name(const_cast<llvm::Value*>(value.get_v()), const_cast<llvm::Type*>(type.get_v()), make_string_ref(name));               \
    if (OB_ISNULL(dest)) {                                                                                                 \
      ret = OB_ERR_UNEXPECTED;                                                                                             \
      LOG_WARN("failed to create cast", K(ret));                                                                           \
    } else {                                                                                                               \
      result.set_v(dest);                                                                                                  \
    }                                                                                                                      \
  }                                                                                                                        \
  return ret;                                                                                                              \
}

DEFINE_CREATE_CAST(ptr_to_int, PtrToInt)
DEFINE_CREATE_CAST(int_to_ptr, IntToPtr)
DEFINE_CREATE_CAST(bit_cast, BitCast)
DEFINE_CREATE_CAST(pointer_cast, PointerCast)
DEFINE_CREATE_CAST(addr_space_cast, AddrSpaceCast)
DEFINE_CREATE_CAST(sext_or_bitcast, SExtOrBitCast)
DEFINE_CREATE_CAST(sext, SExt);

int ObLLVMHelper::create_landingpad(const ObString &name, ObLLVMType &type, ObLLVMLandingPad &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(type.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(name), K(type), K(ret));
  } else {
    llvm::LandingPadInst *inst = jc_->get_builder().CreateLandingPad(type.get_v(), 0, make_string_ref(name));
    if (OB_ISNULL(inst)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create landingpad", K(ret));
    } else {
      result.set_v(inst);
    }
  }
  return ret;
}

int ObLLVMHelper::create_switch(ObLLVMValue &value, ObLLVMBasicBlock &default_block, ObLLVMSwitch &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(value.get_v()) || OB_ISNULL(default_block.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(value), K(default_block), K(ret));
  } else {
    CHECK_INSERT_POINT;
    if (OB_SUCC(ret)) {
      llvm::SwitchInst *inst = jc_->get_builder().CreateSwitch(value.get_v(), default_block.get_v());
      if (OB_ISNULL(inst)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to create switch", K(ret));
      } else {
        result.set_v(inst);
      }
    }
  }
  return ret;
}

int ObLLVMHelper::create_resume(ObLLVMValue &value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(value.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(value), K(ret));
  } else {
    CHECK_INSERT_POINT;
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(jc_->get_builder().CreateResume(value.get_v()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to create resume", K(ret));
      }
    }
  }
  return ret;
}

int ObLLVMHelper::create_unreachable()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else {
    CHECK_INSERT_POINT;
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(jc_->get_builder().CreateUnreachable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to create resume", K(ret));
      }
    }
  }
  return ret;
}

int ObLLVMHelper::create_global_string(const ObString &str, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(str), K(ret));
  } else {
    llvm::Value *value = jc_->get_builder().CreateGlobalStringPtr(make_string_ref(str));
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create switch", K(ret));
    } else {
      result.set_v(value);
    }
  }
  return ret;
}

int ObLLVMHelper::get_or_insert_global(const ObString &name, ObLLVMType &type, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(jc_), K(ret));
  } else if (name.empty() || OB_ISNULL(type.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(name), K(type), K(ret));
  } else {
    llvm::Constant *value = jc_->get_module().getOrInsertGlobal(make_string_ref(name), type.get_v());
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create switch", K(ret));
    } else {
      result.set_v(value);
    }
  }
  return ret;
}

int ObLLVMHelper::set_insert_point(const ObLLVMBasicBlock &block)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(block.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is NULL", K(block), K(ret));
  } else {
    jc_->get_builder().SetInsertPoint(const_cast<llvm::BasicBlock*>(block.get_v()));
  }
  return ret;
}

int ObLLVMHelper::set_debug_location(uint32_t line, uint32_t col, ObLLVMDIScope *scope)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(scope) || OB_ISNULL(scope->get_v())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scope is NULL", K(ret));
  } else {
    jc_->get_builder().SetCurrentDebugLocation(ObDebugLoc::get(line, col, scope->get_v()));
  }
  return ret;
}

int ObLLVMHelper::unset_debug_location(ObLLVMDIScope *scope)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is null", K(ret));
  } else if (OB_ISNULL(scope) || OB_ISNULL(scope->get_v())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scope is null", K(ret));
  } else {
    jc_->get_builder().SetCurrentDebugLocation(ObDebugLoc());
  }
  return ret;
}

int ObLLVMHelper::stack_save(ObLLVMValue &stack)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else {
    llvm::Value *result = jc_->get_builder().CreateCall(
      llvm::Intrinsic::getDeclaration(&(jc_->get_module()), llvm::Intrinsic::stacksave));
    if (OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to call stacksave", K(ret));
    } else {
      stack.set_v(result);
    }
  }
  return ret;
}

int ObLLVMHelper::stack_restore(ObLLVMValue &stack)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else {
    jc_->get_builder().CreateCall(
      llvm::Intrinsic::getDeclaration(&(jc_->get_module()), llvm::Intrinsic::stackrestore), stack.get_v());
  }
  return ret;
}

int ObLLVMHelper::get_null_const(const ObLLVMType &type, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMConstant const_value;
  if (OB_FAIL(ObLLVMConstant::get_null_value(type, const_value))) {
    LOG_WARN("failed to get null value", K(type), K(ret));
  } else {
    result.set_v(const_value.get_v());
  }
  return ret;
}

int ObLLVMHelper::get_array_type(const ObLLVMType &elem_type, uint64_t size, ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  ObLLVMArrayType array_type;
  if (OB_FAIL(ObLLVMArrayType::get(elem_type, size, array_type))) {
    LOG_WARN("failed to get null value", K(elem_type), K(size), K(ret));
  } else {
    type.set_v(array_type.get_v());
  }
  return ret;
}

int ObLLVMHelper::get_uint64_array(const ObIArray<uint64_t> &elem_values, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (elem_values.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("element types is empty", K(ret));
  } else {
    ObFastArray<uint64_t, 64> array(allocator_);
    for (int64_t i = 0; OB_SUCC(ret) && i < elem_values.count(); ++i) {
      if (OB_FAIL(array.push_back(elem_values.at(i)))) {
        LOG_WARN("push_back error", K(i), K(elem_values), K(ret));
      } else { /*do nothing*/ }
    }
    // TODO:
    if (OB_SUCC(ret)) {
      llvm::Constant *value = llvm::ConstantDataArray::get(jc_->get_context(), make_array_ref(array));
      if (OB_ISNULL(value)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to create struct type", K(ret));
      } else {
        result.set_v(value);
      }
    }
  }
  return ret;
}

int ObLLVMHelper::get_string(const ObString &str, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("element types is empty", K(str), K(ret));
  } else {
    llvm::Constant *value = llvm::ConstantDataArray::getString(jc_->get_context(), make_string_ref(str));
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create struct type", K(str), K(ret));
    } else {
      result.set_v(value);
    }
  }
  return ret;
}

int ObLLVMHelper::get_global_string(ObLLVMValue &const_string, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  llvm::Value *string_var = nullptr;
  if (nullptr == (string_var = new llvm::GlobalVariable(
    (jc_->get_module()),
    const_string.get_v()->getType(),
    true,
    llvm::GlobalVariable::PrivateLinkage,
    static_cast<llvm::Constant *>(const_string.get_v()),
    ""))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed allocate memory for GlobalVariable", K(ret), K(sizeof(llvm::GlobalVariable)));
  } else {
    result.set_v(string_var);
  }
  return ret;
}

int ObLLVMHelper::get_const_struct(ObLLVMType &type, ObIArray<ObLLVMValue> &elem_values, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(type.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("type is NULL", K(ret));
  } else {
    ObLLVMStructType struct_type(type.get_v());
    ObSEArray<ObLLVMConstant, 8> const_values;

    for (int64_t i = 0; OB_SUCC(ret) && i < elem_values.count(); ++i) {
      if (OB_FAIL(const_values.push_back(ObLLVMConstant(elem_values.at(i).get_v())))) {
        LOG_WARN("push_back error", K(i), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObLLVMConstant const_result;
      if (OB_FAIL(ObLLVMConstantStruct::get(struct_type, const_values, const_result))) {
        LOG_WARN("failed to get null value", K(ret));
      } else {
        result.set_v(const_result.get_v());
      }
    }
  }
  return ret;
}

int ObLLVMHelper::create_function(const common::ObString &name, ObLLVMFunctionType &type, ObLLVMFunction &function)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(jc_), K(ret));
  } else if (OB_ISNULL(type.get_v())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("element types is empty", K(type), K(ret));
  } else {
    llvm::Function *func = llvm::Function::Create(
      type.get_v(), llvm::Function::ExternalLinkage, make_string_ref(name), jc_->TheModule.get());
    if (OB_ISNULL(func)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create struct type", K(ret));
    } else {
      function.set_v(func);
    }
  }
  return ret;
}

int ObLLVMHelper::create_block(const common::ObString &name, ObLLVMFunction &parent, ObLLVMBasicBlock &block)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(jc_), K(ret));
  } else {
    llvm::BasicBlock *bb = llvm::BasicBlock::Create(jc_->get_context(), make_string_ref(name), parent.get_v());
    if (OB_ISNULL(bb)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create struct type", K(name), K(parent), K(ret));
    } else {
      block.set_v(bb);
    }
  }
  return ret;
}

int ObLLVMHelper::create_struct_type(const common::ObString &name, common::ObIArray<ObLLVMType> &elem_types, ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(jc_), K(ret));
  } else if (elem_types.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("element types is empty", K(ret));
  } else {
    common::ObFastArray<ObIRType*, 64> array(allocator_);
    for (int64_t i = 0; OB_SUCC(ret) && i < elem_types.count(); ++i) {
      if (OB_ISNULL(elem_types.at(i).get_v())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("element type is NULL", K(i), K(elem_types), K(ret));
      } else if (OB_FAIL(array.push_back(elem_types.at(i).get_v()))) {
        LOG_WARN("push_back error", K(i), K(elem_types), K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      llvm::StructType *struct_type = llvm::StructType::create(jc_->get_context(), make_array_ref(array), make_string_ref(name));
      if (OB_ISNULL(struct_type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to create struct type", K(name), K(ret));
      } else {
        type.set_v(struct_type);
      }
    }
  }
  return ret;
}

int ObLLVMHelper::get_llvm_type(common::ObObjType obj_type, ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(jc_), K(ret));
  } else if (obj_type < 0 || obj_type >= common::ObMaxType || OB_ISNULL(OB_IR_TYPE[obj_type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj_type passed in is invalid", K(obj_type), K(ret));
  } else {
    type.set_v(OB_IR_TYPE[obj_type](jc_->get_context(), 0));
  }
  return ret;
}

int ObLLVMHelper::get_void_type(ObLLVMType &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(jc_), K(ret));
  } else {
    type.set_v(llvm::Type::getVoidTy(jc_->get_context()));
  }
  return ret;
}

int ObLLVMHelper::get_int8(int64_t value, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(jc_), K(ret));
  } else {
    result.set_v(llvm::ConstantInt::get(OB_IR_TYPE[common::ObTinyIntType](jc_->get_context()), value));
  }
  return ret;
}

int ObLLVMHelper::get_int16(int64_t value, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(jc_), K(ret));
  } else {
    result.set_v(llvm::ConstantInt::get(OB_IR_TYPE[common::ObSmallIntType](jc_->get_context()), value));
  }
  return ret;
}

int ObLLVMHelper::get_int32(int64_t value, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(jc_), K(ret));
  } else {
    result.set_v(llvm::ConstantInt::get(OB_IR_TYPE[common::ObInt32Type](jc_->get_context()), value));
  }
  return ret;
}

int ObLLVMHelper::get_int64(int64_t value, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(jc_), K(ret));
  } else {
    result.set_v(llvm::ConstantInt::get(OB_IR_TYPE[common::ObIntType](jc_->get_context()), value));
  }
  return ret;
}

int ObLLVMHelper::get_insert_block(ObLLVMBasicBlock &block)
{
  int ret = OB_SUCCESS;
  ObIRBasicBlock *block_ptr = NULL;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(jc_), K(ret));
  } else if (OB_ISNULL(block_ptr = jc_->get_builder().GetInsertBlock())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block is NULL", K(ret));
  } else {
    block.set_v(block_ptr);
  }
  return ret;
}

int ObLLVMHelper::check_insert_point(bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObLLVMBasicBlock block;
  if (OB_FAIL(get_insert_block(block))) {
    LOG_WARN("failed to get insert block", K(ret));
  } else if (OB_ISNULL(block.get_v())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block is NULL", K(ret));
  } else {
    is_valid = !block.is_terminated();
  }
  return ret;
}

int ObDWARFHelper::init()
{
  int ret = OB_SUCCESS;
  std::string s;
  llvm::raw_string_ostream Out(s);
  if (nullptr == (Context = OB_NEWx(core::ObDWARFContext, (&Allocator), DebugBuf, DebugLen))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for DWARFContext", K(ret));
  } else if (OB_FAIL(Context->init())) {
    LOG_WARN("failed to init DWARFContext", K(ret));
  } else if (OB_FAIL(dump(DebugBuf, DebugLen))) {
    LOG_WARN("failed to dump debug info", K(ret));
  } else if (!Context->Context->verify(Out)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to verify Context", K(ret), K(Out.str().c_str()), KP(DebugBuf), K(DebugLen));
  } else {
    std::string s;
    llvm::raw_string_ostream Out(s);
    DIDumpOptions DumpOpts;
    DumpOpts.ShowChildren = true;
    DumpOpts.ShowParents = true;
    DumpOpts.ShowForm = true;
    DumpOpts.SummarizeTypes = true;
    DumpOpts.Verbose = true; 
    Context->Context->dump(Out, DumpOpts);
    Out.flush();
    LOG_INFO("success to init ObDWARFHelper!", K(ret), K(Out.str().c_str()));
  }
  return ret;
}

int ObDWARFHelper::dump(char* DebugBuf, int64_t DebugLen)
{
  int ret = OB_SUCCESS;
  std::string s;
  llvm::raw_string_ostream Out(s);
  core::StringMemoryBuffer MemoryBuf(DebugBuf, DebugLen);
  MemoryBufferRef MemoryRef(MemoryBuf);
  auto BinOrErr = llvm::object::createBinary(MemoryRef);
  if (!BinOrErr) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    auto Bin = std::move(BinOrErr.get());
    llvm::object::ObjectFile *DebugObj = dyn_cast<llvm::object::ObjectFile>(Bin.get());
    if (!DebugObj) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      std::unique_ptr<DWARFContext> Context = DWARFContext::create(*DebugObj);
      DIDumpOptions DumpOpts;
      DumpOpts.ShowChildren = true;
      DumpOpts.ShowParents = true;
      DumpOpts.ShowForm = true;
      DumpOpts.SummarizeTypes = true;
      DumpOpts.Verbose = true;
      Context->verify(Out);
      Context->dump(Out, DumpOpts);
      Out.flush();
    }
  }
  LOG_INFO("Dump LLVM DWARF DebugInfo!\n", K(ret), K(Out.str().c_str()), KP(DebugBuf), K(DebugLen));
  return ret;
}

int ObDWARFHelper::find_all_line_address(common::ObIArray<ObLineAddress>& all_lines)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(Context)) {
    DWARFUnit *cu = Context->Context->getUnitAtIndex(0);
    llvm::DWARFDie die = cu->getDIEAtIndex(1);
    const char *module = die.getName(llvm::DINameKind::ShortName);
    const DWARFDebugLine::LineTable *lt = Context->Context->getLineTableForUnit(cu);
    const DWARFDebugLine::LineTable::RowVector &rows = lt->Rows;
    for (int64_t i = 0; OB_SUCC(ret) && i < rows.size(); ++i) {
      const DWARFDebugLine::Row &row = rows.at(i);
      if (!row.EndSequence && row.IsStmt && row.Line != 0) {

#if defined(__aarch64__)
        int64_t j = 0;
        for (; OB_SUCC(ret) && j < all_lines.count(); j++) {
          if (all_lines.at(j).get_line() == row.Line) {
            if (all_lines.at(j).get_address() > row.Address.Address) {
              all_lines.at(j) = ObLineAddress(row.Address.Address, row.Line, ObString(module));
            }
            break;
          }
        }
        if (j == all_lines.count()) {
          if (OB_FAIL(all_lines.push_back(ObLineAddress(row.Address.Address, row.Line, ObString(module))))) {
            LOG_WARN("failed to push to all_lines array", K(ret));
          }
        }
#else
        if (OB_FAIL(all_lines.push_back(ObLineAddress(row.Address.Address, row.Line, ObString(module))))) {
          LOG_WARN("failed to push all_lines array", K(ret));
        }
#endif

      }
    }
  }
  return ret;
}

int ObDWARFHelper::find_line_by_addr(int64_t pc, int& line_number, bool& found)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(Context)) {
    DWARFUnit *cu = Context->Context->getUnitAtIndex(0);
    const DWARFDebugLine::LineTable *lt = Context->Context->getLineTableForUnit(cu);
    const DWARFDebugLine::LineTable::RowVector &rows = lt->Rows;
    for (int64_t i = 0; i < rows.size(); ++i) {
      const DWARFDebugLine::Row &row = rows.at(i);
      if (!row.EndSequence && row.Address.Address == pc) {
        found = true;
        line_number = row.Line;
      }
    }
  }
  return ret;
}

int ObDWARFHelper::find_address_by_function_line(
  const common::ObString &name, int line, int64_t& address)
{
  int ret = OB_SUCCESS;
  address = -1;
  if (OB_NOT_NULL(Context)) {
    DWARFUnit *cu = Context->Context->getUnitAtIndex(0);
    llvm::DWARFDie die = cu->getDIEAtIndex(1);
    const char *cu_name = die.getName(llvm::DINameKind::ShortName);
    if (0 == name.case_compare(cu_name)) {
      const DWARFDebugLine::LineTable *lt = Context->Context->getLineTableForUnit(cu);
      const DWARFDebugLine::LineTable::RowVector &rows = lt->Rows;
      for (int64_t i = 0; i < rows.size(); ++i) {
        const DWARFDebugLine::Row &row = rows.at(i);
        if (row.IsStmt && row.Line == line) {
          address = row.Address.Address;
          break;
        }
      }
    }
  }
  return ret;
}

int ObDWARFHelper::find_function_line_by_address(
  int64_t& address, common::ObString &name, int &line)
{
  int ret = OB_SUCCESS;
  line = -1;
  if (OB_NOT_NULL(Context)) {
    DWARFUnit *cu = Context->Context->getUnitAtIndex(0);
    const DWARFDebugLine::LineTable *lt = Context->Context->getLineTableForUnit(cu);
    const DWARFDebugLine::LineTable::RowVector &rows = lt->Rows;
    llvm::DWARFDie die = cu->getDIEAtIndex(1);
    const char *cu_name = die.getName(llvm::DINameKind::ShortName);
    for (int64_t i = 0; OB_SUCC(ret) && i < rows.size(); ++i) {
      const DWARFDebugLine::Row &row = rows.at(i);
      if (row.Address.Address == address) {
        name = ObString(cu_name);
        line = row.Line;
        break;
      }
    }
  }
  return ret;
}

int ObDWARFHelper::find_function_from_pc(uint64_t pc, bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  if (OB_NOT_NULL(Context)) {
    DWARFUnit *cu = Context->Context->getUnitAtIndex(0);
    uint32_t die_cnt = cu->getNumDIEs();
    DWARFDie die = cu->getDIEAtIndex(1);
    const char *cu_name = die.getName(llvm::DINameKind::ShortName);
    for (uint32_t i = 0; i < die_cnt; ++i) {
      DWARFDie die = cu->getDIEAtIndex(i);
      if (die.isSubprogramDIE() && die.addressRangeContainsAddress(pc)) {
        found = true;
        break;
      }
    }
  }
  return ret;
}

int ObDWARFHelper::find_function_from_pc(uint64_t pc, ObDIEAddress &func)
{
  int ret = OB_SUCCESS;
  uint64_t find_pc = pc;
  ObDIEAddress check_addr;
  if (OB_NOT_NULL(Context)) {
    DWARFUnit *cu = Context->Context->getUnitAtIndex(0);
    uint32_t die_cnt = cu->getNumDIEs();
    DWARFDie die = cu->getDIEAtIndex(1);
    const char *cu_name = die.getName(llvm::DINameKind::ShortName);
    for (uint32_t i = 0; i < die_cnt; ++i) {
      DWARFDie die = cu->getDIEAtIndex(i);
      if (die.isSubprogramDIE() && die.addressRangeContainsAddress(pc)) {
        uint64_t section_idx = OB_INVALID_ID;
        uint64_t lowpc = OB_INVALID_ID;
        uint64_t highpc = OB_INVALID_ID;
        die.getLowAndHighPC(lowpc, highpc, section_idx);
        func = ObDIEAddress(lowpc, highpc, ObString(cu_name));
      }
    }
  }
  return ret;
}

}
}
