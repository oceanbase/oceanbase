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

//#include "objit/ob_llvm_helper.h"
#include "core/jit_context.h"
#include "core/jit_di_context.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/container/ob_se_array.h"

#include "objit/ob_llvm_di_helper.h"

using namespace llvm;

namespace oceanbase
{
using namespace common;
namespace jit
{

uint64_t ObLLVMDIType::get_size_bits()
{
  return OB_ISNULL(v_) ? 0 : v_->getSizeInBits();
}
  
uint64_t ObLLVMDIType::get_align_bits()
{
  return OB_ISNULL(v_) ? 0 : v_->getAlignInBits();
}

int ObLLVMDIHelper::init(core::JitContext *jc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(jc_ = OB_NEWx(core::JitDIContext, (&allocator_), (jc->get_module())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to new jc", K(ret));
  } else {
    // Add the current debug info version into the module.
    jc->get_module().addModuleFlag(Module::Warning, "Debug Info Version", DEBUG_METADATA_VERSION);
    // Darwin only supports dwarf2.
    if (Triple(sys::getProcessTriple()).isOSDarwin()) {
      jc->get_module().addModuleFlag(llvm::Module::Warning, "Dwarf Version", 2);
    }
  }
  return ret;
}

int ObLLVMDIHelper::create_compile_unit(const char *name)
{
  int ret = OB_SUCCESS;
  ObDICompileUnit *cu = NULL;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(cu = jc_->dbuilder_.createCompileUnit(
    dwarf::DW_LANG_C, jc_->dbuilder_.createFile("OB PL Compiler", "."), name, false, "", 0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create compile unit", K(ret), K(name));
  } else {
    jc_->cu_ = cu;
  }
  return ret;
}

int ObLLVMDIHelper::create_file()
{
  int ret = OB_SUCCESS;
  const char *name = NULL;
  const char *path = NULL;
  if (OB_ISNULL(jc_) || OB_ISNULL(jc_->cu_)
      || OB_ISNULL(name = jc_->cu_->getFilename().data())
      || OB_ISNULL(path = jc_->cu_->getDirectory().data())) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc or cu is NULL", K(ret), K(jc_), K(name), K(path));
  } else if (OB_FAIL(create_file(name, path))) {
    LOG_WARN("failed to create di file", K(ret), K(jc_), K(name), K(path));
  }
  return ret;
}

int ObLLVMDIHelper::create_file(const char *name, const char *path)
{
  int ret = OB_SUCCESS;
  DIFile *file = NULL;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(file = jc_->dbuilder_.createFile(name, path))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create file", K(ret));
  } else {
    jc_->file_ = file;
  }
  return ret;
}

int ObLLVMDIHelper::create_function(const char *name,
                                    ObLLVMDISubroutineType &subroutine_type,
                                    ObLLVMDISubprogram &subprogram)
{
  int ret = OB_SUCCESS;
  ObDISubroutineType *sr_type = subroutine_type.get_v();
  ObDISubprogram *sp = NULL;

  DIFile *Unit = nullptr;
  DIScope *FContext = Unit;
  unsigned LineNo = 0;
  unsigned ScopeLine = LineNo;

  if (OB_ISNULL(jc_)
      || OB_ISNULL(Unit = jc_->file_)
      || OB_ISNULL(FContext = Unit)
      || OB_ISNULL(sr_type = subroutine_type.get_v())) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc or file or sr type is NULL", K(ret));
  } else if (OB_ISNULL(sp = jc_->dbuilder_.createFunction(FContext,
                                                          name,
                                                          StringRef(),
                                                          Unit,
                                                          LineNo,
                                                          sr_type,
                                                          ScopeLine,
                                                          DINode::FlagPrototyped,
                                                          DISubprogram::SPFlagDefinition))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create function", K(ret));
  } else {
    jc_->sp_ = sp;
    subprogram.set_v(sp);
  }
  return ret;
}

int ObLLVMDIHelper::create_local_variable(const ObString &name, uint32_t arg_no, uint32_t line,
                                          ObLLVMDIType &type, ObLLVMDILocalVariable &variable)
{
  int ret = OB_SUCCESS;
  ObDIFile *file = NULL;
  ObDISubprogram *sp = NULL;
  ObDILocalVariable *var_ptr;
  if (name.empty() || OB_ISNULL(jc_) || OB_ISNULL(file = jc_->file_)
      || OB_ISNULL(sp = jc_->sp_) || OB_ISNULL(type.get_v())) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc or file or sp or type is NULL", K(name), K(ret));
  } else {
    var_ptr = (arg_no > 0) ?
        jc_->dbuilder_.createParameterVariable(sp, StringRef(name.ptr(), name.length()),
                                               arg_no, file, line, type.get_v(), true) :
        jc_->dbuilder_.createAutoVariable(sp, StringRef(name.ptr(), name.length()),
                                          file, line, type.get_v(), true);
    if (OB_ISNULL(var_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create local variable", K(name), K(ret));
    } else {
      variable.set_v(var_ptr);
    }
  }
  return ret;
}

int ObLLVMDIHelper::insert_declare(ObLLVMValue &storage, ObLLVMDILocalVariable &variable,
                                   ObLLVMBasicBlock &block)
{
  int ret = OB_SUCCESS;
  ObDISubprogram *sp = NULL;
  ObDIExpression *expr = NULL;
  uint32_t line = 0;
  if (OB_ISNULL(jc_) || OB_ISNULL(sp = jc_->sp_) || OB_ISNULL(storage.get_v())
      || OB_ISNULL(variable.get_v()) || OB_ISNULL(block.get_v())) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc or sp or storage or variable or block is NULL", K(ret));
  } else if (OB_ISNULL(expr = jc_->dbuilder_.createExpression())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create expression", K(ret));
  } else if (OB_ISNULL(jc_->dbuilder_.insertDeclare(storage.get_v(), variable.get_v(),
                                                    expr, ObDebugLoc::get(line, 0, sp),
                                                    block.get_v()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to insert declare", K(ret));
  }
  return ret;
}

int ObLLVMDIHelper::finalize()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else {
    jc_->dbuilder_.finalize();
  }
  return ret;
}

int ObLLVMDIHelper::create_pointer_type(ObLLVMDIType &pointee_type,
                                        ObLLVMDIType &pointer_type)
{
  int ret = OB_SUCCESS;
  DIType *pte_type = pointee_type.get_v();
  DIType *ptr_type = NULL;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(ptr_type = jc_->dbuilder_.createPointerType(pte_type, 64, 64))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create pointer type", K(ret));
  } else {
    pointer_type.set_v(ptr_type);
  }
  return ret;
}

int ObLLVMDIHelper::create_basic_type(ObObjType obj_type,
                                      ObLLVMDIType &basic_type)
{
  int ret = OB_SUCCESS;
  DIType *type_ptr = NULL;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(type_ptr =
    jc_->dbuilder_.createBasicType(basic_type_[obj_type].name_,
                                   basic_type_[obj_type].size_bits_,
                                   //basic_type_[obj_type].align_bits_,
                                   basic_type_[obj_type].encoding_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create basic type", K(ret), K(obj_type));
  } else {
    basic_type.set_v(type_ptr);
  }
  return ret;
}

int ObLLVMDIHelper::create_member_type(const ObString &name, uint64_t offset_bits, uint32_t line,
                                       ObLLVMDIType &base_type, ObLLVMDIType &member_type)
{
  int ret = OB_SUCCESS;
  ObDIFile *file = NULL;
  ObDIScope *scope = NULL;
  uint64_t size_bits = base_type.get_size_bits();
  uint64_t align_bits = base_type.get_align_bits();
  unsigned flags = 0;
  ObDIType *type_ptr = NULL;
  if (name.empty()
      || OB_ISNULL(jc_)
      || OB_ISNULL(file = jc_->file_)
      || OB_ISNULL(scope =
          (line > 0) ? static_cast<ObDIScope *>(jc_->sp_)
                     : static_cast<ObDIScope *>(jc_->file_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("name or jc or file or sp is NULL",
             K(ret), K(name), K(jc_), K(file), K(scope), K(line));
  } else if (OB_ISNULL(type_ptr =
    jc_->dbuilder_.createMemberType(scope,
                                    StringRef(name.ptr(), name.length()),
                                    file,
                                    line,
                                    size_bits,
                                    align_bits,
                                    offset_bits,
                                    llvm::DINode::FlagZero,
                                    base_type.get_v()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create member type",
             K(ret), K(name), K(jc_), K(file), K(scope), K(line));
  } else {
    member_type.set_v(type_ptr);
  }
  return ret;
}

int ObLLVMDIHelper::create_struct_type(
  const ObString &name, uint32_t line, uint64_t size_bits, uint64_t align_bits,
  ObIArray<ObLLVMDIType> &member_types, ObLLVMDIType &struct_type)
{
  //  see DIBuilder::finalize() in IR/DIBuilder.cpp:
  //  70     SmallVector<Metadata *, 4> Variables;
  //  71
  //  72     auto PV = PreservedVariables.find(SP);
  //  73     if (PV != PreservedVariables.end())
  //  74       Variables.append(PV->second.begin(), PV->second.end());
  //  75
  //  76     DINodeArray AV = getOrCreateArray(Variables);
  //  77     TempMDTuple(Temp)->replaceAllUsesWith(AV.get());
  int ret = OB_SUCCESS;
  ObDIFile *file = NULL;
  ObDIScope *scope = NULL;
  unsigned flags = 0;
  ObDIType *type_ptr = NULL;
  if (name.empty()
      || OB_ISNULL(jc_)
      || OB_ISNULL(file = jc_->file_)
      || OB_ISNULL(scope =
          (line > 0) ? static_cast<ObDIScope *>(jc_->sp_)
                     : static_cast<ObDIScope *>(jc_->file_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("name or jc or file or scope is NULL",
             K(ret), K(name), K(jc_), K(file), K(scope), K(line));
  } else {
    SmallVector<Metadata *, 8> element_types;
    for (int i = 0; OB_SUCC(ret) && i < member_types.count(); i++) {
      if (OB_ISNULL(member_types.at(i).get_v())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("member type is NULL", K(ret), K(i), K(member_types.count()));
      } else {
        element_types.push_back(static_cast<ObDIType *>(member_types.at(i).get_v()));
      }
    }
    DINodeArray elements = jc_->dbuilder_.getOrCreateArray(element_types);
    if (OB_ISNULL(type_ptr =
          jc_->dbuilder_.createStructType(scope,
                                          StringRef(name.ptr(), name.length()),
                                          file,
                                          line,
                                          size_bits,
                                          align_bits,
                                          llvm::DINode::FlagZero,
                                          NULL,
                                          elements))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create struct type",
               K(ret), K(name), K(file), K(line), K(size_bits), K(align_bits), K(flags));
    } else {
      struct_type.set_v(type_ptr);
    }
  }
  return ret;
}

int ObLLVMDIHelper::create_array_type(ObLLVMDIType &base_type, int64_t count,
                                      ObLLVMDIType &array_type)
{
  int ret = OB_SUCCESS;
  ObDIType *type_ptr = NULL;
  DISubrange *subrange = NULL;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (OB_ISNULL(subrange = jc_->dbuilder_.getOrCreateSubrange(0, count))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subrange is NULL", K(ret));
  } else if (OB_ISNULL(base_type.get_v())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("base type is NULL", K(ret));
  } else {
    uint64_t size_bits = base_type.get_v()->getSizeInBits();
    uint64_t align_bits = base_type.get_v()->getAlignInBits();
    SmallVector<llvm::Metadata *, 8> subscripts;
    subscripts.push_back(subrange);
    DINodeArray subscript_array = jc_->dbuilder_.getOrCreateArray(subscripts);
    if (OB_ISNULL(type_ptr = jc_->dbuilder_.createArrayType(size_bits, align_bits, base_type.get_v(),
                                                            subscript_array))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create array type", K(ret));
    } else {
      array_type.set_v(type_ptr);
    }
  }
  return ret;
}

int ObLLVMDIHelper::create_subroutine_type(ObIArray<ObLLVMDIType> &member_types,
                                           ObLLVMDISubroutineType &subroutine_type)
{
  int ret = OB_SUCCESS;
  ObDISubroutineType *type_ptr = NULL;
  if (OB_ISNULL(jc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else {
    SmallVector<Metadata *, 8> element_types;
    for (int i = 0; i < member_types.count(); i++) {
      element_types.push_back(static_cast<ObDIType *>(member_types.at(i).get_v()));
    }
    DITypeRefArray element_type_array = jc_->dbuilder_.getOrCreateTypeArray(element_types);
    if (OB_ISNULL(type_ptr = jc_->dbuilder_.createSubroutineType(element_type_array))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create subroutine type", K(ret));
    } else {
      subroutine_type.set_v(type_ptr);
    }
  }
  return ret;
}

int ObLLVMDIHelper::get_current_scope(ObLLVMDIScope &scope)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jc_) || (OB_ISNULL(jc_->file_) && OB_ISNULL(jc_->sp_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("jc is NULL", K(ret));
  } else if (NULL != jc_->sp_) {
    scope.set_v(jc_->sp_);
  } else {
    scope.set_v(jc_->file_);
  }
  return ret;
}

ObLLVMDIHelper::ObDIBasicTypeAttr ObLLVMDIHelper::basic_type_[common::ObMaxType] =
{
  {"null",       0,  0, 0},
  {"tinyint",    8,  8, llvm::dwarf::DW_ATE_signed},
  {"smallint",  16, 16, llvm::dwarf::DW_ATE_signed},
  {"mediumint", 32, 32, llvm::dwarf::DW_ATE_signed},
  {"int",       32, 32, llvm::dwarf::DW_ATE_signed},
  {"bigint",    64, 64, llvm::dwarf::DW_ATE_signed},
  {"tinyint unsigned",    8,  8, llvm::dwarf::DW_ATE_unsigned},
  {"smallint unsigned",  16, 16, llvm::dwarf::DW_ATE_unsigned},
  {"mediumint unsigned", 32, 32, llvm::dwarf::DW_ATE_unsigned},
  {"int unsigned",       32, 32, llvm::dwarf::DW_ATE_unsigned},
  {"bigint unsigned",    64, 64, llvm::dwarf::DW_ATE_unsigned},
  {"float",  32, 32, llvm::dwarf::DW_ATE_float},
  {"double", 64, 64, llvm::dwarf::DW_ATE_float},
  {"float unsigned",  32, 32, llvm::dwarf::DW_ATE_float},
  {"double unsigned", 64, 64, llvm::dwarf::DW_ATE_float},
  {"decimal", 64, 64, llvm::dwarf::DW_ATE_address},
  {"decimal unsigned", 64, 64, llvm::dwarf::DW_ATE_address},

  {"datetime",  64, 64, llvm::dwarf::DW_ATE_signed},
  {"timestamp", 64, 64, llvm::dwarf::DW_ATE_signed},
  {"date",  64, 64, llvm::dwarf::DW_ATE_signed},
  {"time",  64, 64, llvm::dwarf::DW_ATE_signed},
  {"year",  64, 64, llvm::dwarf::DW_ATE_signed},

  {"varchar", 64, 64, llvm::dwarf::DW_ATE_address},
  {"char", 64, 64, llvm::dwarf::DW_ATE_address},
  {"hexstring", 0, 0, 0},

  {"extend", 0, 0, 0},
  {"unknown", 0, 0, 0},
  {"tinytext", 0, 0, 0},
  {"text", 0, 0, 0},
  {"mediumtext", 0, 0, 0},
  {"longtext", 0, 0, 0},
  {"bit", 0, 0, 0},
};

}
}
