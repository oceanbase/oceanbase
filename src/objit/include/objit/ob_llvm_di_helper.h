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

#ifndef OCEANBASE_DEPS_OBJIT_INCLUDE_OBJIT_OB_LLVM_DI_HELPER_H_
#define OCEANBASE_DEPS_OBJIT_INCLUDE_OBJIT_OB_LLVM_DI_HELPER_H_

#include "objit/ob_llvm_helper.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_fast_array.h"
#include "lib/string/ob_string.h"
#include "common/object/ob_object.h"

namespace llvm
{
//class Module;
class DIBuilder;
class Metadata;
class DINode;
class DIScope;
class DICompileUnit;
class DIFile;
class DISubprogram;
class DILocalVariable;
class DIType;
class DISubroutineType;
class DebugLoc;
};

namespace oceanbase
{
namespace jit
{

namespace core
{
class JitContext;
class JitDIContext;
}

class ObLLVMDIScope
{
public:
  ObLLVMDIScope() : v_(NULL) {}
  virtual ~ObLLVMDIScope() {}

  TO_STRING_KV(KP_(v))

  inline const llvm::DIScope *get_v() const { return v_; }
  inline llvm::DIScope *get_v() { return v_; }
  inline void set_v(llvm::DIScope *v) { v_ = v; }

private:
  llvm::DIScope *v_;
};

//class ObLLVMDICompileUnit
//{
//public:
//  ObLLVMDICompileUnit() : v_(NULL) {}
//  ObLLVMDICompileUnit(llvm::DICompileUnit *v) : v_(v) {}
//  virtual ~ObLLVMDICompileUnit() {}
//
//  TO_STRING_KV(KP_(v))
//
//  inline const llvm::DICompileUnit *get_v() const { return v_; }
//  inline llvm::DICompileUnit *&get_v() { return v_; }
//  inline void set_v(llvm::DICompileUnit *v) { v_ = v; }
//  inline char *get_file_name() { return OB_ISNULL(v_) ? NULL : v_->getFilename(); }
//  inline char *get_directory() { return OB_ISNULL(v_) ? NULL : v_->getDirectory(); }
//
//protected:
//  llvm::DICompileUnit *v_;
//};
//
//class ObLLVMDIFile
//{
//public:
//  ObLLVMDIFile() : v_(NULL) {}
//  ObLLVMDIFile(llvm::DIFile *v) : v_(v) {}
//  virtual ~ObLLVMDIFile() {}
//
//  TO_STRING_KV(KP_(v))
//
//  inline const llvm::DIFile *get_v() const { return v_; }
//  inline llvm::DIFile *&get_v() { return v_; }
//  inline void set_v(llvm::DIFile *v) { v_ = v; }
//
//protected:
//  llvm::DIFile *v_;
//};

class ObLLVMDISubprogram
{
public:
  ObLLVMDISubprogram() : v_(NULL) {}
  virtual ~ObLLVMDISubprogram() {}

  TO_STRING_KV(KP_(v))

  inline const llvm::DISubprogram *get_v() const { return v_; }
  inline llvm::DISubprogram *get_v() { return v_; }
  inline void set_v(llvm::DISubprogram *v) { v_ = v; }

private:
  llvm::DISubprogram *v_;
};

class ObLLVMDILocalVariable
{
public:
  ObLLVMDILocalVariable() : v_(NULL) {}
  virtual ~ObLLVMDILocalVariable() {}

  TO_STRING_KV(KP_(v))

  inline const llvm::DILocalVariable *get_v() const { return v_; }
  inline llvm::DILocalVariable *get_v() { return v_; }
  inline void set_v(llvm::DILocalVariable *v) { v_ = v; }

private:
  llvm::DILocalVariable *v_;
};

class ObLLVMDIType
{
public:
  ObLLVMDIType() : v_(NULL) {}
  ObLLVMDIType(llvm::DIType *v) : v_(v) {}
  virtual ~ObLLVMDIType() {}

  TO_STRING_KV(KP_(v))

  inline const llvm::DIType *get_v() const { return v_; }
  inline llvm::DIType *get_v() { return v_; }
  inline void set_v(llvm::DIType *v) { v_ = v; }
  uint64_t get_size_bits();
  uint64_t get_align_bits();

protected:
  llvm::DIType *v_;
};

class ObLLVMDISubroutineType
{
public:
  ObLLVMDISubroutineType() : v_(NULL) {}
  ObLLVMDISubroutineType(llvm::DISubroutineType *v) : v_(v) {}
  virtual ~ObLLVMDISubroutineType() {}

  TO_STRING_KV(KP_(v))

  inline const llvm::DISubroutineType *get_v() const { return v_; }
  inline llvm::DISubroutineType *get_v() { return v_; }
  inline void set_v(llvm::DISubroutineType *v) { v_ = v; }

protected:
  llvm::DISubroutineType *v_;
};

class ObLLVMDIHelper
{
public:
  ObLLVMDIHelper(common::ObIAllocator &allocator)
  : allocator_(allocator),
    jc_(NULL)
  {}
  virtual ~ObLLVMDIHelper() {}

public:
  int init(core::JitContext *jc);
  int create_compile_unit(const char *name);
  int create_file();
  int create_file(const char *name, const char *path);
  int create_function(const char *name, ObLLVMDISubroutineType &subroutine_type,
                      ObLLVMDISubprogram &subprogram);
  int create_local_variable(const common::ObString &name, uint32_t arg_no, uint32_t line,
                            ObLLVMDIType &type, ObLLVMDILocalVariable &variable);
  int insert_declare(ObLLVMValue &storage, ObLLVMDILocalVariable &variable,
                     ObLLVMBasicBlock &block);
  int finalize();

  int create_pointer_type(ObLLVMDIType &pointee_type, ObLLVMDIType &pointer_type);
  int create_basic_type(common::ObObjType obj_type, ObLLVMDIType &basic_type);
  int create_member_type(const common::ObString &name, uint64_t offset_bits, uint32_t line,
                         ObLLVMDIType &base_type, ObLLVMDIType &member_type);
  int create_struct_type(const common::ObString &name, unsigned line,
                         uint64_t size_bits, uint64_t align_bits,
                         common::ObIArray<ObLLVMDIType> &member_types,
                         ObLLVMDIType &struct_type);
  int create_array_type(ObLLVMDIType &base_type, int64_t count,
                        ObLLVMDIType &struct_type);
  int create_subroutine_type(common::ObIArray<ObLLVMDIType> &member_types,
                             ObLLVMDISubroutineType &subroutine_type);

  int get_current_scope(ObLLVMDIScope &scope);

private:
  common::ObIAllocator &allocator_;
  core::JitDIContext *jc_;

  struct ObDIBasicTypeAttr
  {
    const char *name_;
    uint64_t size_bits_;
    uint64_t align_bits_;
    unsigned encoding_;
  };
  static ObDIBasicTypeAttr basic_type_[common::ObMaxType];
};

} // namespace jit
} // namespace oceanbase

#endif /* OCEANBASE_DEPS_OBJIT_INCLUDE_OBJIT_OB_LLVM_DI_HELPER_H_ */
