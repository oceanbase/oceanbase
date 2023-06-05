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

#ifndef OCEANBASE_DEPS_OBJIT_INCLUDE_OBJIT_OB_LLVM_HELPER_H_
#define OCEANBASE_DEPS_OBJIT_INCLUDE_OBJIT_OB_LLVM_HELPER_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_fast_array.h"
#include "lib/string/ob_string.h"
#include "common/object/ob_object.h"

namespace llvm
{
class Type;
class Value;
class StructType;
class ArrayType;
class Constant;
class ConstantDataArray;
class ConstantInt;
class ConstantStruct;
class GlobalVariable;
class FunctionType;
class Function;
class BasicBlock;
class LandingPadInst;
class SwitchInst;
class DWARFContext;
};

namespace oceanbase
{
namespace jit
{

namespace core {
class JitContext;
class ObOrcJit;
class ObJitMemoryManager;
class ObJitAllocator;
class ObDIRawData;
class ObDWARFContext;
}

class ObDIRawData
{
public:
  ObDIRawData() : debug_info_data_(NULL), debug_info_size_(0) {}

  void set_data(char* data) { debug_info_data_ = data; }
  void set_size(int64_t size) { debug_info_size_ = size; }

  char* get_data() { return debug_info_data_; }
  int64_t get_size() { return debug_info_size_; }

  bool empty() { return debug_info_size_ <= 0; }

private:
  char* debug_info_data_;
  int64_t debug_info_size_;
};

class ObLLVMType
{
public:
  ObLLVMType() : v_(NULL) {}
  ObLLVMType(llvm::Type *v) : v_(v) {}
  virtual ~ObLLVMType() {}

  TO_STRING_KV(KP_(v))

  inline const llvm::Type *get_v() const { return v_; }
  inline llvm::Type *&get_v() { return v_; }
  inline void set_v(llvm::Type *v) { v_ = v; }
  inline void reset() { v_ = NULL; }

public:
  int get_pointer_to(ObLLVMType &result);
  int same_as(ObLLVMType &other, bool &same);

//For Debug
public:
  int64_t get_id() const;
  int64_t get_width() const;
  int64_t get_num_child() const;
  ObLLVMType get_child(int64_t i) const;

protected:
  llvm::Type *v_;
};

class ObLLVMValue
{
public:
  ObLLVMValue() : v_(NULL) {}
  ObLLVMValue(llvm::Value *v) : v_(v) {}
  virtual ~ObLLVMValue() {}

  TO_STRING_KV(KP_(v))

  inline const llvm::Value *get_v() const { return v_; }
  inline llvm::Value *get_v() { return v_; }
  inline void set_v(llvm::Value *v) { v_ = v; }
  inline void reset() { v_ = NULL; }

public:
  int get_type(ObLLVMType &result) const;
  int set_name(const common::ObString &name);

//For Debug
public:
  ObLLVMType get_type() const;
  int64_t get_type_id() const;

protected:
  llvm::Value *v_;
};

class ObLLVMStructType : public ObLLVMType
{
public:
  ObLLVMStructType() : ObLLVMType(NULL) {}
  ObLLVMStructType(llvm::Type *v) : ObLLVMType(v) {}
  virtual ~ObLLVMStructType() {}

  inline llvm::StructType *get_v() { return reinterpret_cast<llvm::StructType *>(v_); }
  inline void set_v(llvm::StructType *v) { v_ = reinterpret_cast<llvm::Type *>(v); }

private:
//  llvm::StructType *v_;
};

class ObLLVMArrayType : public ObLLVMType
{
public:
  ObLLVMArrayType() : ObLLVMType(NULL) {}
  virtual ~ObLLVMArrayType() {}

  inline llvm::ArrayType *get_v() { return reinterpret_cast<llvm::ArrayType *>(v_); }
  inline void set_v(llvm::ArrayType *v) { v_ = reinterpret_cast<llvm::Type *>(v); }

public:
  static int get(const ObLLVMType &elem_type, uint64_t size, ObLLVMType &type);

private:
//  llvm::ArrayType *v_;
};

class ObLLVMConstant : public ObLLVMValue
{
public:
  ObLLVMConstant() : ObLLVMValue(NULL) {}
  ObLLVMConstant(llvm::Value *v) : ObLLVMValue(v) {}
  virtual ~ObLLVMConstant() {}

  inline llvm::Constant *get_v() { return reinterpret_cast<llvm::Constant *>(v_); }
  inline void set_v(llvm::Constant *v) { v_ = reinterpret_cast<llvm::Value *>(v); }

public:
  static int get_null_value(const ObLLVMType &type, ObLLVMConstant &result);

private:
//  llvm::Constant *v_;
};

class ObLLVMConstantDataArray : public ObLLVMConstant
{
public:
  ObLLVMConstantDataArray() : ObLLVMConstant(NULL) {}
  virtual ~ObLLVMConstantDataArray() {}

  inline llvm::ConstantDataArray *get_v() { return reinterpret_cast<llvm::ConstantDataArray *>(v_); }
  inline void set_v(llvm::ConstantDataArray *v) { v_ = reinterpret_cast<llvm::Value *>(v); }

private:
//  llvm::ConstantDataArray *v_;
};

class ObLLVMConstantInt : public ObLLVMConstant
{
public:
  ObLLVMConstantInt() : ObLLVMConstant(NULL) {}
  virtual ~ObLLVMConstantInt() {}

  inline llvm::ConstantInt *get_v() { return reinterpret_cast<llvm::ConstantInt *>(v_); }
  inline void set_v(llvm::ConstantInt *v) { v_ = reinterpret_cast<llvm::Value *>(v); }

private:
//  llvm::ConstantInt *v_;
};

class ObLLVMConstantStruct : public ObLLVMConstant
{
public:
  ObLLVMConstantStruct() : ObLLVMConstant(NULL) {}
  virtual ~ObLLVMConstantStruct() {}

  inline llvm::ConstantStruct *get_v() { return reinterpret_cast<llvm::ConstantStruct *>(v_); }
  inline void set_v(llvm::ConstantStruct *v) { v_ = reinterpret_cast<llvm::Value *>(v); }

public:
  static int get(ObLLVMStructType &type, common::ObIArray<ObLLVMConstant> &elem_values,  ObLLVMConstant &result);

private:
//  llvm::ConstantStruct *v_;
};

class ObLLVMGlobalVariable : public ObLLVMConstant
{
public:
  ObLLVMGlobalVariable() : ObLLVMConstant(NULL) {}
  virtual ~ObLLVMGlobalVariable() {}

  inline llvm::GlobalVariable *get_v() { return reinterpret_cast<llvm::GlobalVariable *>(v_); }
  inline void set_v(llvm::GlobalVariable *v) { v_ = reinterpret_cast<llvm::Value *>(v); }

public:
  int set_constant();
  int set_initializer(ObLLVMConstant &value);

private:
//  llvm::GlobalVariable *v_;
};

class ObLLVMFunctionType
{
public:
  ObLLVMFunctionType() : v_(NULL) {}
  virtual ~ObLLVMFunctionType() {}

  TO_STRING_KV(KP_(v))

  inline const llvm::FunctionType *get_v() const { return v_; }
  inline llvm::FunctionType *get_v() { return v_; }
  inline void set_v(llvm::FunctionType *v) { v_ = v; }
  inline void reset() { v_ = NULL; }

public:
  static int get(const ObLLVMType &ret_type, common::ObIArray<ObLLVMType> &arg_types, ObLLVMFunctionType &result);

private:
  llvm::FunctionType *v_;
};

class ObLLVMDISubprogram;
class ObLLVMFunction
{
public:
  ObLLVMFunction() : v_(NULL) {}
  virtual ~ObLLVMFunction() {}

  TO_STRING_KV(KP_(v))

  inline const llvm::Function *get_v() const { return v_; }
  inline llvm::Function *get_v() { return v_; }
  inline void set_v(llvm::Function *v) { v_ = v; }
  inline void reset() { v_ = NULL; }

public:
  int set_personality(ObLLVMFunction &func);
  int get_argument_size(int64_t &size);
  int get_argument(int64_t idx, ObLLVMValue &arg);
  int set_subprogram(ObLLVMDISubprogram *di_subprogram);

private:
  llvm::Function *v_;
};

class ObLLVMBasicBlock
{
public:
  ObLLVMBasicBlock() : v_(NULL) {}
  virtual ~ObLLVMBasicBlock() {}

  TO_STRING_KV(KP_(v))

  inline const llvm::BasicBlock *get_v() const { return v_; }
  inline llvm::BasicBlock *get_v() { return v_; }
  inline void set_v(llvm::BasicBlock *v) { v_ = v; }
  inline void reset() { v_ = NULL; }

public:
  bool is_terminated();

private:
  llvm::BasicBlock *v_;
};

class ObLLVMLandingPad : public ObLLVMValue
{
public:
  ObLLVMLandingPad() : ObLLVMValue(NULL) {}
  virtual ~ObLLVMLandingPad() {}

  inline llvm::LandingPadInst *get_v() { return reinterpret_cast<llvm::LandingPadInst *>(v_); }
  inline void set_v(llvm::LandingPadInst *v) { v_ = reinterpret_cast<llvm::Value *>(v); }

public:
  int set_cleanup();
  int add_clause(ObLLVMConstant &clause);

private:
//  llvm::LandingPadInst *v_;
};

class ObLLVMSwitch : public ObLLVMValue
{
public:
  ObLLVMSwitch() : ObLLVMValue(NULL) {}
  virtual ~ObLLVMSwitch() {}

  TO_STRING_KV(KP_(v))

  inline llvm::SwitchInst *get_v() { return reinterpret_cast<llvm::SwitchInst *>(v_); }
  inline void set_v(llvm::SwitchInst *v) { v_ = reinterpret_cast<llvm::Value *>(v); }

public:
  int add_case(ObLLVMConstantInt &clause, ObLLVMBasicBlock &block);
  int add_case(const ObLLVMValue &clause, ObLLVMBasicBlock &block);

private:
//  llvm::SwitchInst *v_;
};

class ObLLVMDIScope;
class ObLLVMHelper
{
public:
  enum CMPTYPE {
    ICMP_EQ, //< equal
    ICMP_NE, //< not equal
    ICMP_UGT, //< unsigned greater than
    ICMP_UGE, //< unsigned greater or equal
    ICMP_ULT, //< unsigned less than
    ICMP_ULE, //< unsigned less or equal
    ICMP_SGT, //< signed greater than
    ICMP_SGE, //< signed greater or equal
    ICMP_SLT, //< signed less than
    ICMP_SLE, //< signed less or equal
  };

public:
  ObLLVMHelper(common::ObIAllocator &allocator)
    : allocator_(allocator),
      jc_(NULL),
      jit_(NULL){}
  virtual ~ObLLVMHelper();
  int init();
  void final();
  static int initialize();
  void compile_module(bool optimization = true);
  void dump_module();
  void dump_debuginfo();
  int verify_function(ObLLVMFunction &function);
  int verify_module();
  uint64_t get_function_address(const common::ObString &name);
  static void add_symbol(const common::ObString &name, void *value);

  ObDIRawData get_debug_info() const;

public:
  //指令
  int create_br(const ObLLVMBasicBlock &dest);
  int create_cond_br(ObLLVMValue &value, ObLLVMBasicBlock &true_dest, ObLLVMBasicBlock &false_dest);
  int create_call(const common::ObString &name, ObLLVMFunction &callee, common::ObIArray<ObLLVMValue> &args, ObLLVMValue &result);
  int create_call(const common::ObString &name, ObLLVMFunction &callee, common::ObIArray<ObLLVMValue> &args);
  int create_call(const common::ObString &name, ObLLVMFunction &callee, const ObLLVMValue &arg, ObLLVMValue &result);
  int create_call(const common::ObString &name, ObLLVMFunction &callee, const ObLLVMValue &arg);
  int create_invoke(const common::ObString &name, ObLLVMFunction &callee, common::ObIArray<ObLLVMValue> &args, const ObLLVMBasicBlock &normal_dest, const ObLLVMBasicBlock &unwind_dest, ObLLVMValue &result);
  int create_invoke(const common::ObString &name, ObLLVMFunction &callee, common::ObIArray<ObLLVMValue> &args, const ObLLVMBasicBlock &normal_dest, const ObLLVMBasicBlock &unwind_dest);
  int create_invoke(const common::ObString &name, ObLLVMFunction &callee, ObLLVMValue &arg, const ObLLVMBasicBlock &normal_dest, const ObLLVMBasicBlock &unwind_dest, ObLLVMValue &result);
  int create_invoke(const common::ObString &name, ObLLVMFunction &callee, ObLLVMValue &arg, const ObLLVMBasicBlock &normal_dest, const ObLLVMBasicBlock &unwind_dest);
  int create_alloca(const common::ObString &name, const ObLLVMType &type, ObLLVMValue &result);
  int create_store(const ObLLVMValue &src, ObLLVMValue &dest);
  int create_load(const common::ObString &name, ObLLVMValue &ptr, ObLLVMValue &result);
  int create_ialloca(const common::ObString &name, common::ObObjType obj_type, int64_t default_value, ObLLVMValue &result);
  int create_istore(int64_t i, ObLLVMValue &dest);
  int create_icmp_eq(ObLLVMValue &value, int64_t i, ObLLVMValue &result);
  int create_icmp_slt(ObLLVMValue &value, int64_t i, ObLLVMValue &result);
  int create_icmp(ObLLVMValue &value, int64_t i, CMPTYPE type, ObLLVMValue &result);
  int create_icmp(ObLLVMValue &value1, ObLLVMValue &value2,  CMPTYPE type, ObLLVMValue &result);
  int create_inc(ObLLVMValue &value1, ObLLVMValue &result);
  int create_dec(ObLLVMValue &value1, ObLLVMValue &result);
  int create_add(ObLLVMValue &value1, ObLLVMValue &value2, ObLLVMValue &result);
  int create_add(ObLLVMValue &value1, int64_t &value2, ObLLVMValue &result);
  int create_sub(ObLLVMValue &value1, ObLLVMValue &value2, ObLLVMValue &result);
  int create_sub(ObLLVMValue &value1, int64_t &value2, ObLLVMValue &result);
  int create_ret(ObLLVMValue &value);
  int create_gep(const common::ObString &name, ObLLVMValue &value, common::ObIArray<int64_t> &idxs, ObLLVMValue &result);
  int create_gep(const common::ObString &name, ObLLVMValue &value, common::ObIArray<ObLLVMValue> &idxs, ObLLVMValue &result);
  int create_gep(const common::ObString &name, ObLLVMValue &value, int64_t idx, ObLLVMValue &result);
  int create_gep(const common::ObString &name, ObLLVMValue &value, ObLLVMValue &idx, ObLLVMValue &result);
  int create_extract_value(const common::ObString &name, ObLLVMValue &value, uint64_t idx, ObLLVMValue &result);
  int create_const_gep1_64(const common::ObString &name, ObLLVMValue &value, uint64_t idx, ObLLVMValue &result);
  int create_ptr_to_int(const common::ObString &name, const ObLLVMValue &value, const ObLLVMType &type, ObLLVMValue &result);
  int create_int_to_ptr(const common::ObString &name, const ObLLVMValue &value, const ObLLVMType &type, ObLLVMValue &result);
  int create_bit_cast(const common::ObString &name, const ObLLVMValue &value, const ObLLVMType &type, ObLLVMValue &result);
  int create_pointer_cast(const common::ObString &name, const ObLLVMValue &value, const ObLLVMType &type, ObLLVMValue &result);
  int create_addr_space_cast(const common::ObString &name, const ObLLVMValue &value, const ObLLVMType &type, ObLLVMValue &result);
  int create_sext(const common::ObString &name, const ObLLVMValue &value, const ObLLVMType &type, ObLLVMValue &result);
  int create_sext_or_bitcast(const common::ObString &name, const ObLLVMValue &value, const ObLLVMType &type, ObLLVMValue &result);
  int create_landingpad(const common::ObString &name, ObLLVMType &type, ObLLVMLandingPad &result);
  int create_switch(ObLLVMValue &value, ObLLVMBasicBlock &default_block, ObLLVMSwitch &result);
  int create_resume(ObLLVMValue &value);
  int create_unreachable();

  int create_global_string(const common::ObString &str, ObLLVMValue &result);
  int set_insert_point(const ObLLVMBasicBlock &block);
  int set_debug_location(uint32_t line, uint32_t col, ObLLVMDIScope *scope);
  int unset_debug_location(ObLLVMDIScope *scope);
  int get_or_insert_global(const common::ObString &name, ObLLVMType &type, ObLLVMValue &result);
  int stack_save(ObLLVMValue &stack);
  int stack_restore(ObLLVMValue &stack);

  static int get_null_const(const ObLLVMType &type, ObLLVMValue &result);
  static int get_array_type(const ObLLVMType &elem_type, uint64_t size, ObLLVMType &type);
  int get_uint64_array(const common::ObIArray<uint64_t> &elem_values, ObLLVMValue &result);
  int get_string(const common::ObString &str, ObLLVMValue &result);
  int get_global_string(ObLLVMValue &const_string, ObLLVMValue &result);
  static int get_const_struct(ObLLVMType &type, common::ObIArray<ObLLVMValue> &elem_values, ObLLVMValue &result);

  int get_function_type(ObLLVMType &ret_type, common::ObIArray<ObLLVMType> &arg_types, ObLLVMType &result);
  int create_function(const common::ObString &name, ObLLVMFunctionType &type, ObLLVMFunction &function);
  int create_block(const common::ObString &name, ObLLVMFunction &parent, ObLLVMBasicBlock &block);
  int create_struct_type(const common::ObString &name, common::ObIArray<ObLLVMType> &elem_types, ObLLVMType &type);

public:
  int get_llvm_type(common::ObObjType obj_type, ObLLVMType &type);
  int get_void_type(ObLLVMType &type);
  int get_int8(int64_t value, ObLLVMValue &result);
  int get_int16(int64_t value, ObLLVMValue &result);
  int get_int32(int64_t value, ObLLVMValue &result);
  int get_int64(int64_t value, ObLLVMValue &result);
  int get_int_value(const ObLLVMType &value, int64_t i, ObLLVMValue &i_value);
  int get_insert_block(ObLLVMBasicBlock &block);

public:
  core::JitContext *get_jc() { return jc_; }

private:
  int check_insert_point(bool &is_valid);
  static int init_llvm();

private:
  common::ObIAllocator &allocator_;
  core::JitContext *jc_;
  core::ObOrcJit *jit_;
};

typedef common::ObFastArray<ObLLVMType, 8> ObLLVMTypeArray;
typedef common::ObFastArray<ObLLVMValue, 8> ObLLVMValueArray;


/*******************************LLVM DWARF MAPPING**********************************/
//mapping from llvm/Support/Dwarf.h
enum LLVMDwarFConstants {
  // Children flag
  DW_CHILDREN_no = 0x00,
  DW_CHILDREN_yes = 0x01,

  DW_EH_PE_absptr = 0x00,
  DW_EH_PE_omit = 0xff,
  DW_EH_PE_uleb128 = 0x01,
  DW_EH_PE_udata2 = 0x02,
  DW_EH_PE_udata4 = 0x03,
  DW_EH_PE_udata8 = 0x04,
  DW_EH_PE_sleb128 = 0x09,
  DW_EH_PE_sdata2 = 0x0A,
  DW_EH_PE_sdata4 = 0x0B,
  DW_EH_PE_sdata8 = 0x0C,
  DW_EH_PE_signed = 0x08,
  DW_EH_PE_pcrel = 0x10,
  DW_EH_PE_textrel = 0x20,
  DW_EH_PE_datarel = 0x30,
  DW_EH_PE_funcrel = 0x40,
  DW_EH_PE_aligned = 0x50,
  DW_EH_PE_indirect = 0x80
};


class ObDIEAddress
{
public:
  ObDIEAddress()
    : lowpc_(common::OB_INVALID_ID), highpc_(common::OB_INVALID_ID) {}
  ObDIEAddress(int64_t lowpc, int64_t highpc, common::ObString name)
    : lowpc_(lowpc), highpc_(highpc), name_(name) {}

  bool operator==(const ObDIEAddress &other) const
  {
    return lowpc_ == other.lowpc_
      && highpc_ == other.highpc_
      && name_ == other.name_;
  }

  bool valid() const
  {
    return lowpc_ != common::OB_INVALID_ID
            && highpc_ != common::OB_INVALID_ID;
  }

  int64_t lowpc() const { return lowpc_; }
  int64_t highpc() const { return highpc_; }
  common::ObString name() const { return name_; }

  TO_STRING_KV(K(lowpc_), K(highpc_), K(name_));

  int64_t lowpc_;
  int64_t highpc_;
  common::ObString name_;
};

class ObLineAddress
{
public:
  ObLineAddress() : address_(-1), line_(-1), module_() {}
  ObLineAddress(int64_t address, int64_t line, common::ObString module)
    : address_(address), line_(line), module_(module) {}

  int64_t get_address() { return address_; }
  int64_t get_line() { return line_; }
  common::ObString &get_module() { return module_; }

  TO_STRING_KV(K(address_), K(line_), K(module_));

private:
  int64_t address_;
  int64_t line_;
  common::ObString module_;
};

class ObDWARFHelper
{
public:
  ObDWARFHelper(ObIAllocator &allocator, char* debug_buf, int64_t debug_len)
    : Allocator(allocator), DebugBuf(debug_buf), DebugLen(debug_len), Context(nullptr) {}
  ~ObDWARFHelper() {}

  int init();

  int find_all_line_address(common::ObIArray<ObLineAddress>&);
  int find_function_from_pc(uint64_t pc, ObDIEAddress &func);
  int find_function_from_pc(uint64_t pc, bool &found);
  int find_line_by_addr(int64_t, int&, bool&);
  int find_address_by_function_line(const common::ObString&, int, int64_t&);
  int find_function_line_by_address(int64_t&, common::ObString&, int&);

  static int dump(char* DebugBuf, int64_t DebugLen);

  TO_STRING_EMPTY();

private:
  common::ObIAllocator &Allocator;
  char *DebugBuf;
  int64_t DebugLen;
  core::ObDWARFContext *Context;
};

}
}

#endif /* OCEANBASE_DEPS_OBJIT_INCLUDE_OBJIT_OB_LLVM_HELPER_H_ */
