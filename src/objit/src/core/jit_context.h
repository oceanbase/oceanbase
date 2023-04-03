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

#ifndef JIT_CONTEXT_H
#define JIT_CONTEXT_H
#include "core/ob_orc_jit.h"
#include "expr/ob_llvm_type.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/Target/TargetMachine.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"

namespace oceanbase
{
namespace jit
{
namespace core
{
struct JitContext
{
public:
  explicit JitContext()
      : Compile(false), TheContext(nullptr), TheJIT(nullptr)
  {
  }

  int InitializeModule(ObOrcJit &jit);
  void compile();
  int optimize();

  ObLLVMContext& get_context() { return *TheContext; }
  IRBuilder<>& get_builder() { return *Builder; }
  Module& get_module() { return *TheModule; }
  ObOrcJit* get_jit() { return TheJIT; }

public:
  bool Compile;
  
  ObLLVMContext *TheContext;
  std::unique_ptr<IRBuilder<>> Builder;
  std::unique_ptr<Module> TheModule;
  ObOrcJit *TheJIT;
  std::unique_ptr<legacy::FunctionPassManager> TheFPM;
};

class StringMemoryBuffer : public llvm::MemoryBuffer
{
public:
  StringMemoryBuffer(const char *debug_buffer, int64_t debug_length) {
    init(debug_buffer, debug_buffer + debug_length, false);
  }
  BufferKind getBufferKind() const override { return MemoryBuffer_Malloc; }
};

class ObDWARFContext
{
public:
  ObDWARFContext(char* DebugBuf, int64_t DebugLen)
    : MemoryBuf(DebugBuf, DebugLen), MemoryRef(MemoryBuf) {}

  ~ObDWARFContext() {}

  int init();

public:
  StringMemoryBuffer MemoryBuf;
  llvm::MemoryBufferRef MemoryRef;
  std::unique_ptr<llvm::object::Binary> Bin;

  std::unique_ptr<llvm::DWARFContext> Context;
};

} // core
} // jit
} // oceanbase

#endif /* JIT_CONTEXT_H */
