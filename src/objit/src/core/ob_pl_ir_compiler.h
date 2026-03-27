/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBJIT_CORE_PL_IR_COMPILER_H
#define OBJIT_CORE_PL_IR_COMPILER_H

#include <memory>

#include "llvm/ExecutionEngine/Orc/IRCompileLayer.h"
#include "llvm/ExecutionEngine/Orc/JITTargetMachineBuilder.h"
#include "llvm/ExecutionEngine/Orc/Layer.h"

namespace llvm
{
class MemoryBuffer;
class Module;
class ObjectCache;
class TargetMachine;
} // namespace llvm

namespace oceanbase
{

namespace jit
{

namespace core
{

class ObOrcJit;

llvm::orc::IRSymbolMapper::ManglingOptions
irManglingOptionsFromTargetOptions(const llvm::TargetOptions &Opts);

class ObPLIRCompiler : public llvm::orc::IRCompileLayer::IRCompiler
{
public:
  using CompileResult = std::unique_ptr<llvm::MemoryBuffer>;

  /// Construct a simple compile functor with the given target.
  ObPLIRCompiler(ObOrcJit &Engine,
                 std::unique_ptr<llvm::TargetMachine> TM,
                 llvm::ObjectCache *ObjCache = nullptr)
    : IRCompiler(irManglingOptionsFromTargetOptions(TM->Options)),
      JitEngine(Engine),
      TM(std::move(TM)),
      ObjCache(ObjCache)
    {  }

  /// Set an ObjectCache to query before compiling.
  void setObjectCache(llvm::ObjectCache *NewCache) { ObjCache = NewCache; }

  /// Compile a Module to an ObjectFile.
  llvm::Expected<CompileResult> operator()(llvm::Module &M) override;

private:
  llvm::orc::IRSymbolMapper::ManglingOptions
  manglingOptionsForTargetMachine(const llvm::TargetMachine &TM);

  CompileResult tryToLoadFromObjectCache(const llvm::Module &M);
  void notifyObjectCompiled(const llvm::Module &M, const llvm::MemoryBuffer &ObjBuffer);

private:
  ObOrcJit &JitEngine;
  std::unique_ptr<llvm::TargetMachine> TM;
  llvm::ObjectCache *ObjCache = nullptr;
};

} // namespace core
} // namespace jit
} // namespace oceanbase

#endif // OBJIT_CORE_PL_IR_COMPILER_H
