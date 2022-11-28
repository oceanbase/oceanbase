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

#ifndef OBJIT_CORE_ORCJIT_H
#define OBJIT_CORE_ORCJIT_H

#include "llvm/ExecutionEngine/JITSymbol.h"
#include "llvm/ExecutionEngine/Orc/CompileUtils.h"
#include "llvm/ExecutionEngine/Orc/IRCompileLayer.h"
#include "llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h"
#include "llvm/ExecutionEngine/ObjectCache.h"
#include "llvm/IR/DataLayout.h"
#include "llvm/DebugInfo/DWARF/DWARFContext.h"
#include "llvm/ExecutionEngine/Orc/ExecutionUtils.h"
#include "llvm/ExecutionEngine/JITEventListener.h"
#include "llvm/IR/Mangler.h"

#include <map>
#include <string>

#include "core/ob_jit_memory_manager.h"

namespace oceanbase {
namespace jit {
namespace core {
using namespace llvm;

class ObNotifyLoaded
{
public:
  explicit ObNotifyLoaded(
    common::ObIAllocator &Allocator, char *&DebugBuf, int64_t &DebugLen)
      : Allocator(Allocator), DebugBuf(DebugBuf), DebugLen(DebugLen) {}
  virtual ~ObNotifyLoaded() {}

  void operator()(llvm::orc::VModuleKey Key,
                  const object::ObjectFile &Obj,
                  const RuntimeDyld::LoadedObjectInfo &Info);
private:
  common::ObIAllocator &Allocator;
  char* &DebugBuf;
  int64_t &DebugLen;
};


#ifndef ORC2
class ObOrcJit
{
public:
  using ObjLayerT = llvm::orc::LegacyRTDyldObjectLinkingLayer;
  using CompileLayerT = llvm::orc::LegacyIRCompileLayer<ObjLayerT, llvm::orc::SimpleCompiler>;

  explicit ObOrcJit(common::ObIAllocator &Allocator);
  virtual ~ObOrcJit() {};

  llvm::orc::VModuleKey addModule(std::unique_ptr<Module> M);
  JITSymbol lookup(const std::string Name);
  uint64_t get_function_address(const std::string Name);

  LLVMContext &getContext() { return TheContext; }
  const DataLayout &getDataLayout() const { return DL; }

  char* get_debug_info_data() { return DebugBuf; }
  int64_t get_debug_info_size() { return DebugLen; }

private:
  std::string mangle(const std::string &Name) {
    std::string MangledName;
    {
      raw_string_ostream MangledNameStream(MangledName);
      Mangler::getNameWithPrefix(MangledNameStream, Name, DL);
    }
    return MangledName;
  }

  JITSymbol findMangledSymbol(const std::string &Name) {
    const bool ExportedSymbolsOnly = true;

    // Search modules in reverse order: from last added to first added.
    // This is the opposite of the usual search order for dlsym, but makes more
    // sense in a REPL where we want to bind to the newest available definition.
    for (auto H : make_range(ModuleKeys.rbegin(), ModuleKeys.rend()))
      if (auto Sym = CompileLayer.findSymbolIn(H, Name, ExportedSymbolsOnly))
        return Sym;

    // If we can't find the symbol in the JIT, try looking in the host process.
    if (auto SymAddr = RTDyldMemoryManager::getSymbolAddressInProcess(Name))
      return JITSymbol(SymAddr, JITSymbolFlags::Exported);

    return nullptr;
  }

private:
  char *DebugBuf;
  int64_t DebugLen;

  ObJitAllocator JITAllocator;
  ObNotifyLoaded NotifyLoaded;

  LLVMContext TheContext;
  llvm::orc::ExecutionSession ES;
  std::shared_ptr<llvm::orc::SymbolResolver> Resolver;
  std::unique_ptr<TargetMachine> TM;
  const DataLayout DL;
  ObjLayerT ObjectLayer;
  CompileLayerT CompileLayer;
  std::vector<llvm::orc::VModuleKey> ModuleKeys;
};

#else
class ObOrcJit
{
public:
  explicit ObOrcJit(
    common::ObIAllocator &Allocator, llvm::orc::JITTargetMachineBuilder JTMB, DataLayout DL);
  virtual ~ObOrcJit() {};

  Error addModule(std::unique_ptr<Module> M);
  Expected<JITEvaluatedSymbol> lookup(StringRef Name);
  uint64_t get_function_address(const std::string Name);

  static ObOrcJit* create(ObIAllocator &allocator)
  {
    auto JTMB = llvm::orc::JITTargetMachineBuilder::detectHost();

    if (!JTMB)
      return nullptr;

    auto DL = JTMB->getDefaultDataLayoutForTarget();
    if (!DL)
      return nullptr;

    return OB_NEWx(ObOrcJit, (&allocator), allocator, std::move(*JTMB), std::move(*DL));
  }

  LLVMContext &getContext() { return *Ctx.getContext(); }

  const DataLayout &getDataLayout() const { return DL; }

  char* get_debug_info_data() { return DebugBuf; }
  int64_t get_debug_info_size() { return DebugLen; }

private:
  char *DebugBuf;
  int64_t DebugLen;

  ObJitAllocator JITAllocator;
  ObNotifyLoaded NotifyLoaded;

  llvm::orc::ExecutionSession ES;
  llvm::orc::RTDyldObjectLinkingLayer ObjectLayer;
  llvm::orc::IRCompileLayer CompileLayer;

  llvm::DataLayout DL;
  llvm::orc::MangleAndInterner Mangle;
  llvm::orc::ThreadSafeContext Ctx;

  llvm::orc::JITDylib &MainJD;
};
#endif

} // core
} // jit
} // oceanbase

#endif /* OBJIT_CORE_ORCJIT_H */
