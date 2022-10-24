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

typedef ::llvm::LLVMContext ObLLVMContext;
typedef ::llvm::orc::ExecutionSession ObExecutionSession;
typedef ::llvm::orc::SymbolResolver ObSymbolResolver;
typedef ::llvm::TargetMachine ObTargetMachine;
typedef ::llvm::DataLayout ObDataLayout;
typedef ::llvm::orc::VModuleKey ObVModuleKey;
typedef ::llvm::JITSymbol ObJITSymbol;

class ObNotifyLoaded
{
public:
  explicit ObNotifyLoaded(
    common::ObIAllocator &Allocator, char *&DebugBuf, int64_t &DebugLen)
      : Allocator(Allocator), DebugBuf(DebugBuf), DebugLen(DebugLen) {}
  virtual ~ObNotifyLoaded() {}

  void operator()(ObVModuleKey Key,
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
  using ObObjLayerT = llvm::orc::LegacyRTDyldObjectLinkingLayer;
  using ObCompileLayerT = llvm::orc::LegacyIRCompileLayer<ObObjLayerT, llvm::orc::SimpleCompiler>;

  explicit ObOrcJit(common::ObIAllocator &Allocator);
  virtual ~ObOrcJit() {};

  ObVModuleKey addModule(std::unique_ptr<Module> M);
  ObJITSymbol lookup(const std::string Name);
  uint64_t get_function_address(const std::string Name);

  ObLLVMContext &getContext() { return TheContext; }
  const ObDataLayout &getDataLayout() const { return ObDL; }

  char* get_debug_info_data() { return DebugBuf; }
  int64_t get_debug_info_size() { return DebugLen; }

private:
  std::string mangle(const std::string &Name)
  {
    std::string MangledName; {
      raw_string_ostream MangledNameStream(MangledName);
      Mangler::getNameWithPrefix(MangledNameStream, Name, ObDL);
    }
    return MangledName;
  }

  ObJITSymbol findMangledSymbol(const std::string &Name)
  {
    const bool ExportedSymbolsOnly = true;
    for (auto H : make_range(ObModuleKeys.rbegin(), ObModuleKeys.rend())) {
      if (auto Sym = ObCompileLayer.findSymbolIn(H, Name, ExportedSymbolsOnly)) {
        return Sym;
      }
    }
    if (auto SymAddr = RTDyldMemoryManager::getSymbolAddressInProcess(Name)) {
      return ObJITSymbol(SymAddr, JITSymbolFlags::Exported);
    }
    return nullptr;
  }

private:
  char *DebugBuf;
  int64_t DebugLen;

  ObJitAllocator JITAllocator;
  ObNotifyLoaded NotifyLoaded;

  ObLLVMContext TheContext;
  ObExecutionSession ObES;
  std::shared_ptr<ObSymbolResolver> ObResolver;
  std::unique_ptr<ObTargetMachine> ObTM;
  const ObDataLayout ObDL;
  ObObjLayerT ObObjectLayer;
  ObCompileLayerT ObCompileLayer;
  std::vector<ObVModuleKey> ObModuleKeys;
};

#else
class ObOrcJit
{
public:
  explicit ObOrcJit(
    common::ObIAllocator &Allocator, llvm::orc::JITTargetMachineBuilder JTMB, ObDataLayout ObDL);
  virtual ~ObOrcJit() {};

  Error addModule(std::unique_ptr<Module> M);
  Expected<JITEvaluatedSymbol> lookup(StringRef Name);
  uint64_t get_function_address(const std::string Name);

  static ObOrcJit* create(ObIAllocator &allocator)
  {
    auto JTMB = llvm::orc::JITTargetMachineBuilder::detectHost();

    if (!JTMB)
      return nullptr;

    auto ObDL = JTMB->getDefaultDataLayoutForTarget();
    if (!ObDL)
      return nullptr;

    return OB_NEWx(ObOrcJit, (&allocator), allocator, std::move(*JTMB), std::move(*ObDL));
  }

  ObLLVMContext &getContext() { return *Ctx.getContext(); }

  const ObDataLayout &getDataLayout() const { return ObDL; }

  char* get_debug_info_data() { return DebugBuf; }
  int64_t get_debug_info_size() { return DebugLen; }

private:
  char *DebugBuf;
  int64_t DebugLen;

  ObJitAllocator JITAllocator;
  ObNotifyLoaded NotifyLoaded;

  llvm::orc::ObExecutionSession ObES;
  llvm::orc::RTDyldObjectLinkingLayer ObObjectLayer;
  llvm::orc::IRCompileLayer ObCompileLayer;

  llvm::ObDataLayout ObDL;
  llvm::orc::MangleAndInterner Mangle;
  llvm::orc::ThreadSafeContext Ctx;

  llvm::orc::JITDylib &MainJD;
};
#endif

} // core
} // jit
} // oceanbase

#endif /* OBJIT_CORE_ORCJIT_H */
