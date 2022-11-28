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

#define USING_LOG_PREFIX SQL_CG
#include "core/ob_orc_jit.h"

#include <iostream>
#include <algorithm>
#include <memory>
#include <string>
#include <vector>
#include <map>
#include <cassert>

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/StringRef.h"
#include "llvm/Support/MemoryBuffer.h"
#include "llvm/Support/raw_os_ostream.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/ExecutionEngine/RuntimeDyld.h"
#include "llvm/ExecutionEngine/Orc/CompileUtils.h"
#include "llvm/ExecutionEngine/Orc/IRCompileLayer.h"
#include "llvm/ExecutionEngine/Orc/LambdaResolver.h"
#include "llvm/IR/DataLayout.h"
#include "llvm/IR/Mangler.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/DynamicLibrary.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/AsmParser/Parser.h"
#include "llvm/Bitcode/BitcodeWriter.h"
#include "llvm/DebugInfo/DWARF/DWARFContext.h"
#include "llvm/Object/ELFObjectFile.h"
#include "llvm/ExecutionEngine/SectionMemoryManager.h"

using namespace llvm;
using namespace llvm::orc;
using namespace llvm::object;
using namespace ::oceanbase::common;

namespace oceanbase
{
namespace jit
{
namespace core
{

#ifndef ORC2
ObOrcJit::ObOrcJit(common::ObIAllocator &Allocator)
  : DebugBuf(nullptr),
    DebugLen(0),
    JITAllocator(),
    NotifyLoaded(Allocator, DebugBuf, DebugLen),
    TheContext(),
    Resolver(createLegacyLookupResolver(
             ES,
             [this](StringRef Name) { return findMangledSymbol(std::string(Name)); },
             [](Error Err) { cantFail(std::move(Err), "lookupFlags failed"); })),
    TM(EngineBuilder().selectTarget()),
    DL(TM->createDataLayout()),
    ObjectLayer(AcknowledgeORCv1Deprecation,
                ES,
                [this](VModuleKey) {
                  return ObjLayerT::Resources{
                    std::make_shared<ObJitMemoryManager>(JITAllocator), Resolver}; },
                NotifyLoaded),
    CompileLayer(AcknowledgeORCv1Deprecation, ObjectLayer, SimpleCompiler(*TM))
{
  llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
}

VModuleKey ObOrcJit::addModule(std::unique_ptr<Module> M)
{
  auto K = ES.allocateVModule();
  cantFail(CompileLayer.addModule(K, std::move(M)));
  ModuleKeys.push_back(K);
  return K;
}

JITSymbol ObOrcJit::lookup(std::string Name)
{
  return findMangledSymbol(mangle(Name));
}

uint64_t ObOrcJit::get_function_address(const std::string Name)
{
  return static_cast<uint64_t>(cantFail(lookup(Name).getAddress()));
}

#else
static ExitOnError ExitOnErr;

ObOrcJit::ObOrcJit(ObIAllocator &Allocator, JITTargetMachineBuilder JTMB, DataLayout DL)
  : DebugBuf(nullptr),
    DebugLen(0),
    JITAllocator(),
    NotifyLoaded(Allocator, DebugBuf, DebugLen),
    ObjectLayer(ES,
                [this]() { return std::make_unique<ObJitMemoryManager>(JITAllocator); }),
    CompileLayer(ES,
                 ObjectLayer,
                 std::make_unique<ConcurrentIRCompiler>(std::move(JTMB))),
    DL(std::move(DL)),
    Mangle(ES, this->DL),
    Ctx(std::make_unique<LLVMContext>()),
    MainJD(ES.createBareJITDylib("<main>"))
{
  /*
  MainJD.define(absoluteSymbols({
    { Mangle("eh_personality"), pointerToJITTargetAddress(&ObPLEH::eh_personality) }
  }));
  */
  MainJD.addGenerator(
        cantFail(DynamicLibrarySearchGenerator::GetForCurrentProcess(
            DL.getGlobalPrefix())));
}

Error ObOrcJit::addModule(std::unique_ptr<Module> M)
{
  return CompileLayer.add(MainJD, ThreadSafeModule(std::move(M), Ctx));
}

Expected<JITEvaluatedSymbol> ObOrcJit::lookup(StringRef Name)
{
  return ES.lookup({&MainJD}, Mangle(Name.str()));
}

uint64_t ObOrcJit::get_function_address(const std::string Name)
{
  std::cerr << "get_function_address : " << Name << std::endl;
  auto Sym = ExitOnErr(lookup(Name));
  std::cerr << "get_function_address finish : " << Name << std::endl;
  return static_cast<uint64_t>(Sym.getAddress());
}
#endif

void ObNotifyLoaded::operator()(
  llvm::orc::VModuleKey Key,
  const object::ObjectFile &Obj,
  const RuntimeDyld::LoadedObjectInfo &Info)
{
  // object::ObjectFile *ObjBinary = Obj.getBinary();
  // if (ObjBinary != nullptr) {
    object::OwningBinary<object::ObjectFile> DebugObj = Info.getObjectForDebug(Obj);
    if (DebugObj.getBinary() != nullptr) {
      const char* TmpDebugBuf
        = DebugObj.getBinary()->getMemoryBufferRef().getBufferStart();
      DebugLen
        = DebugObj.getBinary()->getMemoryBufferRef().getBufferSize();
      if (OB_NOT_NULL(
        DebugBuf = static_cast<char*>(Allocator.alloc(DebugLen)))) {
        std::memcpy(DebugBuf, TmpDebugBuf, DebugLen);
      }
    }
  // }
}

} // core
} // objit
} // oceanbase
