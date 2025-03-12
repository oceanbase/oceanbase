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
#include "core/ob_orc_jit.h"

#ifdef CPP_STANDARD_20
#include "llvm/ExecutionEngine/JITSymbol.h"
#else
#include "llvm/ExecutionEngine/Orc/LambdaResolver.h"
#endif
#include "core/ob_pl_ir_compiler.h"
#include "llvm/ExecutionEngine/ExecutionEngine.h"

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

DenseMap<StringRef, JITTargetAddress> ObJitGlobalSymbolGenerator::symbol_table;

std::pair<lib::ObMutex, ObNotifyLoaded::KeyEntryMap> ObNotifyLoaded::AllGdbReg;

ObOrcJit::ObOrcJit(common::ObIAllocator &Allocator)
  : DebugBuf(nullptr),
    DebugLen(0),
    JITAllocator(),
    NotifyLoaded(Allocator, DebugBuf, DebugLen, SoObject),
    ObTM(EngineBuilder().selectTarget()),
    ObDL(ObTM->createDataLayout()),
    ObEngineBuilder(),
    ObJitEngine()
{ }

int ObOrcJit::init()
{
  int ret = OB_SUCCESS;

    ObEngineBuilder.setObjectLinkingLayerCreator(
    [this](ExecutionSession &ES, const Triple &TT) {
      auto ObjLinkingLayer =
          std::make_unique<RTDyldObjectLinkingLayer>(
            ES,
            [&]() {
              return std::make_unique<ObJitMemoryManager>(JITAllocator);
          });

      ObjLinkingLayer->registerJITEventListener(NotifyLoaded);
      return ObjLinkingLayer;
    });

    ObEngineBuilder.setCompileFunctionCreator(
      [this] (JITTargetMachineBuilder JTMB)
          -> Expected<std::unique_ptr<IRCompileLayer::IRCompiler>> {
        auto tm = JTMB.createTargetMachine();
        if (!tm) {
          return tm.takeError();
        }
        return std::make_unique<ObPLIRCompiler>(*this, std::move(*tm));
      }
    );

    auto tm_builder_wrapper = JITTargetMachineBuilder::detectHost();

    if (!tm_builder_wrapper) {
      Error err = tm_builder_wrapper.takeError();
      std::string msg = toString(std::move(err));
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get target machine", K(msg.c_str()));
    } else {
      ObEngineBuilder.setJITTargetMachineBuilder(*tm_builder_wrapper);
    }

  return ret;
}

int ObOrcJit::addModule(std::unique_ptr<Module> M, std::unique_ptr<ObLLVMContext> TheContext)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(create_jit_engine())) {
    LOG_WARN("failed to create jit engine", K(ret));
  } else if (OB_ISNULL(ObJitEngine)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL jit engine", K(ret), K(lbt()));
  } else {
    Error err = ObJitEngine->addIRModule(ThreadSafeModule{std::move(M), std::move(TheContext)});

    if (err) {
      std::string msg = toString(std::move(err));

      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to add module to jit engine",
               K(ret), K(msg.c_str()));
    }
  }

  return ret;
}

int ObOrcJit::lookup(const std::string &name, ObJITSymbol &symbol)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ObJitEngine)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL jit engine", K(ret), K(lbt()));
  } else {
    auto value = ObJitEngine->lookup(name);

    if (!value) {
      Error err = value.takeError();

      if (err.isA<SymbolsNotFound>()) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        ret = OB_ERR_UNEXPECTED;
      }

      std::string msg = toString(std::move(err));
      LOG_WARN("failed to lookup symbol in jit engine",
        K(ret),
        "name", name.c_str(),
        "msg", msg.c_str());
    } else {
#ifdef CPP_STANDARD_20
      symbol = JITEvaluatedSymbol(value->getValue(), JITSymbolFlags::Exported);
#else
      symbol = *value;
#endif
    }
  }

  return ret;
}

int ObOrcJit::get_function_address(const std::string &name, uint64_t &addr)
{
  int ret = OB_SUCCESS;

  ObJITSymbol sym = nullptr;

  if (OB_FAIL(lookup(name, sym))) {
    LOG_WARN("failed to lookup symbol addr", K(name.c_str()));
  } else {
    auto value = sym.getAddress();

    if (!value) {
      Error err = value.takeError();
      std::string msg = toString(std::move(err));

      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get symbol address",
               K(ret),
               "name", name.c_str(),
               "msg", msg.c_str());
    } else {
      addr = static_cast<uint64_t>(*value);
    }
  }

  return ret;
}

void ObNotifyLoaded::notifyObjectLoaded(
  ObObjectKey Key,
  const object::ObjectFile &Obj,
  const RuntimeDyld::LoadedObjectInfo &Info)
{
  char *obj_buf = static_cast<char*>(Allocator.alloc(Obj.getData().size()));
  if (OB_NOT_NULL(obj_buf)) {
    MEMCPY(obj_buf, Obj.getData().data(), Obj.getData().size());
    SoObject.assign_ptr(obj_buf, Obj.getData().size());
  }

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

    registerDebugInfoToGdb(Key);
  }
}

void ObNotifyLoaded::notifyFreeingObject(ObObjectKey Key)
{
  if (OB_NOT_NULL(DebugBuf)) {
    deregisterDebugInfoFromGdb(Key);
  }
}

int ObNotifyLoaded::initGdbHelper()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(AllGdbReg.first);

  if (OB_FAIL(AllGdbReg.second.create(1024, ObMemAttr(OB_SYS_TENANT_ID, "PlGdbHelper")))) {
    LOG_WARN("failed to create AllGdbReg map", K(ret));
  }

  return ret;
}

void ObNotifyLoaded::registerDebugInfoToGdb(ObObjectKey Key)
{
  int ret = OB_SUCCESS;

  lib::ObMutexGuard guard(AllGdbReg.first);
  jit_code_entry buffer = {nullptr, nullptr, DebugBuf, static_cast<uint64_t>(DebugLen)};
  jit_code_entry *entry = nullptr;

  if (OB_FAIL(AllGdbReg.second.set_refactored(Key, buffer))) {
    LOG_WARN("failed to set_refactored to AllGdbReg", K(ret));
  } else if (OB_ISNULL(entry = AllGdbReg.second.get(Key))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL entry after set_refactored", K(ret));
  } else {
    jit_code_entry *next = __jit_debug_descriptor.first_entry;
    if (OB_NOT_NULL(next)) {
      next->prev_entry = entry;
    }

    entry->prev_entry = nullptr;
    entry->next_entry = next;

    __jit_debug_descriptor.first_entry = entry;

    __jit_debug_descriptor.relevant_entry = entry;
    __jit_debug_descriptor.action_flag = JIT_REGISTER_FN;
    __jit_debug_register_code();
  }

  LOG_DEBUG("finished registerDebugInfoToGdb", K(ret), K(Key), K(AllGdbReg.second.size()));
}

void ObNotifyLoaded::deregisterDebugInfoFromGdb(ObObjectKey Key)
{
  int ret = OB_SUCCESS;

  lib::ObMutexGuard guard(AllGdbReg.first);

  jit_code_entry *entry = AllGdbReg.second.get(Key);

  if (OB_NOT_NULL(entry)) {
    jit_code_entry *prev = entry->prev_entry;
    jit_code_entry *next = entry->next_entry;

    if (OB_NOT_NULL(prev)) {
      prev->next_entry = next;
    } else {
      __jit_debug_descriptor.first_entry = next;
    }

    if (OB_NOT_NULL(next)) {
      next->prev_entry = prev;
    }

    __jit_debug_descriptor.relevant_entry = entry;
    __jit_debug_descriptor.action_flag = JIT_UNREGISTER_FN;
    __jit_debug_register_code();

    if (OB_FAIL(AllGdbReg.second.erase_refactored(Key))) {
      LOG_WARN("failed to erase jit entry from hashmap", K(ret));
    }
  }

  LOG_DEBUG("finished deregisterDebugInfoFromGdb", K(ret), K(Key), K(entry), K(AllGdbReg.second.size()));
}

int ObOrcJit::add_compiled_object(size_t length, const char *ptr)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(create_jit_engine())) {
    LOG_WARN("failed to create jit engine", K(ret));
  } else if (OB_ISNULL(ObJitEngine)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL jit engine", K(ret), K(lbt()));
  } else {
    Error err =ObJitEngine->addObjectFile(
                MemoryBuffer::getMemBuffer(StringRef(ptr, length), "", false));

    if (err) {
      std::string msg = toString(std::move(err));

      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to add compile result to jit engine",
               K(ret), K(msg.c_str()), K(length), K(ptr));
    }
  }

  return ret;
}

int ObOrcJit::set_optimize_level(ObPLOptLevel level)
{
  int ret = OB_SUCCESS;

  if (level <= ObPLOptLevel::INVALID || level > ObPLOptLevel::O3) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected PLSQL_OPTIMIZE_LEVEL", K(ret), K(level), K(lbt()));
  }

  if (OB_SUCC(ret) && level == ObPLOptLevel::O0) {
    auto &tm_builder = ObEngineBuilder.getJITTargetMachineBuilder();
#ifdef CPP_STANDARD_20
    if (!tm_builder.has_value()) {
#else
    if (!tm_builder.hasValue()) {
#endif
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL JITTargetMachineBuilder", K(ret), K(lbt()));
    } else {
      auto &builder = *tm_builder;
      builder.setCodeGenOptLevel(CodeGenOpt::Level::None);
      builder.getOptions().EnableFastISel = true;
    }
  }

  return ret;
}

int ObOrcJit::create_jit_engine()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(ObJitEngine)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NOT NULL jit engine", K(ret), K(lbt()));
  } else {
    std::unique_ptr<ObJitGlobalSymbolGenerator> symbol_generator = nullptr;

    auto engine_wrapper = ObEngineBuilder.create();

    if (!engine_wrapper) {
      Error err = engine_wrapper.takeError();
      std::string msg = toString(std::move(err));

      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to build LLVM JIT engine", K(msg.c_str()));
    } else {
      ObJitEngine = std::move(*engine_wrapper);
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(ObJitEngine)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL jit engine", K(ret));
    } else if (OB_FAIL(ob_jit_make_unique(symbol_generator))) {
      LOG_WARN("failed to make ObJitGlobalSymbolGenerator unique_ptr", K(ret));
    } else {
      ObJitEngine->getMainJITDylib().addGenerator(std::move(symbol_generator));
    }
  }

  return ret;
}

} // namespace core
} // objit
} // oceanbase
