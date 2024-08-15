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
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/ExecutionEngine/Orc/ExecutionUtils.h"
#include "llvm/ExecutionEngine/JITEventListener.h"
#include "llvm/IR/Mangler.h"

#include <string>

#include "core/ob_jit_memory_manager.h"

namespace oceanbase {
namespace jit {

enum class ObPLOptLevel : int
{
  INVALID = -1,
  O0 = 0,
  O1 = 1,
  O2 = 2,
  O3 = 3
};
template<typename T, typename ...Args>
static inline int ob_jit_make_unique(std::unique_ptr<T> &ptr, Args&&... args) {
  int ret = OB_SUCCESS;

  std::unique_ptr<T> result = nullptr;

  try {
    result = std::make_unique<T>(std::forward<Args>(args)...);
  } catch (const std::bad_alloc &e) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "failed to allocate memory", K(ret), K(e.what()));
  } catch (...) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected exception in std::make_unique", K(ret), K(lbt()));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected NULL ptr of std::make_unque", K(ret), K(lbt()));
  } else {
    ptr = std::move(result);
  }

  return ret;
}

namespace core {
using namespace llvm;

typedef ::llvm::LLVMContext ObLLVMContext;
typedef ::llvm::orc::ExecutionSession ObExecutionSession;
typedef ::llvm::orc::SymbolResolver ObSymbolResolver;
typedef ::llvm::TargetMachine ObTargetMachine;
typedef ::llvm::DataLayout ObDataLayout;
typedef ::llvm::orc::VModuleKey ObVModuleKey;
typedef ::llvm::JITSymbol ObJITSymbol;
typedef ::llvm::orc::JITDylib::DefinitionGenerator ObJitDefinitionGenerator;
typedef ::llvm::JITEventListener ObJitEventListener;

class ObNotifyLoaded: public ObJitEventListener
{
public:
  explicit ObNotifyLoaded(
    common::ObIAllocator &Allocator, char *&DebugBuf, int64_t &DebugLen, ObString &SoObject)
      : Allocator(Allocator), DebugBuf(DebugBuf), DebugLen(DebugLen), SoObject(SoObject) {}
  virtual ~ObNotifyLoaded() {}

  void notifyObjectLoaded(ObVModuleKey Key,
                  const object::ObjectFile &Obj,
                  const RuntimeDyld::LoadedObjectInfo &Info) override;
private:
  common::ObIAllocator &Allocator;
  char* &DebugBuf;
  int64_t &DebugLen;
  ObString &SoObject;
};

class ObJitGlobalSymbolGenerator: public ObJitDefinitionGenerator {
public:
  Error tryToGenerate(orc::LookupKind K,
                      orc::JITDylib &JD,
                      orc::JITDylibLookupFlags JDLookupFlags,
                      const orc::SymbolLookupSet &LookupSet) override
  {
    for (const auto &sym : LookupSet) {
      auto res = symbol_table.find(*sym.first);

      if (res != symbol_table.end()) {
        Error err = JD.define(orc::absoluteSymbols(
                      {{sym.first, JITEvaluatedSymbol(res->second, {})}}));

        if (err) {
          StringRef name = *sym.first;
          std::string msg = toString(std::move(err));

          SERVER_LOG_RET(WARN, OB_ERR_UNEXPECTED,
                         "failed to define SPI interface symbol",
                         "name", ObString(name.size(), name.data()),
                         "msg", msg.c_str(),
                         K(lbt()));

          return err;
        }
      }
    }

    return Error::success();
  }

  static void add_symbol(StringRef name, void *addr) {
    symbol_table[name] = pointerToJITTargetAddress(addr);
  }

private:
  static DenseMap<StringRef, JITTargetAddress> symbol_table;
};

class ObOrcJit
{
public:
  using ObLLJITBuilder = llvm::orc::LLJITBuilder;
  using ObJitEngineT = llvm::orc::LLJIT;

  explicit ObOrcJit(common::ObIAllocator &Allocator);
  virtual ~ObOrcJit() {};

  int addModule(std::unique_ptr<Module> M, std::unique_ptr<ObLLVMContext> TheContext);
  int get_function_address(const std::string &name, uint64_t &addr);

  const ObDataLayout &getDataLayout() const { return ObDL; }

  char* get_debug_info_data() { return DebugBuf; }
  int64_t get_debug_info_size() { return DebugLen; }

  int add_compiled_object(size_t length, const char *ptr);

  const ObString& get_compiled_object() const { return SoObject; }

  int set_optimize_level(ObPLOptLevel level);

  int init();

  const ObDataLayout &get_DL() const { return ObDL; }

private:
  int lookup(const std::string &name, ObJITSymbol &symbol);

  int create_jit_engine();

  static ObJitGlobalSymbolGenerator symbol_generator;

  char *DebugBuf;
  int64_t DebugLen;

  ObJitAllocator JITAllocator;
  ObNotifyLoaded NotifyLoaded;

  std::unique_ptr<ObTargetMachine> ObTM;
  const ObDataLayout ObDL;

  ObString SoObject;

  ObLLJITBuilder ObEngineBuilder;
  std::unique_ptr<ObJitEngineT> ObJitEngine;
};

} // core
} // jit
} // oceanbase

#endif /* OBJIT_CORE_ORCJIT_H */
