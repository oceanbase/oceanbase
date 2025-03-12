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
#ifdef CPP_STANDARD_20
#include "llvm/ExecutionEngine/Orc/Shared/ExecutorAddress.h"
#include "llvm/ExecutionEngine/Orc/Shared/ExecutorSymbolDef.h"
#endif

#include <string>

#include "core/ob_jit_memory_manager.h"
#include "lib/hash/ob_hashmap.h"

// This must be kept in sync with gdb/gdb/jit.h .
extern "C" {

  typedef enum {
    JIT_NOACTION = 0,
    JIT_REGISTER_FN,
    JIT_UNREGISTER_FN
  } jit_actions_t;

  struct jit_code_entry {
    struct jit_code_entry *next_entry;
    struct jit_code_entry *prev_entry;
    const char *symfile_addr;
    uint64_t symfile_size;
  };

  struct jit_descriptor {
    uint32_t version;
    // This should be jit_actions_t, but we want to be specific about the
    // bit-width.
    uint32_t action_flag;
    struct jit_code_entry *relevant_entry;
    struct jit_code_entry *first_entry;
  };

  // We put information about the JITed function in this global, which the
  // debugger reads.  Make sure to specify the version statically, because the
  // debugger checks the version before we can set it during runtime.
  extern struct jit_descriptor __jit_debug_descriptor;

  // Debuggers puts a breakpoint in this function.
  extern void __jit_debug_register_code();

}

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
using namespace llvm::orc;

typedef ::llvm::LLVMContext ObLLVMContext;
typedef ::llvm::orc::ExecutionSession ObExecutionSession;
typedef ::llvm::TargetMachine ObTargetMachine;
typedef ::llvm::DataLayout ObDataLayout;
typedef ::llvm::JITSymbol ObJITSymbol;
typedef ::llvm::JITEventListener ObJitEventListener;
#ifdef CPP_STANDARD_20
typedef ::llvm::orc::DefinitionGenerator ObJitDefinitionGenerator;
using ObObjectKey = ObJitEventListener::ObjectKey;
using ObSymbolDef = ::llvm::orc::ExecutorSymbolDef;
using ObExecutorAddr = ::llvm::orc::ExecutorAddr;
#else
typedef ::llvm::orc::SymbolResolver ObSymbolResolver;
typedef ::llvm::orc::JITDylib::DefinitionGenerator ObJitDefinitionGenerator;
using ObObjectKey = ::llvm::orc::VModuleKey;
using ObSymbolDef = ::llvm::JITEvaluatedSymbol;
using ObExecutorAddr = ::llvm::JITTargetAddress;
#endif

class ObNotifyLoaded: public ObJitEventListener
{
public:
  explicit ObNotifyLoaded(
    common::ObIAllocator &Allocator, char *&DebugBuf, int64_t &DebugLen, ObString &SoObject)
      : Allocator(Allocator), DebugBuf(DebugBuf), DebugLen(DebugLen), SoObject(SoObject) {}
  virtual ~ObNotifyLoaded() {}

  void notifyObjectLoaded(ObObjectKey Key,
                          const object::ObjectFile &Obj,
                          const RuntimeDyld::LoadedObjectInfo &Info) override;
  void notifyFreeingObject(ObObjectKey Key) override;

  static int initGdbHelper();

private:
  void registerDebugInfoToGdb(ObObjectKey Key);
  void deregisterDebugInfoFromGdb(ObObjectKey Key);

private:
  using KeyEntryMap = common::hash::ObHashMap<
                        ObObjectKey, jit_code_entry,
                        common::hash::NoPthreadDefendMode,
                        common::hash::hash_func<ObObjectKey>,
                        common::hash::equal_to<ObObjectKey>,
                        common::hash::SimpleAllocer<common::hash::ObHashTableNode<
                          common::hash::HashMapPair<ObObjectKey, jit_code_entry>>>,
                        common::hash::NormalPointer,
                        common::ObMalloc,
                        2>;
  static std::pair<lib::ObMutex, KeyEntryMap> AllGdbReg;
private:
  common::ObIAllocator &Allocator;
  char* &DebugBuf;
  int64_t &DebugLen;
  ObString &SoObject;
};

class ObJitGlobalSymbolGenerator: public ObJitDefinitionGenerator {
public:
#ifdef CPP_STANDARD_20
  Error tryToGenerate(orc::LookupState &LS,
                      orc::LookupKind K,
#else
  Error tryToGenerate(orc::LookupKind K,
#endif
                      orc::JITDylib &JD,
                      orc::JITDylibLookupFlags JDLookupFlags,
                      const orc::SymbolLookupSet &LookupSet) override
  {
    for (const auto &sym : LookupSet) {
      auto res = symbol_table.find(*sym.first);

      if (res != symbol_table.end()) {
        Error err = JD.define(orc::absoluteSymbols(
                      {{sym.first, ObSymbolDef(ObExecutorAddr(res->second), {})}}));

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

  int init();

  const ObDataLayout &get_DL() const { return ObDL; }

  int set_optimize_level(ObPLOptLevel level);

  inline void update_stack_size(uint64_t stack_size) { StackSize = std::max(StackSize, stack_size); }
  inline uint64_t get_stack_size() const { return StackSize; }

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

  uint64_t StackSize = 0;
};

} // core
} // jit
} // oceanbase

#endif /* OBJIT_CORE_ORCJIT_H */
