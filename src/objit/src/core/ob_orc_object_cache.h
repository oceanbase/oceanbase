/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBORCOBJECTCACHE_H
#define OBORCOBJECTCACHE_H

#include "llvm/ExecutionEngine/ObjectCache.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/MemoryBuffer.h"
#include "llvm/ADT/StringRef.h"

#include <string>
#include <memory>
#include <utility>
#include <vector>
#include <map>
#include <cstdlib>
#include <cstring>

namespace oceanbase {
namespace jit {
namespace core {

class ObOrcObjectCache: public llvm::ObjectCache {
  friend class ObOrcJit;
public:
  ObOrcObjectCache() = default;
  ObOrcObjectCache(const ObOrcObjectCache&) = delete;
  ObOrcObjectCache& operator=(const ObOrcObjectCache&) = delete;
  ObOrcObjectCache(ObOrcObjectCache&&) = delete;

  ~ObOrcObjectCache() {
    if (mem.addr != nullptr) {
      std::free(mem.addr);
      mem.addr = nullptr;
      mem.sz = 0;
    }
  }

  virtual void notifyObjectCompiled(const llvm::Module* M, llvm::MemoryBufferRef ObjRef)
      override {
    mem.sz = ObjRef.getBufferSize();
    mem.addr = (char *) std::malloc(mem.sz);
    std::memcpy(mem.addr, ObjRef.getBufferStart(), ObjRef.getBufferSize());
  }

  virtual std::unique_ptr<llvm::MemoryBuffer> getObject(const llvm::Module* M) override {
    return llvm::MemoryBuffer::getMemBufferCopy(
             llvm::StringRef(mem.addr, mem.sz));
  }

private:
  struct MemStruct {
    char* addr;
    size_t sz;

    MemStruct() : addr(nullptr), sz(0) {
    }
    ~MemStruct() {
    }
  };
  MemStruct mem;
};
}
}
}

#endif /* OBORCOBJECTCACHE_H */
