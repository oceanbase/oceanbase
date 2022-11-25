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
