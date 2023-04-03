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

#ifndef OB_JIT_MEMORY_MANAGER_H
#define OB_JIT_MEMORY_MANAGER_H

#include "llvm/ExecutionEngine/RTDyldMemoryManager.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "core/ob_jit_allocator.h"

namespace oceanbase {
namespace jit {
namespace core {
class ObJitAllocator;

class ObJitMemoryManager : public llvm::RTDyldMemoryManager
{
  explicit ObJitMemoryManager(const ObJitMemoryManager&);
  void operator=(const ObJitMemoryManager&);
public:
  explicit ObJitMemoryManager(ObJitAllocator &allocator)
      : allocator_(allocator)
  {}
  virtual ~ObJitMemoryManager() {}

  /// Allocate a memory block of (at least) the given size suitable for
  /// executable code. The SectionID is a unique identifier assigned by the JIT
  /// engine, and optionally recorded by the memory manager to access a loaded
  /// section.
  virtual uint8_t *allocateCodeSection(
      uintptr_t Size, unsigned Alignment, unsigned SectionID,
      llvm::StringRef SectionName)
  {
    return reinterpret_cast<uint8_t*>(allocator_.alloc(JMT_RWE, Size, Alignment));
  }

  /// Allocate a memory block of (at least) the given size suitable for data.
  /// The SectionID is a unique identifier assigned by the JIT engine, and
  /// optionally recorded by the memory manager to access a loaded section.
  virtual uint8_t *allocateDataSection(
      uintptr_t Size, unsigned Alignment, unsigned SectionID,
      llvm::StringRef SectionName, bool IsReadOnly){
    return reinterpret_cast<uint8_t*>(allocator_.alloc(JMT_RO, Size, Alignment));
  }

  /// This method is called when object loading is complete and section page
  /// permissions can be applied.  It is up to the memory manager implementation
  /// to decide whether or not to act on this method.  The memory manager will
  /// typically allocate all sections as read-write and then apply specific
  /// permissions when this method is called.  Code sections cannot be executed
  /// until this function has been called.  In addition, any cache coherency
  /// operations needed to reliably use the memory are also performed.
  ///
  /// Returns true if an error occurred, false otherwise.
  virtual bool finalizeMemory(std::string *ErrMsg = 0) {
    return allocator_.finalize();
  }

#if defined(__aarch64__)
  /// Inform the memory manager about the total amount of memory required to
  /// allocate all sections to be loaded:
  /// \p CodeSize - the total size of all code sections
  /// \p DataSizeRO - the total size of all read-only data sections
  /// \p DataSizeRW - the total size of all read-write data sections
  ///
  /// Note that by default the callback is disabled. To enable it
  /// redefine the method needsToReserveAllocationSpace to return true.
  virtual void reserveAllocationSpace(uintptr_t CodeSize, uint32_t CodeAlign,
                                      uintptr_t RODataSize,
                                      uint32_t RODataAlign,
                                      uintptr_t RWDataSize,
                                      uint32_t RWDataAlign)
  {
    int64_t sz = CodeSize + CodeAlign + RODataSize + RODataAlign + RWDataSize + RWDataAlign;
    int64_t align = MAX3(CodeAlign, RODataAlign, RWDataAlign);
    allocator_.reserve(JMT_RWE, sz, align);
  }

  /// Override to return true to enable the reserveAllocationSpace callback.
  virtual bool needsToReserveAllocationSpace() { return true; }
#endif

private:
  uint8_t *alloc(uintptr_t Size, unsigned Alignment);

private:
  ObJitAllocator &allocator_;
};

}  // core
}  // jit
}  // oceanbase

#endif /* OB_JIT_MEMORY_MANAGER_H */
