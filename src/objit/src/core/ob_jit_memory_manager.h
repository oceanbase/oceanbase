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
#include "llvm/ADT/StringRef.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "core/ob_jit_allocator.h"

#include <functional>

namespace oceanbase {
namespace jit {
namespace core {
class ObJitAllocator;

class ObJitMemoryManager : public llvm::RTDyldMemoryManager
{
  explicit ObJitMemoryManager(const ObJitMemoryManager&);
  void operator=(const ObJitMemoryManager&);
public:
  using EHFrameCallback = std::function<void(const uint8_t*, size_t)>;
  explicit ObJitMemoryManager(ObJitAllocator &allocator, EHFrameCallback cb = nullptr)
      : allocator_(allocator), eh_frame_cb_(cb)
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
  virtual bool finalizeMemory(std::string *ErrMsg = 0);

  virtual void registerEHFrames(uint8_t *Addr, uint64_t LoadAddr, size_t Size) override;

#if defined(__aarch64__)
  void reserveAllocationSpace(uintptr_t CodeSize,
                              llvm::Align CodeAlign,
                              uintptr_t RODataSize,
                              llvm::Align RODataAlign,
                              uintptr_t RWDataSize,
                              llvm::Align RWDataAlign) override
  {
    int64_t sz = CodeSize + CodeAlign.value() + RODataSize + RODataAlign.value() + RWDataSize + RWDataAlign.value();
    int64_t align = MAX3(CodeAlign.value(), RODataAlign.value(), RWDataAlign.value());
    allocator_.reserve(JMT_RWE, sz, align);
  }
  bool needsToReserveAllocationSpace() override { return true; }
#elif defined(__loongarch64)
  // LoongArch uses an older LLVM where alignment params are uint32_t, not llvm::Align
  void reserveAllocationSpace(uintptr_t CodeSize,
                              uint32_t CodeAlign,
                              uintptr_t RODataSize,
                              uint32_t RODataAlign,
                              uintptr_t RWDataSize,
                              uint32_t RWDataAlign) override
  {
    int64_t sz = CodeSize + CodeAlign + RODataSize + RODataAlign + RWDataSize + RWDataAlign;
    int64_t align = MAX3(CodeAlign, RODataAlign, RWDataAlign);
    allocator_.reserve(JMT_RWE, sz, align);
  }
  bool needsToReserveAllocationSpace() override { return true; }
#endif

private:
  uint8_t *alloc(uintptr_t Size, unsigned Alignment);

private:
  ObJitAllocator &allocator_;
  EHFrameCallback eh_frame_cb_;
};

}  // core
}  // jit
}  // oceanbase

#endif /* OB_JIT_MEMORY_MANAGER_H */
