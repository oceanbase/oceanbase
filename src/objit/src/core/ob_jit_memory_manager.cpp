/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_jit_memory_manager.h"
#include "objit/ob_llvm_helper.h"

namespace oceanbase {
namespace jit {
namespace core {

bool ObJitMemoryManager::finalizeMemory(std::string *ErrMsg) {
  return allocator_.finalize();
}

void ObJitMemoryManager::registerEHFrames(uint8_t *Addr, uint64_t LoadAddr, size_t Size)
{
  UNUSED(LoadAddr);
  if (eh_frame_cb_) {
    eh_frame_cb_(Addr, Size);
  }
}


}  // core
}  // jit
}  // oceanbase
