/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/thread/thread_mgr_interface.h"

namespace oceanbase {
namespace lib {

TLOCAL(uint64_t, TGRunnable::thread_idx_) = 0;
TLOCAL(uint64_t, TGTaskHandler::thread_idx_) = 0;

} // end of namespace lib
} // end of namespace oceanbase
