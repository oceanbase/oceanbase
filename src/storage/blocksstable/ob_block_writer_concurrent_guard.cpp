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

#include "storage/blocksstable/ob_block_writer_concurrent_guard.h"
namespace oceanbase
{

namespace blocksstable
{


ObBlockWriterConcurrentGuard::ObBlockWriterConcurrentGuard(volatile bool &lock)
    : lock_(lock),
      ret_(OB_SUCCESS)
{
#ifndef OB_BUILD_PACKAGE
  if (OB_UNLIKELY(!ATOMIC_BCAS(&lock_, false, true))) {
    ret_ = OB_ERR_UNEXPECTED;
    COMMON_LOG_RET(ERROR, ret_, "Another thread is concurrently accessing the interfaces of the same object. "
               "The current interface is not thread-safe. Please do not perform concurrent operations "
               "on the same object.", K(lock_), K(&lock_), K_(ret), K(lbt()));
    on_error();
  }
#endif
}

ObBlockWriterConcurrentGuard::~ObBlockWriterConcurrentGuard()
{
#ifndef OB_BUILD_PACKAGE
  if (OB_LIKELY(ret_ == OB_SUCCESS)) {
    if (OB_UNLIKELY(!ATOMIC_BCAS(&lock_, true, false))) {
      ret_ = OB_ERR_UNEXPECTED;
      COMMON_LOG_RET(ERROR, ret_, "This scenario should never happen.",
                 K(lock_), K(&lock_), K(ret), K(lbt()));
      on_error();
    }
  }
#endif
}

void ObBlockWriterConcurrentGuard::on_error()
{
  ob_abort();
}

}//end namespace blocksstable
}//end namespace oceanbase