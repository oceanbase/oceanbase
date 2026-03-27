/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/memtable/mvcc/ob_mvcc_define.h"

namespace oceanbase
{
namespace memtable
{

ERRSIM_POINT_DEF(ERRSIM_MEMTABLE_SET_FAIL);
ERRSIM_POINT_DEF(ERRSIM_MEMTABLE_SET_SLEEP);

int memtable_set_injection_error()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ERRSIM_MEMTABLE_SET_FAIL)) {
    TRANS_LOG(WARN, "memtable injection error", K(ret));
  } else if (OB_UNLIKELY(OB_SUCCESS != ERRSIM_MEMTABLE_SET_SLEEP)) {
    ob_usleep(5_ms);
    TRANS_LOG(WARN, "memtable injection sleep finish", K(ret));
  }
  return ret;
}

void memtable_set_injection_sleep()
{
  if (OB_UNLIKELY(OB_SUCCESS != ERRSIM_MEMTABLE_SET_SLEEP)) {
    ob_usleep(5_ms);
    TRANS_LOG_RET(WARN, OB_EAGAIN, "memtable injection sleep finish");
  }
}


} // namespace memtable
} // namespace oceanbase
