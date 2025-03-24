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
