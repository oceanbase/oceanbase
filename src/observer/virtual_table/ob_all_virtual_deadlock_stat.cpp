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

#include "ob_all_virtual_deadlock_stat.h"
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "observer/ob_server_utils.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace observer {

using namespace transaction;
using namespace memtable;

void ObAllVirtualDeadlockStat::reset()
{
  ObVirtualTableScannerIterator::reset();
}

void ObAllVirtualDeadlockStat::destroy()
{
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualDeadlockStat::inner_get_next_row(ObNewRow*& row)
{
  UNUSED(row);
  return OB_NOT_SUPPORTED;
}

}  // namespace observer
}  // namespace oceanbase
