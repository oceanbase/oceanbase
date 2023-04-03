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

#include "ob_all_virtual_memory_context_stat.h"
#include "share/ob_errno.h"

namespace oceanbase
{
using namespace storage;
using namespace lib;
namespace observer
{
ObAllVirtualMemoryContextStat::ObAllVirtualMemoryContextStat()
{
}

ObAllVirtualMemoryContextStat::~ObAllVirtualMemoryContextStat()
{
  reset();
}

int ObAllVirtualMemoryContextStat::inner_get_next_row(common::ObNewRow *&row)
{
  UNUSEDx(row);
  return OB_ITER_END;
}

void ObAllVirtualMemoryContextStat::reset()
{
  is_inited_ = false;
  ObVirtualTableScannerIterator::reset();
}

} /* namespace observer */
} /* namespace oceanbase */
