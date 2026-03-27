/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "storage/blocksstable/ob_batch_datum_rows.h"
#include "storage/direct_load/ob_direct_load_batch_rows.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadBatchDatumRows
{
public:
  ObDirectLoadBatchDatumRows() : allocator_("TLD_BDatumRows")
  {
    allocator_.set_tenant_id(MTL_ID());
  }
  ~ObDirectLoadBatchDatumRows() {}
public:
  ObArenaAllocator allocator_;
  ObDirectLoadBatchRows batch_rows_;
  blocksstable::ObBatchDatumRows datum_rows_;
};

} // namespace storage
} // namespace oceanbase
