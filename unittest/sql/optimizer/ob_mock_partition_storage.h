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

#ifndef _OB_MOCK_PARTITION_STORAGE_H_
#define _OB_MOCK_PARTITION_STORAGE_H_

#include "storage/ob_partition_storage.h"
#include "storage/ob_range_iterator.h"

namespace test {
class MockPartitionStorage : public oceanbase::storage::ObPartitionStorage {
public:
  MockPartitionStorage() : is_default_stat_(false)
  {}
  virtual ~MockPartitionStorage()
  {}
  virtual int get_batch_rows(const oceanbase::storage::ObTableScanParam& param,
      const oceanbase::storage::ObBatch& batch, int64_t& rows, int64_t& rows_unreliable,
      oceanbase::common::ObIArray<oceanbase::common::ObEstRowCountRecord>& est_records) override
  {
    UNUSED(param);
    UNUSED(batch);
    UNUSED(est_records);
    if (is_default_stat_) {
      rows = 0;
    } else if (batch.type_ == oceanbase::storage::ObBatch::T_MULTI_SCAN) {
      rows = 100 * batch.ranges_->count();
    } else {
      rows = 100;
    }
    rows_unreliable = rows;
    return oceanbase::common::OB_SUCCESS;
  }
  void set_default_stat(const bool is_default_stat)
  {
    is_default_stat_ = is_default_stat;
  }

private:
  bool is_default_stat_;
};
}  // namespace test

#endif
