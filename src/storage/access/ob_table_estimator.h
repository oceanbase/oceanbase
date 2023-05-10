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

#ifndef OCEANBASE_STORAGE_OB_TABLE_ESTIMATOR_H_
#define OCEANBASE_STORAGE_OB_TABLE_ESTIMATOR_H_

#include "lib/oblog/ob_log_module.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/memtable/ob_memtable.h"

namespace oceanbase
{
namespace storage
{
class ObTabletHandle;
typedef common::ObIArray<ObITable*> ObTableArray;
typedef common::ObIArray<blocksstable::ObDatumRowkey> ObExtSRowkeyArray;
typedef common::ObIArray<common::ObEstRowCountRecord> ObEstRCRecArray;
typedef common::ObIArray<blocksstable::ObDatumRange> ObExtSRangeArray;

// A helper struct used for passing some identical parameters.
struct ObTableEstimateBaseInput {
  ObTableEstimateBaseInput(
      const common::ObQueryFlag query_flag,
      const uint64_t table_id,
      const transaction::ObTransID tx_id,
      const ObTableArray &tables,
      const ObTabletHandle &tablet_handle) :
      query_flag_(query_flag),
      table_id_(table_id),
      tx_id_(tx_id),
      tables_(tables),
      tablet_handle_(tablet_handle) {}

  OB_INLINE bool is_table_invalid() {
    return !is_valid_id(table_id_) || tables_.count() <= 0;
  }

  const common::ObQueryFlag query_flag_;
  const uint64_t table_id_;
  const transaction::ObTransID tx_id_;
  const ObTableArray &tables_;
  const ObTabletHandle &tablet_handle_;
};

class ObTableEstimator
{
public:
  static int estimate_row_count_for_get(
      ObTableEstimateBaseInput &base_input,
      const ObExtSRowkeyArray &rowkeys,
      ObPartitionEst &part_estimate);
  static int estimate_row_count_for_scan(
      ObTableEstimateBaseInput &base_input,
      const ObExtSRangeArray &ranges,
      ObPartitionEst &part_estimate,
      ObEstRCRecArray &est_records);

  static int estimate_sstable_scan_row_count(
      const ObTableEstimateBaseInput &base_input,
      blocksstable::ObSSTable *sstable,
      const blocksstable::ObDatumRange &key_range,
      ObPartitionEst &part_est);

  static int estimate_memtable_scan_row_count(
      const ObTableEstimateBaseInput &base_input,
      const memtable::ObMemtable *memtable,
      const blocksstable::ObDatumRange &key_range,
      ObPartitionEst &part_est);

  static int estimate_multi_scan_row_count(
      const ObTableEstimateBaseInput &base_input,
      ObITable *current_table,
      const ObExtSRangeArray &ranges,
      ObPartitionEst &part_est);

    TO_STRING_KV("name", "ObTableEstimator");
};


} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_OB_TABLE_ESTIMATOR_H_ */
