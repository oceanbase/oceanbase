// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_STORAGE_MULTIPLE_SKIP_SCAN_MERGE_
#define OCEANBASE_STORAGE_MULTIPLE_SKIP_SCAN_MERGE_

#include "ob_multiple_scan_merge.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;

class ObMultipleSkipScanMerge : public ObMultipleScanMerge
{
public:
  ObMultipleSkipScanMerge();
  virtual ~ObMultipleSkipScanMerge();
  virtual int init(
      ObTableAccessParam &param,
      ObTableAccessContext &context,
      ObGetTableParam &get_table_param) override;
  virtual void reset() override;
  virtual void reuse() override;
  int open(const blocksstable::ObDatumRange &range) { return OB_NOT_SUPPORTED; }
  int open(const blocksstable::ObDatumRange &range, const blocksstable::ObDatumRange &skip_scan_range);
protected:
  virtual int inner_get_next_row(blocksstable::ObDatumRow &row) override;
  virtual int inner_get_next_rows() override;
  virtual int can_batch_scan(bool &can_batch) override;
private:
  const static int32_t START_KEY_OFFSET_OF_SCAN_ROWKEY_RANGE = 0;
  const static int32_t END_KEY_OFFSET_OF_SCAN_ROWKEY_RANGE = 1;
  const static int32_t START_KEY_OFFSET_OF_SCAN_ROWS_RANGE = 2;
  const static int32_t END_KEY_OFFSET_OF_SCAN_ROWS_RANGE = 3;
  const static int32_t SKIP_SCAN_ROWKEY_DATUMS_ARRAY_CNT = 4;
  const static int64_t SKIP_SCAN_CHECK_INTERRUPT_CNT = 100;
  const static int64_t SKIP_SCAN_RETIRE_TO_NORMAL_SCAN_LIMIT = 1000;
  int open_skip_scan(const blocksstable::ObDatumRange &range, const blocksstable::ObDatumRange &skip_scan_range);
  int prepare_range(blocksstable::ObStorageDatum *datums, blocksstable::ObDatumRange &range);
  void prepare_rowkey(blocksstable::ObStorageDatum *datums, const blocksstable::ObDatumRowkey &rowkey,
      const int64_t datum_cnt, const bool is_min);
  int update_scan_rowkey_range();
  int update_scan_rows_range(blocksstable::ObDatumRow &row);
  int shrink_scan_rows_range(bool &exceeded);
  int prepare_scan_row_range();
  void set_border_falg(const bool is_left, const blocksstable::ObDatumRange &src, blocksstable::ObDatumRange &dst);
  OB_INLINE blocksstable::ObStorageDatum* start_key_of_scan_rowkey_range()
  { return datums_ + START_KEY_OFFSET_OF_SCAN_ROWKEY_RANGE * schema_rowkey_cnt_; }
  OB_INLINE blocksstable::ObStorageDatum* end_key_of_scan_rowkey_range()
  { return datums_ + END_KEY_OFFSET_OF_SCAN_ROWKEY_RANGE * schema_rowkey_cnt_; }
  OB_INLINE blocksstable::ObStorageDatum* start_key_of_scan_rows_range()
  { return datums_ + START_KEY_OFFSET_OF_SCAN_ROWS_RANGE * schema_rowkey_cnt_; }
  OB_INLINE blocksstable::ObStorageDatum* end_key_of_scan_rows_range()
  { return datums_ + END_KEY_OFFSET_OF_SCAN_ROWS_RANGE * schema_rowkey_cnt_; }
  OB_INLINE void reuse_datums()
  {
    if (OB_NOT_NULL(datums_)) {
      for (int64_t i = 0; i < datums_cnt_; ++i) {
        datums_[i].reuse();
      }
    }
  }
  OB_INLINE bool should_check_interrupt() const
  {
    return 0 == scan_rowkey_cnt_ % SKIP_SCAN_CHECK_INTERRUPT_CNT;
  }
  OB_INLINE bool should_retire_to_scan() const
  {
    return scan_rowkey_cnt_ > SKIP_SCAN_RETIRE_TO_NORMAL_SCAN_LIMIT;
  }

  enum SkipScanState {
    SCAN_ROWKEY,
    UPDATE_SCAN_ROWS_RANGE,
    SCAN_ROWS,
    UPDATE_SCAN_ROWKEY_RANGE,
    SCAN_FINISHED,
    RETIRED_TO_SCAN,
  };
  SkipScanState state_;
  int64_t scan_rowkey_cnt_;
  int64_t schema_rowkey_cnt_;
  int64_t ss_rowkey_prefix_cnt_;
  blocksstable::ObDatumRange scan_rowkey_range_;
  blocksstable::ObDatumRange scan_rows_range_;
  int64_t datums_cnt_;
  blocksstable::ObStorageDatum *datums_;
  const blocksstable::ObDatumRange *origin_range_;
  const blocksstable::ObDatumRange *skip_scan_range_;
  common::ObArenaAllocator range_allocator_;
  common::ObArenaAllocator rowkey_allocator_;
};

}
}

#endif // OCEANBASE_STORAGE_MULTIPLE_SKIP_SCAN_MERGE_