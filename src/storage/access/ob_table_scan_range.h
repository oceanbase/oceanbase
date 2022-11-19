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

#ifndef OB_STORAGE_TABLE_SCAN_RANGE_H
#define OB_STORAGE_TABLE_SCAN_RANGE_H

#include "common/ob_common_types.h"
#include "share/ob_simple_batch.h"
#include "storage/blocksstable/ob_datum_range.h"

namespace oceanbase
{
namespace storage
{
class ObTableScanParam;

struct ObTableScanRange
{
public:
  ObTableScanRange();
  ~ObTableScanRange() { reset(); }
  int init(ObTableScanParam &scan_param);
  int init(const common::ObSimpleBatch &simple_batch, common::ObIAllocator &allocator);
  void reset();
  OB_INLINE bool is_valid() const { return is_inited_; }
  OB_INLINE bool is_get() const { return GET == status_; }
  OB_INLINE bool is_scan() const { return SCAN == status_; }
  OB_INLINE bool is_empty() const { return EMPTY == status_; }
  OB_INLINE void set_empty() { status_ = EMPTY; }
  OB_INLINE const ObIArray<blocksstable::ObDatumRange> &get_ranges() const { return ranges_; }
  OB_INLINE const ObIArray<blocksstable::ObDatumRowkey> &get_rowkeys() const { return rowkeys_; }
  TO_STRING_KV(K_(rowkeys), K_(ranges), K_(status), K_(is_inited));
private:
  int init_rowkeys(const common::ObIArray<common::ObNewRange> &ranges,
                   const common::ObQueryFlag &scan_flag,
                   const blocksstable::ObStorageDatumUtils *datum_utils);
  int init_ranges(const common::ObIArray<common::ObNewRange> &ranges,
                   const common::ObQueryFlag &scan_flag,
                  const blocksstable::ObStorageDatumUtils *datum_utils);
  int always_false(const common::ObNewRange &range, bool &is_false);
private:
  enum RangeStatus
  {
    EMPTY,
    GET,
    SCAN,
  };
  static const int64_t DEFAULT_RANGE_CNT = 8;
  common::ObSEArray<blocksstable::ObDatumRowkey, DEFAULT_RANGE_CNT> rowkeys_;
  common::ObSEArray<blocksstable::ObDatumRange, DEFAULT_RANGE_CNT> ranges_;
  ObIAllocator *allocator_;
  RangeStatus status_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTableScanRange);
};


} // namespace storage
} // namespace oceanbase
#endif
