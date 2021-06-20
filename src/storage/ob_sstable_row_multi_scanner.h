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

#ifndef OB_SSTABLE_ROW_MULTI_SCANNER_H_
#define OB_SSTABLE_ROW_MULTI_SCANNER_H_

#include "ob_sstable_row_scanner.h"

namespace oceanbase {
namespace storage {

class ObSSTableRowMultiScanner : public ObSSTableRowScanner {
public:
  ObSSTableRowMultiScanner();
  virtual ~ObSSTableRowMultiScanner();
  virtual void reset();
  virtual void reuse();
  virtual int skip_range(int64_t range_idx, const common::ObStoreRowkey* gap_key, const bool include_gap_key) override;

protected:
  virtual int get_handle_cnt(const void* query_range, int64_t& read_handle_cnt, int64_t& micro_handle_cnt);
  virtual int prefetch_read_handle(ObSSTableReadHandle& read_handle);
  virtual int fetch_row(ObSSTableReadHandle& read_handle, const ObStoreRow*& store_row);
  virtual int get_range_count(const void* query_range, int64_t& range_count) const;

private:
  static const int64_t MULTISCAN_READ_HANDLE_CNT = 32;
  static const int64_t MULTISCAN_MICRO_HANDLE_CNT = 32;
  bool has_prefetched_;
  int32_t prefetch_range_idx_;
  const common::ObIArray<common::ObExtStoreRange>* orig_ranges_;
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OB_SSTABLE_ROW_MULTI_SCANNER_H_ */
