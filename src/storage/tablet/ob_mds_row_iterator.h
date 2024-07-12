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

#ifndef OCEANBASE_STORAGE_OB_MDS_ROW_ITERATOR
#define OCEANBASE_STORAGE_OB_MDS_ROW_ITERATOR

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "common/row/ob_row_iterator.h"
#include "storage/access/ob_multiple_merge.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_table_scan_range.h"
#include "storage/meta_mem/ob_tablet_handle.h"

namespace oceanbase
{
namespace storage
{
class ObTableScanParam;
class ObTabletHandle;

class ObMdsRowIterator : public common::ObNewRowIterator
{
public:
  ObMdsRowIterator();
  virtual ~ObMdsRowIterator();
  ObMdsRowIterator(const ObMdsRowIterator&) = delete;
  ObMdsRowIterator &operator=(const ObMdsRowIterator&) = delete;
public:
  int init(
      ObTableScanParam &scan_param,
      const ObTabletHandle &tablet_handle,
      ObStoreCtx &store_ctx);
  virtual void reset() override;
public:
  virtual int get_next_row(ObNewRow *&row) override;
  virtual int get_next_row() override;
  virtual int get_next_rows(int64_t &count, int64_t capacity) override;
  int get_next_row(blocksstable::ObDatumRow *&row);

  int get_next_mds_kv(common::ObIAllocator &allocator, mds::MdsDumpKV &kv);
private:
  int init_get_table_param(
      const ObTableScanParam &scan_param,
      const ObTabletHandle &tablet_handle);
  int init_and_open_iter(ObTableScanParam &scan_param);
  int init_and_open_single_get_merge(ObTableScanParam &scan_param);
  int init_and_open_multiple_get_merge(ObTableScanParam &scan_param);
  int init_and_open_multiple_scan_merge(ObTableScanParam &scan_param);
  static int convert(
      common::ObIAllocator &allocator,
      const blocksstable::ObDatumRow &row,
      mds::MdsDumpKV &kv);
public:
  TO_STRING_KV(K_(is_inited),
               K_(access_param),
               K_(access_ctx),
               K_(get_table_param),
               K_(table_scan_range),
               KP_(table_scan_param),
               KP_(multiple_merge));
private:
  bool is_inited_;
  ObTableAccessParam access_param_;
  ObTableAccessContext access_ctx_;
  ObGetTableParam get_table_param_; // no need?
  ObTableScanRange table_scan_range_;
  ObTableScanParam *table_scan_param_;
  ObMultipleMerge *multiple_merge_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MDS_ROW_ITERATOR
