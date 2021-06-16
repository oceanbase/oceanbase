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

#ifndef OCEANBASE_STORAGE_OB_TABLE_SCAN_ITERATOR_
#define OCEANBASE_STORAGE_OB_TABLE_SCAN_ITERATOR_

#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/container/ob_se_array.h"
#include "common/row/ob_row_iterator.h"
#include "common/rowkey/ob_rowkey.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "storage/ob_col_map.h"
#include "storage/ob_i_store.h"
#include "storage/ob_range_iterator.h"
#include "storage/ob_multiple_merge.h"
#include "storage/ob_single_merge.h"
#include "storage/ob_multiple_get_merge.h"
#include "storage/ob_multiple_scan_merge.h"
#include "storage/ob_multiple_multi_scan_merge.h"
#include "storage/ob_index_merge.h"
#include "storage/ob_row_sample_iterator.h"
#include "storage/ob_block_sample_iterator.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_block_cache_working_set.h"
#include "storage/transaction/ob_trans_service.h"
namespace oceanbase {
namespace storage {
class ObTableScanParam;
class ObPartitionStore;
class ObIStoreRowFilter;
class ObTableScanStoreRowIterator {
public:
  ObTableScanStoreRowIterator();
  virtual ~ObTableScanStoreRowIterator();
  int init(transaction::ObTransService& trans_service, const ObStoreCtx& ctx, ObTableScanParam& scan_param,
      ObPartitionStore& partition_store);
  virtual int get_next_row(ObStoreRow*& row);
  virtual void reset();
  int rescan(const ObRangeArray& key_ranges, const ObPosArray& range_array_pos);
  int switch_iterator(const int64_t range_array_idx);

private:
  static const int64_t LOOP_RESCAN_BUFFER_SIZE = 8 * 1024;  // 8K
  int prepare_table_param();
  int prepare_table_context(const ObIStoreRowFilter* row_filter);
  int init_scan_iter(const bool index_back, ObMultipleMerge& iter);
  void reuse_row_iters();
  int open_iter();

  bool is_inited_;
  ObSingleMerge* single_merge_;
  ObMultipleGetMerge* get_merge_;
  ObMultipleScanMerge* scan_merge_;
  ObMultipleMultiScanMerge* multi_scan_merge_;
  ObIndexMerge* index_merge_;
  ObRowSampleIterator* row_sample_iterator_;
  ObBlockSampleIterator* block_sample_iterator_;
  ObTableAccessParam main_table_param_;
  ObTableAccessContext main_table_ctx_;
  ObTableAccessParam index_table_param_;
  ObTableAccessContext index_table_ctx_;
  ObGetTableParam get_table_param_;

  transaction::ObTransService* trans_service_;
  ObStoreCtx ctx_;
  ObTableScanParam* scan_param_;
  ObPartitionStore* partition_store_;
  ObRangeIterator range_iter_;
  ObQueryRowIterator* main_iter_;

  ObIStoreRowFilter* row_filter_;
  blocksstable::ObBlockCacheWorkingSet block_cache_ws_;

  bool is_iter_opened_;
  bool use_fuse_row_cache_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableScanStoreRowIterator);
};

class ObTableScanRangeArrayRowIterator : public common::ObNewRowIterator {
public:
  ObTableScanRangeArrayRowIterator();
  virtual ~ObTableScanRangeArrayRowIterator();
  int init(const bool is_reverse_scan, ObTableScanStoreRowIterator& iter);
  virtual int get_next_row(common::ObNewRow*& row) override;
  virtual int get_next_row() override
  {
    common::ObNewRow* r = NULL;
    return get_next_row(r);
  }
  virtual void reset();
  int set_range_array_idx(const int64_t range_array_idx);

private:
  bool is_inited_;
  ObTableScanStoreRowIterator* row_iter_;
  ObStoreRow* cur_row_;
  int64_t curr_range_array_idx_;
  bool is_reverse_scan_;
};

class ObTableScanIterIterator : public common::ObNewIterIterator {
public:
  static constexpr int64_t RP_MAX_FREE_LIST_NUM = 1024;
  static constexpr const char LABEL[] = "RPTblScanIterIter";

public:
  ObTableScanIterIterator();
  virtual ~ObTableScanIterIterator();
  virtual int get_next_iter(common::ObNewRowIterator*& iter);
  int init(transaction::ObTransService& trans_service, const ObStoreCtx& ctx, ObTableScanParam& scan_param,
      ObPartitionStore& partition_store);
  int rescan(ObTableScanParam& scan_param);
  virtual void reset();

private:
  int get_next_range();

private:
  ObTableScanStoreRowIterator store_row_iter_;
  ObTableScanRangeArrayRowIterator range_row_iter_;
  int64_t range_array_cursor_;
  int64_t range_array_cnt_;
  bool is_reverse_scan_;
};

class ObTableScanIterator : public common::ObNewRowIterator {
public:
  static constexpr int64_t RP_MAX_FREE_LIST_NUM = 1024;
  static constexpr const char LABEL[] = "RPTableScanIter";

public:
  ObTableScanIterator();
  virtual ~ObTableScanIterator();
  virtual int get_next_row(common::ObNewRow*& row) override;
  virtual int get_next_row() override
  {
    common::ObNewRow* r = NULL;
    return get_next_row(r);
  }
  int init(transaction::ObTransService& trans_service, const ObStoreCtx& ctx, ObTableScanParam& scan_param,
      ObPartitionStore& partition_store);
  int rescan(ObTableScanParam& scan_param);
  virtual void reset() override;

private:
  ObTableScanIterIterator iter_;
  ObNewRowIterator* row_iter_;
};

class ObTableScanNewRowIterator {
public:
  explicit ObTableScanNewRowIterator(ObNewIterIterator& iter);
  virtual ~ObTableScanNewRowIterator();
  int get_next_row(common::ObNewRow*& row);

private:
  ObNewIterIterator& iter_;
  ObNewRowIterator* row_iter_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_TABLE_SCAN_ITERATOR_
