/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_SPATIAL_INDEX_LOOKUP_OP_H_
#define OBDEV_SRC_SQL_DAS_OB_DAS_SPATIAL_INDEX_LOOKUP_OP_H_
#include "sql/das/ob_das_scan_op.h"
#include "storage/ob_store_row_comparer.h"
#include "storage/ob_parallel_external_sort.h"
namespace oceanbase
{
namespace sql
{


class ObRowKeyCompare {
public:
  ObRowKeyCompare(int &sort_ret) : result_code_(sort_ret) {}
  bool operator()(const ObRowkey *left, const ObRowkey *right);
  int &result_code_;
};

class ObSpatialIndexLookupOp : public ObLocalIndexLookupOp
{
public:
  ObSpatialIndexLookupOp(ObIAllocator &allocator) :
                             ObLocalIndexLookupOp(),
                             mbr_filters_(NULL),
                             cmp_ret_(0),
                             comparer_(cmp_ret_),
                             sorter_(allocator),
                             is_sorted_(false),
                             is_whole_range_(false),
                             is_inited_(false) {}
  virtual ~ObSpatialIndexLookupOp();

  int init(const ObDASScanCtDef *lookup_ctdef,
           ObDASScanRtDef *lookup_rtdef,
           const ObDASScanCtDef *index_ctdef,
           ObDASScanRtDef *index_rtdef,
           transaction::ObTxDesc *tx_desc,
           transaction::ObTxReadSnapshot *snapshot,
           storage::ObTableScanParam &scan_param);
  int reset_lookup_state();
  int filter_by_mbr(const ObObj &mbr_obj, bool &pass_through);
  int get_next_row();
  int revert_iter();

private:
  int process_data_table_rowkey();
  int save_rowkeys();
private:
  static const int64_t SORT_MEMORY_LIMIT = 32L * 1024L * 1024L;
  static const int64_t MAX_NUM_PER_BATCH = 1000;

  const ObMbrFilterArray *mbr_filters_;
  int cmp_ret_;
  ObRowKeyCompare comparer_;
  ObExternalSort<ObRowkey, ObRowKeyCompare> sorter_; // use ObRowKeyCompare to compare rowkey
  ObRowkey last_rowkey_; // store last index row for distinct, who allocs the memory? // no need to use ObExtStoreRowkey
  bool is_sorted_;
  bool is_whole_range_;
  bool is_inited_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_DAS_OB_DAS_SPATIAL_INDEX_LOOKUP_OP_H_ */
