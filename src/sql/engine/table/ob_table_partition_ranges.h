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

#ifndef OB_TABLE_PARTITION_RANGES_H_
#define OB_TABLE_PARTITION_RANGES_H_

#include "share/schema/ob_schema_struct.h"
#include "sql/ob_phy_table_location.h"

namespace oceanbase
{
namespace sql
{
struct ObExpr;
struct ObEvalCtx;

class ObPartitionScanRanges
{
  OB_UNIS_VERSION_V(1);
public:
  ObPartitionScanRanges()
    : ranges_(),
      partition_id_(common::OB_INVALID_INDEX),
      deserialize_allocator_(NULL) {}
  explicit ObPartitionScanRanges(common::ObIAllocator *allocator)
    : ranges_(),
      partition_id_(common::OB_INVALID_INDEX),
      deserialize_allocator_(allocator) {}
  virtual ~ObPartitionScanRanges() {}
  void set_des_allocator(common::ObIAllocator *deserialize_allocator) { deserialize_allocator_ = deserialize_allocator; }

  virtual const common::ObIArray<common::ObNewRange> &get_ranges() const { return ranges_; }
  common::ObSEArray<common::ObNewRange, 16> ranges_;
  int64_t partition_id_;
  TO_STRING_KV(K_(partition_id),
               K_(ranges));
private:
  common::ObIAllocator *deserialize_allocator_;
};

class ObMultiPartitionsRangesWarpper
{
  OB_UNIS_VERSION_V(1);
private:
  enum GetRangeMode {
    PARTITION_PARTICLE,
    RANGE_PARTICLE,
  };
public:
  explicit ObMultiPartitionsRangesWarpper() :
  allocator_(common::ObModIds::OB_SQL_TABLE_LOOKUP),
  partitions_ranges_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_),
  mode_(PARTITION_PARTICLE), range_offset_(0) {}
  virtual ~ObMultiPartitionsRangesWarpper() {};
  int init(int64_t expect_partition_cnt);
  int get_next_ranges(int64_t idx,
                      int64_t &partition_id,
                      common::ObIArray<common::ObNewRange> &ranges,
                      int64_t &next_idx);
  int get_partition_ranges_by_idx(int64_t idx, ObPartitionScanRanges *&partition_ranges);
  int get_partition_ranges_by_partition_id(int64_t partition_id, ObPartitionScanRanges *&partition_ranges);
  int get_new_partition_ranges(ObPartitionScanRanges *&partition_ranges);
  int add_range(int64_t part_id, int64_t ref_table_id,
                const common::ObNewRow *row, const bool table_has_hidden_pk, int64_t &part_row_cnt);
  int add_range(ObEvalCtx &eval_ctx,
                int64_t part_id,
                int64_t ref_table_id,
                const common::ObIArray<ObExpr *> &exprs,
                const bool table_has_hidden_pk, int64_t &part_row_cnt);
  inline int64_t count() const { return partitions_ranges_.count(); }
  inline void set_partition_mode() { mode_ = PARTITION_PARTICLE; }
  inline void set_range_mode() { mode_ = RANGE_PARTICLE; }

  void release();
  int64_t to_string(char* buf, const int64_t buf_len) const;
  void set_mem_attr(const common::ObMemAttr &attr) { allocator_.set_attr(attr); }

private:
  int init_main_table_rowkey(const int64_t column_count, common::ObNewRow &row);

private:
  common::ObArenaAllocator allocator_;
  common::ObArray<ObPartitionScanRanges*, common::ObIAllocator &> partitions_ranges_;
  common::ObNewRow main_table_rowkey_;
  GetRangeMode mode_;
  // no need to serialize
  int64_t range_offset_;
};

}
}

#endif
