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
#ifndef _OB_TABLE_AGGREGATION_H
#define _OB_TABLE_AGGREGATION_H
#include "share/table/ob_table.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
namespace oceanbase
{
namespace table
{
////////////////////////////////////////////////////////////////
// structs of table aggregation
////////////////////////////////////////////////////////////////

class ObTableAggCalculator
{
public:
  const static int64_t INVALID_PROJECT_ID = -1;
public:
  ObTableAggCalculator(const ObTableQuery &query)
      : allocator_(common::ObModIds::TABLE_PROC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        size_(query.get_aggregations().count()),
        query_aggs_(query.get_aggregations()),
        projs_(),
        agg_columns_(query.get_select_columns()),
        results_(allocator_),
        sums_(allocator_),
        counts_(allocator_),
        deep_copy_buffers_(allocator_)
  {
  }
  virtual ~ObTableAggCalculator() {}
public:
  int init();
  bool is_exist() { return query_aggs_.count() != 0; }

  // Aggregates rows scanned from storage
  //
  // @param [in]  row        The rows scanned from storage.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int aggregate(const common::ObNewRow &row);

  // Sum and average are calculated after all rows are aggregated
  //
  // @return void.
  void final_aggregate();

  const common::ObIArray<ObObj> &get_aggregate_results() { return results_; }

  // Set projs_
  //
  // @param [in]  projs      The projs from ctx.
  // @return void.
  void set_projs(const common::ObIArray<uint64_t> &projs) { projs_ = &projs; }

  const common::ObIArray<common::ObString>& get_agg_columns() const { return agg_columns_; }
public:
  TO_STRING_KV(K_(size),
               K_(query_aggs),
               K_(projs),
               K_(results),
               K_(sums),
               K_(counts));
private:
  // Aggregate the max values of the idx column.
  //
  // @param [in]  idx        The aggregation idx.
  // @param [in]  row        The rows scanned from storage.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int aggregate_max(uint64_t idx, const ObNewRow &row);

  // Aggregate the min values of the idx column.
  //
  // @param [in]  idx        The aggregation idx.
  // @param [in]  row        The rows scanned from storage.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int aggregate_min(uint64_t idx, const ObNewRow &row);

  // Aggregate the sum values of the idx column.
  //
  // @param [in]  idx        The aggregation idx.
  // @param [in]  row        The rows scanned from storage.
  // @param [in]  key_word   The key word of count, column name or '*', like 'count(c1)', 'count(*)'.
  // @return void

  void aggregate_count(uint64_t idx, const ObNewRow &row, const ObString &key_word);
  // Aggregate the sum values of the idx column.
  //
  // @param [in]  idx   The aggregation idx.
  // @param [in]  row   The rows scanned from storage.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int aggregate_sum(uint64_t idx, const ObNewRow &row);

  // Aggregate the average values of the idx column.
  //
  // @param [in]  idx   The aggregation idx.
  // @param [in]  row   The rows scanned from storage.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int aggregate_avg(uint64_t idx, const ObNewRow &row);

  // Deep copy value while doing min/max aggregation.
  //
  // @param [in]  src        The src object which need deep copy.
  // @param [in]  dst        The dst object which need deep copy to.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int deep_copy_value(int64_t idx, const ObObj &src, ObObj &dst);

  // Deep copy while object need deep copy, shadow copy in other object.
  //
  // @param [in]  src        The src object which need assign.
  // @param [in]  dst        The dst object which need assign to.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int assign_value(int64_t idx, const ObObj &src, ObObj &dst);
private:
  struct TempBuffer
  {
  public:
    TempBuffer()
        : buf_(nullptr),
          size_(0)
    {}
    TO_STRING_KV(K_(size));
  public:
    char *buf_;
    uint64_t size_;
  };
private:
  common::ObArenaAllocator allocator_;
  int64_t size_;
  const common::ObIArray<table::ObTableAggregation> &query_aggs_; // agg info from user
  const common::ObIArray<uint64_t> *projs_; // agg cell index in schema
  const common::ObIArray<common::ObString> &agg_columns_; // agg column info from user
  common::ObFixedArray<ObObj, common::ObIAllocator> results_; // agg result which need to add rpc response
  common::ObFixedArray<double, common::ObIAllocator> sums_; // sum info
  common::ObFixedArray<int64_t, common::ObIAllocator> counts_; // count info
  common::ObFixedArray<TempBuffer, common::ObIAllocator> deep_copy_buffers_; // for min/max agg
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableAggCalculator);
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLE_AGGREGATION_H */