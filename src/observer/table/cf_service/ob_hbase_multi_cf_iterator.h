/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_HBASE_MULTI_CF_ITERATOR_H
#define _OB_HBASE_MULTI_CF_ITERATOR_H

#include "observer/table/common/ob_hbase_common_struct.h"
#include "observer/table/cf_service/ob_hbase_cf_iterator.h"
#include "observer/table/ob_table_merge_filter.h"

namespace oceanbase
{
namespace table
{
class ObHbaseCFQuery
{
public:
  ObHbaseCFQuery()
  : query_(),
    hbase_query_(query_, false)
  {}
  ~ObHbaseCFQuery() = default;

public:
  OB_INLINE const ObTableQuery &get_query() const { return query_; }
  OB_INLINE ObTableQuery &get_query() { return query_; }
  OB_INLINE const ObHbaseQuery &get_hbase_query() const { return hbase_query_; }
  OB_INLINE ObHbaseQuery &get_hbase_query() { return hbase_query_; }
  OB_INLINE void set_table_id(const uint64_t table_id) { hbase_query_.set_table_id(table_id); }
  OB_INLINE void set_table_name(const ObString &table_name) { hbase_query_.set_table_name(table_name); }

  TO_STRING_KV(K_(query), K_(hbase_query));
private:
  ObTableQuery query_;
  ObHbaseQuery hbase_query_;
};

class ObHbaseMultiCFIterator : public ObHbaseQueryResultIterator
{
  using ResultMergeIterator = ObMergeTableQueryResultIterator;
public:
  ObHbaseMultiCFIterator(const ObHbaseQuery &query, ObTableExecCtx &exec_ctx);
  virtual ~ObHbaseMultiCFIterator()
  {
    if (OB_NOT_NULL(merge_iter_)) {
      merge_iter_->~ResultMergeIterator();
      merge_iter_ = nullptr;
    }
    for (int64_t i = 0; i < cf_iters_.count(); i++) {
      if (OB_NOT_NULL(cf_iters_.at(i))) {
        cf_iters_.at(i)->~ObHbaseCFIterator();
        cf_iters_.at(i) = nullptr;
      }
    }
    cf_iters_.reset();
    for (int64_t i = 0; i < cf_queries_.count(); i++) {
      if (OB_NOT_NULL(cf_queries_.at(i))) {
        cf_queries_.at(i)->~ObHbaseCFQuery();
        cf_queries_.at(i) = nullptr;
      }
    }
    cf_queries_.reset();
    if (OB_NOT_NULL(compare_)) {
      compare_->~ObTableMergeFilterCompare();
      compare_ = nullptr;
    }
    allocator_.reset();
  }
  virtual int init();
  virtual void close() 
  {
    for (int64_t i = 0; i < cf_iters_.count(); i++) {
      if (OB_NOT_NULL(cf_iters_.at(i))) {
        cf_iters_.at(i)->close();
      }
    }
  }
  virtual int get_next_result(ObTableQueryResult &hbase_wide_rows) override;
  virtual int get_next_result(ObTableQueryIterableResult &hbase_wide_rows) override;
  virtual int get_next_result(ObTableQueryIterableResult *&hbase_wide_rows) override;
  virtual bool has_more_result() const override;
  virtual hfilter::Filter *get_filter() const override { return nullptr; }
  TO_STRING_KV(K_(hbase_query), K_(cf_queries), K_(cf_iters));
private:
  template <typename T>
  int inner_get_next_result(T& hbase_wide_rows)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(merge_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "unexpected null merge iter", K(ret));
    } else if (OB_FAIL(merge_iter_->get_next_result(hbase_wide_rows))) {
      if (ret != OB_ITER_END) {
        COMMON_LOG(WARN, "fail to get next result");
      } else {
        COMMON_LOG(DEBUG, "merge iter end", K(ret), KP(merge_iter_));
      }
    }
    COMMON_LOG(DEBUG, "hbase multi cf iterator get one result", K(ret), K(hbase_wide_rows));
    return ret;
  }
  int init_cf_queries(ObTableExecCtx &exec_ctx, const ObHbaseQuery &hbase_query);
  int init_cf_iters();
  int update_tablet_ids_by_part_ids(common::ObIArray<std::pair<int64_t, int64_t>> &part_subpart_ids,
                                   const share::schema::ObSimpleTableSchemaV2 &table_schema,
                                   ObTableQuery &query);
  static bool schema_cmp_func_for_reverse(const schema::ObSimpleTableSchemaV2 *lhs,
                                          const schema::ObSimpleTableSchemaV2 *rhs)
  {
    return lhs->get_table_name() > rhs->get_table_name();
  }
  static bool schema_cmp_func(const schema::ObSimpleTableSchemaV2 *lhs,
                              const schema::ObSimpleTableSchemaV2 *rhs)
  {
    return lhs->get_table_name() < rhs->get_table_name();
  }
private:
  common::ObArenaAllocator allocator_; 
  const ObHbaseQuery &hbase_query_;
  ObTableExecCtx &exec_ctx_;
  common::ObSEArray<ObHbaseCFQuery *, 4> cf_queries_;
  common::ObSEArray<ObHbaseCFIterator *, 4> cf_iters_;
  ObTableMergeFilterCompare *compare_;
  ObMergeTableQueryResultIterator *merge_iter_;
};

} // end of namespace table
} // end of namespace oceanbase

#endif // _OB_HBASE_COLUMN_FAMILY_ITERATOR_H
