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

#ifndef SQL_ENGINE_AGGREGATE_OB_HASH_VARIANT
#define SQL_ENGINE_AGGREGATE_OB_HASH_VARIANT
#include "ob_exec_hash_struct_vec.h"
#include "share/aggregate/processor.h"
#include <boost/variant/variant.hpp>

namespace oceanbase
{
namespace sql
{


using HashAggSetPtr = boost::variant<ObExtendHashTableVec<ObGroupRowBucket> *,
                                     ObExtendHashTableVec<ObGroupRowBucketInline> *>;



struct SizeVisitor : public boost::static_visitor<int64_t>
{
  template <typename T>
  int64_t operator() (T &t) const
  {
    return t->size();
  }
};

struct MemUsedVisitor : public boost::static_visitor<int64_t>
{
  template <typename T>
  int64_t operator() (T &t) const
  {
    return t->mem_used();
  }
};

struct InitVisitor : public boost::static_visitor<int>
{
  InitVisitor(ObIAllocator *allocator,
              lib::ObMemAttr &mem_attr,
              const common::ObIArray<ObExpr *> &gby_exprs,
              const int64_t hash_expr_cnt,
              ObEvalCtx *eval_ctx,
              int64_t max_batch_size,
              bool nullable,
              bool all_int64,
              int64_t op_id,
              bool use_sstr_aggr,
              int64_t aggr_row_size,
              int64_t initial_size,
              bool auto_extend) : allocator_(allocator),
                                  mem_attr_(mem_attr),
                                  gby_exprs_(gby_exprs),
                                  hash_expr_cnt_(hash_expr_cnt),
                                  eval_ctx_(eval_ctx),
                                  max_batch_size_(max_batch_size),
                                  nullable_(nullable),
                                  all_int64_(all_int64),
                                  op_id_(op_id),
                                  aggr_row_size_(aggr_row_size),
                                  use_sstr_aggr_(use_sstr_aggr),
                                  initial_size_(initial_size),
                                  auto_extend_(auto_extend) {}
  template <typename T>
  int operator() (T &t)
  {
    return t->init(allocator_,
                   mem_attr_,
                   gby_exprs_,
                   hash_expr_cnt_,
                   eval_ctx_,
                   max_batch_size_,
                   nullable_,
                   all_int64_,
                   op_id_,
                   use_sstr_aggr_,
                   aggr_row_size_,
                   initial_size_,
                   auto_extend_);
  }
  ObIAllocator *allocator_;
  lib::ObMemAttr &mem_attr_;
  const common::ObIArray<ObExpr *> &gby_exprs_;
  int64_t hash_expr_cnt_;
  ObEvalCtx *eval_ctx_;
  int64_t max_batch_size_;
  bool nullable_;
  bool all_int64_;
  int64_t op_id_;
  int64_t aggr_row_size_;
  bool use_sstr_aggr_;
  int64_t initial_size_;
  bool auto_extend_;
};

struct GetBktNumVisitor : public boost::static_visitor<int64_t>
{
  template <typename T>
  int64_t operator() (T &t) const
  {
    return t->get_bucket_num();
  }
};

struct GetBktSizeVisitor : public boost::static_visitor<int64_t>
{
  template <typename T>
  int64_t operator() (T &t) const
  {
    return t->get_bucket_size();
  }
};

struct IsInitVisitor : public boost::static_visitor<bool>
{
  template <typename T>
  bool operator() (T &t) const
  {
    return t->is_inited();
  }
};

struct ReuseVisitor : public boost::static_visitor<>
{
  template <typename T>
  void operator() (T &t)
  {
    return t->reuse();
  }
};

struct ResizeVisitor : public boost::static_visitor<int>
{
  ResizeVisitor(ObIAllocator *allocator, int64_t bucket_num) : allocator_(allocator), bucket_num_(bucket_num) {}
  template <typename T>
  int operator() (T &t)
  {
    return t->resize(allocator_, bucket_num_);
  }
  ObIAllocator *allocator_;
  int64_t bucket_num_;
};

struct DestroyVisitor : public boost::static_visitor<>
{
  template <typename T>
  void operator() (T &t)
  {
    return t->destroy();
  }
};

struct InitGroupStoreVisitor : public boost::static_visitor<int>
{
  InitGroupStoreVisitor(int64_t dir_id, ObSqlMemoryCallback *callback,
                        ObIAllocator &alloc, ObIOEventObserver *observer,
                        int64_t max_batch_size, int64_t agg_row_size)
                        : dir_id_(dir_id), callback_(callback),
                          alloc_(alloc), observer_(observer),
                          max_batch_size_(max_batch_size), agg_row_size_(agg_row_size) {}
  template <typename T>
  int operator() (T &t)
  {
    return t->init_group_store(dir_id_, callback_, alloc_,
                               observer_, max_batch_size_,
                               agg_row_size_);
  }
  int64_t dir_id_;
  ObSqlMemoryCallback *callback_;
  ObIAllocator &alloc_;
  ObIOEventObserver *observer_;
  int64_t max_batch_size_;
  int64_t agg_row_size_;
};

struct ResetGroupStoreVisitor : public boost::static_visitor<>
{
  template <typename T>
  void operator() (T &t)
  {
    return t->reset_group_store();
  }
};

struct GetRowMetaVisitor : public boost::static_visitor<const RowMeta &>
{
  template <typename T>
  const RowMeta &operator() (T &t) const
  {
    return t->get_row_meta();
  }
};

struct GetNextBatchVisitor : public boost::static_visitor<int>
{
  GetNextBatchVisitor(const ObCompactRow **rows,
                      const int64_t max_rows,
                      int64_t &read_rows) : rows_(rows), max_rows_(max_rows),
                                            read_rows_(read_rows) {}
  template <typename T>
  int operator() (T &t)
  {
    return t->get_next_batch(rows_, max_rows_, read_rows_);
  }
  const ObCompactRow **rows_;
  int64_t max_rows_;
  int64_t &read_rows_;
};

struct AppendBatchVisitor : public boost::static_visitor<int>
{
  AppendBatchVisitor(const common::ObIArray<ObExpr *> &gby_exprs,
                     const ObBatchRows &child_brs,
                     const bool *is_dumped,
                     const uint64_t *hash_values,
                     const common::ObIArray<int64_t> &lengths,
                     char **batch_new_rows,
                     int64_t &agg_group_cnt,
                     bool need_reinit_vectors) : gby_exprs_(gby_exprs),
                                                 child_brs_(child_brs),
                                                 is_dumped_(is_dumped),
                                                 hash_values_(hash_values),
                                                 lengths_(lengths),
                                                 batch_new_rows_(batch_new_rows),
                                                 agg_group_cnt_(agg_group_cnt),
                                                 need_reinit_vectors_(need_reinit_vectors) {}
  template <typename T>
  int operator() (T &t)
  {
    return t->append_batch(gby_exprs_, child_brs_, is_dumped_, hash_values_, lengths_,
                           batch_new_rows_, agg_group_cnt_, need_reinit_vectors_);
  }
  const common::ObIArray<ObExpr *> &gby_exprs_;
  const ObBatchRows &child_brs_;
  const bool *is_dumped_;
  const uint64_t *hash_values_;
  const common::ObIArray<int64_t> &lengths_;
  char **batch_new_rows_;
  int64_t &agg_group_cnt_;
  bool need_reinit_vectors_;
};

struct ProcessBatchVisitor : public boost::static_visitor<int>
{
  ProcessBatchVisitor(const common::ObIArray<ObExpr *> &gby_exprs,
                      const ObBatchRows &child_brs,
                      const bool *is_dumped,
                      const uint64_t *hash_values,
                      const common::ObIArray<int64_t> &lengths,
                      const bool can_append_batch,
                      const ObGbyBloomFilterVec *bloom_filter,
                      char **batch_old_rows,
                      char **batch_new_rows,
                      int64_t &agg_row_cnt,
                      int64_t &agg_group_cnt,
                      BatchAggrRowsTable *batch_aggr_rows,
                      bool need_reinit_vectors) : gby_exprs_(gby_exprs),
                                                  child_brs_(child_brs),
                                                  is_dumped_(is_dumped),
                                                  hash_values_(hash_values),
                                                  lengths_(lengths),
                                                  can_append_batch_(can_append_batch),
                                                  bloom_filter_(bloom_filter),
                                                  batch_old_rows_(batch_old_rows),
                                                  batch_new_rows_(batch_new_rows),
                                                  agg_row_cnt_(agg_row_cnt),
                                                  agg_group_cnt_(agg_group_cnt),
                                                  batch_aggr_rows_(batch_aggr_rows),
                                                  need_reinit_vectors_(need_reinit_vectors) {}
  template <typename T>
  int operator() (T &t)
  {
    return t->process_batch(gby_exprs_, child_brs_, is_dumped_,
                            hash_values_, lengths_, can_append_batch_,
                            bloom_filter_, batch_old_rows_, batch_new_rows_,
                            agg_row_cnt_, agg_group_cnt_, batch_aggr_rows_,
                            need_reinit_vectors_);
  }
  const common::ObIArray<ObExpr *> &gby_exprs_;
  const ObBatchRows &child_brs_;
  const bool *is_dumped_;
  const uint64_t *hash_values_;
  const common::ObIArray<int64_t> &lengths_;
  const bool can_append_batch_;
  const ObGbyBloomFilterVec *bloom_filter_;
  char **batch_old_rows_;
  char **batch_new_rows_;
  int64_t &agg_row_cnt_;
  int64_t &agg_group_cnt_;
  BatchAggrRowsTable *batch_aggr_rows_;
  bool need_reinit_vectors_;
};

struct GetProbeCntVisitor : public boost::static_visitor<int64_t>
{
  template <typename T>
  int64_t operator() (T &t) const
  {
    return t->get_probe_cnt();
  }
};

struct IsSstrAggrValidVisitor : public boost::static_visitor<bool>
{
  template <typename T>
  bool operator() (T &t) const
  {
    return t->is_sstr_aggr_valid();
  }
};

struct PrefetchVisitor :public boost::static_visitor<>
{
  PrefetchVisitor(const ObBatchRows &brs, uint64_t *hash_vals)
               : brs_(&brs), hash_vals_(hash_vals) {}
  template <typename T>
  void operator() (T &t) const
  {
    return t->prefetch(*brs_, hash_vals_);
  }
  const ObBatchRows *brs_;
  uint64_t *hash_vals_;
};



template <typename CB>
struct ForeachBucketHashVisitor : public boost::static_visitor<int>
{
  ForeachBucketHashVisitor(CB &cb) : cb_(cb) {}
  template <typename T>
  int operator() (T &t)
  {
    return t->foreach_bucket_hash(cb_);
  }
  CB &cb_;
};




class ObAggrHashTableWapper
{
public:
  enum class Type {
    INVALID = -1,
    INLINE,
    OUTLINE,
    MAX
  };
  ObAggrHashTableWapper() : hash_table_ptr_(), real_ptr_(nullptr), outline_ptr_(nullptr), inline_ptr_(nullptr), type_(Type::INVALID), inited_(false) {}
  int prepare_hash_table(const int64_t item_size, const uint64_t tenant_id, common::ObIAllocator &alloc);
  void destroy();
  int64_t size() const
  {
    SizeVisitor visitor;
    return boost::apply_visitor(visitor, hash_table_ptr_);
  }
  int64_t mem_used() const
  {
    MemUsedVisitor visitor;
    return boost::apply_visitor(visitor, hash_table_ptr_);
  }
  int init(ObIAllocator *allocator,
           lib::ObMemAttr &mem_attr,
           const common::ObIArray<ObExpr *> &gby_exprs,
           const int64_t hash_expr_cnt,
           ObEvalCtx *eval_ctx,
           int64_t max_batch_size,
           bool nullable,
           bool all_int64,
           int64_t op_id,
           bool use_sstr_aggr,
           int64_t aggr_row_size,
           int64_t initial_size,
           bool auto_extend)
  {
    InitVisitor visitor(allocator, mem_attr, gby_exprs,
                        hash_expr_cnt, eval_ctx, max_batch_size,
                        nullable, all_int64, op_id,
                        use_sstr_aggr, aggr_row_size, initial_size,
                        auto_extend);
    return boost::apply_visitor(visitor, hash_table_ptr_);
  }

  bool is_inited() const
  {
    bool res = false;
    if (inited_) {
      IsInitVisitor visitor;
      res = boost::apply_visitor(visitor, hash_table_ptr_);
    }
    return res;
  }

  int64_t get_bucket_num() const
  {
    GetBktNumVisitor visitor;
    return boost::apply_visitor(visitor, hash_table_ptr_);
  }

  int64_t get_bucket_size() const
  {
    GetBktSizeVisitor visitor;
    return boost::apply_visitor(visitor, hash_table_ptr_);
  }

  void reuse()
  {
    if (inited_) {
      ReuseVisitor visitor;
      return boost::apply_visitor(visitor, hash_table_ptr_);
    }
  }
  int resize(ObIAllocator *allocator, int64_t bucket_num)
  {
    ResizeVisitor visitor(allocator, bucket_num);
    return boost::apply_visitor(visitor, hash_table_ptr_);
  }
  int init_group_store(int64_t dir_id, ObSqlMemoryCallback *callback,
                       ObIAllocator &alloc, ObIOEventObserver *observer,
                       int64_t max_batch_size, int64_t agg_row_size)
  {
    InitGroupStoreVisitor visitor(dir_id, callback, alloc, observer, max_batch_size, agg_row_size);
    return boost::apply_visitor(visitor, hash_table_ptr_);
  }

  void reset_group_store()
  {
    ResetGroupStoreVisitor visitor;
    return boost::apply_visitor(visitor, hash_table_ptr_);
  }

  const RowMeta &get_row_meta() const
  {
    GetRowMetaVisitor visitor;
    return boost::apply_visitor(visitor, hash_table_ptr_);
  }

  int append_batch(const common::ObIArray<ObExpr *> &gby_exprs,
                   const ObBatchRows &child_brs,
                   const bool *is_dumped,
                   const uint64_t *hash_values,
                   const common::ObIArray<int64_t> &lengths,
                   char **batch_new_rows,
                   int64_t &agg_group_cnt,
                   bool need_reinit_vectors = false)
  {
    AppendBatchVisitor visitor(gby_exprs, child_brs, is_dumped, hash_values, lengths,
                               batch_new_rows, agg_group_cnt, need_reinit_vectors);
    return boost::apply_visitor(visitor, hash_table_ptr_);
  }
  int process_batch(const common::ObIArray<ObExpr *> &gby_exprs,
                    const ObBatchRows &child_brs,
                    const bool *is_dumped,
                    const uint64_t *hash_values,
                    const common::ObIArray<int64_t> &lengths,
                    const bool can_append_batch,
                    const ObGbyBloomFilterVec *bloom_filter,
                    char **batch_old_rows,
                    char **batch_new_rows,
                    int64_t &agg_row_cnt,
                    int64_t &agg_group_cnt,
                    BatchAggrRowsTable *batch_aggr_rows,
                    bool need_reinit_vectors = false)
  {
    ProcessBatchVisitor visitor(gby_exprs, child_brs, is_dumped,
                                hash_values, lengths, can_append_batch,
                                bloom_filter, batch_old_rows, batch_new_rows,
                                agg_row_cnt, agg_group_cnt, batch_aggr_rows,
                                need_reinit_vectors);
    return boost::apply_visitor(visitor, hash_table_ptr_);
  }
  void prefetch(const ObBatchRows &brs, uint64_t *hash_vals) const
  {
    PrefetchVisitor visitor(brs, hash_vals);
    return boost::apply_visitor(visitor, hash_table_ptr_);
  }

  int get_next_batch(const ObCompactRow **rows, const int64_t max_rows, int64_t &read_rows)
  {
    GetNextBatchVisitor visitor(rows, max_rows, read_rows);
    return boost::apply_visitor(visitor, hash_table_ptr_);
  }

  int64_t get_probe_cnt() const
  {
    GetProbeCntVisitor visitor;
    return boost::apply_visitor(visitor, hash_table_ptr_);
  }

  template <typename CB>
  int foreach_bucket_hash(CB &cb)
  {
    ForeachBucketHashVisitor<CB> visitor(cb);
    return boost::apply_visitor(visitor, hash_table_ptr_);
  }

  bool is_sstr_aggr_valid() const
  {
    IsSstrAggrValidVisitor visitor;
    return boost::apply_visitor(visitor, hash_table_ptr_);
  }

private:
  HashAggSetPtr hash_table_ptr_;
  void *real_ptr_;
  // TODO: remove following members @peihan.dph
  ObExtendHashTableVec<ObGroupRowBucket> *outline_ptr_;
  ObExtendHashTableVec<ObGroupRowBucketInline> *inline_ptr_;
  Type type_;
  bool inited_;
};



}//ns sql
}//ns oceanbase

#endif
