/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_TABLE_REDIS_OB_REDIS_OPERATOR_H
#define OCEANBASE_TABLE_REDIS_OB_REDIS_OPERATOR_H

#include "observer/table/redis/ob_redis_service.h"
#include "share/table/ob_table.h"
#include "observer/table/ob_table_batch_service.h"
#include "observer/table/ob_table_cache.h"
#include "observer/table/ob_table_filter.h"
#include "observer/table/ob_table_query_common.h"
#include "share/table/ob_redis_parser.h"

namespace oceanbase
{
namespace table
{
#define QUERY_ITER_START(ctx, query, tb_ctx, iter)                                      \
  ObTableQueryResultIterator *iter = nullptr;                                           \
  observer::ObReqTimeGuard req_timeinfo_guard;                                          \
  ObTableApiCacheGuard cache_guard;                                                     \
  ObTableApiSpec *spec = nullptr;                                                       \
  ObTableApiExecutor *executor = nullptr;                                               \
  ObTableQueryResult result;                                                            \
  const int64_t timeout_ts = ctx.timeout_ts_;                                           \
  ObTableApiScanRowIterator row_iter;                                                   \
  if (OB_FAIL(ret)) {                                                                   \
  } else if (OB_FAIL(init_scan_tb_ctx(cache_guard, query, tb_ctx))) {                   \
    COMMON_LOG(WARN, "fail to init scan table context", K(ret), K(query));              \
  } else if (OB_FAIL(tb_ctx.init_trans(ctx.trans_param_->trans_desc_,                   \
                                       ctx.trans_param_->tx_snapshot_))) {              \
    COMMON_LOG(WARN, "fail to init trans", K(ret));                                     \
  } else if (OB_FAIL(cache_guard.get_spec<TABLE_API_EXEC_SCAN>(&tb_ctx, spec))) {       \
    COMMON_LOG(WARN, "fail to get spec from cache", K(ret));                            \
  } else if (OB_FAIL(spec->create_executor(tb_ctx, executor))) {                        \
    COMMON_LOG(WARN, "fail to generate executor", K(ret), K(tb_ctx));                   \
  } else if (OB_FAIL(ObTableQueryUtils::generate_query_result_iterator(                 \
                 ctx.allocator_, query, false, result, tb_ctx, iter))) {                \
    COMMON_LOG(WARN, "fail to generate query result iterator", K(ret));                 \
  } else if (OB_FAIL(row_iter.open(static_cast<ObTableApiScanExecutor *>(executor)))) { \
    COMMON_LOG(WARN, "fail to open scan row iterator", K(ret));                         \
  } else {                                                                              \
    iter->set_scan_result(&row_iter);                                                   \
  }                                                                                     \
  if (OB_SUCC(ret)) {

#define QUERY_ITER_END(iter)                                \
  if (OB_LIKELY(ret == OB_ITER_END)) {                      \
    ret = OB_SUCCESS;                                       \
  }                                                         \
  int tmp_ret = OB_SUCCESS;                                 \
  if (OB_SUCCESS != (tmp_ret = row_iter.close())) {         \
    COMMON_LOG(WARN, "fail to close row iter", K(tmp_ret)); \
    ret = COVER_SUCC(tmp_ret);                              \
  }                                                         \
  if (OB_NOT_NULL(spec)) {                                  \
    spec->destroy_executor(executor);                       \
  }                                                         \
  if (OB_NOT_NULL(iter)) {                                  \
    iter->~ObTableQueryResultIterator();                    \
    iter = nullptr;                                         \
  }                                                         \
  }

class CommandOperator
{
public:
  explicit CommandOperator(ObRedisCtx &redis_ctx)
      : redis_ctx_(redis_ctx), op_temp_allocator_(ObMemAttr(MTL_ID(), "RedisCmdOp"))
  {}
  virtual ~CommandOperator() = default;

  int process_table_batch_op(const ObTableBatchOperation &req_ops, ResultFixedArray &results,
                             bool returning_affected_entity = false, bool returning_rowkey = false);

  int process_table_single_op(const table::ObTableOperation &op, ObTableOperationResult &op_res,
                              bool returning_affected_entity = false,
                              bool returning_rowkey = false);

  int process_table_query_count(ObIAllocator &allocator, const ObTableQuery &query,
                                int64_t &row_count);

protected:
  const int64_t HASH_SET_ROWKEY_SIZE = 3;
  static const ObString COUNT_STAR_PROPERTY;  // count(*)
  ObRedisCtx &redis_ctx_;
  common::ObArenaAllocator op_temp_allocator_;

  int build_hash_set_rowkey(int64_t db, const ObString &key, bool is_data, bool is_min,
                            common::ObRowkey &rowkey);
  int build_hash_set_rowkey(int64_t db, const ObString &key, bool is_data, const ObString &member,
                            common::ObRowkey &rowkey);
  int build_hash_set_rowkey_entity(int64_t db, const ObString &key, bool is_data,
                                   const ObString &member, ObITableEntity *&entity);
  int build_range(const common::ObRowkey &start_key, const common::ObRowkey &end_key,
                  ObNewRange *&range, bool inclusive_start = true, bool inclusive_end = true);
  int init_table_ctx(const ObTableOperation &op, ObTableCtx &tb_ctx,
                     bool returning_affected_entity = false, bool returning_rowkey = false);
  int init_scan_tb_ctx(ObTableApiCacheGuard &cache_guard, const ObTableQuery &query,
                       ObTableCtx &tb_ctx);
  int init_batch_ctx(const ObTableBatchOperation &req_ops, ObTableBatchCtx &batch_ctx);
  int hashset_to_array(const common::hash::ObHashSet<ObString> &hash_set,
                       ObIArray<ObString> &ret_arr);

private:
  DISALLOW_COPY_AND_ASSIGN(CommandOperator);
};

class FilterStrBuffer
{
public:
  explicit FilterStrBuffer(ObIAllocator &allocator) : buffer_(&allocator)
  {}
  virtual ~FilterStrBuffer()
  {}

  int add_value_compare(hfilter::CompareOperator cmp_op, const ObString &col_name,
                        const ObString &cmp_str);

  int add_conjunction(bool is_and);

  OB_INLINE int get_filter_str(ObString &filter_str)
  {
    return buffer_.get_result_string(filter_str);
  }

private:
  int cmp_op_to_str(hfilter::CompareOperator cmp_op, ObString &str);

  common::ObStringBuffer buffer_;
  DISALLOW_COPY_AND_ASSIGN(FilterStrBuffer);
};

}  // namespace table
}  // namespace oceanbase
#endif /* OCEANBASE_TABLE_REDIS_OB_REDIS_OPERATOR_H */
