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
#include "share/table/redis/ob_redis_parser.h"
#include "observer/table/redis/ob_redis_iterator.h"
#include "observer/table/redis/ob_redis_meta.h"
#include "observer/table/ob_table_multi_batch_service.h"

namespace oceanbase
{
namespace table
{
#define QUERY_ITER_START(ctx, query, tb_ctx, iter, meta_val)                             \
  ObTableQueryResultIterator *iter = nullptr;                                            \
  observer::ObReqTimeGuard req_timeinfo_guard;                                           \
  ObTableApiCacheGuard cache_guard;                                                      \
  ObTableApiSpec *spec = nullptr;                                                        \
  ObTableApiExecutor *executor = nullptr;                                                \
  ObTableQueryResult result;                                                             \
  const int64_t timeout_ts = ctx.timeout_ts_;                                            \
  ObTableApiScanRowIterator *row_iter = nullptr;                                         \
  if (OB_FAIL(ret)) {                                                                    \
  } else if (OB_FAIL(init_scan_tb_ctx(cache_guard, query, meta_val, tb_ctx))) {          \
    SERVER_LOG(WARN, "fail to init scan table context", K(ret), K(query));               \
  } else if (OB_FAIL(tb_ctx.init_trans(ctx.trans_param_->trans_desc_,                    \
                                       ctx.trans_param_->tx_snapshot_))) {               \
    SERVER_LOG(WARN, "fail to init trans", K(ret));                                      \
  } else if (OB_FAIL(cache_guard.get_spec<TABLE_API_EXEC_SCAN>(&tb_ctx, spec))) {        \
    SERVER_LOG(WARN, "fail to get spec from cache", K(ret));                             \
  } else if (OB_FAIL(spec->create_executor(tb_ctx, executor))) {                         \
    SERVER_LOG(WARN, "fail to generate executor", K(ret), K(tb_ctx));                    \
  } else if (OB_FAIL(ObTableQueryUtils::generate_query_result_iterator(                  \
            tb_ctx.get_allocator(), query, false, result, tb_ctx, iter))) {                 \
    SERVER_LOG(WARN, "fail to generate query result iterator", K(ret));                  \
  } else if (OB_FAIL(ObTableQueryUtils::get_scan_row_interator(tb_ctx, row_iter))) {     \
    SERVER_LOG(WARN, "fail to get scan row iterator", K(ret));                           \
  } else if (OB_FAIL(row_iter->open(static_cast<ObTableApiScanExecutor *>(executor)))) { \
    SERVER_LOG(WARN, "fail to open scan row iterator", K(ret));                          \
  } else {                                                                               \
    iter->set_scan_result(row_iter);                                                     \
  }                                                                                      \
  if (OB_SUCC(ret)) {

#define QUERY_ITER_END(iter)                                  \
  if (OB_LIKELY(ret == OB_ITER_END)) {                        \
    ret = OB_SUCCESS;                                         \
  }                                                           \
  if (OB_NOT_NULL(row_iter)) {                                \
    int tmp_ret = OB_SUCCESS;                                 \
    if (OB_SUCCESS != (tmp_ret = row_iter->close())) {        \
      SERVER_LOG(WARN, "fail to close row iter", K(tmp_ret)); \
      ret = COVER_SUCC(tmp_ret);                              \
    }                                                         \
  }                                                           \
  if (OB_NOT_NULL(spec)) {                                    \
    spec->destroy_executor(executor);                         \
  }                                                           \
  if (OB_NOT_NULL(iter)) {                                    \
    iter->~ObTableQueryResultIterator();                      \
    iter = nullptr;                                           \
  }                                                           \
  }

enum RedisOpFlags {
  NONE = 0,
  RETURN_AFFECTED_ROWS = 1 << 0,
  RETURN_ROWKEY = 1 << 1,
  RETURN_REDIS_META = 1 << 2,
  DEL_SKIP_SCAN = 1 << 3,
  BATCH_NOT_ATOMIC = 1 << 4, // for batch
  RETURN_AFFECTED_ENTITY = 1 << 5, // for append, increment
  USE_PUT =  1 << 6 // use put replace insup when mode = minimal
};

typedef common::ObFixedArray<ObTableOperationResult, common::ObIAllocator> ResultFixedArray;

class CommandOperator
{
public:
  explicit CommandOperator(ObRedisCtx &redis_ctx)
      : redis_ctx_(redis_ctx),
        op_temp_allocator_(redis_ctx.allocator_),
        op_entity_factory_(*redis_ctx.entity_factory_),
        model_(ObRedisModel::INVALID),
        tablet_ids_(OB_MALLOC_NORMAL_BLOCK_SIZE,
                    ModulePageAllocator(op_temp_allocator_, "RedisCmd")),
        binlog_row_image_type_(TABLEAPI_SESS_POOL_MGR->get_binlog_row_image())
  {}
  virtual ~CommandOperator() {}

  int process_table_batch_op(const ObTableBatchOperation &req_ops,
                             ResultFixedArray &results,
                             ObRedisMeta *meta = nullptr,
                             uint8_t flags = RedisOpFlags::NONE,
                             common::ObIAllocator *allocator = nullptr,
                             ObITableEntityFactory* entity_factory = nullptr,
                             ObIArray<ObTabletID> *tablet_ids = nullptr);
  int process_table_multi_batch_op(const ObTableMultiBatchRequest &req,
                                   ObTableMultiBatchResult &result,
                                   uint8_t flags = RedisOpFlags::NONE);

  int process_table_single_op(const table::ObTableOperation &op,
                              ObTableOperationResult &op_res,
                              ObRedisMeta *meta = nullptr,
                              uint8_t flags = RedisOpFlags::NONE,
                              common::ObIAllocator *allocator = nullptr,
                              ObITableEntityFactory* entity_factory = nullptr);

  int process_table_query_count(ObIAllocator &allocator,
                                const ObTableQuery &query,
                                const ObRedisMeta *meta,
                                int64_t &row_count);
  int build_complex_type_rowkey_entity(int64_t db,
                                       const ObString &key,
                                       bool is_data,
                                       const ObString &member,
                                       ObITableEntity *&entity);

  int insup_meta(int64_t db, const ObString &key, ObRedisModel model);
  int check_and_insup_meta(int64_t db,
                           const ObString &key,
                           ObRedisModel model,
                           bool &do_insup,
                           ObRedisMeta *&meta);
  int add_complex_type_subkey_scan_range(int64_t db, const ObString &key, ObTableQuery &query);
  static int build_complex_type_rowkey(
      ObIAllocator &allocator,
      int64_t db,
      const ObString &key,
      bool is_data,
      const ObString &member,
      common::ObRowkey &rowkey);
  int do_group_complex_type_set();
  int do_group_complex_type_subkey_exists(ObRedisModel model);

protected:
  int build_range(const common::ObRowkey &start_key,
                  const common::ObRowkey &end_key,
                  ObNewRange *&range,
                  bool inclusive_start = true,
                  bool inclusive_end = true);
  int init_table_ctx(const ObTableOperation &op, ObTableCtx &tb_ctx,
                     ObRedisMeta *meta = nullptr,
                     uint8_t flags = RedisOpFlags::NONE,
                     bool is_batch_ctx = false);
  int init_scan_tb_ctx(ObTableApiCacheGuard &cache_guard,
                       const ObTableQuery &query,
                       const ObRedisMeta *meta,
                       ObTableCtx &tb_ctx);
  int init_batch_ctx(const ObTableBatchOperation &req_ops, uint8_t flags, ObTableBatchCtx &batch_ctx);

  int do_get_meta_entity(ObITableEntity &req_meta_entity, ObITableEntity *&res_meta_entity);

protected:
  // common functions for ZSET/SET/HASH
  // return OB_ITER_END if meta not exist or is expired
  int get_meta(int64_t db, const ObString &key, ObRedisModel model, ObRedisMeta *&meta_info);
  int build_complex_type_rowkey(int64_t db,
                                const ObString &key,
                                bool is_next_prefix,
                                common::ObRowkey &rowkey);
  int put_meta(int64_t db, const ObString &key, ObRedisModel model, const ObRedisMeta &meta_info);
  int gen_meta_entity(int64_t db, const ObString &key, ObRedisModel model,
                      const ObRedisMeta &meta_info, ObITableEntity *&put_meta_entity);
  int add_meta_select_columns(ObTableQuery &query);
  int build_string_rowkey_entity(int64_t db, const ObString &key, ObITableEntity *&entity);
  int get_varbinary_from_entity(const ObITableEntity &entity,
                                const ObString &col_name,
                                ObString &value);
  bool is_incr_out_of_range(int64_t old_val, int64_t incr);
  bool is_incrby_out_of_range(double old_val, double incr);
  bool is_incrby_out_of_range(long double old_val, long double incr);
  int get_subkey_from_entity(
      ObIAllocator &allocator,
      const ObITableEntity &entity,
      ObString &subkey);
  int get_insert_ts_from_entity(const ObITableEntity &entity, int64_t &insert_ts);
  int del_complex_key(
      ObRedisModel model,
      int64_t db,
      const common::ObString &key,
      bool del_meta,
      bool& is_exist);
  int build_del_query(int64_t db, const ObString &key, ObTableQuery &query);
  int build_del_ops(ObRedisModel model,
                    int db,
                    const ObString &key,
                    const ObTableQuery &query,
                    ObTableBatchOperation &del_ops,
                    ObRedisMeta *meta = nullptr);
  int delete_results(const ResultFixedArray &results,
                     const ObArray<ObRowkey> &rowkeys,
                     int64_t &del_num);
  int fake_del_meta(ObRedisModel model,
                                    int64_t db,
                                    const ObString &key,
                                    ObRedisMeta *meta_info = nullptr);
  int get_complex_type_count(int64_t db, const common::ObString &key, int64_t &total_count);
  int fake_del_empty_key_meta(ObRedisModel model,
                              int64_t db,
                              const ObString &key,
                              ObRedisMeta *meta_info = nullptr);
  int get_tablet_id_by_rowkey(ObTableCtx &tb_ctx, const ObRowkey &rowkey, ObTabletID &tablet_id);
  int init_tablet_ids_by_ops(const ObIArray<ObITableOp *> &ops);
  int is_row_expire(const ObNewRow *old_row, const ObRedisMeta *meta, bool &is_old_row_expire);
  // for batch
  int reply_batch_res(const ResultFixedArray &batch_res);
  // for group service
  int get_group_metas(ObIAllocator &allocator, ObRedisModel model, ObIArray<ObRedisMeta *> &metas);
  virtual int fill_set_batch_op(const ObRedisOp &op,
                                ObIArray<ObTabletID> &tablet_ids,
                                ObTableBatchOperation &batch_op);
  int group_get_complex_type_data(const ObString &property_name, ResultFixedArray &batch_res);
  ObTableOperation put_or_insup(const ObITableEntity &entity);
  int put_or_insup(const ObITableEntity &entity, ObTableBatchOperation& batch_op);
  bool can_use_put(const ObITableEntity &entity);

protected:
  static const ObString COUNT_STAR_PROPERTY;  // count(*)
  ObRedisCtx &redis_ctx_;
  common::ObIAllocator &op_temp_allocator_;
  ObITableEntityFactory &op_entity_factory_;
  ObRedisModel model_; // to init tb ctx
  ObString fmt_redis_msg_;
  // remember call tablet_ids_.reuse() before use it
  common::ObSEArray<ObTabletID, 16> tablet_ids_;
  int64_t binlog_row_image_type_;

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

struct RedisKeyNode {
  RedisKeyNode(int64_t db, const ObString &key, ObTabletID tablet_id) : db_(db), key_(key), tablet_id_(tablet_id) {}
  RedisKeyNode() : db_(0), key_(), tablet_id_() {}
  // uint64_t hash() const;
  int hash(uint64_t &res) const;
  bool operator==(const RedisKeyNode &other) const
  {
    return (other.db_ == db_) && (other.key_ == key_);
  }

  TO_STRING_KV(K(db_), K(key_), K(tablet_id_));

  int64_t db_;
  ObString key_;
  ObTabletID tablet_id_;
};

}  // namespace table
}  // namespace oceanbase
#endif /* OCEANBASE_TABLE_REDIS_OB_REDIS_OPERATOR_H */
