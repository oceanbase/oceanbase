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

#define USING_LOG_PREFIX SERVER
#include "ob_redis_list_operator.h"
#include "src/share/table/ob_table.h"
#include "src/share/table/redis/ob_redis_parser.h"
#include "share/table/redis/ob_redis_error.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;

int ListCommandOperator::build_list_type_rowkey(ObIAllocator &allocator,
                                                int64_t db,
                                                const ObString &key,
                                                bool is_data,
                                                int64_t idx,
                                                common::ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *obj_ptr = nullptr;
  if (OB_ISNULL(
          obj_ptr =
              static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * ObRedisUtil::LIST_ROWKEY_NUM)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else if (OB_FAIL(ObRedisCtx::reset_objects(obj_ptr, ObRedisUtil::LIST_ROWKEY_NUM))) {
    LOG_WARN("fail to init object", K(ret));
  } else {
    obj_ptr[0].set_int(db);
    obj_ptr[1].set_varbinary(key);
    obj_ptr[2].set_tinyint(is_data ? 1 : 0);
    obj_ptr[3].set_int(idx);
    rowkey.assign(obj_ptr, ObRedisUtil::LIST_ROWKEY_NUM);
  }
  return ret;
}

int ListCommandOperator::gen_entity_with_rowkey(
    int64_t db,
    const ObString &key,
    int64_t idx,
    bool is_data,
    ObITableEntity *&entity,
    ObITableEntityFactory *entity_factory)
{
  int ret = OB_SUCCESS;
  ObRowkey rowkey;
  if (OB_ISNULL(entity_factory)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null entity_factory_", K(ret));
  } else if (OB_ISNULL(entity = entity_factory->alloc())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("fail to allocate entity", K(ret));
  } else if (OB_FAIL(build_list_type_rowkey(redis_ctx_.allocator_, db, key, is_data, idx, rowkey))) {
    LOG_WARN("fail to build list type rowkey", K(ret));
  } else if (OB_FAIL(entity->set_rowkey(rowkey))) {
    LOG_WARN("fail to set rowkey", K(ret));
  }
  return ret;
}

bool ListCommandOperator::idx_overflowed(int64_t idx)
{
  return idx < INT64_MIN + ObRedisListMeta::INDEX_STEP + 1 || idx > INT64_MAX - ObRedisListMeta::INDEX_STEP;
}

int ListCommandOperator::list_entity_set_value(
    const ObString &value,
    const int64_t insert_ts,
    ObITableEntity *value_entity)
{
  int ret = OB_SUCCESS;
  ObObj value_obj;
  value_obj.set_varbinary(value);
  ObObj insert_tm_obj;
  insert_tm_obj.set_timestamp(insert_ts);
  ObObj null_obj;
  null_obj.set_null();
  if (OB_ISNULL(value_entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("value entity is null", K(ret));
  } else if (OB_FAIL(value_entity->set_property(ObRedisUtil::VALUE_PROPERTY_NAME, value_obj))) {
    LOG_WARN("fail to set value property", K(ret), K(value_obj));
  } else if (OB_FAIL(value_entity->set_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, insert_tm_obj))) {
    LOG_WARN("fail to set value property", K(ret), K(value_obj));
  } else if (OB_FAIL(value_entity->set_property(ObRedisUtil::EXPIRE_TS_PROPERTY_NAME, null_obj))) {
    SERVER_LOG(WARN, "fail to set meta value", K(ret), K(null_obj));
  }

  return ret;
}

int ListCommandOperator::build_push_ops(
    ObTableBatchOperation &push_value_ops,
    int64_t db,
    const ObString &key,
    ObRedisListMeta &list_meta,
    int64_t cur_ts,
    const ObIArray<ObString> &values,
    bool push_left,
    bool need_push_meta /*true*/)
{
  int ret = OB_SUCCESS;

  int64_t left_idx = list_meta.left_idx_;
  int64_t right_idx = list_meta.right_idx_;
  int64_t idx(0);
  // meta not exist, push into a new list
  if (list_meta.count_ == 0) {
    idx = ObRedisListMeta::INIT_INDEX;
    left_idx = ObRedisListMeta::INIT_INDEX;
    right_idx = ObRedisListMeta::INIT_INDEX;
  } else {
    // move position first and then push the data
    if (push_left) {
      left_idx -= ObRedisListMeta::INDEX_STEP;
      idx = left_idx;
    } else {
      right_idx += ObRedisListMeta::INDEX_STEP;
      idx = right_idx;
    }
  }

  ObObj insert_ts_obj;
  insert_ts_obj.set_timestamp(cur_ts);
  for (int64_t i = 0; OB_SUCC(ret) && i < values.count(); ++i) {
    ObITableEntity *value_entity = nullptr;

    if (OB_UNLIKELY(idx_overflowed(idx))) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INDEX_OVERFLOW_ERR);
      LOG_WARN("index overflow", K(ret), K(idx), K(list_meta));
    } else if (OB_FAIL(gen_entity_with_rowkey(db, key, idx, true/*is_data*/, value_entity, &op_entity_factory_))) {
      LOG_WARN("fail to gen entity with rowkey", K(ret));
    } else if (OB_FAIL(list_entity_set_value(values.at(i), ObTimeUtility::fast_current_time(), value_entity))) {
      LOG_WARN("fail to set value property", K(ret), K(values.at(i)));
    } else if (OB_FAIL(push_value_ops.insert_or_update(*value_entity))) {
      LOG_WARN("fail to push back", K(ret));
    } else {
      if (push_left) {
        left_idx -= ObRedisListMeta::INDEX_STEP;
        idx = left_idx;
      } else {
        right_idx += ObRedisListMeta::INDEX_STEP;
        idx = right_idx;
      }
    }
  }
  // generate meta
  if (OB_SUCC(ret)) {
    list_meta.count_ += values.count();
    list_meta.left_idx_ = push_left ? left_idx + ObRedisListMeta::INDEX_STEP : left_idx;
    list_meta.right_idx_ = push_left ? right_idx : right_idx - ObRedisListMeta::INDEX_STEP;
    ObITableEntity *meta_entity = nullptr;
    if (OB_FAIL(gen_meta_entity(db, key, ObRedisModel::LIST, list_meta, meta_entity))) {
      LOG_WARN("fail to put meta into batch operation", K(ret), K(key), K(list_meta));
    } else if (OB_FAIL(push_value_ops.insert_or_update(*meta_entity))) {
      LOG_WARN("fail to push back", K(ret), KPC(meta_entity));
    }
  }

  return ret;
}

int ListCommandOperator::put_data_to_table(
    int64_t db,
    const ObString &key,
    ObRedisListMeta &list_meta,
    const ObIArray<ObString> &values,
    bool push_left)
{
  int ret = OB_SUCCESS;

  int64_t cur_ts = ObTimeUtility::current_time();
  ObTableBatchOperation push_value_ops;
  ResultFixedArray results(op_temp_allocator_);
  if (OB_FAIL(build_push_ops(push_value_ops, db, key, list_meta, cur_ts, values, push_left))) {
    LOG_WARN("fail to build push ops", K(ret), K(push_value_ops));
  } else if (OB_FAIL(process_table_batch_op(push_value_ops, results))) {
    LOG_WARN("fail to process table batch op", K(ret), K(push_value_ops));
  }
  return ret;
}

int ListCommandOperator::do_push(int64_t db, const ObString &key, const ObIArray<ObString> &values, bool push_left, bool need_exit)
{
  int ret = OB_SUCCESS;
  // 1. Get meta data and check whether it has expired.
  ObRedisMeta *meta = nullptr;
  if (OB_FAIL(get_meta(db, key, ObRedisModel::LIST, meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get list meta", K(ret));
    } else {
      if (!need_exit) {
        ret = OB_SUCCESS;
      } else {
        RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisFmt::ZERO);
      }
    }
  }
  ObRedisListMeta *list_meta = reinterpret_cast<ObRedisListMeta*>(meta);
  // 2. push data into a new list
  if (OB_SUCC(ret) && OB_FAIL(put_data_to_table(db, key, *list_meta, values, push_left))) {
    LOG_WARN("fail to insert_or_update data to table", K(ret), K(values), K(push_left));
  }

  // 3. gen response
  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_int(list_meta->count_))) {
      LOG_WARN("fail to set result int", K(ret), K(*list_meta));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int ListCommandOperator::pop_single_count_list(
    int64_t db,
    const ObString &key,
    ObRedisListMeta &list_meta,
    const bool pop_left,
    ObString &res_value)
{
  int ret = OB_SUCCESS;
  // Get data directly
  ObITableEntity *data_req_entity = nullptr;
  ObITableEntity *data_res_entity = nullptr;
  ObObj null_obj;
  ObTableOperation op;
  if (list_meta.left_idx_ != list_meta.right_idx_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("list count is 1 but left_idx != right_idx", K(list_meta));
  } else if (OB_FAIL(gen_entity_with_rowkey(db, key, list_meta.left_idx_, true/*is_data*/, data_req_entity, &op_entity_factory_))) {
    LOG_WARN("fail to gen entity with rowkey", K(ret));
  } else {
    ObTableOperation op = ObTableOperation::retrieve(*data_req_entity);
    ObTableOperationResult result;
    if (OB_FAIL(process_table_single_op(op, result, nullptr/*meta*/, RedisOpFlags::RETURN_AFFECTED_ROWS))) {
      LOG_WARN("fail to get list meta", K(ret), KP(data_req_entity));
    } else if (OB_FAIL(result.get_entity(data_res_entity))) {
      LOG_WARN("fail to get entity", K(ret), K(result));
    } else if (OB_FALSE_IT(op_entity_factory_.free(data_req_entity))) {
    } else {
      ObObj res_value_obj;
      if (OB_FAIL(data_res_entity->get_property(ObRedisUtil::VALUE_PROPERTY_NAME, res_value_obj))) {
        LOG_WARN("fail to get data value", K(ret), K(res_value_obj));
      } else if (OB_FAIL(res_value_obj.get_varbinary(res_value))) {
        LOG_WARN("fail to get res_value from res_value_obj", K(ret), K(res_value_obj));
      } else {
        ListElement old_borded = std::make_pair(list_meta.left_idx_, res_value_obj);
        ListElement new_borded;
        if (OB_FAIL(update_list_after_pop(db, key, old_borded, new_borded, pop_left, list_meta))) {
          LOG_WARN("fail to update list after pop", K(ret), K(res_value_obj), K(list_meta));
        }
      }
    }
  }

  return ret;
}

void ListCommandOperator::update_ins_region_after_pop(bool pop_left, ObRedisListMeta &list_meta)
{
  bool need_reset = (pop_left && list_meta.left_idx_ > list_meta.ins_region_right_) ||
                    (!pop_left && list_meta.right_idx_ < list_meta.ins_region_left_);
  if (need_reset) {
    list_meta.reset_ins_region();
  } else {
    if (pop_left && list_meta.left_idx_ > list_meta.ins_region_left_) {
      list_meta.ins_region_left_ = list_meta.left_idx_;
    } else if (!pop_left && list_meta.right_idx_ < list_meta.ins_region_right_) {
      list_meta.ins_region_right_ = list_meta.right_idx_;
    }
    format_ins_region(list_meta);
  }
}

int ListCommandOperator::update_list_after_pop(
    int64_t db,
    const ObString &key,
    const ListElement &old_borded,
    const ListElement &new_borded,
    const bool pop_left,
    ObRedisListMeta &list_meta)
{
  int ret = OB_SUCCESS;

  ObTableBatchOperation batch_ops;
  ResultFixedArray results(op_temp_allocator_);
  ObITableEntity *del_data_entity = nullptr;
  // delete old borderd
  if (OB_FAIL(gen_entity_with_rowkey(db, key, old_borded.first, true/*is_data*/, del_data_entity, &op_entity_factory_))) {
    LOG_WARN("fail to gen entity with rowkey", K(ret));
  } else if (OB_FAIL(batch_ops.del(*del_data_entity))) {
    LOG_WARN("fail to push back", K(ret));
  } else {
    if (list_meta.count_ > 1) {
      // update meta
      list_meta.count_ -= 1;
      if (pop_left) {
        list_meta.left_idx_ = new_borded.first;
      } else {
        list_meta.right_idx_ = new_borded.first;
      }

      // update ins region
      if (list_meta.has_ins_region()) {
        update_ins_region_after_pop(pop_left, list_meta);
      }

      ObITableEntity *meta_entity = nullptr;
      if (OB_FAIL(gen_meta_entity(db, key, ObRedisModel::LIST, list_meta, meta_entity))) {
        LOG_WARN("fail to put meta into batch operation", K(ret), K(key), K(list_meta));
      } else if (OB_FAIL(batch_ops.insert_or_update(*meta_entity))) {
        LOG_WARN("fail to push back", K(ret));
      }
    } else {
      // delete meta
      ObITableEntity *new_meta_entity = nullptr;
      if (OB_FAIL(gen_entity_with_rowkey(db, key, ObRedisListMeta::META_INDEX, false/*is_data*/, new_meta_entity, &op_entity_factory_))) {
        LOG_WARN("fail to gen entity with rowkey", K(ret));
      } else if (OB_FAIL(batch_ops.del(*new_meta_entity))) {
        LOG_WARN("fail to push back data_req_entity", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(process_table_batch_op(batch_ops, results))) {
    LOG_WARN("fail to process table batch op", K(ret), K(batch_ops));
  }

  return ret;
}

int ListCommandOperator::get_varbinary_by_deep(const ObObj &src_obj, ObString &res_value)
{
  int ret = OB_SUCCESS;

  ObString src_str;
  if (OB_FAIL(src_obj.get_varbinary(src_str))) {
    LOG_WARN("fail to get varbinary", K(ret), K(src_obj));
  } else if (OB_FAIL(ob_write_string(redis_ctx_.allocator_, src_str, res_value))) {
    LOG_WARN("fail to write string", K(ret), K(src_str));
  }

  return ret;
}

int ListCommandOperator::pop_multi_count_list(
    int64_t db,
    const ObString &key,
    const bool pop_left,
    ObRedisListMeta &list_meta,
    ObString &res_value)
{
  int ret = OB_SUCCESS;

  // query leftest or rightest idx, after pop (idx range: [left_idx,right_idx] limit 2)
  ObTableQuery query;
  ObSEArray<ListElement, 2> borderd_elements;
  ListQueryCond query_cond(
      key,
      db,
      list_meta.left_idx_,
      list_meta.right_idx_,
      2,
      0,
      pop_left,
      true/*need_query_all*/);
  if (OB_FAIL(build_list_query(query_cond, query))) {
    LOG_WARN("fail to build list query", K(ret), K(query_cond));
  } else {
    SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
    {
      QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, &list_meta)
      ObTableQueryResult *one_result = nullptr;
      const ObITableEntity *result_entity = nullptr;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter->get_next_result(one_result))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next result", K(ret));
          }
        }
        one_result->rewind();
        while (OB_SUCC(ret)) {
          ObObj idx_obj;
          int64_t idx;
          ObObj value_obj;
          if (OB_FAIL(one_result->get_next_entity(result_entity))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", K(ret));
            }
          } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::REDIS_INDEX_NAME, idx_obj))) {
            LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
          } else if (OB_FAIL(idx_obj.get_int(idx))) {
            LOG_WARN("fail to get idx", K(ret), K(idx_obj));
          } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::VALUE_PROPERTY_NAME, value_obj))) {
            LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
          } else if (OB_FAIL(borderd_elements.push_back(std::make_pair(idx, value_obj)))) {
            LOG_WARN("fail to push back member", K(ret), K(value_obj));
          }
        }
      }
      if (OB_SUCC(ret) || OB_ITER_END == ret) {
        if (borderd_elements.count() != 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("idx array count not match", K(ret), K(borderd_elements));
        } else if (
            (pop_left && borderd_elements[0].first != list_meta.left_idx_) ||
            (!pop_left && borderd_elements[0].first != list_meta.right_idx_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("idx array not match", K(ret), K(borderd_elements), K(list_meta), K(pop_left));
        } else if (OB_FAIL(update_list_after_pop(db, key, borderd_elements[0], borderd_elements[1], pop_left, list_meta))) {
          LOG_WARN("fail to update list after pop", K(ret), K(borderd_elements), K(list_meta));
        } else if (OB_FAIL(get_varbinary_by_deep(borderd_elements[0].second, res_value))) {
          // note: deep copy is required, because after the iterator destruct, the memory of the query results will
          // be released
          LOG_WARN("fail to get res value by deep", K(ret), K(borderd_elements));
        }
      }

      QUERY_ITER_END(iter)
    }
  }

  return ret;
}

int ListCommandOperator::do_pop(int64_t db, const ObString &key, bool pop_left)
{
  int ret = OB_SUCCESS;

  ObString res_value;
  ObRedisListMeta *list_meta = nullptr;
  ObRedisMeta *meta = nullptr;
  if (OB_FAIL(get_meta(db, key, ObRedisModel::LIST, meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do list expire if needed", K(ret), K(key));
    } else {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisFmt::NULL_BULK_STRING);
    }
  } else if (FALSE_IT(list_meta = reinterpret_cast<ObRedisListMeta*>(meta))) {
  } else if (list_meta->count_ == 1) {
    if (OB_FAIL(pop_single_count_list(db, key, *list_meta, pop_left, res_value))) {
      LOG_WARN("fail to pop one count list", K(ret), K(*list_meta));
    }
  } else {
    if (OB_FAIL(pop_multi_count_list(db, key, pop_left, *list_meta, res_value))) {
      LOG_WARN("fail to pop multi count list", K(ret), K(*list_meta));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_bulk_string(res_value))) {
      LOG_WARN("fail to set result int", K(ret), K(*list_meta));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int ListCommandOperator::build_scan_range(const ListQueryCond &query_cond, bool is_data, ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  ObRowkey start_key;
  ObRowkey end_key;
  ObNewRange *range = nullptr;
  if (OB_FAIL(build_rowkey(query_cond.db_, query_cond.key_, is_data, &query_cond.left_border_, true, start_key))) {
    LOG_WARN("fail to build start key", K(ret), K(query_cond.db_), K(query_cond.key_));
  } else if (OB_FAIL(build_rowkey(query_cond.db_, query_cond.key_, is_data, &query_cond.right_border_, false, end_key))) {
    LOG_WARN("fail to build start key", K(ret), K(query_cond.db_), K(query_cond.key_));
  } else if (OB_FAIL(build_range(start_key, end_key, range, query_cond.include_start_, query_cond.include_end_))) {
    LOG_WARN("fail to build range", K(ret), K(start_key), K(end_key));
  } else if (OB_FAIL(query.add_scan_range(*range))) {
    LOG_WARN("fail to add scan range", K(ret));
  }

  return ret;
}

int ListCommandOperator::build_scan_range(
    int64_t db,
    const ObString &key,
    bool is_data,
    const int64_t *less_index,
    const int64_t *max_index,
    ObTableQuery &query)
{
  ListQueryCond query_cond(key, db, *less_index, *max_index);
  return build_scan_range(query_cond, is_data, query);
}

int ListCommandOperator::build_rowkey(
    int64_t db,
    const ObString &key,
    bool is_data,
    const int64_t *index,
    bool is_min,
    ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *obj_ptr = nullptr;
  if (OB_ISNULL(
          obj_ptr = static_cast<ObObj *>(op_temp_allocator_.alloc(sizeof(ObObj) * ObRedisUtil::LIST_ROWKEY_NUM)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory for ObObj failed", K(ret));
  } else if (OB_FAIL(ObRedisCtx::reset_objects(obj_ptr, ObRedisUtil::LIST_ROWKEY_NUM))) {
    LOG_WARN("fail to init object", K(ret));
  } else {
    obj_ptr[0].set_int(db);
    obj_ptr[1].set_varbinary(key);
    obj_ptr[2].set_bool(is_data);
    if (OB_ISNULL(index)) {
      if (is_min) {
        obj_ptr[3].set_min_value();
      } else {
        obj_ptr[3].set_max_value();
      }
    } else {
      obj_ptr[3].set_int(*index);
    }

    rowkey.assign(obj_ptr, ObRedisUtil::LIST_ROWKEY_NUM);
  }
  return ret;
}

int ListCommandOperator::get_query_forward_offset(
    const int64_t count,
    int64_t offset,
    bool &is_query_forward,
    int64_t &query_offset)
{
  int ret = OB_SUCCESS;
  if ((offset > 0 && offset > count - 1) || (offset < 0 && offset < -count)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("index is out of range", K(ret), K(offset), K(count));
  } else {
    if (offset < 0) {
      if (offset >= -(count / 2)) {
        is_query_forward = false;
        query_offset = -offset - 1;
      } else {
        query_offset = count + offset;
      }
    } else if (offset > 0 && offset > (count / 2)) {
      is_query_forward = false;
      query_offset = count - offset - 1;
    } else {
      query_offset = offset;
    }
  }

  return ret;
}

int ListCommandOperator::build_index_query(
    int64_t db,
    const ObString &key,
    const int64_t offset,
    bool need_query_all,
    const ObRedisListMeta &list_meta,
    ObTableQuery &query)
{
  int ret = OB_SUCCESS;

  bool is_query_forward = true;
  int64_t query_offset = offset;
  if (OB_FAIL(get_query_forward_offset(list_meta.count_, offset, is_query_forward, query_offset))) {
    LOG_WARN("fail to get query forward offset", K(ret), K(offset), K(list_meta));
  } else {
    ListQueryCond query_cond(
        key,
        db,
        list_meta.left_idx_,
        list_meta.right_idx_,
        1/*limit*/,
        query_offset,
        is_query_forward,
        need_query_all);
    if (OB_FAIL(build_list_query(query_cond, query))) {
      LOG_WARN("fail to build list query", K(ret), K(query_cond));
    }
  }

  return ret;
}

int ListCommandOperator::query_index_result(const ObTableQuery &query,
                                            ObString &res_value,
                                            const ObRedisListMeta *list_meta)
{
  int ret = OB_SUCCESS;
  // query element
  SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
  {
    QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, list_meta)
    ObTableQueryResult *one_result = nullptr;
    const ObITableEntity *result_entity = nullptr;
    ObObj value_obj;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next_result(one_result))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next result", K(ret));
        }
      }
      one_result->rewind();
      while (OB_SUCC(ret)) {
        if (OB_FAIL(one_result->get_next_entity(result_entity))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next result", K(ret));
          }
        } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::VALUE_PROPERTY_NAME, value_obj))) {
          LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
        }
      }
    }
    if (OB_SUCC(ret) || OB_ITER_END == ret) {
      if (value_obj.is_null()) {
        RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisFmt::NULL_BULK_STRING);
      } else if (OB_FAIL(get_varbinary_by_deep(value_obj, res_value))) {
        // note: deep copy is required, because after the iterator destruct, the memory of the query results will
        // be released
        LOG_WARN("fail to get res value by deep", K(ret), K(value_obj));
      }
    }
    QUERY_ITER_END(iter)
  }
  return ret;
}

int ListCommandOperator::do_index(int64_t db, const ObString &key, int64_t offset)
{
  int ret = OB_SUCCESS;

  ObString res_value;
  // 1. Get meta data and check whether it has expired.
  ObRedisListMeta *list_meta = nullptr;
  ObRedisMeta *meta = nullptr;
  ObTableQuery query;
  if (OB_FAIL(get_meta(db, key, ObRedisModel::LIST, meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do list expire if needed", K(ret), K(key));
    } else {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisFmt::NULL_BULK_STRING);
    }
  } else if (FALSE_IT(list_meta = reinterpret_cast<ObRedisListMeta*>(meta))) {
  } else if (OB_FAIL(build_index_query(db, key, offset, true, *list_meta, query))) {
    if (ret == OB_ARRAY_OUT_OF_RANGE) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisFmt::NULL_BULK_STRING);
    }
    LOG_WARN("fail to build index query", K(ret), K(key), K(offset), K(*list_meta));
  } else if (OB_FAIL(query_index_result(query, res_value, list_meta))) {
    LOG_WARN("fail to query index result", K(ret), K(query));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_bulk_string(res_value))) {
      LOG_WARN("fail to set result int", K(ret), K(*list_meta));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int ListCommandOperator::update_element(int64_t db, ObRedisMeta *meta, const ObString &key, int64_t index, const ObString &value)
{
  int ret = OB_SUCCESS;

  ObITableEntity *req_entity;
  ObObj value_obj;
  value_obj.set_varbinary(value);
  if (OB_FAIL(gen_entity_with_rowkey(db, key, index, true/*is_data*/, req_entity, &op_entity_factory_))) {
    LOG_WARN("fail to gen entity with rowkey", K(ret));
  } else if (OB_FAIL(req_entity->set_property(ObRedisUtil::VALUE_PROPERTY_NAME, value_obj))) {
    LOG_WARN("fail to set property", K(ret), K(ObRedisUtil::VALUE_PROPERTY_NAME), K(value_obj));
  } else {
    ObTableOperation op = ObTableOperation::update(*req_entity);
    ObTableOperationResult result;
    if (OB_FAIL(process_table_single_op(op, result, meta))) {
      LOG_WARN("fail to process table op", K(ret), K(op));
    }
  }

  return ret;
}

int ListCommandOperator::query_set_index(const ObTableQuery &query,
                                         int64_t &res_idx,
                                         ObRedisListMeta *meta/* = nullptr*/)
{
  int ret = OB_SUCCESS;
  // query element
  SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
  {
    QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, meta)
    ObTableQueryResult *one_result = nullptr;
    const ObITableEntity *result_entity = nullptr;
    ObObj index_obj;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next_result(one_result))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next result", K(ret));
        }
      }
      one_result->rewind();
      while (OB_SUCC(ret)) {
        if (OB_FAIL(one_result->get_next_entity(result_entity))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next result", K(ret));
          }
        } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::REDIS_INDEX_NAME, index_obj))) {
          LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
        }
      }
    }
    if (OB_SUCC(ret) || OB_ITER_END == ret) {
      if (index_obj.is_null()) {
        RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::NO_SUCH_KEY_ERR);
      } else if (OB_FAIL(index_obj.get_int(res_idx))) {
        LOG_WARN("fail to get int", K(ret), K(index_obj));
      }
    }
    QUERY_ITER_END(iter)
  }
  return ret;
}

int ListCommandOperator::do_set(int64_t db, const ObString &key, int64_t offset, const ObString &value)
{
  int ret = OB_SUCCESS;

  // 1. Get meta data and check whether it has expired.
  ObRedisListMeta *list_meta = nullptr;
  ObRedisMeta *meta = nullptr;
  ObTableQuery query;
  int64_t set_index = 0;
  if (OB_FAIL(get_meta(db, key, ObRedisModel::LIST, meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do list expire if needed", K(ret), K(key));
    } else {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::NO_SUCH_KEY_ERR);
    }
  } else if (FALSE_IT(list_meta = reinterpret_cast<ObRedisListMeta*>(meta))) {
  } else if (OB_FAIL(build_index_query(db, key, offset, false/*need_query_all*/, *list_meta, query))) {
    if (ret == OB_ARRAY_OUT_OF_RANGE) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_OUT_RANGE_ERR);
    }
    LOG_WARN("fail to build index query", K(ret), K(key), K(offset), K(*list_meta));
  } else if (OB_FAIL(query_set_index(query, set_index, list_meta))) {
    LOG_WARN("fail to query set index", K(ret), K(query));
  } else if (OB_FAIL(update_element(db, meta, key, set_index, value))) {
    LOG_WARN("fail to update element", K(ret), K(set_index), K(value));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_fmt_res(ObRedisFmt::OK))) {
      LOG_WARN("fail to set fmt res", K(ret));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int ListCommandOperator::adjust_range_param(const int64_t count, int64_t &start, int64_t &end)
{
  int ret = OB_SUCCESS;

  if (start < 0) {
    start += count;
  }
  if (end < 0) {
    end += count;
  }
  if (start < 0) {
    start = 0;
  }
  if (end >= count) {
    end = count - 1;
  }
  if (start > end || start >= count) {
    RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisFmt::EMPTY_ARRAY);
    LOG_WARN("index is out of range", K(ret), K(start), K(end));
  }

  return ret;
}

int ListCommandOperator::build_range_query(
    int64_t db,
    const ObString &key,
    const ObRedisListMeta &list_meta,
    int64_t start,
    int64_t end,
    ObTableQuery &query)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(adjust_range_param(list_meta.count_, start, end))) {
    LOG_WARN("fail to get query forward offset", K(ret), K(start), K(end));
  } else {
    int64_t query_limit = end - start + 1;
    int64_t query_offset = start;

    ListQueryCond query_cond(
        key,
        db,
        list_meta.left_idx_,
        list_meta.right_idx_,
        query_limit,
        query_offset,
        true/*is_forward_order*/,
        true/*need_query_all*/);
    if (OB_FAIL(build_list_query(query_cond, query))) {
      LOG_WARN("fail to build list query", K(ret), K(query_cond));
    }
  }

  return ret;
}

int ListCommandOperator::query_range_result(const ObTableQuery &query,
                                            ObIArray<ObString> &res_values,
                                            ObRedisListMeta *meta/*= nullptr*/)
{
  int ret = OB_SUCCESS;
  // query element
  SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
  {
    QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, meta)
    ObTableQueryResult *one_result = nullptr;
    const ObITableEntity *result_entity = nullptr;
    ObObj value_obj;
    ObString value_str;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next_result(one_result))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next result", K(ret));
        }
      }
      one_result->rewind();
      while (OB_SUCC(ret)) {
        if (OB_FAIL(one_result->get_next_entity(result_entity))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next result", K(ret));
          }
        } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::VALUE_PROPERTY_NAME, value_obj))) {
          LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
        } else if (OB_FAIL(get_varbinary_by_deep(value_obj, value_str))) {
          // note: deep copy is required, because after the iterator destruct, the memory of the query results will
          // be released
          LOG_WARN("fail to get res value by deep", K(ret), K(value_obj));
        } else if (OB_FAIL(res_values.push_back(value_str))) {
          LOG_WARN("fail to push back", K(ret), K(value_str));
        }
      }
    }
    if (OB_SUCC(ret) || OB_ITER_END == ret) {
      if (res_values.count() != query.get_limit()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query result count is unexpected error", K(ret), K(res_values.count()), K(query.get_limit()));
      }
    }
    QUERY_ITER_END(iter)
  }
  return ret;
}

int ListCommandOperator::do_range(int64_t db, const ObString &key, int64_t start, int64_t end)
{
  int ret = OB_SUCCESS;

  ObArray<ObString> res_values(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(op_temp_allocator_, "RedisList"));
  // 1. Get meta data and check whether it has expired.
  ObRedisListMeta *list_meta = nullptr;
  ObRedisMeta *meta = nullptr;
  ObTableQuery query;
  if (OB_FAIL(get_meta(db, key, ObRedisModel::LIST, meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do list expire if needed", K(ret), K(key));
    } else {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisFmt::EMPTY_ARRAY);
    }
  } else if (FALSE_IT(list_meta = reinterpret_cast<ObRedisListMeta*>(meta))) {
  } else if (OB_FAIL(build_range_query(db, key, *list_meta, start, end, query))) {
    LOG_WARN("fail to build range query", K(ret), K(key), K(start), K(end), K(*list_meta));
  } else if (OB_FAIL(res_values.reserve(query.get_limit()))) {
    LOG_WARN("fail to reserve", K(ret), K(query.get_limit()));
  } else if (OB_FAIL(query_range_result(query, res_values, list_meta))) {
    LOG_WARN("fail to query range result", K(ret), K(query));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_array(res_values))) {
      LOG_WARN("fail to set result array", K(ret), K(*list_meta));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int ListCommandOperator::build_list_query(const ListQueryCond &query_cond, ObTableQuery &query)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(build_scan_range(query_cond, true, query))) {
    LOG_WARN("fail to build member scan range", K(ret), K(query_cond));
  } else if (!query_cond.need_query_all_) {
    if (OB_FAIL(query.add_select_column(ObRedisUtil::REDIS_INDEX_NAME))) {
      LOG_WARN("fail to add select column", K(ret), K(query_cond));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(query.set_scan_order(
            query_cond.is_query_forward_ ? ObQueryFlag::ScanOrder::Forward : ObQueryFlag::ScanOrder::Reverse))) {
      LOG_WARN("fail to set scan order", K(ret), K(query_cond));
    } else if (OB_FAIL(query.set_limit(query_cond.limit_))) {
      LOG_WARN("fail to set limit", K(ret), K(query_cond));
    } else if (OB_FAIL(query.set_offset(query_cond.offset_))) {
      LOG_WARN("fail to set offset", K(ret), K(query_cond));
    }
  }

  return ret;
}

int ListCommandOperator::adjust_trim_param(const int64_t count, int64_t &start, int64_t &end, bool& is_del_all)
{
  int ret = OB_SUCCESS;
  // convert start and end to positive
  start = start < 0 ? start + count : start;
  end = end < 0 ? end + count : end;

  if (start > end || (start < 0 && end < 0) || (start > count - 1 && end > count - 1)) {
    is_del_all = true;
  } else {
    // ensure the op index valid
    if (start < 0) {
      start = 0;
    } else if (start > count - 1) {
      start = count - 1;
    }
    if (end < 0) {
      end = 0;
    } else if (end > count - 1) {
      end = count - 1;
    }
  }

  return ret;
}

int ListCommandOperator::build_trim_querys(
    int64_t db,
    const ObString &key,
    const ObRedisListMeta &list_meta,
    int64_t &start,
    int64_t &end,
    bool& is_del_all,
    ObIArray<ObTableQuery> &querys)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(adjust_trim_param(list_meta.count_, start, end, is_del_all))) {
    LOG_WARN("fail to adjust trim param", K(ret), K(start), K(end), K(list_meta));
  } else if (!is_del_all) {
    int64_t left_del_count = start;
    int64_t right_del_count = list_meta.count_ - end - 1;
    if (left_del_count > 0) {
      ObTableQuery left_region_query;
      ListQueryCond query_cond(
          key,
          db,
          list_meta.left_idx_,
          list_meta.right_idx_,
          left_del_count + 1,
          0,
          true/*is_forward_order*/,
          false/*need_query_all*/);
      if (OB_FAIL(build_list_query(query_cond, left_region_query))) {
        LOG_WARN("fail to build list query cond", K(ret), K(query_cond));
      } else if (OB_FAIL(querys.push_back(left_region_query))) {
        LOG_WARN("fail to push back", K(ret), K(left_region_query));
      }
    }
    if (OB_SUCC(ret) && right_del_count > 0) {
      ObTableQuery right_region_query;
      ListQueryCond query_cond(
          key,
          db,
          list_meta.left_idx_,
          list_meta.right_idx_,
          right_del_count + 1,
          0,
          false/*is_forward_order*/,
          false/*need_query_all*/);
      if (OB_FAIL(build_list_query(query_cond, right_region_query))) {
        LOG_WARN("fail to build list query cond", K(ret), K(query_cond));
      } else if (OB_FAIL(querys.push_back(right_region_query))) {
        LOG_WARN("fail to push back", K(ret), K(right_region_query));
      }
    }
  }

  return ret;
}

void ListCommandOperator::format_ins_region(ObRedisListMeta &list_meta)
{
  list_meta.ins_region_left_ = ((list_meta.ins_region_left_ % ObRedisListMeta::INDEX_STEP) == 0)
                                   ? list_meta.ins_region_left_
                                   : get_region_left_bord_idx(list_meta.ins_region_left_);
  list_meta.ins_region_right_ = ((list_meta.ins_region_right_ % ObRedisListMeta::INDEX_STEP) == 0)
                                    ? list_meta.ins_region_right_
                                    : get_region_right_bord_idx(list_meta.ins_region_right_);
}

void ListCommandOperator::update_ins_region_after_trim(ObRedisListMeta &list_meta)
{
  if (list_meta.ins_region_right_ < list_meta.left_idx_ || list_meta.ins_region_left_ > list_meta.right_idx_) {
    list_meta.reset_ins_region();
  } else if (list_meta.ins_region_left_ < list_meta.left_idx_ && list_meta.ins_region_right_ > list_meta.right_idx_) {
    list_meta.ins_region_left_ = list_meta.left_idx_;
    list_meta.ins_region_right_ = list_meta.right_idx_;
  } else if (list_meta.ins_region_left_ > list_meta.left_idx_ && list_meta.ins_region_right_ < list_meta.right_idx_) {
  } else {
    if (list_meta.ins_region_left_ < list_meta.left_idx_) {
      list_meta.ins_region_left_ = list_meta.left_idx_;
    }
    if (list_meta.ins_region_right_ > list_meta.right_idx_) {
      list_meta.ins_region_right_ = list_meta.right_idx_;
    }
  }
  format_ins_region(list_meta);
}

int ListCommandOperator::update_meta_after_trim(
    int64_t db,
    const ObString &key,
    ObRedisListMeta &list_meta,
    const ObIArray<int64_t> &new_border_idxs,
    const int64_t start,
    const int64_t end)
{
  int ret = OB_SUCCESS;

  int64_t left_del_count = start;
  int64_t right_del_count = list_meta.count_ - end - 1;

  list_meta.count_ -= (left_del_count + right_del_count);
  if (new_border_idxs.count() == 2) {
    list_meta.left_idx_ = new_border_idxs.at(0);
    list_meta.right_idx_ = new_border_idxs.at(1);
  } else if (left_del_count > 0) {
    list_meta.left_idx_ = new_border_idxs.at(0);
  } else if (right_del_count > 0) {
    list_meta.right_idx_ = new_border_idxs.at(0);
  }

  // update ins region
  if (list_meta.has_ins_region()) {
    update_ins_region_after_trim(list_meta);
  }

  ObITableEntity *new_meta_entity = nullptr;
  if (OB_FAIL(gen_meta_entity(db, key, ObRedisModel::LIST, list_meta, new_meta_entity))) {
    LOG_WARN("fail to put meta into batch operation", K(ret), K(key), K(list_meta));
  } else {
    ObTableOperation op;
    if (list_meta.count_ > 0) {
      op = ObTableOperation::insert_or_update(*new_meta_entity);
    } else {
      op = ObTableOperation::del(*new_meta_entity);
    }
    ObTableOperationResult result;
    if (OB_FAIL(process_table_single_op(op, result))) {
      LOG_WARN("fail to process table op", K(ret), K(op));
    }
  }

  return ret;
}

int ListCommandOperator::build_trim_ops(
    const ObIArray<ObTableQuery> &querys,
    ObTableBatchOperation &del_ops,
    ObIArray<int64_t> &new_border_idxs,
    ObRedisListMeta *meta/*= nullptr*/)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < querys.count(); ++i) {
    const ObTableQuery &query = querys.at(i);
    int64_t result_count = 0;
    // query element
    SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
    {
      QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, meta)
      ObTableQueryResult *one_result = nullptr;
      const ObITableEntity *result_entity = nullptr;
      ObObj index_obj;
      int64_t index = 0;
      ObObj db_obj;
      ObObj key_obj;
      int64_t db = 0;
      ObString key;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter->get_next_result(one_result))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next result", K(ret));
          }
        }
        one_result->rewind();
        while (OB_SUCC(ret)) {
          if (OB_FAIL(one_result->get_next_entity(result_entity))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", K(ret));
            }
          } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::REDIS_INDEX_NAME, index_obj))) {
            LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
          } else if (OB_FAIL(index_obj.get_int(index))) {
            LOG_WARN("fail to get index from obj", K(ret), K(index_obj));
          } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::DB_PROPERTY_NAME, db_obj))) {
            LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
          } else if (OB_FAIL(db_obj.get_int(db))) {
            LOG_WARN("fail to get index from obj", K(ret), K(db_obj));
          } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::RKEY_PROPERTY_NAME, key_obj))) {
            LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
          } else if (OB_FAIL(get_varbinary_by_deep(key_obj, key))) {
            LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
          } else if (++result_count == query.get_limit()) {
            // the last element of the query is the new boundary
            if (OB_FAIL(new_border_idxs.push_back(index))) {
              LOG_WARN("fail to push back", K(ret), K(index));
            }
          } else {
            ObITableEntity *del_data_entity = nullptr;
            if (OB_FAIL(gen_entity_with_rowkey(db, key, index, true/*is_data*/, del_data_entity, &op_entity_factory_))) {
              LOG_WARN("fail to gen entity with rowkey", K(ret));
            } else if (OB_FAIL(del_ops.del(*del_data_entity))) {
              LOG_WARN("fail to push back", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret) || OB_ITER_END == ret) {
        if (result_count != query.get_limit()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("query result count is unexpected error", K(ret), K(result_count), K(query.get_limit()));
        }
      }
      QUERY_ITER_END(iter)
    }
  }
  return ret;
}

int ListCommandOperator::trim_list(
    int64_t db,
    const ObString &key,
    ObRedisListMeta &list_meta,
    int64_t start,
    int64_t end)
{
  int ret = OB_SUCCESS;

  bool is_del_all = false;
  ObSEArray<ObTableQuery, 2> querys;
  if (OB_FAIL(build_trim_querys(db, key, list_meta, start, end, is_del_all, querys))) {
    LOG_WARN("fail to build range query", K(ret), K(key), K(start), K(end), K(list_meta));
  } else if (!is_del_all) {
    ObTableBatchOperation del_ops;
    ObSEArray<int64_t, 2> new_border_idxs;
    if (OB_FAIL(build_trim_ops(querys, del_ops, new_border_idxs, &list_meta))) {
      LOG_WARN("fail to build trim ops", K(ret), K(querys));
    } else if (new_border_idxs.count() != querys.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new border idxs count is unexpected error", K(ret), K(new_border_idxs.count()), K(querys.count()));
    } else if (OB_FAIL(update_meta_after_trim(db, key, list_meta, new_border_idxs, start, end))) {
      LOG_WARN("fail to update meta after trim", K(ret), K(list_meta), K(new_border_idxs), K(start), K(end));
    } else {
      // del elements
      ResultFixedArray results(op_temp_allocator_);
      if (OB_FAIL(process_table_batch_op(del_ops, results))) {
        LOG_WARN("fail to process table batch op", K(ret), K(del_ops));
      }
    }
  } else if (OB_FAIL(del_key(key, db, &list_meta))){
    LOG_WARN("fail to del key", K(ret), K(key));
  }

  return ret;
}

int ListCommandOperator::do_trim(int64_t db, const ObString &key, int64_t start, int64_t end)
{
  int ret = OB_SUCCESS;

  // 1. Get meta data and check whether it has expired.
  ObRedisListMeta *list_meta = nullptr;
  ObRedisMeta *meta = nullptr;

  if (OB_FAIL(get_meta(db, key, ObRedisModel::LIST, meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do list expire if needed", K(ret), K(key));
    } else {
      // Key does not exist return ok
      ret = OB_SUCCESS;
    }
  } else if (FALSE_IT(list_meta = reinterpret_cast<ObRedisListMeta*>(meta))) {
  } else if (OB_FAIL(trim_list(db, key, *list_meta, start, end))) {
    LOG_WARN("fail to trim list", K(ret), K(key), K(start), K(end), K(*list_meta));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_fmt_res(ObRedisFmt::OK))) {
      LOG_WARN("fail to set fmt res", K(ret));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int ListCommandOperator::get_pivot_index(
    const int64_t db,
    const ObString &key,
    const ObString &pivot,
    const ObRedisListMeta &list_meta,
    int64_t &pivot_idx)
{
  int ret = OB_SUCCESS;
  ObTableQuery query;

  ListQueryCond query_cond(
      key, db, list_meta.left_idx_, list_meta.right_idx_, 1, 0, true/*is_forward_order*/, false/*need_query_all*/);
  if (OB_FAIL(build_list_query(query_cond, query))) {
    LOG_WARN("fail to build list query cond", K(ret), K(query_cond));
  } else {
    // add filter
    FilterStrBuffer buffer(op_temp_allocator_);
    ObString filter_str;
    if (OB_FAIL(buffer.add_value_compare(hfilter::CompareOperator::EQUAL, ObRedisUtil::VALUE_PROPERTY_NAME, pivot))) {
      LOG_WARN("fail to gen compare filter string", K(ret), K(pivot));
    } else if (OB_FAIL(buffer.get_filter_str(filter_str))) {
      LOG_WARN("fail to get filter str");
    } else {
      query.set_filter(filter_str);
    }
  }

  if (OB_SUCC(ret)) {
    // query element
    SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
    {
      QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, &list_meta)
      ObTableQueryResult *one_result = nullptr;
      const ObITableEntity *result_entity = nullptr;
      ObObj index_obj;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter->get_next_result(one_result))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next result", K(ret));
          }
        }
        one_result->rewind();
        while (OB_SUCC(ret)) {
          if (OB_FAIL(one_result->get_next_entity(result_entity))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", K(ret));
            }
          } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::REDIS_INDEX_NAME, index_obj))) {
            LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
          } else if (OB_FAIL(index_obj.get_int(pivot_idx))) {
            LOG_WARN("fail to get index from obj", K(ret), K(index_obj));
          }
        }
      }
      if (OB_SUCC(ret) || OB_ITER_END == ret) {
        if (index_obj.is_null()) {
          RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisFmt::MINUS_ONE);
          LOG_WARN("result is not exist", K(ret), K(index_obj));
        }
      }
      QUERY_ITER_END(iter)
    }
  }

  return ret;
}

int ListCommandOperator::gen_entity_from_other(
    common::ObIAllocator &alloc,
    const ObITableEntity &src_entity,
    ObITableEntity &dst_entity)
{
  int ret = OB_SUCCESS;

  ObObj insert_tm_obj;
  ObObj dst_insert_tm_obj;
  ObObj value_obj;
  ObObj dst_value_obj;

  if (OB_FAIL(src_entity.get_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, insert_tm_obj))) {
    LOG_WARN("fail to get insert_tm_obj", K(ret), K(src_entity));
  } else if (OB_FAIL(src_entity.get_property(ObRedisUtil::VALUE_PROPERTY_NAME, value_obj))) {
    LOG_WARN("fail to get value_obj", K(ret), K(src_entity));
  } else if (OB_FAIL(ob_write_obj(alloc, insert_tm_obj, dst_insert_tm_obj))) {
    LOG_WARN("fail to write obj", K(ret), K(insert_tm_obj));
  } else if (OB_FAIL(ob_write_obj(alloc, value_obj, dst_value_obj))) {
    LOG_WARN("fail to write obj", K(ret), K(value_obj));
  } else if (OB_FAIL(dst_entity.set_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, dst_insert_tm_obj))) {
    LOG_WARN("fail to set insert_tm_obj", K(ret), K(src_entity));
  } else if (OB_FAIL(dst_entity.set_property(ObRedisUtil::VALUE_PROPERTY_NAME, dst_value_obj))) {
    LOG_WARN("fail to set value_obj", K(ret), K(src_entity));
  }

  return ret;
}

int ListCommandOperator::gen_entity_from_other(const ObITableEntity &src_entity, ObITableEntity &dst_entity)
{
  int ret = OB_SUCCESS;

  ObObj insert_tm_obj;
  ObObj value_obj;

  if (OB_FAIL(src_entity.get_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, insert_tm_obj))) {
    LOG_WARN("fail to get insert_tm_obj", K(ret), K(src_entity));
  } else if (OB_FAIL(src_entity.get_property(ObRedisUtil::VALUE_PROPERTY_NAME, value_obj))) {
    LOG_WARN("fail to get value_obj", K(ret), K(src_entity));
  } else if (OB_FAIL(dst_entity.set_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, insert_tm_obj))) {
    LOG_WARN("fail to set insert_tm_obj", K(ret), K(src_entity));
  } else if (OB_FAIL(dst_entity.set_property(ObRedisUtil::VALUE_PROPERTY_NAME, value_obj))) {
    LOG_WARN("fail to set value_obj", K(ret), K(src_entity));
  }

  return ret;
}

int ListCommandOperator::query_count(
    const int64_t db,
    const ObString &key,
    const ObRedisListMeta &list_meta,
    const int64_t left_inx,
    const int64_t right_inx,
    int64_t &row_cnt)
{
  int ret = OB_SUCCESS;

  ObTableQuery query;
  ListQueryCond query_cond(
      key,
      db,
      left_inx,
      right_inx,
      -1/*limit_count*/,
      0/*offset*/,
      true/*is_forward_order*/,
      false/*need_query_all*/);
  if (OB_FAIL(build_list_query(query_cond, query))) {
    LOG_WARN("fail to build list query cond", K(ret), K(query_cond));
  } else if (OB_FAIL(process_table_query_count(op_temp_allocator_, query, &list_meta, row_cnt))) {
    LOG_WARN("fail to process table query count", K(ret), K(db), K(key), K(query));
  }

  return ret;
}

void ListCommandOperator::update_border_after_redict(
    const int64_t pivot_bord_idx,
    const int64_t row_cnt,
    const int64_t index_step,
    const bool is_redict_left,
    ObRedisListMeta &list_meta)
{
  if (is_redict_left) {
    list_meta.left_idx_ = min(list_meta.left_idx_, pivot_bord_idx - row_cnt * index_step);
  } else {
    list_meta.right_idx_ = max(list_meta.right_idx_, pivot_bord_idx + row_cnt * index_step);
  }
}

int ListCommandOperator::do_redict_idx(
    const int64_t db,
    const ObString &key,
    const int64_t pivot_idx,
    const int64_t left_bord_idx,
    const int64_t right_bord_idx,
    const int64_t index_step,
    const bool is_redict_left,
    const bool is_before_pivot,
    ObRedisListMeta &list_meta,
    int64_t &insert_idx)
{
  int ret = OB_SUCCESS;
  int64_t pivot_bord_idx = is_redict_left ? right_bord_idx : left_bord_idx;
  int64_t idx_step_sign = is_redict_left ? -1 : 1;
  int64_t row_cnt = 0;
  if (OB_FAIL(query_count(db, key, list_meta, left_bord_idx, right_bord_idx, row_cnt))) {
    LOG_WARN("fail to build list query cond", K(ret));
  } else {
    ObTableQuery query;
    ListQueryCond query_cond(
        key,
        db,
        left_bord_idx,
        right_bord_idx,
        -1,
        0,
        is_redict_left ? true : false/*is_forward_order*/,
        true/*need_query_all*/);
    if (OB_FAIL(build_list_query(query_cond, query))) {
      LOG_WARN("fail to build list query cond", K(ret), K(query_cond));
    } else {
      int64_t has_move_cnt = 0;
      // query element
      SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
      {
        common::ObArenaAllocator redict_tmp_alloc(ObMemAttr(MTL_ID(), "RedisListRed"));
        table::ObTableEntityFactory<table::ObTableEntity> redict_entity_factory("RedisListRedFac", MTL_ID());
        ObTableBatchOperation insert_ops;
        QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, &list_meta)
        ObTableQueryResult *one_result = nullptr;
        const ObITableEntity *result_entity = nullptr;
        ObITableEntity *new_entity = nullptr;
        ObObj idx_obj;
        int64_t idx = 0;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(iter->get_next_result(one_result))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", K(ret));
            }
          }
          one_result->rewind();
          while (OB_SUCC(ret)) {
            if (OB_FAIL(one_result->get_next_entity(result_entity))) {
              if (OB_ITER_END != ret) {
                LOG_WARN("fail to get next result", K(ret));
              }
            } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::REDIS_INDEX_NAME, idx_obj))) {
              LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
            } else if (OB_FAIL(idx_obj.get_int(idx))) {
              RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INDEX_OVERFLOW_ERR);
              LOG_WARN("fail to get idx", K(ret), K(idx_obj));
            } else if (has_move_cnt < row_cnt) {
              int64_t new_idx = pivot_bord_idx + idx_step_sign * (row_cnt - has_move_cnt) * index_step;
              if (OB_UNLIKELY(idx_overflowed(new_idx))) {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("index overflow", K(ret), K(new_idx), K(list_meta));
              } else if (
                  pivot_idx == idx && ((!is_redict_left && !is_before_pivot) || (is_redict_left && is_before_pivot))) {
                // insert new idx
                insert_idx = new_idx;
                has_move_cnt++;
                new_idx = pivot_bord_idx + idx_step_sign * (row_cnt - has_move_cnt) * ObRedisListMeta::INDEX_STEP;
                if (OB_UNLIKELY(idx_overflowed(new_idx))) {
                  RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INDEX_OVERFLOW_ERR);
                  LOG_WARN("index overflow", K(ret), K(new_idx), K(list_meta));
                }
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(gen_entity_with_rowkey(db, key, idx, true/*is_data*/, new_entity, &redict_entity_factory))) {
                  LOG_WARN("fail to gen entity with rowkey", K(ret));
                } else if (OB_FAIL(gen_entity_from_other(*result_entity, *new_entity))) {
                  LOG_WARN("fail to set property", K(ret));
                } else {
                  // del now entity
                  ObTableOperation op = ObTableOperation::del(*new_entity);
                  ObTableOperationResult result;
                  if (OB_FAIL(process_table_single_op(
                          op,
                          result,
                          &list_meta,
                          RedisOpFlags::DEL_SKIP_SCAN,
                          &redict_tmp_alloc,
                          &redict_entity_factory))) {
                    LOG_WARN("fail to del list meta", K(ret), KP(new_entity));
                  } else if (OB_FAIL(gen_entity_with_rowkey(db, key, new_idx, true/*is_data*/, new_entity, &redict_entity_factory))) {
                    LOG_WARN("fail to gen entity with rowkey", K(ret));
                  } else if (OB_FAIL(gen_entity_from_other(redict_tmp_alloc, *result_entity, *new_entity))) {
                    LOG_WARN("fail to set property", K(ret));
                  } else if (OB_FAIL(insert_ops.insert(*new_entity))) {
                    LOG_WARN("fail to push back", K(ret));
                  } else {
                    has_move_cnt++;
                    if (pivot_idx == idx &&
                        ((!is_redict_left && is_before_pivot) || (is_redict_left && !is_before_pivot))) {
                      // insert new idx
                      insert_idx =
                          pivot_bord_idx + idx_step_sign * (row_cnt - has_move_cnt) * ObRedisListMeta::INDEX_STEP;
                      has_move_cnt++;
                      if (OB_UNLIKELY(idx_overflowed(insert_idx))) {
                        RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INDEX_OVERFLOW_ERR);
                        LOG_WARN("index overflow", K(ret), K(insert_idx), K(list_meta));
                      }
                    }
                  }
                }
              }
            }
            // process
            if (OB_SUCC(ret) && redict_tmp_alloc.used() + redict_entity_factory.get_used_mem() >
                                    ListCommandOperator::REDICT_MEM_LIMIT) {
              common::ObArenaAllocator list_tmp_alloc(ObMemAttr(MTL_ID(), "RedisListTmp"));
              table::ObTableEntityFactory<table::ObTableEntity> list_tmp_entity_factory("RedisListTmpFac", MTL_ID());
              ResultFixedArray results(list_tmp_alloc);
              if (OB_FAIL(process_table_batch_op(
                      insert_ops,
                      results,
                      &list_meta,
                      RedisOpFlags::NONE,
                      &list_tmp_alloc,
                      &list_tmp_entity_factory))) {
                LOG_WARN("fail to process table batch op", K(ret), K(insert_ops));
              } else {
                redict_entity_factory.free_and_reuse();
                redict_tmp_alloc.reuse();
                insert_ops.reset();
              }
            }
          }
        }

        // del old and insert new
        if (OB_LIKELY(ret == OB_ITER_END)) {
          common::ObArenaAllocator list_tmp_alloc(ObMemAttr(MTL_ID(), "RedisListTmp"));
          table::ObTableEntityFactory<table::ObTableEntity> list_tmp_entity_factory("RedisListTmpFac", MTL_ID());
          ResultFixedArray results(list_tmp_alloc);
          if (OB_FAIL(process_table_batch_op(
                  insert_ops, results, &list_meta, RedisOpFlags::NONE, &list_tmp_alloc, &list_tmp_entity_factory))) {
            LOG_WARN("fail to process table batch op", K(ret), K(insert_ops));
          }
        }
        QUERY_ITER_END(iter)
      }
    }

    if (OB_SUCC(ret)) {
      update_border_after_redict(pivot_bord_idx, row_cnt, index_step, is_redict_left, list_meta);
    }
  }

  return ret;
}

int64_t ListCommandOperator::get_region_left_bord_idx(int64_t pivot_idx)
{
  int64_t left_bord_idx = floor(pivot_idx / static_cast<double>(ObRedisListMeta::INDEX_STEP)) * ObRedisListMeta::INDEX_STEP;
  return left_bord_idx;
}

int64_t ListCommandOperator::get_region_right_bord_idx(int64_t pivot_idx)
{
  return (pivot_idx / ObRedisListMeta::INDEX_STEP) * ObRedisListMeta::INDEX_STEP +
         (pivot_idx > 0 ? ObRedisListMeta::INDEX_STEP : 0);
}

void ListCommandOperator::update_ins_region_after_rdct(
    bool is_redict_left,
    int64_t pivot_bord_idx,
    ObRedisListMeta &list_meta)
{
  bool has_region_after_rdct = false;

  if (is_redict_left) {
    has_region_after_rdct = pivot_bord_idx < list_meta.ins_region_right_;
    list_meta.ins_region_left_ = has_region_after_rdct ? pivot_bord_idx : list_meta.ins_region_left_;
  } else {
    has_region_after_rdct = pivot_bord_idx > list_meta.ins_region_left_;
    list_meta.ins_region_right_ = has_region_after_rdct ? pivot_bord_idx : list_meta.ins_region_right_;
  }

  if (!has_region_after_rdct) {
    list_meta.reset_ins_region();
  } else {
    format_ins_region(list_meta);
  }
}

int ListCommandOperator::redict_idx(
    const int64_t db,
    const ObString &key,
    const int64_t pivot_idx,
    const bool is_before_pivot,
    ObRedisListMeta &list_meta,
    int64_t &insert_idx)
{
  int ret = OB_SUCCESS;
  bool is_redict_left = false;
  if (pivot_idx < (list_meta.left_idx_ + (list_meta.right_idx_ - list_meta.left_idx_) / 2)) {
    is_redict_left = true;
  }

  int64_t left_bord_idx = 0;
  int64_t right_bord_idx = 0;
  if (is_redict_left) {
    left_bord_idx = list_meta.left_idx_;
    right_bord_idx = get_region_right_bord_idx(pivot_idx);
  } else {
    left_bord_idx = get_region_left_bord_idx(pivot_idx);
    right_bord_idx = list_meta.right_idx_;
  }
  int64_t rdt_beg_us = ObTimeUtility::fast_current_time();
  if (OB_FAIL(do_redict_idx(
          db,
          key,
          pivot_idx,
          left_bord_idx,
          right_bord_idx,
          ObRedisListMeta::INDEX_STEP,
          is_redict_left,
          is_before_pivot,
          list_meta,
          insert_idx))) {
    LOG_WARN("fail to do redict idx", K(ret), K(db), K(key), K(pivot_idx), K(is_before_pivot), K(list_meta));
  } else {
    // update ins region
    int64_t pivot_bord_idx = is_redict_left ? right_bord_idx : left_bord_idx;
    update_ins_region_after_rdct(is_redict_left, pivot_bord_idx, list_meta);
  }
  int64_t rdt_end_us = ObTimeUtility::fast_current_time();
  LOG_INFO(
      "redict cost", K(ret), K(rdt_end_us - rdt_beg_us), K(db), K(key), K(pivot_idx), K(is_redict_left), K(list_meta));
  return ret;
}

void ListCommandOperator::updata_ins_region_after_insert(
    bool is_before_pivot,
    int64_t adjacent_idx,
    int64_t pivot_idx,
    ObRedisListMeta &list_meta)
{
  list_meta.ins_region_left_ = is_before_pivot ? std::min(list_meta.ins_region_left_, adjacent_idx)
                                               : std::min(list_meta.ins_region_left_, pivot_idx);
  list_meta.ins_region_right_ = is_before_pivot ? std::max(list_meta.ins_region_right_, pivot_idx)
                                                : std::max(list_meta.ins_region_right_, adjacent_idx);
  format_ins_region(list_meta);
}

int ListCommandOperator::get_insert_index(
    const int64_t db,
    const ObString &key,
    const int64_t pivot_idx,
    const bool is_before_pivot,
    ObRedisListMeta &list_meta,
    int64_t &insert_idx)
{

  int ret = OB_SUCCESS;
  int64_t adjacent_idx = 0;
  if (is_before_pivot &&
      (pivot_idx == list_meta.left_idx_ || list_meta.count_ == 1)) {
    insert_idx = list_meta.left_idx_ - ObRedisListMeta::INDEX_STEP;
  } else if (
      !is_before_pivot &&
      (pivot_idx == list_meta.right_idx_ || list_meta.count_ == 1)) {
    insert_idx = list_meta.right_idx_ + ObRedisListMeta::INDEX_STEP;
  } else {
    if (!list_meta.has_ins_region()) {
      adjacent_idx =
          is_before_pivot ? pivot_idx - ObRedisListMeta::INDEX_STEP : pivot_idx + ObRedisListMeta::INDEX_STEP;
    } else {
      if (is_before_pivot && pivot_idx <= list_meta.ins_region_left_) {
        adjacent_idx = pivot_idx - ObRedisListMeta::INDEX_STEP;
      } else if (!is_before_pivot && pivot_idx >= list_meta.ins_region_right_) {
        adjacent_idx = pivot_idx + ObRedisListMeta::INDEX_STEP;
      } else if (get_adjacent_index(db, key, list_meta, pivot_idx, is_before_pivot, adjacent_idx)) {
        LOG_WARN("fail to get adjacent index", K(ret), K(pivot_idx), K(is_before_pivot));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (std::abs(adjacent_idx - pivot_idx) > 1) {
      insert_idx = is_before_pivot ? adjacent_idx + ((pivot_idx - adjacent_idx) >> 1)
                                   : pivot_idx + ((adjacent_idx - pivot_idx) >> 1);
      // update ins region
      updata_ins_region_after_insert(is_before_pivot, adjacent_idx, pivot_idx, list_meta);
    } else if (OB_FAIL(redict_idx(db, key, pivot_idx, is_before_pivot, list_meta, insert_idx))) {
      LOG_WARN("fail to redict idx", K(ret), K(pivot_idx), K(adjacent_idx));
    }
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(idx_overflowed(insert_idx))) {
    RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INDEX_OVERFLOW_ERR);
    LOG_WARN("index overflow", K(ret), K(insert_idx), K(list_meta));
  }

  return ret;
}

int ListCommandOperator::get_adjacent_index(
    const int64_t db,
    const ObString &key,
    const ObRedisListMeta &list_meta,
    const int64_t pivot_idx,
    const bool is_before_pivot,
    int64_t &adjacent_idx)
{
  int ret = OB_SUCCESS;
  ObTableQuery query;

  if (is_before_pivot) {
    // find the first one smaller than pivot_idx
    ListQueryCond query_cond(
        key,
        db,
        list_meta.left_idx_,
        pivot_idx,
        1,
        1,
        false/*is_forward_order*/,
        false/*need_query_all*/);
    if (OB_FAIL(build_list_query(query_cond, query))) {
      LOG_WARN("fail to build list query cond", K(ret), K(query_cond));
    }
  } else {
    // find the first one larger than pivot_idx
    ListQueryCond query_cond(
        key,
        db,
        pivot_idx,
        list_meta.right_idx_,
        1,
        1,
        true/*is_forward_order*/,
        false/*need_query_all*/);
    if (OB_FAIL(build_list_query(query_cond, query))) {
      LOG_WARN("fail to build list query cond", K(ret), K(query_cond));
    }
  }

  if (OB_SUCC(ret)) {
    // query element
    SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
    {
      QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, &list_meta)
      ObTableQueryResult *one_result = nullptr;
      const ObITableEntity *result_entity = nullptr;
      ObObj index_obj;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter->get_next_result(one_result))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next result", K(ret));
          }
        }
        one_result->rewind();
        while (OB_SUCC(ret)) {
          if (OB_FAIL(one_result->get_next_entity(result_entity))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", K(ret));
            }
          } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::REDIS_INDEX_NAME, index_obj))) {
            LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
          } else if (OB_FAIL(index_obj.get_int(adjacent_idx))) {
            LOG_WARN("fail to get index from obj", K(ret), K(index_obj));
          }
        }
      }
      if (OB_SUCC(ret) || OB_ITER_END == ret) {
        if (index_obj.is_null()) {
          RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisFmt::NULL_BULK_STRING);
          LOG_WARN("result is not exist", K(ret), K(index_obj));
        }
      }
      QUERY_ITER_END(iter)
    }
  }
  return ret;
}

int ListCommandOperator::do_insert(
    int64_t db,
    const ObString &key,
    bool is_before_pivot,
    const ObString &pivot,
    const ObString &value)
{
  int ret = OB_SUCCESS;

  // 1. Get meta data and check whether it has expired.
  ObRedisListMeta *list_meta = nullptr;
  ObRedisMeta *meta = nullptr;
  ObTableQuery query;
  int64_t pivot_idx = 0;
  int64_t insert_idx = 0;
  if (OB_FAIL(get_meta(db, key, ObRedisModel::LIST, meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do list expire if needed", K(ret), K(key));
    } else {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisFmt::ZERO);
    }
  } else if (FALSE_IT(list_meta = reinterpret_cast<ObRedisListMeta*>(meta))) {
  } else if (OB_FAIL(get_pivot_index(db, key, pivot, *list_meta, pivot_idx))) {
    LOG_WARN("fail to get pivot index", K(ret), K(key), K(pivot));
  } else if (OB_FAIL(get_insert_index(db, key,  pivot_idx, is_before_pivot, *list_meta, insert_idx))) {
    LOG_WARN("fail to get insert index", K(ret), K(key), K(pivot_idx), K(is_before_pivot));
  } else {
    ObTableBatchOperation put_value_ops;
    ResultFixedArray results(op_temp_allocator_);

    list_meta->count_ += 1;
    list_meta->left_idx_ = std::min(list_meta->left_idx_, insert_idx);
    list_meta->right_idx_ = std::max(list_meta->right_idx_, insert_idx);

    ObITableEntity *meta_entity = nullptr;
    ObITableEntity *new_data_entity;
    ObObj value_obj;
    if (OB_FAIL(gen_meta_entity(db, key, ObRedisModel::LIST, *list_meta, meta_entity))) {
      LOG_WARN("fail to put meta into batch operation", K(ret), K(key), K(*list_meta));
    } else if (OB_FAIL(put_value_ops.insert_or_update(*meta_entity))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(gen_entity_with_rowkey(
                   db, key, insert_idx, true /*is_data*/, new_data_entity, &op_entity_factory_))) {
      LOG_WARN("fail to gen entity with rowkey", K(ret));
    } else if (OB_FAIL(list_entity_set_value(value, ObTimeUtility::fast_current_time(), new_data_entity))) {
      LOG_WARN("fail to set value into entity", K(ret), K(value));
    } else if (OB_FAIL(put_value_ops.insert_or_update(*new_data_entity))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(process_table_batch_op(put_value_ops, results))) {
      LOG_WARN("fail to process table batch op", K(ret), K(put_value_ops));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_int(list_meta->count_))) {
      LOG_WARN("fail to set fmt res", K(ret), K(list_meta->count_));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}
int ListCommandOperator::do_get_len(int64_t db, const ObString &key)
{
  int ret = OB_SUCCESS;

  // 1. Get meta data and check whether it has expired.
  ObRedisMeta *meta = nullptr;
  ObTableQuery query;
  int64_t pivot_idx = 0;
  int64_t insert_idx = 0;
  if (OB_FAIL(get_meta(db, key, ObRedisModel::LIST, meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do list expire if needed", K(ret), K(key));
    } else {
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret)) {
    ObRedisListMeta *list_meta = reinterpret_cast<ObRedisListMeta *>(meta);
    if (OB_ISNULL(meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta should not be null", K(ret));
    } else if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_int(list_meta->count_))) {
      LOG_WARN("fail to set fmt res", K(ret), K(list_meta->count_));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int ListCommandOperator::get_new_border_idxs(
    int64_t db,
    const ObString &key,
    bool need_update_left_idx,
    bool need_update_right_idx,
    ObRedisListMeta &list_meta,
    ObIArray<int64_t> &new_border_idxs)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObTableQuery *, 2> querys;

  if (need_update_left_idx) {
    ListQueryCond query_cond(
        key,
        db,
        list_meta.left_idx_,
        list_meta.right_idx_,
        1,
        0,
        true/*is_query_forward*/,
        false/*need_query_all*/);
    ObTableQuery *query = nullptr;
    if (OB_ISNULL(query = OB_NEWx(ObTableQuery, &op_temp_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else if (OB_FAIL(build_list_query(query_cond, *query))) {
      LOG_WARN("fail to build list query cond", K(ret), K(query_cond));
    } else if (OB_FAIL(querys.push_back(query))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  if (OB_SUCC(ret) && need_update_right_idx) {
    ListQueryCond query_cond(
        key,
        db,
        list_meta.left_idx_,
        list_meta.right_idx_,
        1,
        0,
        false/*is_query_forward*/,
        false/*need_query_all*/);
    ObTableQuery *query = nullptr;
    if (OB_ISNULL(query = OB_NEWx(ObTableQuery, &op_temp_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else if (OB_FAIL(build_list_query(query_cond, *query))) {
      LOG_WARN("fail to build list query cond", K(ret), K(query_cond));
    } else if (OB_FAIL(querys.push_back(query))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }

  for (int i = 0; OB_SUCC(ret) && i < querys.count(); ++i) {
    ObTableQuery *query = querys.at(i);
    // query element
    SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
    {
      QUERY_ITER_START(redis_ctx_, *query, tb_ctx, iter, &list_meta)
      ObTableQueryResult *one_result = nullptr;
      const ObITableEntity *result_entity = nullptr;
      ObObj index_obj;
      int64_t index = 0;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter->get_next_result(one_result))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next result", K(ret));
          }
        }
        one_result->rewind();
        while (OB_SUCC(ret)) {
          if (OB_FAIL(one_result->get_next_entity(result_entity))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", K(ret));
            }
          } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::REDIS_INDEX_NAME, index_obj))) {
            LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
          } else if (OB_FAIL(index_obj.get_int(index))) {
            LOG_WARN("fail to get index from obj", K(ret), K(index_obj));
          } else if (OB_FAIL(new_border_idxs.push_back(index))) {
            LOG_WARN("fail to push back", K(ret), K(index));
          }
        }
      }
      if (OB_SUCC(ret) || OB_ITER_END == ret) {
        if (index_obj.is_null()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("query result count is unexpected error", K(ret), K(index_obj));
        }
      }
      QUERY_ITER_END(iter)
    }
  }

  if (OB_SUCC(ret) && new_border_idxs.count() != querys.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new border idxs count is unexpected error", K(ret), K(new_border_idxs.count()));
  }

  return ret;
}

void ListCommandOperator::after_ins_region_after_rem(
    int64_t del_leftmost_idx,
    int64_t del_rightmost_idx,
    ObRedisListMeta &list_meta)
{
  if (del_leftmost_idx < list_meta.ins_region_left_) {
    if (del_leftmost_idx < list_meta.left_idx_) {
      list_meta.ins_region_left_ = list_meta.left_idx_;
    } else {
      list_meta.ins_region_left_ = ((del_leftmost_idx % ObRedisListMeta::INDEX_STEP) == 0)
                                       ? del_leftmost_idx - ObRedisListMeta::INDEX_STEP
                                       : get_region_left_bord_idx(del_leftmost_idx);
    }
    list_meta.ins_region_left_ =
        list_meta.ins_region_left_ < list_meta.left_idx_ ? list_meta.left_idx_ : list_meta.ins_region_left_;
  }
  if (del_rightmost_idx > list_meta.ins_region_right_) {
    if (del_rightmost_idx > list_meta.right_idx_) {
      list_meta.ins_region_right_ = list_meta.right_idx_;
    } else {
      list_meta.ins_region_right_ = ((del_rightmost_idx % ObRedisListMeta::INDEX_STEP) == 0)
                                        ? del_rightmost_idx + ObRedisListMeta::INDEX_STEP
                                        : get_region_right_bord_idx(del_rightmost_idx);
    }
    list_meta.ins_region_right_ =
        list_meta.ins_region_right_ > list_meta.right_idx_ ? list_meta.right_idx_ : list_meta.ins_region_right_;
  }
  format_ins_region(list_meta);
}

int ListCommandOperator::update_meta_after_rem(
    int64_t db,
    const ObString &key,
    ObRedisListMeta &list_meta,
    const int64_t rem_count,
    int64_t del_leftmost_idx,
    int64_t del_rightmost_idx)
{
  int ret = OB_SUCCESS;
  bool need_update_left_idx = false;
  bool need_update_right_idx = false;
  if (del_leftmost_idx == list_meta.left_idx_) {
    need_update_left_idx = true;
  }
  if (del_rightmost_idx == list_meta.right_idx_) {
    need_update_right_idx = true;
  }

  ObTableBatchOperation batch_ops;
  ResultFixedArray results(op_temp_allocator_);
  if (rem_count == list_meta.count_) {
    ObITableEntity *meta_entity = nullptr;
    if (OB_FAIL(gen_entity_with_rowkey(db, key, ObRedisListMeta::META_INDEX, false/*is_data*/, meta_entity, &op_entity_factory_))) {
      LOG_WARN("fail to gen entity with rowkey", K(ret));
    } else if (OB_FAIL(batch_ops.del(*meta_entity))) {
      LOG_WARN("fail to push back data_req_entity", K(ret));
    }
  } else {
    if (!need_update_left_idx && !need_update_right_idx) {
      list_meta.count_ -= rem_count;
    } else {
      ObSEArray<int64_t, 2> new_border_idxs;
      if (OB_FAIL(get_new_border_idxs(db, key, need_update_left_idx, need_update_right_idx, list_meta, new_border_idxs))) {
        LOG_WARN("fail to get new border idxs", K(ret), K(list_meta));
      } else {
        list_meta.count_ -= rem_count;
        if (new_border_idxs.count() == 2) {
          list_meta.left_idx_ = new_border_idxs.at(0);
          list_meta.right_idx_ = new_border_idxs.at(1);
        } else if (need_update_left_idx) {
          list_meta.left_idx_ = new_border_idxs.at(0);
        } else if (need_update_right_idx) {
          list_meta.right_idx_ = new_border_idxs.at(0);
        }
      }
    }
    // update ins region
    if (OB_SUCC(ret)) {
      after_ins_region_after_rem(del_leftmost_idx, del_rightmost_idx, list_meta);
    }

    if (OB_SUCC(ret)) {
      ObITableEntity *meta_entity = nullptr;
      if (OB_FAIL(gen_meta_entity(db,
                                  key,
                                  ObRedisModel::LIST,
                                  list_meta,
                                  meta_entity))) {
        LOG_WARN("fail to put meta into batch operation", K(ret), K(key), K(list_meta));
      } else if (OB_FAIL(batch_ops.insert_or_update(*meta_entity))) {
        LOG_WARN("fail to push back data_req_entity", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(process_table_batch_op(batch_ops, results))) {
      LOG_WARN("fail to process table batch op", K(ret), K(batch_ops));
    }
  }
  return ret;
}

int ListCommandOperator::build_rem_querys(
    int64_t db,
    const ObString &key,
    const int64_t count,
    const ObString &value,
    const ObRedisListMeta &list_meta,
    ObTableQuery &query)
{
  int ret = OB_SUCCESS;

  int64_t query_limit = 0;
  bool is_query_forward = true;
  if (count == 0) {
    query_limit = list_meta.count_;
  } else if (count > 0) {
    query_limit = min(count, list_meta.count_);
  } else {
    if (OB_UNLIKELY(count == INT64_MIN)) {
      query_limit = list_meta.count_;
    } else {
      query_limit = min(-count, list_meta.count_);
    }
    is_query_forward = false;
  }

  ListQueryCond query_cond(
      key,
      db,
      list_meta.left_idx_,
      list_meta.right_idx_,
      query_limit,
      0,
      is_query_forward,
      false/*need_query_all*/);
  if (OB_FAIL(build_list_query(query_cond, query))) {
    LOG_WARN("fail to build list query cond", K(ret), K(query_cond));
  } else {
    // add filter
    FilterStrBuffer buffer(op_temp_allocator_);
    ObString filter_str;
    if (OB_FAIL(buffer.add_value_compare(hfilter::CompareOperator::EQUAL, ObRedisUtil::VALUE_PROPERTY_NAME, value))) {
      LOG_WARN("fail to gen compare filter string", K(ret), K(value));
    } else if (OB_FAIL(buffer.get_filter_str(filter_str))) {
      LOG_WARN("fail to get filter str");
    } else {
      query.set_filter(filter_str);
    }
  }

  return ret;
}

int ListCommandOperator::do_rem(int64_t db, const ObString &key, int64_t count, const ObString &value)
{
  int ret = OB_SUCCESS;

  // 1. Get meta data and check whether it has expired.
  ObRedisListMeta *list_meta = nullptr;
  ObRedisMeta *meta = nullptr;
  ObTableQuery query;
  int64_t real_rem_count = 0;

  if (OB_FAIL(get_meta(db, key, ObRedisModel::LIST, meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do list expire if needed", K(ret), K(key));
    } else {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisFmt::ZERO);
    }
  } else if (FALSE_IT(list_meta = reinterpret_cast<ObRedisListMeta*>(meta))) {
  } else if (OB_FAIL(build_rem_querys(db, key, count, value, *list_meta, query))) {
    LOG_WARN("fail to build range query", K(ret), K(key), K(count), K(value), K(*list_meta));
  } else {
    ObTableBatchOperation del_ops;
    int64_t first_del_index = INT_MIN;
    int64_t last_del_index = INT_MIN;

    // query element
    SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
    {
      QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, list_meta)
      ObTableQueryResult *one_result = nullptr;
      const ObITableEntity *result_entity = nullptr;
      ObObj index_obj;
      int64_t index = 0;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter->get_next_result(one_result))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next result", K(ret));
          }
        }
        one_result->rewind();
        while (OB_SUCC(ret)) {
          if (OB_FAIL(one_result->get_next_entity(result_entity))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", K(ret));
            }
          } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::REDIS_INDEX_NAME, index_obj))) {
            LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
          } else if (OB_FAIL(index_obj.get_int(index))) {
            LOG_WARN("fail to get index from obj", K(ret), K(index_obj));
          } else {
            ObITableEntity *del_data_entity = nullptr;
            if (OB_FAIL(gen_entity_with_rowkey(db, key, index, true/*is_data*/, del_data_entity, &op_entity_factory_))) {
              LOG_WARN("fail to gen entity with rowkey", K(ret));
            } else if (OB_FAIL(del_ops.del(*del_data_entity))) {
              LOG_WARN("fail to push back", K(ret));
            } else {
              if (first_del_index == INT_MIN) {
                first_del_index = index;
              }
              ++real_rem_count;
            }
          }
        }
      }
      if (OB_SUCC(ret) || ret == OB_ITER_END) {
        last_del_index = index;
      }
      QUERY_ITER_END(iter)
    }

    if (OB_SUCC(ret)) {
      // del elements
      ResultFixedArray results(op_temp_allocator_);
      if (OB_FAIL(process_table_batch_op(del_ops, results))) {
        LOG_WARN("fail to process table batch op", K(ret), K(del_ops));
      } else if (OB_FAIL(update_meta_after_rem(
                     db, key, *list_meta, real_rem_count, min(first_del_index, last_del_index), max(first_del_index, last_del_index)))) {
        LOG_WARN(
            "fail to update meta after rem",
            K(ret),
            K(*list_meta),
            K(real_rem_count),
            K(first_del_index),
            K(last_del_index));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_int(real_rem_count))) {
      LOG_WARN("fail to set fmt res", K(ret));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int ListCommandOperator::build_del_ops(const ObTableQuery &query,
                                       ObTableBatchOperation &del_ops,
                                       ObRedisListMeta *meta/*= nullptr*/)
{
  int ret = OB_SUCCESS;

  int64_t result_count = 0;
  // query element
  SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
  {
    QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, meta)
    ObTableQueryResult *one_result = nullptr;
    const ObITableEntity *result_entity = nullptr;
    ObObj index_obj;
    ObObj db_obj;
    ObObj key_obj;
    int64_t index = 0;
    int64_t db = 0;
    ObString key;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next_result(one_result))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next result", K(ret));
        }
      }
      one_result->rewind();
      while (OB_SUCC(ret)) {
        if (OB_FAIL(one_result->get_next_entity(result_entity))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next result", K(ret));
          }
        } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::REDIS_INDEX_NAME, index_obj))) {
          LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
        } else if (OB_FAIL(index_obj.get_int(index))) {
          LOG_WARN("fail to get index from obj", K(ret), K(index_obj));
        } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::DB_PROPERTY_NAME, db_obj))) {
          LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
        } else if (OB_FAIL(db_obj.get_int(db))) {
          LOG_WARN("fail to get index from obj", K(ret), K(db_obj));
        } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::RKEY_PROPERTY_NAME, key_obj))) {
          LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
        } else if (OB_FAIL(get_varbinary_by_deep(key_obj, key))) {
          LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
        } else {
          ObITableEntity *del_data_entity = nullptr;
          if (OB_FAIL(gen_entity_with_rowkey(db, key, index, true/*is_data*/, del_data_entity, &op_entity_factory_))) {
            LOG_WARN("fail to gen entity with rowkey", K(ret));
          } else if (OB_FAIL(del_ops.del(*del_data_entity))) {
            LOG_WARN("fail to push back", K(ret));
          } else {
            result_count++;
          }
        }
      }
    }
    if (OB_SUCC(ret) || OB_ITER_END == ret) {
      if (result_count != query.get_limit()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query result count is unexpected error", K(ret), K(result_count), K(query.get_limit()));
      }
    }
    QUERY_ITER_END(iter)
  }
  return ret;
}

int ListCommandOperator::del_key(const ObString &key, int64_t db, ObRedisListMeta *list_meta)
{
  int ret = OB_SUCCESS;

  ObTableBatchOperation del_ops;
  ObTableQuery query;
  if (OB_ISNULL(list_meta)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null list meta", K(ret));
  } else if (OB_FAIL(build_range_query(db, key, *list_meta, 0, -1, query))) {
    LOG_WARN("fail to build range query", K(ret), K(key), K(*list_meta));
  } else if (OB_FAIL(build_del_ops(query, del_ops, list_meta))) {
    LOG_WARN("fail to build del ops", K(ret), K(query));
  } else {
    // del elements
    ResultFixedArray results(op_temp_allocator_);
    if (OB_FAIL(process_table_batch_op(del_ops, results))) {
      LOG_WARN("fail to process table batch op", K(ret), K(del_ops));
    } else if (OB_FAIL(fake_del_meta(ObRedisModel::LIST, db, key, list_meta))) {
      LOG_WARN("fail to delete complex type meta", K(db), K(key), KPC(list_meta));
    }
  }

  return ret;
}

int ListCommandOperator::do_del(int64_t db, const ObString &key, bool &is_exist)
{
  int ret = OB_SUCCESS;

  // 1. Get meta data and check whether it has expired.
  ObRedisListMeta *list_meta = nullptr;
  ObRedisMeta *meta = nullptr;

  is_exist = true;
  if (OB_FAIL(get_meta(db, key, ObRedisModel::LIST, meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do list expire if needed", K(ret), K(key));
    } else {
      is_exist = false;
      ret = OB_SUCCESS;
    }
  } else if (FALSE_IT(list_meta = reinterpret_cast<ObRedisListMeta*>(meta))) {
  } else if (OB_FAIL(del_key(key, db, list_meta))) {
    LOG_WARN("fail to del key", K(ret), K(key));
  }

  return ret;
}

int ListCommandOperator::do_same_keys_push(
    ObTableBatchOperation &batch_op,
    ObIArray<ObTabletID>& tablet_ids,
    ObIArray<ObITableOp *> &ops,
    const ObIArray<ObRedisMeta *> &metas,
    const ObString &key,
    int64_t same_key_start_pos,
    int64_t same_key_end_pos)
{
  int ret = OB_SUCCESS;

  int64_t cur_ts = ObTimeUtility::fast_current_time();
  // do (same_key_start_pos, same_key_end_pos)
  ObRedisListMeta *list_meta = reinterpret_cast<ObRedisListMeta *>(metas.at(same_key_start_pos));
  bool list_is_empty = true;
  if (list_meta->is_exists()) {
    list_is_empty = false;
  }
  for (int64_t i = same_key_start_pos; OB_SUCC(ret) && i <= same_key_end_pos; ++i) {
    ObRedisOp *redis_op = reinterpret_cast<ObRedisOp *>(ops.at(i));
    Push *push = reinterpret_cast<Push *>(redis_op->cmd());
    if (list_is_empty && push->need_exist()) {
      if (OB_FAIL(redis_op->response().set_fmt_res(ObRedisFmt::ZERO))) {
        LOG_WARN("fail to set res int", K(ret), KPC(redis_op));
      }
    } else {
      list_is_empty = false;
      bool need_push_meta = (i == same_key_end_pos) ? true : false;
      int64_t push_size = need_push_meta ? push->get_values().count() + 1 : push->get_values().count();
      if (OB_FAIL(build_push_ops(
              batch_op,
              redis_op->db(),
              key,
              *list_meta,
              cur_ts,
              push->get_values(),
              push->is_push_left(),
              need_push_meta))) {
        LOG_WARN("fail to build push ops", K(ret), KPC(redis_op));
        if (ObRedisErr::is_redis_error(ret)) {
          RESPONSE_REDIS_ERROR(redis_op->response(), fmt_redis_msg_.ptr());
          ret = COVER_REDIS_ERROR(ret);
        }
      } else if (OB_FAIL(redis_op->response().set_res_int(list_meta->count_))) {
        LOG_WARN("fail to set res int", K(ret), KPC(redis_op));
      } else {
        for (int64_t i = 0; i < push_size && OB_SUCC(ret); i++) {
          if (OB_FAIL(tablet_ids.push_back(redis_op->tablet_id()))) {
            LOG_WARN("fail to push back tablet_id", K(ret), K(redis_op->tablet_id()));
          }
        }  // end for
      }
    }
  }  // end for

  return ret;
}

// Define the comparison function
bool ListCommandOperator::compare_ob_redis_ops(ObITableOp *&op_a, ObITableOp *&op_b)
{
  ObRedisOp *redis_op_a = reinterpret_cast<ObRedisOp *>(op_a);
  ObRedisOp *redis_op_b = reinterpret_cast<ObRedisOp *>(op_b);
  ObString key_a, key_b;
  redis_op_a->get_key(key_a);
  redis_op_b->get_key(key_b);
  return key_a < key_b;
}

void ListCommandOperator::sort_group_ops() {
  ObRedisBatchCtx &group_ctx = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_);
  ObIArray<ObITableOp *> &ops = group_ctx.ops();
  ObITableOp **end = &ops.at(ops.count() - 1);
  ++end;
  lib::ob_sort(&ops.at(0), end, ListCommandOperator::compare_ob_redis_ops);
}
int ListCommandOperator::do_group_push()
{
  int ret = OB_SUCCESS;

  // Sort ops by key to find the same key
  sort_group_ops();
  ObIArray<ObITableOp *> &ops = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_).ops();
  // Get all op's meta
  ObArray<ObRedisMeta *> metas(OB_MALLOC_NORMAL_BLOCK_SIZE,
                              ModulePageAllocator(op_temp_allocator_, "RedisGPush"));
  if (OB_FAIL(get_group_metas(op_temp_allocator_, ObRedisModel::LIST, metas))) {
    LOG_WARN("fail to get group metas", K(ret));
  } else {
    ObTableBatchOperation batch_op;
    // Construct the entity that requires push and the entity of meta
    ObString last_key;
    int64_t same_key_start_pos = 0, same_key_end_pos = 0;
    int64_t pos = 0;
    for (; OB_SUCC(ret) && pos < ops.count(); ++pos) {
      ObRedisOp *redis_op = reinterpret_cast<ObRedisOp *>(ops.at(pos));
      ObString key;
      if (OB_FAIL(redis_op->get_key(key))) {
        LOG_WARN("fail to get key", K(ret), KPC(redis_op));
      } else if (!last_key.empty() && last_key != key) {
        same_key_end_pos = pos - 1;
        // do (same_key_start_pos, same_key_end_pos)
        if (OB_FAIL(
                do_same_keys_push(batch_op, tablet_ids_, ops, metas, last_key, same_key_start_pos, same_key_end_pos))) {
          LOG_WARN("fail to do same keys push", K(ret), KPC(redis_op));
        } else {
          same_key_start_pos = pos;
        }
      }
      last_key = key;
    }
    // do (same_key_start_pos, end_pos - 1)
    if (OB_SUCC(ret)) {
      same_key_end_pos = pos - 1;
      if (OB_FAIL(do_same_keys_push(batch_op, tablet_ids_, ops, metas, last_key, same_key_start_pos, same_key_end_pos))) {
        LOG_WARN("fail to do same keys push", K(ret), K(ops));
      }
    }
    ResultFixedArray results(op_temp_allocator_);
    if (OB_SUCC(ret) && OB_FAIL(process_table_batch_op(
                            batch_op, results, nullptr, RedisOpFlags::NONE, nullptr, nullptr, &tablet_ids_))) {
      LOG_WARN("fail to process table batch op", K(ret), K(batch_op));
    }
  }

  return ret;
}

int ListCommandOperator::deep_copy_list_entity(
    const ObITableEntity *result_entity,
    ObITableEntity *&result_copy_entity,
    ListData &list_data)
{
  int ret = OB_SUCCESS;
  ObObj tmp_res_obj;
  ObITableEntity *tmp_entity = nullptr;
  if (OB_FAIL(result_entity->get_property(ObRedisUtil::REDIS_INDEX_NAME, tmp_res_obj))) {
    LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
  } else if (OB_FAIL(tmp_res_obj.get_int(list_data.index_))) {
    LOG_WARN("fail to get idx", K(ret), K(tmp_res_obj));
  } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, tmp_res_obj))) {
    LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
  } else if (OB_FAIL(tmp_res_obj.get_timestamp(list_data.insert_ts_))) {
    LOG_WARN("fail to get idx", K(ret), K(tmp_res_obj));
  } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::VALUE_PROPERTY_NAME, tmp_res_obj))) {
    LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
  } else if (OB_FAIL(get_varbinary_by_deep(tmp_res_obj, list_data.value_))) {
    // note: deep copy is required, because after the iterator destruct, the memory of the query results
    // will be released
    LOG_WARN("fail to get res value by deep", K(ret), K(tmp_res_obj));
  } else if (OB_FAIL(gen_entity_with_rowkey(
                 list_data.db_, list_data.key_, list_data.index_, true, tmp_entity, &op_entity_factory_))) {
    LOG_WARN("fail to gen entity with rowkey", K(ret), K(list_data));
  } else if (OB_FAIL(list_entity_set_value(list_data.value_, list_data.insert_ts_, tmp_entity))) {
    LOG_WARN("fail to set value", K(ret), KPC(tmp_entity));
  } else {
    result_copy_entity = tmp_entity;
  }
  return ret;
}

int ListCommandOperator::copy_list_entity(
    const ObITableEntity *result_entity,
    ObITableEntity *&result_copy_entity,
    ListData &list_data)
{
  int ret = OB_SUCCESS;
  ObObj tmp_res_obj;
  ObITableEntity *tmp_entity = nullptr;
  if (OB_FAIL(result_entity->get_property(ObRedisUtil::REDIS_INDEX_NAME, tmp_res_obj))) {
    LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
  } else if (OB_FAIL(tmp_res_obj.get_int(list_data.index_))) {
    LOG_WARN("fail to get idx", K(ret), K(tmp_res_obj));
  } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, tmp_res_obj))) {
    LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
  } else if (OB_FAIL(tmp_res_obj.get_timestamp(list_data.insert_ts_))) {
    LOG_WARN("fail to get idx", K(ret), K(tmp_res_obj));
  } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::VALUE_PROPERTY_NAME, tmp_res_obj))) {
    LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
  } else if (OB_FAIL(tmp_res_obj.get_varbinary(list_data.value_))) {
    LOG_WARN("fail to get res value", K(ret), K(tmp_res_obj));
  } else if (OB_FAIL(gen_entity_with_rowkey(
                 list_data.db_, list_data.key_, list_data.index_, true, tmp_entity, &op_entity_factory_))) {
    LOG_WARN("fail to gen entity with rowkey", K(ret), K(list_data));
  } else if (OB_FAIL(list_entity_set_value(list_data.value_, list_data.insert_ts_, tmp_entity))) {
    LOG_WARN("fail to set value", K(ret), KPC(tmp_entity));
  } else {
    result_copy_entity = tmp_entity;
  }
  return ret;
}

int ListCommandOperator::gen_group_pop_res(
    const ObIArray<ListElemEntity> &res_idx_entitys,
    ObRedisListMeta *list_meta,
    int64_t same_key_start_pos,
    int64_t same_key_end_pos,
    ObTableBatchOperation &batch_op_insup,
    ObTableBatchOperation &batch_op_del,
    ObIArray<ObTabletID> &tablet_insup_ids,
    ObIArray<ObTabletID> &tablet_del_ids)
{
  int ret = OB_SUCCESS;
  ObIArray<ObITableOp *> &ops = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_).ops();
  ObRedisOp *redis_op = reinterpret_cast<ObRedisOp *>(ops.at(same_key_start_pos));
  Pop *pop = reinterpret_cast<Pop *>(redis_op->cmd());

  if (res_idx_entitys.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("idx array count not match", K(ret), K(res_idx_entitys));
  } else if (
      (pop->is_pop_left() && res_idx_entitys.at(0).first != list_meta->left_idx_) ||
      (!pop->is_pop_left() && res_idx_entitys.at(0).first != list_meta->right_idx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("idx array not match", K(ret), K(res_idx_entitys), K(list_meta), K(pop->is_pop_left()));
  } else {
    int64_t pos_op_right = same_key_end_pos;
    int64_t pos_res_right = list_meta->count_ > (same_key_end_pos - same_key_start_pos + 1)
                                ? same_key_end_pos
                                : same_key_start_pos + res_idx_entitys.count() - 1;
    for (int64_t pos_op = pos_op_right; OB_SUCC(ret) && pos_op > pos_res_right; --pos_op) {
      redis_op = reinterpret_cast<ObRedisOp *>(ops.at(pos_op));
      if (OB_FAIL(redis_op->response().set_fmt_res(ObRedisFmt::NULL_BULK_STRING))) {
        LOG_WARN("fail to set res nil", K(ret), KPC(redis_op));
      }
    }
    for (int64_t pos_op = same_key_start_pos, pos_res = 0; OB_SUCC(ret) && pos_op <= pos_res_right;
         ++pos_op, ++pos_res) {
      ObString res_value;
      redis_op = reinterpret_cast<ObRedisOp *>(ops.at(pos_op));
      ObObj value_obj;
      if (OB_FAIL(res_idx_entitys.at(pos_res).second->get_property(ObRedisUtil::VALUE_PROPERTY_NAME, value_obj))) {
        LOG_WARN("fail to get member from result enrity", K(ret), KPC(res_idx_entitys.at(pos_res).second));
      } else if (OB_FAIL(value_obj.get_varbinary(res_value))) {
        LOG_WARN("fail to get res value", K(ret), K(value_obj));
      } else if (OB_FAIL(redis_op->response().set_res_bulk_string(res_value))) {
        LOG_WARN("fail to set res int", K(ret), KPC(redis_op));
      } else if (OB_FAIL(batch_op_del.del(*res_idx_entitys.at(pos_res).second))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(tablet_del_ids.push_back(redis_op->tablet_id_))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      // update meta
      if (list_meta->count_ <= (same_key_end_pos - same_key_start_pos + 1)) {
        ObITableEntity *meta_entity = nullptr;
        if (OB_FAIL(gen_meta_entity(redis_op->db(), pop->key(), ObRedisModel::LIST, *list_meta, meta_entity))) {
          LOG_WARN("fail to put meta into batch operation", K(ret), K(pop->key()), K(list_meta));
        } else if (OB_FAIL(batch_op_del.del(*meta_entity))) {
          LOG_WARN("fail to push back", K(ret));
        } else if (OB_FAIL(tablet_del_ids.push_back(redis_op->tablet_id_))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {
        list_meta->count_ -= (same_key_end_pos - same_key_start_pos + 1);
        if (pop->is_pop_left()) {
          list_meta->left_idx_ = res_idx_entitys.at(res_idx_entitys.count() - 1).first;
        } else {
          list_meta->right_idx_ = res_idx_entitys.at(res_idx_entitys.count() - 1).first;
        }
        // update ins region
        if (list_meta->has_ins_region()) {
          update_ins_region_after_pop(pop->is_pop_left(), *list_meta);
        }
        ObITableEntity *meta_entity = nullptr;
        if (OB_FAIL(gen_meta_entity(redis_op->db(), pop->key(), ObRedisModel::LIST, *list_meta, meta_entity))) {
          LOG_WARN("fail to put meta into batch operation", K(ret), K(pop->key()), K(list_meta));
        } else if (OB_FAIL(batch_op_insup.insert_or_update(*meta_entity))) {
          LOG_WARN("fail to push back", K(ret));
        } else if (OB_FAIL(tablet_insup_ids.push_back(redis_op->tablet_id_))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ListCommandOperator::do_same_keys_pop(
    ObTableBatchOperation &batch_op_insup,
    ObTableBatchOperation &batch_op_del,
    ObIArray<ObTabletID> &tablet_insup_ids,
    ObIArray<ObTabletID> &tablet_del_ids,
    const ObIArray<ObRedisMeta *> &metas,
    const ObString &key,
    int64_t same_key_start_pos,
    int64_t same_key_end_pos)
{
  int ret = OB_SUCCESS;
  ObIArray<ObITableOp *> &ops = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_).ops();
  ObRedisListMeta *list_meta = reinterpret_cast<ObRedisListMeta *>(metas.at(same_key_start_pos));
  ObRedisOp *redis_op = reinterpret_cast<ObRedisOp *>(ops.at(same_key_start_pos));
  Pop *pop = reinterpret_cast<Pop *>(redis_op->cmd());
  if (!list_meta->is_exists()) {
    if (OB_FAIL(ops_return_nullstr(ops, same_key_start_pos, same_key_end_pos))) {
      LOG_WARN("fail to ops return nullstr", K(ret), K(same_key_start_pos), K(same_key_end_pos));
    }
  } else {
    ObTableQuery query;
    ObSEArray<ListElemEntity, 2> res_idx_entitys(
        OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(op_temp_allocator_, "RedisGPop"));
    ListQueryCond query_cond(
        key,
        redis_op->db(),
        list_meta->left_idx_,
        list_meta->right_idx_,
        same_key_end_pos - same_key_start_pos + 2 /*limit*/,
        0 /*offset*/,
        pop->is_pop_left(),
        true /*need_query_all*/);
    if (OB_FAIL(build_list_query(query_cond, query))) {
      LOG_WARN("fail to build list query", K(ret), K(query_cond));
    } else {
      SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
      {
        QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, list_meta)
        ObTableQueryResult *one_result = nullptr;
        const ObITableEntity *result_entity = nullptr;
        ListData list_data;
        list_data.db_ = redis_op->db();
        list_data.key_ = key;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(iter->get_next_result(one_result))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", K(ret));
            }
          }
          one_result->rewind();
          while (OB_SUCC(ret)) {
            if (OB_FAIL(one_result->get_next_entity(result_entity))) {
              if (OB_ITER_END != ret) {
                LOG_WARN("fail to get next result", K(ret));
              }
            } else {
              ObITableEntity *result_copy_entity = nullptr;
              if (OB_FAIL(deep_copy_list_entity(result_entity, result_copy_entity, list_data))) {
                LOG_WARN("fail to deep copy list entity", K(ret), KPC(result_entity));
              } else if (OB_FAIL(res_idx_entitys.push_back({list_data.index_, result_copy_entity}))) {
                LOG_WARN("fail to push back entity", K(ret), KPC(result_copy_entity));
              }
            }
          }
        }
        if (OB_SUCC(ret) || OB_ITER_END == ret) {
          if (OB_FAIL(gen_group_pop_res(
                  res_idx_entitys,
                  list_meta,
                  same_key_start_pos,
                  same_key_end_pos,
                  batch_op_insup,
                  batch_op_del,
                  tablet_insup_ids,
                  tablet_del_ids))) {
            LOG_WARN("fail to gen group pop res", K(ret), K(same_key_start_pos), K(same_key_end_pos));
          }
        }
        QUERY_ITER_END(iter)
      }
    }
  }
  return ret;
}

// auxiliary function, check multiple acquisition conditions
bool check_multi_get_cond(
    int64_t same_key_start_pos,
    int64_t same_key_end_pos,
    ObRedisOp *&redis_op,
    const ObRedisListMeta *meta)
{
  bool can_use_multi = true;
  if (meta->has_ins_region()) {
    Pop *pop = reinterpret_cast<Pop *>(redis_op->cmd());
    if (pop->is_pop_left() && (meta->left_idx_ + (same_key_end_pos - same_key_start_pos) *
                                                     ObRedisListMeta::INDEX_STEP) >= meta->ins_region_left_) {
      can_use_multi = false;
    } else if (
        !pop->is_pop_left() && (meta->right_idx_ - (same_key_end_pos - same_key_start_pos) *
                                                       ObRedisListMeta::INDEX_STEP) <= meta->ins_region_right_) {
      can_use_multi = false;
    }
  }

  return can_use_multi;
}

int ListCommandOperator::gen_same_key_batch_get_op(
    const ObRedisListMeta &meta,
    const ObRedisOp &redis_op,
    ObString key,
    int64_t same_key_start_pos,
    int64_t same_key_end_pos,
    ObTableBatchOperation &batch_op_get,
    ObIArray<ObTabletID> &tablet_get_ids)
{
  int ret = OB_SUCCESS;

  const Pop *pop = reinterpret_cast<const Pop *>(redis_op.cmd());
  for (int64_t i = 0; i < same_key_end_pos - same_key_start_pos + 1; ++i) {
    int64_t idx = pop->is_pop_left() ? meta.left_idx_ + i * ObRedisListMeta::INDEX_STEP
                                     : meta.right_idx_ - i * ObRedisListMeta::INDEX_STEP;
    ObITableEntity *entity = nullptr;
    if (OB_FAIL(
            gen_entity_with_rowkey(redis_op.db(), key, idx, true /*is_data*/, entity, &op_entity_factory_))) {
      LOG_WARN("fail to gen entity with rowkey", K(ret));
    } else if (OB_FAIL(batch_op_get.retrieve(*entity))) {
      LOG_WARN("fail to retrieve entity", K(ret), KPC(entity));
    } else if (OB_FAIL(tablet_get_ids.push_back(redis_op.tablet_id_))) {
      LOG_WARN("fail to push back tablet id", K(ret));
    }
  }

  return ret;
}

int ListCommandOperator::ops_return_nullstr(
    ObIArray<ObITableOp *> &ops,
    int64_t same_key_start_pos,
    int64_t same_key_end_pos)
{
  int ret = OB_SUCCESS;

  for (int64_t pos_op = same_key_end_pos; OB_SUCC(ret) && pos_op >= same_key_start_pos; --pos_op) {
    ObRedisOp *redis_op = reinterpret_cast<ObRedisOp *>(ops.at(pos_op));
    if (OB_FAIL(redis_op->response().set_fmt_res(ObRedisFmt::NULL_BULK_STRING))) {
      LOG_WARN("fail to set res nil", K(ret), KPC(redis_op));
    }
  }

  return ret;
}

int ListCommandOperator::check_can_use_multi_get(
    const ObArray<ObRedisMeta *> &metas,
    ObIArray<ObITableOp *> &ops,
    ObTableBatchOperation &batch_op_get,
    ObIArray<ObTabletID> &tablet_get_ids,
    bool &can_use_multi_get)
{
  int ret = OB_SUCCESS;

  ObString last_key;
  int64_t same_key_start_pos = 0, same_key_end_pos = 0;
  int64_t pos = 0;
  for (; OB_SUCC(ret) && can_use_multi_get == true && pos < ops.count(); ++pos) {
    ObRedisOp *redis_op = reinterpret_cast<ObRedisOp *>(ops.at(pos));
    ObString key;
    if (OB_FAIL(redis_op->get_key(key))) {
      LOG_WARN("fail to get key", K(ret), KPC(redis_op));
    } else if (!last_key.empty() && last_key != key) {
      same_key_end_pos = pos - 1;
      // do (same_key_start_pos, same_key_end_pos)
      ObRedisListMeta *list_meta = static_cast<ObRedisListMeta *>(metas[same_key_start_pos]);
      can_use_multi_get = check_multi_get_cond(same_key_start_pos, same_key_end_pos, redis_op, list_meta);
      if (can_use_multi_get &&
          OB_FAIL(gen_same_key_batch_get_op(
              *list_meta, *redis_op, last_key, same_key_start_pos, same_key_end_pos, batch_op_get, tablet_get_ids))) {
        LOG_WARN("fail to gen same key batch get op", K(ret), K(same_key_start_pos), K(same_key_end_pos));
      }

      same_key_start_pos = pos;
    }
    last_key = key;
  }
  // do (same_key_start_pos, end_pos - 1)
  if (OB_SUCC(ret) && can_use_multi_get) {
    same_key_end_pos = pos - 1;
    ObRedisOp *redis_op = reinterpret_cast<ObRedisOp *>(ops.at(same_key_start_pos));
    ObRedisListMeta *list_meta = static_cast<ObRedisListMeta *>(metas[same_key_start_pos]);
    can_use_multi_get = check_multi_get_cond(same_key_start_pos, same_key_end_pos, redis_op, list_meta);
    if (can_use_multi_get &&
        OB_FAIL(gen_same_key_batch_get_op(
            *list_meta, *redis_op, last_key, same_key_start_pos, same_key_end_pos, batch_op_get, tablet_get_ids))) {
      LOG_WARN("fail to gen same key batch get op", K(ret), K(same_key_start_pos), K(same_key_end_pos));
    }
  }

  return ret;
}

int ListCommandOperator::update_meta_after_multi_pop(
    ObRedisListMeta *list_meta,
    ObTableBatchOperation &batch_op_insup,
    ObTableBatchOperation &batch_op_del,
    ObIArray<ObTabletID> &tablet_insup_ids,
    ObIArray<ObTabletID> &tablet_del_ids,
    int64_t same_key_start_pos,
    int64_t same_key_end_pos)
{
  int ret = OB_SUCCESS;

  ObIArray<ObITableOp *> &ops = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_).ops();
  ObRedisOp *redis_op = reinterpret_cast<ObRedisOp *>(ops.at(same_key_start_pos));
  Pop *pop = reinterpret_cast<Pop *>(redis_op->cmd());

  if (list_meta->count_ <= (same_key_end_pos - same_key_start_pos + 1)) {
    ObITableEntity *meta_entity = nullptr;
    if (OB_FAIL(gen_meta_entity(redis_op->db(), pop->key(), ObRedisModel::LIST, *list_meta, meta_entity))) {
      LOG_WARN("fail to put meta into batch operation", K(ret), K(pop->key()), K(list_meta));
    } else if (OB_FAIL(batch_op_del.del(*meta_entity))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(tablet_del_ids.push_back(redis_op->tablet_id_))) {
      LOG_WARN("fail to push back", K(ret));
    }
  } else {
    list_meta->count_ -= (same_key_end_pos - same_key_start_pos + 1);
    if (pop->is_pop_left()) {
      list_meta->left_idx_ += (same_key_end_pos - same_key_start_pos + 1) * ObRedisListMeta::INDEX_STEP;
    } else {
      list_meta->right_idx_ -= (same_key_end_pos - same_key_start_pos + 1) * ObRedisListMeta::INDEX_STEP;
    }
    // update ins region
    if (list_meta->has_ins_region()) {
      update_ins_region_after_pop(pop->is_pop_left(), *list_meta);
    }
    ObITableEntity *meta_entity = nullptr;
    if (OB_FAIL(gen_meta_entity(redis_op->db(), pop->key(), ObRedisModel::LIST, *list_meta, meta_entity))) {
      LOG_WARN("fail to put meta into batch operation", K(ret), K(pop->key()), K(list_meta));
    } else if (OB_FAIL(batch_op_insup.insert_or_update(*meta_entity))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(tablet_insup_ids.push_back(redis_op->tablet_id_))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  return ret;
}

int ListCommandOperator::do_same_keys_multi_pop(
    const ResultFixedArray& batch_res,
    ObTableBatchOperation &batch_op_insup,
    ObTableBatchOperation &batch_op_del,
    ObIArray<ObTabletID> &tablet_insup_ids,
    ObIArray<ObTabletID> &tablet_del_ids,
    const ObIArray<ObRedisMeta *> &metas,
    const ObString &key,
    int64_t same_key_start_pos,
    int64_t same_key_end_pos)
{
  int ret = OB_SUCCESS;

  ObIArray<ObITableOp *> &ops = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_).ops();
  ObRedisListMeta *list_meta = reinterpret_cast<ObRedisListMeta *>(metas.at(same_key_start_pos));
  ObRedisOp *redis_op = reinterpret_cast<ObRedisOp *>(ops.at(same_key_start_pos));
  Pop *pop = reinterpret_cast<Pop *>(redis_op->cmd());

  for (int i = same_key_start_pos; OB_SUCC(ret) && i <= same_key_end_pos; ++i) {
    redis_op = reinterpret_cast<ObRedisOp *>(ops.at(i));
    if (batch_res[i].get_return_rows() > 0) {
      ListData list_data;
      list_data.db_ = redis_op->db();
      list_data.key_ = pop->key();
      ObString val;
      const ObITableEntity *res_entity = nullptr;
      ObITableEntity *result_copy_entity = nullptr;
      if (OB_FAIL(batch_res[i].get_entity(res_entity))) {
        LOG_WARN("fail to get entity", K(ret), K(batch_res[i]));
      } else if (OB_FAIL(get_varbinary_from_entity(*res_entity, ObRedisUtil::VALUE_PROPERTY_NAME, val))) {
        LOG_WARN("fail to get value from entity", K(ret), KPC(res_entity));
      } else if (OB_FAIL(redis_op->response().set_res_bulk_string(val))) {
        LOG_WARN("fail to set bulk string", K(ret), K(val));
      } else if (OB_FAIL(copy_list_entity(res_entity, result_copy_entity, list_data))) {
        LOG_WARN("fail to copy list entity", K(ret));
      } else if (OB_FAIL(batch_op_del.del(*result_copy_entity))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(tablet_del_ids.push_back(redis_op->tablet_id_))) {
        LOG_WARN("fail to push back", K(ret));
      }
    } else {
      if (OB_FAIL(redis_op->response().set_fmt_res(ObRedisFmt::NULL_BULK_STRING))) {
        LOG_WARN("fail to set bulk string", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    // update meta
    if (OB_FAIL(update_meta_after_multi_pop(
            list_meta, batch_op_insup, batch_op_del, tablet_insup_ids, tablet_del_ids, same_key_start_pos, same_key_end_pos))) {
      LOG_WARN("fail to update meta after multi pop", K(ret));
    }
  }
  return ret;
}

int ListCommandOperator::do_group_pop_use_multi_get(
    const ObArray<ObRedisMeta *> &metas,
    const ObTableBatchOperation &batch_op_get,
    ObIArray<ObTabletID> &tablet_get_ids,
    ObTableBatchOperation &batch_op_insup,
    ObIArray<ObTabletID> &tablet_insup_ids,
    ObTableBatchOperation &batch_op_del,
    ObIArray<ObTabletID> &tablet_del_ids)
{
  int ret = OB_SUCCESS;
  ObIArray<ObITableOp *> &ops = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_).ops();
  ResultFixedArray batch_res(op_temp_allocator_);
  if (OB_FAIL(process_table_batch_op(
          batch_op_get,
          batch_res,
          nullptr,
          RedisOpFlags::NONE,
          &op_temp_allocator_,
          &op_entity_factory_,
          &tablet_get_ids))) {
    LOG_WARN("fail to process table batch op", K(ret));
  } else if (batch_res.count() != ops.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch res count not match", K(ret), K(batch_res.count()), K(ops.count()));
  } else {
    ObString last_key;
    int64_t same_key_start_pos = 0, same_key_end_pos = 0;
    int64_t pos = 0;
    for (; OB_SUCC(ret) && pos < ops.count(); ++pos) {
      ObRedisOp *redis_op = reinterpret_cast<ObRedisOp *>(ops.at(pos));
      ObString key;
      if (OB_FAIL(redis_op->get_key(key))) {
        LOG_WARN("fail to get key", K(ret), KPC(redis_op));
      } else if (!last_key.empty() && last_key != key) {
        same_key_end_pos = pos - 1;
        ObRedisListMeta *list_meta = reinterpret_cast<ObRedisListMeta *>(metas.at(same_key_start_pos));
        // do (same_key_start_pos, same_key_end_pos)
        if (!list_meta->is_exists()) {
          if (OB_FAIL(ops_return_nullstr(ops, same_key_start_pos, same_key_end_pos))) {
            LOG_WARN("fail to ops return nullstr", K(ret), K(same_key_start_pos), K(same_key_end_pos));
          }
        } else if (OB_FAIL(do_same_keys_multi_pop(
                       batch_res,
                       batch_op_insup,
                       batch_op_del,
                       tablet_insup_ids,
                       tablet_del_ids,
                       metas,
                       last_key,
                       same_key_start_pos,
                       same_key_end_pos))) {
          LOG_WARN("fail to do same keys multi pop", K(ret), K(same_key_start_pos), K(same_key_end_pos));
        }
        same_key_start_pos = pos;
      }
      last_key = key;
    }
    if (OB_SUCC(ret)) {
      same_key_end_pos = pos - 1;
      // do (same_key_start_pos, same_key_end_pos)
      ObRedisListMeta *list_meta = reinterpret_cast<ObRedisListMeta *>(metas.at(same_key_start_pos));
      if (!list_meta->is_exists()) {
        if (OB_FAIL(ops_return_nullstr(ops, same_key_start_pos, same_key_end_pos))) {
          LOG_WARN("fail to ops return nullstr", K(ret), K(same_key_start_pos), K(same_key_end_pos));
        }
      } else if (OB_FAIL(do_same_keys_multi_pop(
                     batch_res,
                     batch_op_insup,
                     batch_op_del,
                     tablet_insup_ids,
                     tablet_del_ids,
                     metas,
                     last_key,
                     same_key_start_pos,
                     same_key_end_pos))) {
        LOG_WARN("fail to do same keys multi pop", K(ret), K(same_key_start_pos), K(same_key_end_pos));
      }
      same_key_start_pos = pos;
    }
  }
  return ret;
}

int ListCommandOperator::do_group_pop_use_query(
    const ObArray<ObRedisMeta *> &metas,
    const ObTableBatchOperation &batch_op_get,
    ObIArray<ObTabletID> &tablet_get_ids,
    ObTableBatchOperation &batch_op_insup,
    ObIArray<ObTabletID> &tablet_insup_ids,
    ObTableBatchOperation &batch_op_del,
    ObIArray<ObTabletID> &tablet_del_ids)
{
  int ret = OB_SUCCESS;
  ObIArray<ObITableOp *> &ops = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_).ops();
  ObString last_key;
  int64_t same_key_start_pos = 0, same_key_end_pos = 0;
  int64_t pos = 0;
  for (; OB_SUCC(ret) && pos < ops.count(); ++pos) {
    ObRedisOp *redis_op = reinterpret_cast<ObRedisOp *>(ops.at(pos));
    ObString key;
    if (OB_FAIL(redis_op->get_key(key))) {
      LOG_WARN("fail to get key", K(ret), KPC(redis_op));
    } else if (!last_key.empty() && last_key != key) {
      same_key_end_pos = pos - 1;
      // do (same_key_start_pos, same_key_end_pos)
      if (OB_FAIL(do_same_keys_pop(
              batch_op_insup,
              batch_op_del,
              tablet_insup_ids,
              tablet_del_ids,
              metas,
              last_key,
              same_key_start_pos,
              same_key_end_pos))) {
        LOG_WARN(
            "fail to do same keys pop",
            K(ret),
            K(batch_op_insup),
            K(batch_op_del),
            K(tablet_insup_ids),
            K(tablet_del_ids));
      } else {
        same_key_start_pos = pos;
      }
    }
    last_key = key;
  }
  // do (same_key_start_pos, end_pos - 1)
  if (OB_SUCC(ret)) {
    same_key_end_pos = pos - 1;
    if (OB_FAIL(do_same_keys_pop(
            batch_op_insup,
            batch_op_del,
            tablet_insup_ids,
            tablet_del_ids,
            metas,
            last_key,
            same_key_start_pos,
            same_key_end_pos))) {
      LOG_WARN(
          "fail to do same keys pop",
          K(ret),
          K(batch_op_insup),
          K(batch_op_del),
          K(tablet_insup_ids),
          K(tablet_del_ids));
    }
  }
  return ret;
}

int ListCommandOperator::do_group_pop()
{
  int ret = OB_SUCCESS;

  // Sort ops by key to find the same key
  sort_group_ops();
  ObIArray<ObITableOp *> &ops = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_).ops();
  // Get all op's meta
  ObArray<ObRedisMeta *> metas(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(op_temp_allocator_, "RedisGPop"));
  if (OB_FAIL(get_group_metas(op_temp_allocator_, ObRedisModel::LIST, metas))) {
    LOG_WARN("fail to get group metas", K(ret));
  } else {
    ObTableBatchOperation batch_op_get;
    ObSEArray<ObTabletID, 16> tablet_get_ids;
    ObTableBatchOperation batch_op_insup;
    ObTableBatchOperation batch_op_del;
    ObSEArray<ObTabletID, 16> tablet_insup_ids;
    ObSEArray<ObTabletID, 16> tablet_del_ids;
    bool can_use_multi_get = true;
    if (OB_FAIL(check_can_use_multi_get(metas, ops, batch_op_get, tablet_get_ids, can_use_multi_get))) {
      LOG_WARN("fail to check can use multi", K(ret));
    } else if (can_use_multi_get) {
      // ret = do_group_pop_get();
      if (OB_FAIL(do_group_pop_use_multi_get(
              metas, batch_op_get, tablet_get_ids, batch_op_insup, tablet_insup_ids, batch_op_del, tablet_del_ids))) {
        LOG_WARN("fail to do group pop use multi get", K(ret));
      }
    } else {
      // do_group_pop_query()
      if (OB_FAIL(do_group_pop_use_query(
              metas, batch_op_get, tablet_get_ids, batch_op_insup, tablet_insup_ids, batch_op_del, tablet_del_ids))) {
        LOG_WARN("fail to do group pop use query", K(ret));
      }
    }

    ResultFixedArray results_insup(op_temp_allocator_);
    ResultFixedArray results_del(op_temp_allocator_);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(process_table_batch_op(
                   batch_op_del,
                   results_del,
                   nullptr /*meta*/,
                   RedisOpFlags::DEL_SKIP_SCAN,
                   nullptr /*allocator*/,
                   nullptr /*entity_factory*/,
                   &tablet_del_ids))) {
      LOG_WARN("fail to process table batch op", K(ret), K(batch_op_del));
    } else if (OB_FAIL(process_table_batch_op(
                   batch_op_insup, results_insup, nullptr, RedisOpFlags::NONE, nullptr, nullptr, &tablet_insup_ids))) {
      LOG_WARN("fail to process table batch op", K(ret), K(batch_op_insup));
    }
  }

  return ret;
}
