
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
#include "lib/utility/ob_fast_convert.h"
#include "src/share/table/ob_redis_parser.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;

int ListMetaData::decode_meta_value(const ObObj &meta_value_obj)
{
  int ret = OB_SUCCESS;
  ObString meta_value;
  if (OB_FAIL(meta_value_obj.get_varbinary(meta_value))) {
    LOG_WARN("fail to get meta from obj", K(ret), K(meta_value_obj));
  } else {
    // meta_value: count:left_idx:right_idx
    ObString left_idx_str;
    ObString right_idx_str;
    ObString count_str = meta_value.split_on(META_SPLIT_FLAG);
    if (count_str.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid meta value", K(ret), K(meta_value));
    } else if (OB_FALSE_IT(left_idx_str = meta_value.split_on(META_SPLIT_FLAG))) {
    } else if (left_idx_str.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid meta value", K(ret), K(meta_value));
    } else if (OB_FALSE_IT(right_idx_str = meta_value)) {
    } else if (right_idx_str.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid meta value", K(ret), K(meta_value));
    } else {
      bool is_valid;
      count_ = ObFastAtoi<int64_t>::atoi(count_str.ptr(), count_str.ptr() + count_str.length(), is_valid);
      if (!is_valid) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid meta value", K(ret), K(meta_value));
      } else if (OB_FALSE_IT(
                     left_idx_ = ObFastAtoi<int64_t>::atoi(
                         left_idx_str.ptr(), left_idx_str.ptr() + left_idx_str.length(), is_valid))) {
      } else if (!is_valid) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid meta value", K(ret), K(meta_value));
      } else if (OB_FALSE_IT(
                     right_idx_ = ObFastAtoi<int64_t>::atoi(
                         right_idx_str.ptr(), right_idx_str.ptr() + right_idx_str.length(), is_valid))) {
      } else if (!is_valid) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid meta value", K(ret), K(meta_value));
      }
    }
  }

  return ret;
}

int ListMetaData::encode_meta_value(ObIAllocator &allocator, ObObj &meta_value_obj)
{
  int ret = OB_SUCCESS;
  ObFastFormatInt count_i(count_);
  ObString count_str(count_i.length(), count_i.ptr());

  ObFastFormatInt left_idx_i(left_idx_);
  ObString left_idx_str(left_idx_i.length(), left_idx_i.ptr());

  ObFastFormatInt right_idx_i(right_idx_);
  ObString right_idx_str(right_idx_i.length(), right_idx_i.ptr());
  const char flag = META_SPLIT_FLAG;

  ObString meta_value;
  ObStringBuffer buffer(&allocator);
  if (OB_FAIL(buffer.reserve(2 + count_i.length() + left_idx_i.length() + right_idx_i.length()))) {
    LOG_WARN("fail to reserve memory for string buffer", K(ret), KP(this));
  } else if (OB_FAIL(buffer.append(count_str))) {
    LOG_WARN("fail to append count_str", K(ret), K(count_str));
  } else if (OB_FAIL(buffer.append(&flag, ObRedisUtil::FLAG_LEN))) {
    LOG_WARN("fail to append flag", K(ret), K(flag));
  } else if (OB_FAIL(buffer.append(left_idx_str))) {
    LOG_WARN("fail to append left_idx_str", K(ret), K(left_idx_str));
  } else if (OB_FAIL(buffer.append(&flag, ObRedisUtil::FLAG_LEN))) {
    LOG_WARN("fail to append flag", K(ret), K(flag));
  } else if (OB_FAIL(buffer.append(right_idx_str))) {
    LOG_WARN("fail to append right_idx_str", K(ret), K(right_idx_str));
  } else if (OB_FAIL(buffer.get_result_string(meta_value))) {
    LOG_WARN("fail to get result string", K(ret), KP(this));
  } else {
    meta_value_obj.set_varbinary(meta_value);
  }

  return ret;
}

int ListMetaData::get_meta_from_entity(const ObITableEntity &meta_entity)
{
  int ret = OB_SUCCESS;
  ObObj meta_value_obj;
  ObObj ttl_obj;
  if (OB_FAIL(meta_entity.get_property(ObRedisUtil::REDIS_VALUE_NAME, meta_value_obj))) {
    LOG_WARN("fail to get meta value", K(ret), K(meta_entity));
  } else if (OB_FAIL(decode_meta_value(meta_value_obj))) {
    LOG_WARN("fail to decode meta value", K(ret), K(meta_value_obj));
  } else if (OB_FAIL(meta_entity.get_property(ObRedisUtil::REDIS_EXPIRE_NAME, ttl_obj))) {
    LOG_WARN("fail to get expire ts", K(ret), K(meta_entity));
  } else if (OB_FAIL(ttl_obj.get_timestamp(ttl_))) {
    LOG_WARN("fail to get ttl from obj", K(ret), K(ttl_obj));
  }

  return ret;
}

int ListMetaData::put_meta_into_entity(ObIAllocator &allocator, ObITableEntity &meta_entity)
{
  int ret = OB_SUCCESS;
  ObObj meta_value_obj;
  ObObj ttl_obj;
  ObObj insert_ts_obj;
  insert_ts_obj.set_timestamp(ObTimeUtility::current_time());
  if (OB_FAIL(encode_meta_value(allocator, meta_value_obj))) {
    LOG_WARN("fail to encode meta value", K(ret));
  } else if (OB_FAIL(meta_entity.set_property(ObRedisUtil::REDIS_VALUE_NAME, meta_value_obj))) {
    LOG_WARN("fail to set meta value", K(ret), K(meta_value_obj));
  } else if (OB_FALSE_IT(get_ttl_obj(ttl_obj))) {
  } else if (OB_FAIL(meta_entity.set_property(ObRedisUtil::REDIS_EXPIRE_NAME, ttl_obj))) {
    LOG_WARN("fail to convert datetime to timestamp", K(ret), K(ttl_));
  } else if (OB_FAIL(meta_entity.set_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, insert_ts_obj))) {
    LOG_WARN("fail to set value property", K(ret), K(insert_ts_obj));
  }

  return ret;
}

bool ListMetaData::is_expired()
{
  return ttl_ > 0 && ttl_ < ObTimeUtility::current_time();
}

int ListCommandOperator::get_list_meta(ListMetaData &list_meta)
{
  int ret = OB_SUCCESS;

  ObITableEntity *get_meta_entity = nullptr;
  ObITableEntity *res_meta_entity = nullptr;
  ObObj null_obj;
  if (OB_FAIL(gen_entity_with_rowkey(ListMetaData::META_INDEX, false/*is_data*/, get_meta_entity))) {
    LOG_WARN("fail to gen entity with rowkey", K(ret));
  } else if (OB_FAIL(get_meta_entity->set_property(ObRedisUtil::REDIS_VALUE_NAME, null_obj))) {
    LOG_WARN("fail to set property", K(ret), K(null_obj));
  } else if (OB_FAIL(get_meta_entity->set_property(ObRedisUtil::REDIS_EXPIRE_NAME, null_obj))) {
    LOG_WARN("fail to set property", K(ret), K(null_obj));
  } else {
    ObTableOperation op = ObTableOperation::retrieve(*get_meta_entity);
    ObTableOperationResult result;

    if (OB_FAIL(process_table_single_op(op, result, true))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get list meta", K(ret), KP(get_meta_entity));
      }
    } else if (OB_FAIL(result.get_entity(res_meta_entity))) {
      LOG_WARN("fail to get entity", K(ret), K(result));
    } else if (OB_FAIL(list_meta.get_meta_from_entity(*res_meta_entity))) {
      LOG_WARN("fail get meta from entity", K(ret), KP(res_meta_entity));
    } else {
      // reuse entity
      redis_ctx_.entity_factory_->free(get_meta_entity);
      redis_ctx_.entity_factory_->free(res_meta_entity);
    }
  }

  return ret;
}

int ListCommandOperator::gen_entity_with_rowkey(int64_t idx, bool is_data, ObITableEntity *&entity)
{
  int ret = OB_SUCCESS;
  const ObITableEntity &req_entity = redis_ctx_.request_.get_request().table_operation_.entity();
  ObITableEntity *tmp_entity = nullptr;
  if (OB_ISNULL(redis_ctx_.entity_factory_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null entity_factory_", K(ret));
  } else if (OB_ISNULL(tmp_entity = redis_ctx_.entity_factory_->alloc())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("fail to allocate entity", K(ret));
  } else if (FALSE_IT(tmp_entity->set_allocator(&op_temp_allocator_))) {
  } else {
    ObObj idx_obj;
    idx_obj.set_int(idx);
    ObObj is_data_obj;
    is_data_obj.set_bool(is_data);
    if (OB_FAIL(tmp_entity->set_rowkey(req_entity.get_rowkey()))) {
      LOG_WARN("fail to set rowkey", K(ret), K(req_entity));
    } else if (OB_FAIL(tmp_entity->set_rowkey_value(ListMetaData::INDEX_SEQNUM_IN_ROWKEY, idx_obj))) {
      LOG_WARN("fail to set index seqnum in rowkey", K(ret), K(idx_obj));
    } else if (OB_FAIL(tmp_entity->set_rowkey_value(ObRedisUtil::ROWKEY_IS_DATA_IDX, is_data_obj))) {
      LOG_WARN("fail to set is_data in rowkey", K(ret), K(is_data_obj));
    } else {
      entity = tmp_entity;
    }
  }
  return ret;
}

bool ListCommandOperator::idx_overflowed(int64_t idx)
{
  return idx < INT64_MIN + ListMetaData::INDEX_STEP + 1 || idx > INT64_MAX - ListMetaData::INDEX_STEP;
}

void ListCommandOperator::gen_fmt_redis_err(const ObString &err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRedisParser::encode_error(redis_ctx_.allocator_, err_msg, fmt_redis_err_msg_))) {
    LOG_WARN("fail to encode error msg", K(ret), K(err_msg));
  } else {
    have_redis_err_ = true;
  }
}

int ListCommandOperator::put_data_to_table(ListMetaData &list_meta, const ObIArray<ObString> &values, bool push_left)
{
  int ret = OB_SUCCESS;

  ObTableBatchOperation push_value_ops;
  ResultFixedArray results(op_temp_allocator_);
  int64_t left_idx = list_meta.left_idx_;
  int64_t right_idx = list_meta.right_idx_;
  int64_t idx(0);
  // meta not exist, push into a new list
  if (list_meta.count_ == 0) {
    idx = ListMetaData::INIT_INDEX;
    left_idx = ListMetaData::INIT_INDEX;
    right_idx = ListMetaData::INIT_INDEX;
  } else {
    // move position first and then push the data
    if (push_left) {
      left_idx -= ListMetaData::INDEX_STEP;
      idx = left_idx;
    } else {
      right_idx += ListMetaData::INDEX_STEP;
      idx = right_idx;
    }
  }

  int64_t cur_ts = ObTimeUtility::current_time();
  ObObj insert_ts_obj;
  insert_ts_obj.set_timestamp(cur_ts);
  for (int64_t i = 0; OB_SUCC(ret) && i < values.count(); ++i) {
    ObITableEntity *value_entity = nullptr;
    ObObj value_obj;
    value_obj.set_varbinary(values.at(i));
    if (idx_overflowed(idx)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("index overflow", K(ret), K(idx), K(list_meta));
      gen_fmt_redis_err(ObString::make_string("index overflow"));
    } else if (OB_FAIL(gen_entity_with_rowkey(idx, true/*is_data*/, value_entity))) {
      LOG_WARN("fail to gen entity with rowkey", K(ret));
    } else if (OB_FAIL(value_entity->set_property(ObRedisUtil::REDIS_VALUE_NAME, value_obj))) {
      LOG_WARN("fail to set value property", K(ret), K(value_obj));
    } else if (OB_FAIL(value_entity->set_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, insert_ts_obj))) {
      LOG_WARN("fail to set value property", K(ret), K(insert_ts_obj));
    } else if (OB_FAIL(push_value_ops.insert_or_update(*value_entity))) {
      LOG_WARN("fail to push back", K(ret));
    } else {
      if (push_left) {
        left_idx -= ListMetaData::INDEX_STEP;
        idx = left_idx;
      } else {
        right_idx += ListMetaData::INDEX_STEP;
        idx = right_idx;
      }
    }
  }
  // generate meta
  if (OB_SUCC(ret)) {
    list_meta.count_ += values.count();
    list_meta.left_idx_ = push_left ? left_idx + ListMetaData::INDEX_STEP : left_idx;
    list_meta.right_idx_ = push_left ? right_idx : right_idx - ListMetaData::INDEX_STEP;

    ObITableEntity *meta_entity = nullptr;
    if (OB_FAIL(gen_entity_with_rowkey(ListMetaData::META_INDEX, false/*is_data*/, meta_entity))) {
      LOG_WARN("fail to gen entity with rowkey", K(ret));
    } else if (OB_FAIL(list_meta.put_meta_into_entity(op_temp_allocator_, *meta_entity))) {
      LOG_WARN("fail to encode meta value", K(ret), K(list_meta));
    } else if (OB_FAIL(push_value_ops.insert_or_update(*meta_entity))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(process_table_batch_op(push_value_ops, results, false, false))) {
      LOG_WARN("fail to process table batch op", K(ret), K(push_value_ops));
    }
  }
  return ret;
}

int ListCommandOperator::delete_list_by_key(const ObString &key)
{
  // todo(mjaywu): Delete the list
  int ret = OB_SUCCESS;

  return ret;
}

int ListCommandOperator::do_push(const ObString &key, const ObIArray<ObString> &values, bool push_left, bool need_exit)
{
  int ret = OB_SUCCESS;
  // 1. Get meta data and check whether it has expired.
  ListMetaData list_meta;
  if (OB_FAIL(do_list_expire_if_needed(key, list_meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get list meta", K(ret));
    } else {
      if (!need_exit) {
        ret = OB_SUCCESS;
        have_redis_err_ = false;
      } else {
        have_redis_err_ = true;
        fmt_redis_err_msg_ = ObString::make_string(ObRedisUtil::FMT_ZERO);
      }
    }
  }

  // 2. push data into a new list
  if (OB_SUCC(ret) && OB_FAIL(put_data_to_table(list_meta, values, push_left))) {
    LOG_WARN("fail to insert_or_update data to table", K(ret), K(values), K(push_left));
  }

  // 3. gen response
  if (OB_SUCC(ret)) {
    if (OB_FAIL(redis_ctx_.response_.set_res_int(list_meta.count_))) {
      LOG_WARN("fail to set result int", K(ret), K(list_meta));
    }
  } else if (have_redis_err_) {
    ret = OB_SUCCESS;
    if (OB_FAIL(redis_ctx_.response_.set_fmt_res(fmt_redis_err_msg_))) {
      LOG_WARN("fail to set fmt res", K(ret), K(fmt_redis_err_msg_));
    }
  } else {
    redis_ctx_.response_.return_table_error(ret);
  }

  return ret;
}

int ListCommandOperator::pop_single_count_list(ListMetaData &list_meta, const bool pop_left, ObString &res_value)
{
  int ret = OB_SUCCESS;
  // Get data directly
  ObITableEntity *data_req_entity = nullptr;
  ObITableEntity *data_res_entity = nullptr;
  ObObj null_obj;
  if (list_meta.left_idx_ != list_meta.right_idx_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("list count is 1 but left_idx != right_idx", K(list_meta));
  } else if (OB_FAIL(gen_entity_with_rowkey(list_meta.left_idx_, true/*is_data*/, data_req_entity))) {
    LOG_WARN("fail to gen entity with rowkey", K(ret));
  } else if (OB_FAIL(data_req_entity->set_property(ObRedisUtil::REDIS_VALUE_NAME, null_obj))) {
    LOG_WARN("fail to set value property", K(ret), K(null_obj));
  } else {
    ObTableOperation op = ObTableOperation::retrieve(*data_req_entity);
    ObTableOperationResult result;
    if (OB_FAIL(process_table_single_op(op, result, true))) {
      LOG_WARN("fail to get list meta", K(ret), KP(data_req_entity));
    } else if (OB_FAIL(result.get_entity(data_res_entity))) {
      LOG_WARN("fail to get entity", K(ret), K(result));
    } else if (OB_FALSE_IT(redis_ctx_.entity_factory_->free(data_req_entity))) {
    } else {
      ObObj res_value_obj;
      if (OB_FAIL(data_res_entity->get_property(ObRedisUtil::REDIS_VALUE_NAME, res_value_obj))) {
        LOG_WARN("fail to get data value", K(ret), K(res_value_obj));
      } else if (OB_FAIL(res_value_obj.get_varbinary(res_value))) {
        LOG_WARN("fail to get res_value from res_value_obj", K(ret), K(res_value_obj));
      } else {
        ListElement old_borded = std::make_pair(list_meta.left_idx_, res_value_obj);
        ListElement new_borded;
        if (OB_FAIL(update_list_after_pop(old_borded, new_borded, pop_left, list_meta))) {
          LOG_WARN("fail to update list after pop", K(ret), K(res_value_obj), K(list_meta));
        }
      }
    }
  }

  return ret;
}

int ListCommandOperator::update_list_after_pop(
    const ListElement &old_borded,
    const ListElement &new_borded,
    const bool pop_left,
    ListMetaData &list_meta)
{
  int ret = OB_SUCCESS;

  ObTableBatchOperation batch_ops;
  ResultFixedArray results(op_temp_allocator_);
  ObITableEntity *del_data_entity = nullptr;
  // delete old borderd
  if (OB_FAIL(gen_entity_with_rowkey(old_borded.first, true/*is_data*/, del_data_entity))) {
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
      ObITableEntity *new_meta_entity = nullptr;
      if (OB_FAIL(gen_entity_with_rowkey(ListMetaData::META_INDEX, false/*is_data*/, new_meta_entity))) {
        LOG_WARN("fail to gen entity with rowkey", K(ret));
      } else if (OB_FAIL(list_meta.put_meta_into_entity(op_temp_allocator_, *new_meta_entity))) {
        LOG_WARN("fail to encode meta value", K(ret), K(list_meta));
      } else if (OB_FAIL(batch_ops.insert_or_update(*new_meta_entity))) {
        LOG_WARN("fail to push back", K(ret));
      }
    } else {
      // delete meta
      ObITableEntity *new_meta_entity = nullptr;
      if (OB_FAIL(gen_entity_with_rowkey(ListMetaData::META_INDEX, false/*is_data*/, new_meta_entity))) {
        LOG_WARN("fail to gen entity with rowkey", K(ret));
      } else if (OB_FAIL(batch_ops.del(*new_meta_entity))) {
        LOG_WARN("fail to push back data_req_entity", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(process_table_batch_op(batch_ops, results, false, false))) {
    LOG_WARN("fail to process table batch op", K(ret), K(batch_ops));
  }

  return ret;
}

int ListCommandOperator::get_res_value_by_deep(const ObObj &src_obj, ObString &res_value)
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
    const ObString &key,
    const bool pop_left,
    ListMetaData &list_meta,
    ObString &res_value)
{
  int ret = OB_SUCCESS;

  // query leftest or rightest idx, after pop (idx range: [left_idx,right_idx] limit 2)
  ObTableQuery query;
  ObSEArray<ListElement, 2> borderd_elements;
  int64_t db = redis_ctx_.get_request_db();
  ListQueryCond query_cond(
      key, db, list_meta.left_idx_, list_meta.right_idx_, 2, 0, pop_left, true /*is_query_value*/);
  if (OB_FAIL(build_list_query(query_cond, query))) {
    LOG_WARN("fail to build list query", K(ret), K(query_cond));
  } else {
    SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
    {
      QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter)
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
          } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::REDIS_VALUE_NAME, value_obj))) {
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
          LOG_WARN("idx array not match", K(ret), K(borderd_elements), K(list_meta));
        } else if (OB_FAIL(update_list_after_pop(borderd_elements[0], borderd_elements[1], pop_left, list_meta))) {
          LOG_WARN("fail to update list after pop", K(ret), K(borderd_elements), K(list_meta));
        } else if (OB_FAIL(get_res_value_by_deep(borderd_elements[0].second, res_value))) {
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

int ListCommandOperator::do_pop(const ObString &key, bool pop_left)
{
  int ret = OB_SUCCESS;

  ObString res_value;
  ListMetaData list_meta;
  if (OB_FAIL(do_list_expire_if_needed(key, list_meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do list expire if needed", K(ret), K(key));
    }
  } else if (list_meta.count_ == 1) {
    if (OB_FAIL(pop_single_count_list(list_meta, pop_left, res_value))) {
      LOG_WARN("fail to pop one count list", K(ret), K(list_meta));
    }
  } else {
    if (OB_FAIL(pop_multi_count_list(key, pop_left, list_meta, res_value))) {
      LOG_WARN("fail to pop multi count list", K(ret), K(list_meta));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(redis_ctx_.response_.set_res_bulk_string(res_value))) {
      LOG_WARN("fail to set result int", K(ret), K(list_meta));
    }
  } else if (have_redis_err_) {
    ret = OB_SUCCESS;
    if (OB_FAIL(redis_ctx_.response_.set_fmt_res(fmt_redis_err_msg_))) {
      LOG_WARN("fail to set fmt res", K(ret), K(fmt_redis_err_msg_));
    }
  } else {
    redis_ctx_.response_.return_table_error(ret);
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
  int ret = OB_SUCCESS;
  ObRowkey start_key;
  ObRowkey end_key;
  ObNewRange *range = nullptr;
  if (OB_FAIL(build_rowkey(db, key, true/*is data*/, less_index, true/*is_min*/, start_key))) {
    LOG_WARN("fail to build start key", K(ret), K(db), K(key));
  } else if (OB_FAIL(build_rowkey(db, key, true/*is data*/, max_index, false/*is_min*/, end_key))) {
    LOG_WARN("fail to build start key", K(ret), K(db), K(key));
  } else if (OB_FAIL(build_range(start_key, end_key, range))) {
    LOG_WARN("fail to build range", K(ret), K(start_key), K(end_key));
  } else if (OB_FAIL(query.add_scan_range(*range))) {
    LOG_WARN("fail to add scan range", K(ret));
  }

  return ret;
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
          obj_ptr = static_cast<ObObj *>(op_temp_allocator_.alloc(sizeof(ObObj) * ObRedisUtil::COMPLEX_ROWKEY_NUM)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("ArgNodeContent cast to string invalid value", K(ret));
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

    rowkey.assign(obj_ptr, ObRedisUtil::COMPLEX_ROWKEY_NUM);
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
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("index is out of range", K(ret), K(offset), K(count));
    have_redis_err_ = true;
    fmt_redis_err_msg_ = ObString::make_string(ObRedisUtil::NULL_BULK_STRING);
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

int ListCommandOperator::do_list_expire_if_needed(const ObString &key, ListMetaData &list_meta)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_list_meta(list_meta)) && ret != OB_ITER_END) {
    LOG_WARN("fail to get list meta", K(ret));
  } else if (ret == OB_ITER_END) {
    have_redis_err_ = true;
    fmt_redis_err_msg_ = ObString::make_string(ObRedisUtil::NULL_BULK_STRING);
  } else if (list_meta.is_expired()) {
    LOG_INFO("list meta is expired", K(ret), K(list_meta));
    if (OB_FAIL(delete_list_by_key(key))) {
      LOG_WARN("fail to delete list", K(ret), K(key));
    } else {
      ret = OB_ITER_END;
      have_redis_err_ = true;
      fmt_redis_err_msg_ = ObString::make_string(ObRedisUtil::NULL_BULK_STRING);
    }
  }

  return ret;
}

int ListCommandOperator::build_index_query(
    const ObString &key,
    const int64_t offset,
    bool is_query_value,
    const ListMetaData &list_meta,
    ObTableQuery &query)
{
  int ret = OB_SUCCESS;

  bool is_query_forward = true;
  int64_t query_offset = offset;
  int64_t db = redis_ctx_.get_request_db();
  if (OB_FAIL(get_query_forward_offset(list_meta.count_, offset, is_query_forward, query_offset))) {
    LOG_WARN("fail to get query forward offset", K(ret), K(offset), K(list_meta));
  } else {
    ListQueryCond query_cond(
        key, db, list_meta.left_idx_, list_meta.right_idx_, 1, query_offset, is_query_forward, is_query_value);
    if (OB_FAIL(build_list_query(query_cond, query))) {
      LOG_WARN("fail to build list query", K(ret), K(query_cond));
    }
  }

  return ret;
}

int ListCommandOperator::query_index_result(const ObTableQuery &query, ObString &res_value)
{
  int ret = OB_SUCCESS;
  // query element
  SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
  {
    QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter)
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
        } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::REDIS_VALUE_NAME, value_obj))) {
          LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
        }
      }
    }
    if (OB_SUCC(ret) || OB_ITER_END == ret) {
      if (value_obj.is_null()) {
        have_redis_err_ = true;
        fmt_redis_err_msg_ = ObString::make_string(ObRedisUtil::NULL_BULK_STRING);
      } else if (OB_FAIL(get_res_value_by_deep(value_obj, res_value))) {
        // note: deep copy is required, because after the iterator destruct, the memory of the query results will
        // be released
        LOG_WARN("fail to get res value by deep", K(ret), K(value_obj));
      }
    }
    QUERY_ITER_END(iter)
  }
  return ret;
}

int ListCommandOperator::do_index(const ObString &key, int64_t offset)
{
  int ret = OB_SUCCESS;

  ObString res_value;
  // 1. Get meta data and check whether it has expired.
  ListMetaData list_meta;
  ObTableQuery query;
  if (OB_FAIL(do_list_expire_if_needed(key, list_meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do list expire if needed", K(ret), K(key));
    }
  } else if (OB_FAIL(build_index_query(key, offset, true, list_meta, query))) {
    LOG_WARN("fail to build index query", K(ret), K(key), K(offset), K(list_meta));
  } else if (OB_FAIL(query_index_result(query, res_value))) {
    LOG_WARN("fail to query index result", K(ret), K(query));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(redis_ctx_.response_.set_res_bulk_string(res_value))) {
      LOG_WARN("fail to set result int", K(ret), K(list_meta));
    }
  } else if (have_redis_err_) {
    ret = OB_SUCCESS;
    if (OB_FAIL(redis_ctx_.response_.set_fmt_res(fmt_redis_err_msg_))) {
      LOG_WARN("fail to set fmt res", K(ret), K(fmt_redis_err_msg_));
    }
  } else {
    redis_ctx_.response_.return_table_error(ret);
  }

  return ret;
}

int ListCommandOperator::update_element(int64_t index, const ObString &value)
{
  int ret = OB_SUCCESS;

  ObITableEntity *req_entity;
  ObObj value_obj;
  value_obj.set_varbinary(value);
  if (OB_FAIL(gen_entity_with_rowkey(index, true/*is_data*/, req_entity))) {
    LOG_WARN("fail to gen entity with rowkey", K(ret));
  } else if (OB_FAIL(req_entity->set_property(ObRedisUtil::REDIS_VALUE_NAME, value_obj))) {
    LOG_WARN("fail to set property", K(ret), K(ObRedisUtil::REDIS_VALUE_NAME), K(value_obj));
  } else {
    ObTableOperation op = ObTableOperation::insert_or_update(*req_entity);
    ObTableOperationResult result;
    if (OB_FAIL(process_table_single_op(op, result))) {
      LOG_WARN("fail to process table op", K(ret), K(op));
    }
  }

  return ret;
}

int ListCommandOperator::query_set_index(const ObTableQuery &query, int64_t &res_idx)
{
  int ret = OB_SUCCESS;
  // query element
  SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
  {
    QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter)
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
        ret = OB_ENTRY_NOT_EXIST;
        gen_fmt_redis_err("no such key");
      } else if (OB_FAIL(index_obj.get_int(res_idx))) {
        LOG_WARN("fail to get int", K(ret), K(index_obj));
      }
    }
    QUERY_ITER_END(iter)
  }
  return ret;
}

int ListCommandOperator::do_set(const ObString &key, int64_t offset, const ObString &value)
{
  int ret = OB_SUCCESS;

  // 1. Get meta data and check whether it has expired.
  ListMetaData list_meta;
  ObTableQuery query;
  int64_t set_index = 0;
  if (OB_FAIL(do_list_expire_if_needed(key, list_meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do list expire if needed", K(ret), K(key));
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      gen_fmt_redis_err("no such key");
    }
  } else if (OB_FAIL(build_index_query(key, offset, false, list_meta, query))) {
    LOG_WARN("fail to build index query", K(ret), K(key), K(offset), K(list_meta));
  } else if (OB_FAIL(query_set_index(query, set_index))) {
    LOG_WARN("fail to query set index", K(ret), K(query));
  } else if (OB_FAIL(update_element(set_index, value))) {
    LOG_WARN("fail to update element", K(ret), K(set_index), K(value));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(redis_ctx_.response_.set_fmt_res(ObRedisUtil::FMT_OK))) {
      LOG_WARN("fail to set fmt res", K(ret));
    }
  } else if (have_redis_err_) {
    ret = OB_SUCCESS;
    if (OB_FAIL(redis_ctx_.response_.set_fmt_res(fmt_redis_err_msg_))) {
      LOG_WARN("fail to set fmt res", K(ret), K(fmt_redis_err_msg_));
    }
  } else {
    redis_ctx_.response_.return_table_error(ret);
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
  if (end >= count) {
    end = count - 1;
  }
  if (start > end || start >= count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("index is out of range", K(ret), K(start), K(end));
    have_redis_err_ = true;
    fmt_redis_err_msg_ = ObString::make_string(ObRedisUtil::FMT_EMPTY_ARRAY);
  }

  return ret;
}

int ListCommandOperator::build_range_query(
    const ObString &key,
    const ListMetaData &list_meta,
    int64_t start,
    int64_t end,
    ObTableQuery &query)
{
  int ret = OB_SUCCESS;

  int64_t db = redis_ctx_.get_request_db();
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
        true /*is_forward_order*/,
        true /*is_query_value*/);
    if (OB_FAIL(build_list_query(query_cond, query))) {
      LOG_WARN("fail to build list query", K(ret), K(query_cond));
    }
  }

  return ret;
}

int ListCommandOperator::query_range_result(const ObTableQuery &query, ObIArray<ObString> &res_values)
{
  int ret = OB_SUCCESS;
  // query element
  SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
  {
    QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter)
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
        } else if (OB_FAIL(result_entity->get_property(ObRedisUtil::REDIS_VALUE_NAME, value_obj))) {
          LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
        } else if (OB_FAIL(get_res_value_by_deep(value_obj, value_str))) {
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

int ListCommandOperator::do_range(const ObString &key, int64_t start, int64_t end)
{
  int ret = OB_SUCCESS;

  ObArray<ObString> res_values;
  // 1. Get meta data and check whether it has expired.
  ListMetaData list_meta;
  ObTableQuery query;
  if (OB_FAIL(do_list_expire_if_needed(key, list_meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do list expire if needed", K(ret), K(key));
    } else {
      have_redis_err_ = true;
      fmt_redis_err_msg_ = ObString::make_string(ObRedisUtil::FMT_EMPTY_ARRAY);
    }
  } else if (OB_FAIL(build_range_query(key, list_meta, start, end, query))) {
    LOG_WARN("fail to build range query", K(ret), K(key), K(start), K(end), K(list_meta));
  } else if (OB_FAIL(res_values.reserve(query.get_limit()))) {
    LOG_WARN("fail to reserve", K(ret), K(query.get_limit()));
  } else if (OB_FAIL(query_range_result(query, res_values))) {
    LOG_WARN("fail to query range result", K(ret), K(query));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(redis_ctx_.response_.set_res_array(res_values))) {
      LOG_WARN("fail to set result array", K(ret), K(list_meta));
    }
  } else if (have_redis_err_) {
    ret = OB_SUCCESS;
    if (OB_FAIL(redis_ctx_.response_.set_fmt_res(fmt_redis_err_msg_))) {
      LOG_WARN("fail to set fmt res", K(ret), K(fmt_redis_err_msg_));
    }
  } else {
    redis_ctx_.response_.return_table_error(ret);
  }

  return ret;
}

int ListCommandOperator::build_list_query(const ListQueryCond &query_cond, ObTableQuery &query)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(build_scan_range(
          query_cond.db_, query_cond.key_, true/*is data*/, &query_cond.left_border_, &query_cond.right_border_, query))) {
    LOG_WARN("fail to build member scan range", K(ret), K(query_cond));
  } else if (OB_FAIL(query.add_select_column(ObRedisUtil::REDIS_INDEX_NAME))) {
    LOG_WARN("fail to add select column", K(ret), K(query_cond));
  } else if (query_cond.need_query_value_ && OB_FAIL(query.add_select_column(ObRedisUtil::REDIS_VALUE_NAME))) {
    LOG_WARN("fail to add select column", K(ret), K(query_cond));
  } else if (OB_FAIL(query.set_scan_order(
                 query_cond.is_query_forward_ ? ObQueryFlag::ScanOrder::Forward : ObQueryFlag::ScanOrder::Reverse))) {
    LOG_WARN("fail to set scan order", K(ret), K(query_cond));
  } else if (OB_FAIL(query.set_limit(query_cond.limit_))) {
    LOG_WARN("fail to set limit", K(ret), K(query_cond));
  } else if (OB_FAIL(query.set_offset(query_cond.offset_))) {
    LOG_WARN("fail to set offset", K(ret), K(query_cond));
  }

  return ret;
}

int ListCommandOperator::adjust_trim_param(const int64_t count, int64_t &start, int64_t &end)
{
  int ret = OB_SUCCESS;
  // convert start and end to positive
  start = start < 0 ? start + count : start;
  end = end < 0 ? end + count : end;

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
  return ret;
}

int ListCommandOperator::build_trim_querys(
    const ObString &key,
    const ListMetaData &list_meta,
    int64_t &start,
    int64_t &end,
    ObIArray<ObTableQuery> &querys)
{
  int ret = OB_SUCCESS;

  int64_t db = redis_ctx_.get_request_db();
  if (OB_FAIL(adjust_trim_param(list_meta.count_, start, end))) {
    LOG_WARN("fail to adjust trim param", K(ret), K(start), K(end), K(list_meta));
  } else {
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
          true /*is_forward_order*/,
          false /*is_query_value*/);
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
          false /*is_forward_order*/,
          false /*is_query_value*/);
      if (OB_FAIL(build_list_query(query_cond, right_region_query))) {
        LOG_WARN("fail to build list query cond", K(ret), K(query_cond));
      } else if (OB_FAIL(querys.push_back(right_region_query))) {
        LOG_WARN("fail to push back", K(ret), K(right_region_query));
      }
    }
  }

  return ret;
}

int ListCommandOperator::update_meta_after_trim(
    ListMetaData &list_meta,
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

  ObITableEntity *new_meta_entity = nullptr;
  // update meta
  if (OB_FAIL(gen_entity_with_rowkey(ListMetaData::META_INDEX, false/*is_data*/, new_meta_entity))) {
    LOG_WARN("fail to gen entity with rowkey", K(ret));
  } else if (OB_FAIL(list_meta.put_meta_into_entity(op_temp_allocator_, *new_meta_entity))) {
    LOG_WARN("fail to encode meta value", K(ret), K(list_meta));
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
    ObIArray<int64_t> &new_border_idxs)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < querys.count(); ++i) {
    const ObTableQuery &query = querys.at(i);
    int64_t result_count = 0;
    // query element
    SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
    {
      QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter)
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
          } else if (++result_count == query.get_limit()) {
            // the last element of the query is the new boundary
            if (OB_FAIL(new_border_idxs.push_back(index))) {
              LOG_WARN("fail to push back", K(ret), K(index));
            }
          } else {
            ObITableEntity *del_data_entity = nullptr;
            if (OB_FAIL(gen_entity_with_rowkey(index, true/*is_data*/, del_data_entity))) {
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

int ListCommandOperator::trim_list(const ObString &key, ListMetaData &list_meta, int64_t start, int64_t end)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObTableQuery, 2> querys;
  if (OB_FAIL(build_trim_querys(key, list_meta, start, end, querys))) {
    LOG_WARN("fail to build range query", K(ret), K(key), K(start), K(end), K(list_meta));
  } else {
    ObTableBatchOperation del_ops;
    ObSEArray<int64_t, 2> new_border_idxs;
    if (OB_FAIL(build_trim_ops(querys, del_ops, new_border_idxs))) {
      LOG_WARN("fail to build trim ops", K(ret), K(querys));
    } else if (new_border_idxs.count() != querys.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new border idxs count is unexpected error", K(ret), K(new_border_idxs.count()), K(querys.count()));
    } else if (OB_FAIL(update_meta_after_trim(list_meta, new_border_idxs, start, end))) {
      LOG_WARN("fail to update meta after trim", K(ret), K(list_meta), K(new_border_idxs), K(start), K(end));
    } else {
      // del elements
      ResultFixedArray results(op_temp_allocator_);
      if (OB_FAIL(process_table_batch_op(del_ops, results, false, false))) {
        LOG_WARN("fail to process table batch op", K(ret), K(del_ops));
      }
    }
  }

  return ret;
}

int ListCommandOperator::do_trim(const ObString &key, int64_t start, int64_t end)
{
  int ret = OB_SUCCESS;

  // 1. Get meta data and check whether it has expired.
  ListMetaData list_meta;

  if (OB_FAIL(do_list_expire_if_needed(key, list_meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do list expire if needed", K(ret), K(key));
    } else {
      // Key does not exist return ok
      ret = OB_SUCCESS;
      have_redis_err_ = false;
    }
  } else if (OB_FAIL(trim_list(key, list_meta, start, end))) {
    LOG_WARN("fail to trim list", K(ret), K(key), K(start), K(end), K(list_meta));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(redis_ctx_.response_.set_fmt_res(ObRedisUtil::FMT_OK))) {
      LOG_WARN("fail to set fmt res", K(ret));
    }
  } else if (have_redis_err_) {
    ret = OB_SUCCESS;
    if (OB_FAIL(redis_ctx_.response_.set_fmt_res(fmt_redis_err_msg_))) {
      LOG_WARN("fail to set fmt res", K(ret), K(fmt_redis_err_msg_));
    }
  } else {
    redis_ctx_.response_.return_table_error(ret);
  }

  return ret;
}

int ListCommandOperator::get_pivot_index(
    const int64_t db,
    const ObString &key,
    const ObString &pivot,
    const ListMetaData &list_meta,
    int64_t &pivot_inx)
{
  int ret = OB_SUCCESS;
  ObTableQuery query;

  ListQueryCond query_cond(
      key, db, list_meta.left_idx_, list_meta.right_idx_, 1, 0, true /*is_forward_order*/, false /*is_query_value*/);
  if (OB_FAIL(build_list_query(query_cond, query))) {
    LOG_WARN("fail to build list query cond", K(ret), K(query_cond));
  } else {
    // add filter
    FilterStrBuffer buffer(op_temp_allocator_);
    ObString filter_str;
    if (OB_FAIL(buffer.add_value_compare(hfilter::CompareOperator::EQUAL, ObRedisUtil::REDIS_VALUE_NAME, pivot))) {
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
      QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter)
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
          } else if (OB_FAIL(index_obj.get_int(pivot_inx))) {
            LOG_WARN("fail to get index from obj", K(ret), K(index_obj));
          }
        }
      }
      if (OB_SUCC(ret) || OB_ITER_END == ret) {
        if (index_obj.is_null()) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("result is not exist", K(ret), K(index_obj));
          have_redis_err_ = true;
          fmt_redis_err_msg_ = ObString::make_string(ObRedisUtil::FMT_MINUS_ONE);
        }
      }
      QUERY_ITER_END(iter)
    }
  }

  return ret;
}

int ListCommandOperator::get_insert_index(
    const int64_t db,
    const ObString &key,
    const ListMetaData &list_meta,
    const int64_t pivot_inx,
    const bool is_before_pivot,
    int64_t &insert_idx)
{

  int ret = OB_SUCCESS;

  if (is_before_pivot && (pivot_inx == list_meta.left_idx_ || list_meta.count_ == 1)) {
    insert_idx = list_meta.left_idx_ - ListMetaData::INDEX_STEP;
  } else if (!is_before_pivot && (pivot_inx == list_meta.right_idx_ || list_meta.count_ == 1)) {
    insert_idx = list_meta.right_idx_ + ListMetaData::INDEX_STEP;
  } else {
    int64_t adjacent_idx = 0;
    if (get_adjacent_index(db, key, list_meta, pivot_inx, is_before_pivot, adjacent_idx)) {
      LOG_WARN("fail to get adjacent index", K(ret), K(pivot_inx), K(is_before_pivot));
    } else {
      if (std::abs(adjacent_idx - pivot_inx) > 1) {
        if (is_before_pivot) {
          insert_idx = adjacent_idx + ((pivot_inx - adjacent_idx) >> 1);
        } else {
          insert_idx = pivot_inx + ((adjacent_idx - pivot_inx) >> 1);
        }
      } else {
        // note: the position of the element needs to be readjusted, which is not supported for the time being.
        ret = OB_NOT_SUPPORTED;
        LOG_WARN(
            "not support adjusting the position of the element",
            K(ret),
            K(pivot_inx),
            K(adjacent_idx),
            K(is_before_pivot));
        gen_fmt_redis_err(ObString::make_string("index overflow"));
      }
    }
  }

  if (OB_SUCC(ret) && idx_overflowed(insert_idx)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("index overflow", K(ret), K(insert_idx), K(list_meta));
    gen_fmt_redis_err(ObString::make_string("index overflow"));
  }

  return ret;
}

int ListCommandOperator::get_adjacent_index(
    const int64_t db,
    const ObString &key,
    const ListMetaData &list_meta,
    const int64_t pivot_inx,
    const bool is_before_pivot,
    int64_t &adjacent_idx)
{
  int ret = OB_SUCCESS;
  ObTableQuery query;

  if (is_before_pivot) {
    // find the first one smaller than pivot_inx
    ListQueryCond query_cond(
        key, db, list_meta.left_idx_, pivot_inx, 1, 1, false /*is_forward_order*/, false /*is_query_value*/);
    if (OB_FAIL(build_list_query(query_cond, query))) {
      LOG_WARN("fail to build list query cond", K(ret), K(query_cond));
    }
  } else {
    // find the first one larger than pivot_inx
    ListQueryCond query_cond(
        key, db, pivot_inx, list_meta.right_idx_, 1, 1, true /*is_forward_order*/, false /*is_query_value*/);
    if (OB_FAIL(build_list_query(query_cond, query))) {
      LOG_WARN("fail to build list query cond", K(ret), K(query_cond));
    }
  }

  if (OB_SUCC(ret)) {
    // query element
    SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
    {
      QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter)
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
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("result is not exist", K(ret), K(index_obj));
          have_redis_err_ = true;
          fmt_redis_err_msg_ = ObString::make_string(ObRedisUtil::NULL_BULK_STRING);
        }
      }
      QUERY_ITER_END(iter)
    }
  }
  return ret;
}

int ListCommandOperator::do_insert(
    const ObString &key,
    bool is_before_pivot,
    const ObString &pivot,
    const ObString &value)
{
  int ret = OB_SUCCESS;

  // 1. Get meta data and check whether it has expired.
  ListMetaData list_meta;
  ObTableQuery query;
  int64_t db = redis_ctx_.get_request_db();
  int64_t pivot_inx = 0;
  int64_t insert_idx = 0;
  if (OB_FAIL(do_list_expire_if_needed(key, list_meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do list expire if needed", K(ret), K(key));
    } else {
      have_redis_err_ = true;
      fmt_redis_err_msg_ = ObString::make_string(ObRedisUtil::FMT_ZERO);
    }
  } else if (OB_FAIL(get_pivot_index(db, key, pivot, list_meta, pivot_inx))) {
    LOG_WARN("fail to get pivot index", K(ret), K(key), K(pivot));
  } else if (OB_FAIL(get_insert_index(db, key, list_meta, pivot_inx, is_before_pivot, insert_idx))) {
    LOG_WARN("fail to get adjacent index", K(ret), K(key), K(pivot_inx), K(is_before_pivot));
  } else {
    ObTableBatchOperation put_value_ops;
    ResultFixedArray results(op_temp_allocator_);

    list_meta.count_ += 1;
    list_meta.left_idx_ = std::min(list_meta.left_idx_, insert_idx);
    list_meta.right_idx_ = std::max(list_meta.right_idx_, insert_idx);

    ObITableEntity *meta_entity = nullptr;
    ObITableEntity *new_data_entity;
    ObObj value_obj;
    ObObj insert_ts_obj;
    insert_ts_obj.set_timestamp(ObTimeUtility::current_time());
    if (OB_FAIL(gen_entity_with_rowkey(ListMetaData::META_INDEX, false/*is_data*/, meta_entity))) {
      LOG_WARN("fail to gen entity with rowkey", K(ret));
    } else if (OB_FAIL(list_meta.put_meta_into_entity(op_temp_allocator_, *meta_entity))) {
      LOG_WARN("fail to encode meta value", K(ret), K(list_meta));
    } else if (OB_FAIL(put_value_ops.insert_or_update(*meta_entity))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FALSE_IT(value_obj.set_varbinary(value))) {
    } else if (OB_FAIL(gen_entity_with_rowkey(insert_idx, true/*is_data*/, new_data_entity))) {
      LOG_WARN("fail to gen entity with rowkey", K(ret));
    } else if (OB_FAIL(new_data_entity->set_property(ObRedisUtil::REDIS_VALUE_NAME, value_obj))) {
      LOG_WARN("fail to add column", K(ret), K(value));
    } else if (OB_FAIL(new_data_entity->set_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, insert_ts_obj))) {
      LOG_WARN("fail to set value property", K(ret), K(insert_ts_obj));
    } else if (OB_FAIL(put_value_ops.insert_or_update(*new_data_entity))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(process_table_batch_op(put_value_ops, results, false, false))) {
      LOG_WARN("fail to process table batch op", K(ret), K(put_value_ops));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(redis_ctx_.response_.set_res_int(list_meta.count_))) {
      LOG_WARN("fail to set fmt res", K(ret), K(list_meta.count_));
    }
  } else if (have_redis_err_) {
    ret = OB_SUCCESS;
    if (OB_FAIL(redis_ctx_.response_.set_fmt_res(fmt_redis_err_msg_))) {
      LOG_WARN("fail to set fmt res", K(ret), K(fmt_redis_err_msg_));
    }
  } else {
    redis_ctx_.response_.return_table_error(ret);
  }

  return ret;
}
int ListCommandOperator::do_get_len(const ObString &key)
{
  int ret = OB_SUCCESS;

  // 1. Get meta data and check whether it has expired.
  ListMetaData list_meta;
  ObTableQuery query;
  int64_t db = 0;
  int64_t pivot_inx = 0;
  int64_t insert_idx = 0;
  if (OB_FAIL(do_list_expire_if_needed(key, list_meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do list expire if needed", K(ret), K(key));
    } else {
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(redis_ctx_.response_.set_res_int(list_meta.count_))) {
      LOG_WARN("fail to set fmt res", K(ret), K(list_meta.count_));
    }
  } else if (have_redis_err_) {
    ret = OB_SUCCESS;
    if (OB_FAIL(redis_ctx_.response_.set_fmt_res(fmt_redis_err_msg_))) {
      LOG_WARN("fail to set fmt res", K(ret), K(fmt_redis_err_msg_));
    }
  } else {
    redis_ctx_.response_.return_table_error(ret);
  }

  return ret;
}

int ListCommandOperator::get_new_border_idxs(
    const ObString &key,
    bool need_update_left_idx,
    bool need_update_right_idx,
    ListMetaData &list_meta,
    ObIArray<int64_t> &new_border_idxs)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObTableQuery *, 2> querys;
  int64_t db = redis_ctx_.get_request_db();

  if (need_update_left_idx) {
    ListQueryCond query_cond(
        key, db, list_meta.left_idx_, list_meta.right_idx_, 1, 0, true /*is_query_forward*/, false /*is_query_value*/);
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
        false /*is_query_forward*/,
        false /*is_query_value*/);
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
      QUERY_ITER_START(redis_ctx_, *query, tb_ctx, iter)
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

int ListCommandOperator::update_meta_after_rem(
    const ObString &key,
    ListMetaData &list_meta,
    const int64_t rem_count,
    bool need_update_left_idx,
    bool need_update_right_idx)
{
  int ret = OB_SUCCESS;
  ObTableBatchOperation batch_ops;
  ResultFixedArray results(op_temp_allocator_);
  if (rem_count == list_meta.count_) {
    ObITableEntity *meta_entity = nullptr;
    if (OB_FAIL(gen_entity_with_rowkey(ListMetaData::META_INDEX, false/*is_data*/, meta_entity))) {
      LOG_WARN("fail to gen entity with rowkey", K(ret));
    } else if (OB_FAIL(batch_ops.del(*meta_entity))) {
      LOG_WARN("fail to push back data_req_entity", K(ret));
    }
  } else {
    if (!need_update_left_idx && !need_update_right_idx) {
      list_meta.count_ -= rem_count;
    } else {
      ObSEArray<int64_t, 2> new_border_idxs;
      if (OB_FAIL(get_new_border_idxs(key, need_update_left_idx, need_update_right_idx, list_meta, new_border_idxs))) {
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
    if (OB_SUCC(ret)) {
      ObITableEntity *meta_entity = nullptr;
      if (OB_FAIL(gen_entity_with_rowkey(ListMetaData::META_INDEX, false/*is_data*/, meta_entity))) {
        LOG_WARN("fail to gen entity with rowkey", K(ret));
      } else if (OB_FAIL(list_meta.put_meta_into_entity(op_temp_allocator_, *meta_entity))) {
        LOG_WARN("fail to encode meta value", K(ret), K(list_meta));
      } else if (OB_FAIL(batch_ops.insert_or_update(*meta_entity))) {
        LOG_WARN("fail to push back data_req_entity", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(process_table_batch_op(batch_ops, results, false, false))) {
      LOG_WARN("fail to process table batch op", K(ret), K(batch_ops));
    }
  }
  return ret;
}

int ListCommandOperator::build_rem_querys(
    const ObString &key,
    const int64_t count,
    const ObString &value,
    const ListMetaData &list_meta,
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

  int64_t db = redis_ctx_.get_request_db();
  ListQueryCond query_cond(
      key, db, list_meta.left_idx_, list_meta.right_idx_, query_limit, 0, is_query_forward, false /*is_query_value*/);
  if (OB_FAIL(build_list_query(query_cond, query))) {
    LOG_WARN("fail to build list query cond", K(ret), K(query_cond));
  } else {
    // add filter
    FilterStrBuffer buffer(op_temp_allocator_);
    ObString filter_str;
    if (OB_FAIL(buffer.add_value_compare(hfilter::CompareOperator::EQUAL, ObRedisUtil::REDIS_VALUE_NAME, value))) {
      LOG_WARN("fail to gen compare filter string", K(ret), K(value));
    } else if (OB_FAIL(buffer.get_filter_str(filter_str))) {
      LOG_WARN("fail to get filter str");
    } else {
      query.set_filter(filter_str);
    }
  }

  return ret;
}

int ListCommandOperator::do_rem(const ObString &key, int64_t count, const ObString &value)
{
  int ret = OB_SUCCESS;

  // 1. Get meta data and check whether it has expired.
  ListMetaData list_meta;
  ObTableQuery query;
  int64_t real_rem_count = 0;

  if (OB_FAIL(do_list_expire_if_needed(key, list_meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do list expire if needed", K(ret), K(key));
    } else {
      have_redis_err_ = true;
      fmt_redis_err_msg_ = ObString::make_string(ObRedisUtil::FMT_ZERO);
    }
  } else if (OB_FAIL(build_rem_querys(key, count, value, list_meta, query))) {
    LOG_WARN("fail to build range query", K(ret), K(key), K(count), K(value), K(list_meta));
  } else {
    ObTableBatchOperation del_ops;
    bool need_update_left_idx = false;
    bool need_update_right_idx = false;

    // query element
    SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
    {
      QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter)
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
            if (OB_FAIL(gen_entity_with_rowkey(index, true/*is_data*/, del_data_entity))) {
              LOG_WARN("fail to gen entity with rowkey", K(ret));
            } else if (OB_FAIL(del_ops.del(*del_data_entity))) {
              LOG_WARN("fail to push back", K(ret));
            } else {
              if (index == list_meta.left_idx_) {
                need_update_left_idx = true;
              } else if (index == list_meta.right_idx_) {
                need_update_right_idx = true;
              }
              ++real_rem_count;
            }
          }
        }
      }
      QUERY_ITER_END(iter)
    }

    if (OB_SUCC(ret)) {
      // del elements
      ResultFixedArray results(op_temp_allocator_);
      if (OB_FAIL(process_table_batch_op(del_ops, results, false, false))) {
        LOG_WARN("fail to process table batch op", K(ret), K(del_ops));
      } else if (OB_FAIL(update_meta_after_rem(
                     key, list_meta, real_rem_count, need_update_left_idx, need_update_right_idx))) {
        LOG_WARN(
            "fail to update meta after rem",
            K(ret),
            K(list_meta),
            K(real_rem_count),
            K(need_update_left_idx),
            K(need_update_right_idx));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(redis_ctx_.response_.set_res_int(real_rem_count))) {
      LOG_WARN("fail to set fmt res", K(ret));
    }
  } else if (have_redis_err_) {
    ret = OB_SUCCESS;
    if (OB_FAIL(redis_ctx_.response_.set_fmt_res(fmt_redis_err_msg_))) {
      LOG_WARN("fail to set fmt res", K(ret), K(fmt_redis_err_msg_));
    }
  } else {
    redis_ctx_.response_.return_table_error(ret);
  }

  return ret;
}

int ListCommandOperator::build_del_ops(const ObTableQuery &query, ObTableBatchOperation &del_ops)
{
  int ret = OB_SUCCESS;

  int64_t result_count = 0;
  // query element
  SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
  {
    QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter)
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
          if (OB_FAIL(gen_entity_with_rowkey(index, true/*is_data*/, del_data_entity))) {
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

int ListCommandOperator::do_del(const ObString &key)
{
  int ret = OB_SUCCESS;

  // 1. Get meta data and check whether it has expired.
  ListMetaData list_meta;
  ObTableQuery query;
  ObTableBatchOperation del_ops;
  if (OB_FAIL(do_list_expire_if_needed(key, list_meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do list expire if needed", K(ret), K(key));
    }
  } else if (OB_FAIL(build_range_query(key, list_meta, 0, -1, query))) {
    LOG_WARN("fail to build range query", K(ret), K(key), K(list_meta));
  } else if (OB_FAIL(build_del_ops(query, del_ops))) {
    LOG_WARN("fail to build del ops", K(ret), K(query));
  } else {
    ObITableEntity *new_meta_entity = nullptr;
    // update meta
    if (OB_FAIL(gen_entity_with_rowkey(ListMetaData::META_INDEX, false/*is_data*/, new_meta_entity))) {
      LOG_WARN("fail to gen entity with rowkey", K(ret));
    } else if (OB_FAIL(del_ops.del(*new_meta_entity))) {
      LOG_WARN("fail to push back", K(ret));
    } else {
      // del elements
      ResultFixedArray results(op_temp_allocator_);
      if (OB_FAIL(process_table_batch_op(del_ops, results, false, false))) {
        LOG_WARN("fail to process table batch op", K(ret), K(del_ops));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(redis_ctx_.response_.set_fmt_res(ObRedisUtil::FMT_OK))) {
      LOG_WARN("fail to set fmt res", K(ret), K(list_meta.count_));
    }
  } else if (have_redis_err_) {
    ret = OB_SUCCESS;
    if (OB_FAIL(redis_ctx_.response_.set_fmt_res(fmt_redis_err_msg_))) {
      LOG_WARN("fail to set fmt res", K(ret), K(fmt_redis_err_msg_));
    }
  } else {
    redis_ctx_.response_.return_table_error(ret);
  }

  return ret;
}
