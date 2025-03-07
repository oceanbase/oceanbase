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
#include "ob_redis_hash_operator.h"
#include "observer/table/redis/ob_redis_rkey.h"
#include "observer/table/redis/ob_redis_context.h"
#include "share/table/redis/ob_redis_error.h"

namespace oceanbase
{
using namespace observer;
using namespace common;
using namespace share;
using namespace sql;
namespace table
{
int HashCommandOperator::do_hset_inner(int64_t db, const ObString &key,
                                       const RedisCommand::FieldValMap &field_val_map,
                                       int64_t &insert_num)
{
  int ret = OB_SUCCESS;
  bool is_insup = false;
  ObRedisMeta *meta = nullptr;
  if (OB_FAIL(check_and_insup_meta(db, key, ObRedisModel::HASH, is_insup, meta))) {
    LOG_WARN("fail to check and insup meta", K(ret), K(key), K(db));
  }

  ObTableBatchOperation ops;
  ops.set_entity_factory(redis_ctx_.entity_factory_);
  int64_t cur_ts = ObTimeUtility::fast_current_time();

  for (RedisCommand::FieldValMap::const_iterator iter = field_val_map.begin();
       OB_SUCC(ret) && iter != field_val_map.end();
       ++iter) {
    ObITableEntity *value_entity = nullptr;
    if (OB_FAIL(build_value_entity(db, key, iter->first, iter->second, cur_ts, value_entity))) {
      LOG_WARN(
          "fail to build score entity", K(ret), K(iter->first), K(iter->second), K(db), K(key));
    } else if (OB_FAIL(ops.insert_or_update(*value_entity))) {
      LOG_WARN("fail to push back insert or update op", K(ret), KPC(value_entity));
    }
  }

  ResultFixedArray results(op_temp_allocator_);
  if (OB_SUCC(ret) && OB_FAIL(process_table_batch_op(ops, results))) {
    LOG_WARN("fail to process table batch op", K(ret));
  }

  insert_num = 0;
  for (int i = 0; OB_SUCC(ret) && i < results.count(); ++i) {
    if (is_insup || !results[i].get_is_insertup_do_update()) {
      ++insert_num;
    }
  }

  return ret;
}

int HashCommandOperator::do_hset(int64_t db, const ObString &key,
                                 const RedisCommand::FieldValMap &field_val_map)
{
  int ret = OB_SUCCESS;
  int64_t insert_num = 0;

  if (OB_SUCC(ret) && OB_FAIL(do_hset_inner(db, key, field_val_map, insert_num))) {
    LOG_WARN("fail to do inner hset", K(ret), K(db), K(key));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_int(insert_num))) {
      LOG_WARN("fail to set response int", K(ret), K(insert_num));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int HashCommandOperator::do_hmset(int64_t db, const ObString &key,
                                  const RedisCommand::FieldValMap &field_val_map)
{
  int ret = OB_SUCCESS;
  int64_t insert_num = 0;

  if (OB_SUCC(ret) && OB_FAIL(do_hset_inner(db, key, field_val_map, insert_num))) {
    LOG_WARN("fail to do inner hset", K(ret), K(db), K(key));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_fmt_res(ObRedisFmt::OK))) {
      LOG_WARN("fail to set OK response", K(ret));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int HashCommandOperator::put_value_and_meta(int64_t db, const ObString &key, const ObString &field,
                                            const ObString &value, ObRedisMeta *meta)
{
  int ret = OB_SUCCESS;
  ObITableEntity *data_entity = nullptr;
  ObObj null_expire_ts;
  null_expire_ts.set_null();
  if (OB_FAIL(
          build_value_entity(db, key, field, value, ObTimeUtility::current_time(), data_entity))) {
    LOG_WARN("fail to build value entity", K(ret), K(field), K(value), K(db), K(key));
  } else if (OB_FAIL(data_entity->set_property(ObRedisUtil::EXPIRE_TS_PROPERTY_NAME, null_expire_ts))) {
    LOG_WARN("fail to set row key value", K(ret), K(null_expire_ts));
  } else if (OB_NOT_NULL(meta)) {
    ObITableEntity *put_meta_entity = nullptr;
    ObTableBatchOperation ops;
    ObArenaAllocator tmp_alloc("RedisHash", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ResultFixedArray results(tmp_alloc);
    if (OB_FAIL(gen_meta_entity(db, key, ObRedisModel::HASH, *meta, put_meta_entity))) {
      LOG_WARN("fail to generate meta entity", K(ret), K(db), K(key), KPC(meta));
    } else if (OB_FAIL(ops.insert_or_update(*put_meta_entity))) {
      LOG_WARN("fail to put meta entity", K(ret));
    } else if (OB_FAIL(ops.insert_or_update(*data_entity))) {
      LOG_WARN("fail to put data entity", K(ret));
    } else if (OB_FAIL(process_table_batch_op(ops, results))) {
      LOG_WARN("fail to process table batch op", K(ret));
    }
  } else {
    ObTableOperation op = ObTableOperation::insert_or_update(*data_entity);
    ObTableOperationResult op_res;
    if (OB_FAIL(process_table_single_op(op, op_res))) {
      LOG_WARN("fail to process table single op", K(ret), K(op));
    }
  }
  return ret;
}

int HashCommandOperator::do_hsetnx(int64_t db, const ObString &key, const ObString &field,
                                   const ObString &new_value)
{
  int ret = OB_SUCCESS;
  // 1. check if meta exists
  bool is_meta_exists = true;
  bool is_data_exists = true;
  ObRedisMeta *meta = nullptr;  // unused
  if (OB_FAIL(get_meta(db, key, ObRedisModel::HASH, meta))) {
    if (ret == OB_ITER_END) {
      is_meta_exists = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to check hash expire", K(ret), K(db), K(key));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!is_meta_exists) {
    // insert both meta and data
    if (OB_FAIL(put_value_and_meta(db, key, field, new_value, meta))) {
      LOG_WARN("fail to do single put", K(ret), K(db), K(key), K(field), K(new_value));
    }
  } else {
    ObString value;  // unused
    if (OB_FAIL(get_field_value(db, key, field, value))) {
      if (ret == OB_ITER_END) {
        is_data_exists = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get field value", K(ret), K(field), K(db), K(key));
      }
    }
    if (OB_SUCC(ret) && !is_data_exists) {
      meta = nullptr;
      if (OB_FAIL(put_value_and_meta(db, key, field, new_value, meta))) {
        LOG_WARN("fail to do single put", K(ret), K(db), K(key), K(field), K(new_value));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_)
                    .response_.set_res_int((!is_meta_exists || !is_data_exists) ? 1 : 0))) {
      LOG_WARN("fail to set response int", K(ret), K(is_meta_exists), K(is_data_exists));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int HashCommandOperator::do_hexists(int64_t db, const ObString &key, const ObString &field)
{
  int ret = OB_SUCCESS;
  ObString value;  // unused
  bool exists = true;
  if (OB_FAIL(get_field_value(db, key, field, value))) {
    if (ret == OB_ITER_END) {
      exists = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get field value", K(ret), K(field), K(db), K(key));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_int(exists ? 1 : 0))) {
      LOG_WARN("fail to set response int", K(ret), K(exists));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int HashCommandOperator::do_hget(int64_t db, const ObString &key, const ObString &field)
{
  int ret = OB_SUCCESS;
  ObString value;
  if (OB_FAIL(get_field_value(db, key, field, value))) {
    if (ret == OB_ITER_END) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisFmt::NULL_BULK_STRING);
    } else {
      LOG_WARN("fail to get field value", K(ret), K(field), K(db), K(key));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_bulk_string(value))) {
      LOG_WARN("fail to set bulk string", K(ret), K(value));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int HashCommandOperator::do_hget_all(int64_t db, const ObString &key, bool need_fields,
                                     bool need_vals)
{
  int ret = OB_SUCCESS;
  ObTableQuery query;
  if (!need_fields && !need_vals) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(need_fields), K(need_vals));
  } else if (OB_FAIL(add_complex_type_subkey_scan_range(db, key, query))) {
    LOG_WARN("fail to add hash set scan range", K(ret), K(db), K(key));
  } else if (need_fields && OB_FAIL(query.add_select_column(ObRedisUtil::RKEY_PROPERTY_NAME))) {
    LOG_WARN("fail to add select column", K(ret));
  } else if (need_vals && OB_FAIL(query.add_select_column(ObRedisUtil::VALUE_PROPERTY_NAME))) {
    LOG_WARN("fail to add select column", K(ret));
  } else {
    SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
    {
      ObRedisSetMeta *null_meta = nullptr;
      QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, null_meta)
      ObArray<ObString> ret_arr(OB_MALLOC_NORMAL_BLOCK_SIZE,
                                ModulePageAllocator(op_temp_allocator_, "RedisHGet"));
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
          ObString member;
          if (OB_FAIL(one_result->get_next_entity(result_entity))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", K(ret));
            }
          } else if (need_fields) {
            ObString field;
            if (OB_FAIL(get_subkey_from_entity(op_temp_allocator_, *result_entity, field))) {
              LOG_WARN("fail to get subkey from entity", K(ret), KPC(result_entity));
            } else if (OB_FAIL(ret_arr.push_back(field))) {
              LOG_WARN("fail to push back", K(ret));
            }
          }
          if (OB_SUCC(ret) && need_vals) {
            ObString value;
            if (OB_FAIL(get_varbinary_from_entity(
                    *result_entity, ObRedisUtil::VALUE_PROPERTY_NAME, value))) {
              LOG_WARN("fail to get member from entity", K(ret), KPC(result_entity));
            } else if (OB_FAIL(ret_arr.push_back(value))) {
              LOG_WARN("fail to push back", K(ret));
            }
          }
        }
      }
      if (ret == OB_ITER_END) {
        if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_array(ret_arr))) {
          LOG_WARN("fail to set result array", K(ret));
        }
      }
      QUERY_ITER_END(iter)
    }
  }

  if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int HashCommandOperator::do_hlen(int64_t db, const ObString &key)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  if (OB_FAIL(get_complex_type_count(db, key, len))) {
    LOG_WARN("fail to do hlen inner", K(ret), K(db), K(key));
  } else if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(len))) {
    LOG_WARN("fail to set result", K(ret), K(len));
  }

  if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

// return OB_ITER_END if not exists or expired
int HashCommandOperator::get_field_value(int64_t db, const ObString &key, const ObString &field,
                                         ObString &value, ObRedisMeta *meta /* = nullptr*/)
{
  int ret = OB_SUCCESS;
  ObITableEntity *entity = nullptr;
  if (OB_FAIL(build_complex_type_rowkey_entity(db, key, true /*not meta*/, field, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(field), K(db), K(key));
  } else {
    ObTableOperation op = ObTableOperation::retrieve(*entity);
    ObTableOperationResult op_res;
    ObITableEntity *res_entity = nullptr;
    if (OB_FAIL(process_table_single_op(op, op_res, meta))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to process table single op", K(op));
      }
    } else if (OB_FAIL(op_res.get_entity(res_entity))) {
      LOG_WARN("fail to get result entity", K(ret));
    } else if (OB_FAIL(get_varbinary_from_entity(
                   *res_entity, ObRedisUtil::VALUE_PROPERTY_NAME, value))) {
      LOG_WARN("fail to get value from entity", K(ret), KPC(res_entity));
    }
  }
  return ret;
}

int HashCommandOperator::build_value_entity(int64_t db, const ObString &key, const ObString &field,
                                            const ObString &value, int64_t insert_ts,
                                            ObITableEntity *&entity)
{
  int ret = OB_SUCCESS;
  ObObj value_obj;
  value_obj.set_varbinary(value);
  ObObj ts_obj;
  ts_obj.set_timestamp(insert_ts);
  ObObj expire_obj;
  expire_obj.set_null();
  if (OB_FAIL(build_complex_type_rowkey_entity(db, key, true /*not meta*/, field, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(field), K(db), K(key));
  } else if (OB_FAIL(entity->set_property(ObRedisUtil::EXPIRE_TS_PROPERTY_NAME, expire_obj))) {
    LOG_WARN("fail to set expire ts property", K(ret), K(expire_obj));
  } else if (OB_FAIL(entity->set_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, ts_obj))) {
    LOG_WARN("fail to set insert ts property", K(ret), K(ts_obj));
  } else if (OB_FAIL(entity->set_property(ObRedisUtil::VALUE_PROPERTY_NAME, value_obj))) {
    LOG_WARN("fail to set value property", K(ret), K(value_obj));
  }
  return ret;
}

int HashCommandOperator::do_hmget(int64_t db, const ObString &key, const ObIArray<ObString> &fields)
{
  int ret = OB_SUCCESS;
  ObRedisMeta *meta_info = nullptr;
  bool is_meta_exists = true;
  ObFixedArray<ObString, ObIAllocator> res_arr(redis_ctx_.allocator_, fields.count());
  if (OB_FAIL(get_meta(db, key, ObRedisModel::HASH, meta_info))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to check hash expire", K(ret), K(db), K(key));
    } else {
      is_meta_exists = false;
      ret = OB_SUCCESS;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!is_meta_exists) {
    for (int i = 0; OB_SUCC(ret) && i < fields.count(); ++i) {
      if (OB_FAIL(res_arr.push_back(ObString::make_empty_string()))) {
        LOG_WARN("fail to push back nil result", K(ret));
      }
    }
  } else {
    for (int i = 0; OB_SUCC(ret) && i < fields.count(); ++i) {
      ObString value;
      if (OB_FAIL(get_field_value(db, key, fields.at(i), value, meta_info))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to get field value", K(ret), K(db), K(key), K(fields.at(i)));
        } else {
          // empty bulk string (nil in redis)
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(res_arr.push_back(value))) {
        LOG_WARN("fail to push back value", K(ret), K(value));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_array(res_arr))) {
      LOG_WARN("fail to set array", K(ret));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int HashCommandOperator::do_hdel(int64_t db, const ObString &key,
                                 const HashCommand::FieldSet &field_set)
{
  int ret = OB_SUCCESS;
  ObRedisMeta *meta_info = nullptr;
  bool is_meta_exists = true;
  int64_t del_num = 0;
  if (OB_FAIL(get_meta(db, key, ObRedisModel::HASH, meta_info))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to check hash expire", K(ret), K(db), K(key));
    } else {
      is_meta_exists = false;
      ret = OB_SUCCESS;
    }
  }

  int64_t hlen = 0;
  if (is_meta_exists) {
    ObTableBatchOperation ops;
    ops.set_entity_factory(redis_ctx_.entity_factory_);
    ObArray<ObRowkey> rowkeys(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(op_temp_allocator_, "RedisHDel"));
    if (OB_FAIL(rowkeys.reserve(field_set.size()))) {
      LOG_WARN("fail to reserve count", K(ret), K(field_set.size()));
    }

    for (HashCommand::FieldSet::const_iterator iter = field_set.begin();
        OB_SUCC(ret) && iter != field_set.end();
        ++iter) {
      ObITableEntity *value_entity = nullptr;
      if (OB_FAIL(build_complex_type_rowkey_entity(db, key, true /*not meta*/, iter->first, value_entity))) {
        LOG_WARN("fail to build rowkey entity", K(ret), K(iter->first), K(db), K(key));
      } else if (OB_FAIL(ops.retrieve(*value_entity))) {
        LOG_WARN("fail to push back insert or update op", K(ret), KPC(value_entity));
      } else if (rowkeys.push_back(value_entity->get_rowkey())) {
        LOG_WARN("fail to push back rowkey", K(ret), K(value_entity->get_rowkey()), K(rowkeys.count()));
      }
    }

    ResultFixedArray results(op_temp_allocator_);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(process_table_batch_op(ops, results, meta_info))) {
      LOG_WARN("fail to process table batch op", K(ret));
    } else if (OB_FAIL(delete_results(results, rowkeys, del_num))) {
      LOG_WARN("fail to delete results", K(ret), K(results));
    } else if (OB_FAIL(fake_del_empty_key_meta(ObRedisModel::HASH, db, key, meta_info))) {
      LOG_WARN("fail to delete empty key meta", K(ret), K(db), K(key));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_int(del_num))) {
      LOG_WARN("fail to set int", K(ret), K(del_num));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int HashCommandOperator::do_hincrby(int64_t db, const ObString &key, const ObString &field, int64_t increment)
{
  int ret = OB_SUCCESS;
  bool is_meta_exists = true;
  ObRedisMeta *meta = nullptr;  // unused
  int64_t new_val = increment;
  if (OB_FAIL(get_meta(db, key, ObRedisModel::HASH, meta))) {
    if (ret == OB_ITER_END) {
      is_meta_exists = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to check hash expire", K(ret), K(db), K(key));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!is_meta_exists) {
    // insert both meta and data
    ObFastFormatInt ffi(new_val);
    ObString new_value(ffi.length(), ffi.ptr());
    if (OB_FAIL(put_value_and_meta(db, key, field, new_value, meta))) {
      LOG_WARN("fail to put value and meta");
    }
  } else {
    bool is_data_exists = true;
    ObString value;
    if (OB_FAIL(get_field_value(db, key, field, value))) {
      if (ret == OB_ITER_END) {
        is_data_exists = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get field value", K(ret), K(field), K(db), K(key));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (is_data_exists) {
      int64_t old_val = 0;
      if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(value, old_val))) {
        RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::HASH_VALUE_ERR);
        LOG_WARN("value is not an integer or out of range", K(ret), K(value));
      } else if (is_incr_out_of_range(old_val, increment)) {
        RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INCR_OVERFLOW);
        LOG_WARN("increment or decrement would overflow", K(ret), K(old_val), K(increment));
      } else {
        new_val += old_val;
      }
    }

    if (OB_SUCC(ret)) {
      ObFastFormatInt ffi(new_val);
      ObString new_val_str(ffi.length(), ffi.ptr());
      if (OB_FAIL(put_value_and_meta(db, key, field, new_val_str, nullptr))) {
        LOG_WARN("fail to put value and meta");
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_int(new_val))) {
      LOG_WARN("fail to set int", K(ret), K(new_val));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int HashCommandOperator::do_hincrbyfloat(int64_t db, const ObString &key, const ObString &field, long double increment)
{
  int ret = OB_SUCCESS;
  bool is_meta_exists = true;
  ObRedisMeta *meta = nullptr;  // unused
  long double new_val = increment;
  ObString new_value;
  if (OB_FAIL(get_meta(db, key, ObRedisModel::HASH, meta))) {
    if (ret == OB_ITER_END) {
      is_meta_exists = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to check hash expire", K(ret), K(db), K(key));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!is_meta_exists) {
    // insert both meta and data
    if (OB_FAIL(ObRedisHelper::long_double_to_string(op_temp_allocator_, new_val, new_value))) {
      LOG_WARN("fail to do double to string", K(ret));
    } else if (OB_FAIL(put_value_and_meta(db, key, field, new_value, meta))) {
      LOG_WARN("fail to put value and meta");
    }
  } else {
    bool is_data_exists = true;
    ObString value;
    if (OB_FAIL(get_field_value(db, key, field, value))) {
      if (ret == OB_ITER_END) {
        is_data_exists = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get field value", K(ret), K(field), K(db), K(key));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (is_data_exists) {
      bool is_valid = false;
      long double old_val = 0.0;
      if (OB_FAIL(ObRedisHelper::string_to_long_double(value, old_val))) {
        RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::HASH_VALUE_FLOAT_ERR);
        LOG_WARN("fail to convert string to double ", K(ret), K(value));
      } else if (is_incrby_out_of_range(old_val, increment)) {
        RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::FLOAT_ERR);
        LOG_WARN("increment or decrement would overflow", K(ret));
      } else {
        new_val += old_val;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObRedisHelper::long_double_to_string(op_temp_allocator_, new_val, new_value))) {
        LOG_WARN("fail to do double to string", K(ret));
      } else if (OB_FAIL(put_value_and_meta(db, key, field, new_value, nullptr))) {
        LOG_WARN("fail to put value and meta");
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_bulk_string(new_value))) {
      LOG_WARN("fail to set int", K(ret), K(new_value));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int HashCommandOperator::fill_set_batch_op(const ObRedisOp &op,
                                           ObIArray<ObTabletID> &tablet_ids,
                                           ObTableBatchOperation &batch_op)
{
  int ret = OB_SUCCESS;
  const HSet *hset = reinterpret_cast<const HSet*>(op.cmd());
  if (OB_ISNULL(hset)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null hset op", K(ret));
  } else if (OB_FAIL(tablet_ids.push_back(op.tablet_id()))) {
    LOG_WARN("fail to push back tablet id", K(ret));
  }
  int64_t cur_ts = ObTimeUtility::fast_current_time();
  const HSet::FieldValMap &field_val_map = hset->field_val_map();
  RedisCommand::FieldValMap::const_iterator iter = field_val_map.begin();
  ObITableEntity *value_entity = nullptr;
  ObString key;
  for (; OB_SUCC(ret) && iter != field_val_map.end(); ++iter) { // loop fields
    if (OB_FAIL(op.get_key(key))) {
      LOG_WARN("fail to get key", K(ret), K(op));
    } else if (OB_FAIL(build_value_entity(op.db(), key, iter->first, iter->second, cur_ts, value_entity))) {
      LOG_WARN("fail to build score entity", K(ret), K(iter->first), K(iter->second), K(op.db()), K(key));
    } else {
      if (OB_FAIL(batch_op.insert_or_update(*value_entity))) {
        LOG_WARN("fail to push back insert or update op", K(ret), KPC(value_entity));
      }
    }
  }
  return ret;
}

int HashCommandOperator::do_group_hget()
{
  int ret = OB_SUCCESS;
  ResultFixedArray batch_res(op_temp_allocator_);
  if (OB_FAIL(group_get_complex_type_data(ObRedisUtil::VALUE_PROPERTY_NAME, batch_res))) {
    LOG_WARN("fail to group get complex type data", K(ret), K(ObRedisUtil::VALUE_PROPERTY_NAME));
  } else if (OB_FAIL(reply_batch_res(batch_res))) {
    LOG_WARN("fail to reply batch res", K(ret));
  }
  return ret;
}

int HashCommandOperator::do_group_hsetnx()
{
  int ret = OB_SUCCESS;
  ObRedisBatchCtx &group_ctx = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_);
  group_ctx.entity_factory_ = &op_entity_factory_;
  // 1. get metas
  ObArray<ObRedisMeta *> metas(OB_MALLOC_NORMAL_BLOCK_SIZE,
                              ModulePageAllocator(op_temp_allocator_, "RedisGHstNX"));
  if (OB_FAIL(get_group_metas(op_temp_allocator_, model_, metas))) {
    LOG_WARN("fail to get group metas", K(ret));
  }

  // 2. try get data if meta exists
  ObTableBatchOperation batch_ops;
  tablet_ids_.reuse();
  for (int i = 0; i < metas.count() && OB_SUCC(ret); ++i) {
    ObRedisOp *op = reinterpret_cast<ObRedisOp*>(group_ctx.ops().at(i));
    if (OB_ISNULL(op) || OB_ISNULL(op->cmd())) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null hget op", K(ret), KP(op), KP(op->cmd()));
    } else if (metas.at(i)->is_exists()) {
      HSetNX *cmd = reinterpret_cast<HSetNX*>(op->cmd());
      ObITableEntity *entity = nullptr;
      if (OB_FAIL(build_complex_type_rowkey_entity(
          op->db(), cmd->key(), true /*not meta*/, cmd->field_, entity))) {
        LOG_WARN("fail to build rowkey entity", K(ret), K(cmd->key()), K(op->db()), K(cmd->field_));
      } else if(OB_FAIL(entity->add_retrieve_property(ObRedisUtil::VALUE_PROPERTY_NAME))) {
        LOG_WARN("fail to add retrive property", K(ret), KPC(entity));
      } else if (OB_FAIL(batch_ops.retrieve(*entity))) {
        LOG_WARN("fail to add get op", K(ret), KPC(entity));
      } else if (OB_FAIL(tablet_ids_.push_back(op->tablet_id_))) {
        LOG_WARN("fail to push back tablet id", K(ret), K(op->tablet_id_));
      }
    }
  }
  ResultFixedArray batch_get_res(op_temp_allocator_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(process_table_batch_op(
          batch_ops, batch_get_res, nullptr, RedisOpFlags::NONE, &op_temp_allocator_, &op_entity_factory_, &tablet_ids_))) {
    LOG_WARN("fail to process table batch op", K(ret));
  }

  // 3. insup and reply
  int batch_get_pos = 0;
  RedisKeySet kv_set;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(kv_set.create(group_ctx.ops().count(), ObMemAttr(MTL_ID(), "RedisGpHSet")))) {
    LOG_WARN("failed to create fd hash map", K(ret));
  } else {
    tablet_ids_.reuse();
    batch_ops.reset();
  }
  int64_t cur_ts = ObTimeUtility::fast_current_time();
  for (int i = 0; i < group_ctx.ops().count() && OB_SUCC(ret); ++i) {
    ObRedisOp *op = reinterpret_cast<ObRedisOp*>(group_ctx.ops().at(i));
    HSetNX *cmd = reinterpret_cast<HSetNX*>(op->cmd());
    ObRedisMeta *meta = metas.at(i);
    // detect is key already process
    RedisKeyNode node(op->db(), cmd->key(), op->tablet_id_);
    int tmp_ret = OB_SUCCESS;
    bool data_exists = false;
    if (OB_TMP_FAIL(kv_set.set_refactored(node, 0/* conver if exists*/))) {
      if (tmp_ret == OB_HASH_EXIST) {
        data_exists = true;
      } else {
        ret = tmp_ret;
        LOG_WARN("fail to do set_refactored", K(ret));
      }
    } else if (!metas.at(i)->is_exists()) {
      // insup meta
      ObITableEntity *meta_entity = nullptr;
      if (OB_FAIL(meta->build_meta_rowkey(op->db(), cmd->key(), redis_ctx_, meta_entity))) {
        LOG_WARN("fail to build meta with rowkey", K(ret), K(op->db()), K(cmd->key()));
      } else if (OB_FAIL(meta->put_meta_into_entity(op_temp_allocator_, *meta_entity))) {
        LOG_WARN("fail to encode meta value", K(ret), KPC(meta));
      } else if (OB_FAIL(batch_ops.insert_or_update(*meta_entity))) {
        LOG_WARN("fail to add insert or update to batch", K(ret), KPC(meta_entity));
      } else if (OB_FAIL(tablet_ids_.push_back(op->tablet_id_))) {
        LOG_WARN("fail to push back tablet id", K(ret), K(op->tablet_id_));
      }
    } else if (batch_get_res.at(batch_get_pos).get_return_rows() > 0) {
      data_exists = true;
    }


    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(op->response().set_res_int(data_exists ? 0 : 1))) {
      LOG_WARN("fail to set response", K(ret), K(i));
    } else if (!data_exists) {
      // insup data and reply 1
      ObITableEntity *value_entity = nullptr;
      if (OB_FAIL(build_value_entity(op->db(), cmd->key(), cmd->field_, cmd->new_value_, cur_ts, value_entity))) {
        LOG_WARN("fail to build score entity", K(ret), K(cmd->field_), K(cmd->new_value_), K(op->db()), K(cmd->key()));
      } else if (OB_FAIL(batch_ops.insert_or_update(*value_entity))) {
        LOG_WARN("fail to push back insert or update op", K(ret), KPC(value_entity));
      } else if (OB_FAIL(tablet_ids_.push_back(op->tablet_id_))) {
        LOG_WARN("fail to push back tablet id", K(ret), K(op->tablet_id_));
      }
    }

    if (metas.at(i)->is_exists()) {
      batch_get_pos++;
    }
  }

  // do batch insup
  ResultFixedArray batch_insup_res(op_temp_allocator_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(process_table_batch_op(
          batch_ops, batch_insup_res, nullptr, RedisOpFlags::NONE, &op_temp_allocator_, &op_entity_factory_, &tablet_ids_))) {
    LOG_WARN("fail to process table batch op", K(ret));
  }

  return ret;
}

}  // namespace table
}  // namespace oceanbase
