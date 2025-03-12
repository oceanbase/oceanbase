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
#include "ob_redis_generic_operator.h"
#include "ob_redis_string_operator.h"
#include "ob_redis_list_operator.h"
#include "ob_redis_zset_operator.h"
#include "src/observer/table/redis/ob_redis_rkey.h"
#include "share/table/redis/ob_redis_error.h"

namespace oceanbase
{
using namespace observer;
using namespace common;
using namespace share;
using namespace sql;
namespace table
{

int GenericCommandOperator::update_model_expire(
  int64_t db,
  const ObString &key,
  int64_t expire_ts,
  ObRedisModel model,
  ExpireStatus &expire_status)
{
  int ret = OB_SUCCESS;
  expire_status = ExpireStatus::INVALID;
  if (model == ObRedisModel::STRING) {
    ObObj expire_obj;
    if (expire_ts != OB_INVALID_TIMESTAMP) {
      expire_obj.set_timestamp(expire_ts);
    } else {
      expire_obj.set_null();
    }
    ObITableEntity *entity = nullptr;
    if (OB_FAIL(build_string_rowkey_entity(db, key, entity))) {
      LOG_WARN("fail to build string rowkey entity", K(ret), K(db), K(key));
    } else if (OB_FAIL(entity->set_property(ObRedisUtil::EXPIRE_TS_PROPERTY_NAME, expire_obj))) {
      LOG_WARN("fail to set value", K(ret), K(expire_obj), KPC(entity));
    } else {
      ObTableOperation update_op = ObTableOperation::update(*entity);
      ObTableOperationResult op_res;
      if (OB_FAIL(process_table_single_op(update_op, op_res, nullptr, RedisOpFlags::RETURN_REDIS_META))) {
        if (ret == OB_ITER_END) {
          ret = OB_SUCCESS;
          expire_status = ExpireStatus::NOT_EXISTS;
        } else {
          LOG_WARN("fail to process table single op", K(ret), K(update_op));
        }
      } else {
        expire_status = (op_res.get_affected_rows() > 0) ? ExpireStatus::NOT_EXPIRED : ExpireStatus::NOT_EXISTS;
      }
    }
  } else {
    ObRedisMeta *meta = nullptr;
    if (OB_FAIL(get_meta(db, key, model, meta))) {
      if (ret == OB_ITER_END) {
        expire_status = ExpireStatus::NOT_EXISTS;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get meta", K(ret), K(db), K(key), K(model));
      }
    } else {
      expire_status = (meta->get_expire_ts() == INT64_MAX) ? ExpireStatus::PERSIST : ExpireStatus::NOT_EXPIRED;
      ObITableEntity *meta_entity = nullptr;
      ObObj expire_obj;
      expire_obj.set_timestamp(expire_ts);
      if (OB_FAIL(meta->build_meta_rowkey(db, key, redis_ctx_, meta_entity))) {
        LOG_WARN("fail to gen entity with rowkey", K(ret));
      } else if (OB_FAIL(meta_entity->set_property(ObRedisUtil::EXPIRE_TS_PROPERTY_NAME,
                                                  expire_obj))) {
        LOG_WARN("fail to get expire ts", K(ret));
      } else {
        ObTableOperation op = ObTableOperation::update(*meta_entity);
        ObTableOperationResult result;
        if (OB_FAIL(process_table_single_op(op, result, meta, RedisOpFlags::RETURN_REDIS_META))) {
          LOG_WARN("fail to process table op", K(ret), K(op));
        }
      }
    }
  }
  if (OB_SUCC(ret) && expire_status == ExpireStatus::INVALID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid expire status with ret == OB_SUCCESS", K(ret), K(expire_status));
  }
  return ret;
}

int GenericCommandOperator::do_expire_at_us(int64_t db, const ObString &key, int64_t expire_ts)
{
  int ret = OB_SUCCESS;
  ObRedisCmdCtx *cmd_ctx = redis_ctx_.cmd_ctx_;
  if (OB_ISNULL(cmd_ctx)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null ObRedisCmdCtx", K(ret));
  }
  bool is_existed = false;
  for (int i = ObRedisModel::STRING; OB_SUCC(ret) && i < ObRedisModel::INVALID; ++i) {
    redis_ctx_.cur_table_idx_ = i;
    model_ = static_cast<ObRedisModel>(i);
    ExpireStatus cur_status = ExpireStatus::INVALID;
    if (OB_FAIL(update_model_expire(db, key, expire_ts, static_cast<ObRedisModel>(i), cur_status))) {
      LOG_WARN("fail to update expire", K(ret), K(db), K(key), K(expire_ts));
    } else if (cur_status != ExpireStatus::NOT_EXISTS) {
      is_existed = true;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(is_existed ? 1 : 0))) {
      LOG_WARN("fail to set fmt res", K(ret));
    }
  }
  return ret;
}

int GenericCommandOperator::do_expire_at(int64_t db, const ObString &key, int64_t expire_ts,
                                         int64_t conv_unit)
{
  int ret = OB_SUCCESS;
  bool is_existed = false;
  if (conv_unit <= 0 || (expire_ts > INT64_MAX / conv_unit) || (expire_ts < INT64_MIN / conv_unit)) {
    RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::EXPIRE_TIME_ERR);
  } else if (OB_FAIL(do_expire_at_us(db, key, expire_ts * conv_unit))) {
    LOG_WARN("fail to do expire at us", K(ret), K(db), K(key));
  }
  if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int GenericCommandOperator::do_expire(int64_t db, const ObString &key, int64_t expire_diff,
                                      int64_t conv_unit)
{
  int ret = OB_SUCCESS;
  int64_t cur_ts = ObTimeUtility::fast_current_time();
  ObRedisModel model;

  if (conv_unit <= 0 || (INT64_MAX - cur_ts) / conv_unit < expire_diff) {
    RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::EXPIRE_TIME_ERR);
  } else if (OB_FAIL(do_expire_at_us(db, key, cur_ts + expire_diff * conv_unit))) {
    LOG_WARN("fail to do expire at us", K(ret), K(db), K(key));
  }

  if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int GenericCommandOperator::do_model_ttl(int64_t db, const ObString &key, ObRedisModel model,
                                         int64_t conv_unit, int64_t &status)
{
  int ret = OB_SUCCESS;
  ObRedisMeta *meta = nullptr;
  bool has_expire_ts = false;
  int64_t expire_ts = OB_INVALID_TIMESTAMP;
  if (conv_unit <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to do model ttl", K(ret));
  } else if (model == ObRedisModel::STRING) {
    StringCommandOperator string_op(redis_ctx_);
    ObString value;
    bool is_existed = true;
    if (OB_FAIL(string_op.get_value(db, key, value, expire_ts, is_existed))) {
      LOG_WARN("fail to get value");
    } else if (!is_existed) {
      status = -2;
    } else if (expire_ts == OB_INVALID_TIMESTAMP) {
      status = -1;
    } else {
      has_expire_ts = true;
    }
  } else {
    int64_t row_cnt = 0;
    if (OB_FAIL(get_meta(db, key, model, meta))) {
      if (ret == OB_ITER_END) {
        status = -2;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get meta", K(ret), K(db), K(key), K(model));
      }
    } else if (meta->get_expire_ts() == INT64_MAX) {
      status = -1;
    } else {
      expire_ts = meta->get_expire_ts();
      has_expire_ts = true;
    }
  }

  if (OB_SUCC(ret) && has_expire_ts) {
    int64_t now = ObTimeUtility::fast_current_time();
    // show in specific unit
    status = static_cast<int64_t>(std::round((expire_ts - now) / static_cast<double>(conv_unit)));
  }
  return ret;
}

int GenericCommandOperator::do_ttl(int64_t db, const ObString &key, int64_t conv_unit)
{
  int ret = OB_SUCCESS;
  ObRedisCmdCtx *cmd_ctx = redis_ctx_.cmd_ctx_;
  if (OB_ISNULL(cmd_ctx)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null ObRedisCmdCtx", K(ret));
  }
  int64_t ttl = -2;
  for (int i = ObRedisModel::STRING; OB_SUCC(ret) && i < ObRedisModel::INVALID; ++i) {
    redis_ctx_.cur_table_idx_ = i;
    model_ = static_cast<ObRedisModel>(i);
    int64_t status = 0;
    if (OB_FAIL(do_model_ttl(db, key, static_cast<ObRedisModel>(i), conv_unit, status))) {
      LOG_WARN("fail to update expire", K(ret), K(db), K(key), K(static_cast<ObRedisModel>(i)));
    } else {
      ttl = OB_MAX(status, ttl);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_int(ttl))) {
      LOG_WARN("fail to set fmt res", K(ret));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int GenericCommandOperator::get_subkey_count_by_meta(
  int64_t db,
  const ObString &key,
  const ObRedisMeta *meta,
  ObRedisModel model,
  int64_t &row_cnt)
{
  int ret = OB_SUCCESS;
  ObTableQuery query;
  row_cnt = 0;
  if (model == ObRedisModel::LIST) {
    const ObRedisListMeta *list_meta = reinterpret_cast<const ObRedisListMeta *>(meta);
    row_cnt = list_meta->count_;
  } else {
    if (OB_FAIL(add_complex_type_subkey_scan_range(db, key, query))) {
      LOG_WARN("fail to add complex type scan range", K(ret), K(db), K(key));
    } else if (OB_FAIL(process_table_query_count(op_temp_allocator_, query, meta, row_cnt))) {
      LOG_WARN("fail to process table query count", K(ret), K(db), K(key), K(query));
    }
  }
  return ret;
}

int GenericCommandOperator::is_key_exists(int64_t db, const ObString &key, ObRedisModel model,
                                          bool &exists)
{
  int ret = OB_SUCCESS;
  if (model == ObRedisModel::STRING) {
    StringCommandOperator string_op(redis_ctx_);
    if (OB_FAIL(string_op.is_key_exists(db, key, exists))) {
      LOG_WARN("fail to check is key exists", K(ret), K(db), K(key));
    }
  } else {
    ObRedisMeta *meta = nullptr;
    exists = false;
    int64_t row_cnt = 0;
    if (OB_FAIL(get_meta(db, key, model, meta))) {
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get meta", K(ret), K(db), K(key), K(model));
      }
    } else if (OB_FAIL(get_subkey_count_by_meta(db, key, meta, model, row_cnt))) {
      LOG_WARN("fail to get subkey count by meta", K(ret), K(db), K(key), K(model));
    } else if (row_cnt > 0) {
      exists = true;
    }
  }
  return ret;
}

int GenericCommandOperator::do_exists(int64_t db, const common::ObIArray<common::ObString> &keys)
{
  int ret = OB_SUCCESS;
  int exists_cnt = 0;
  ObRedisCmdCtx *cmd_ctx = redis_ctx_.cmd_ctx_;
  if (OB_ISNULL(cmd_ctx)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null ObRedisCmdCtx", K(ret));
  }
  for (int i = ObRedisModel::STRING; OB_SUCC(ret) && i < ObRedisModel::INVALID; ++i) {
    redis_ctx_.cur_table_idx_ = i;
    model_ = static_cast<ObRedisModel>(i);
    for (int j = 0; OB_SUCC(ret) && j < keys.count(); ++j) {
      redis_ctx_.cur_rowkey_idx_ = j;
      bool is_existed = false;
      if (OB_FAIL(is_key_exists(db, keys.at(j), static_cast<ObRedisModel>(i), is_existed))) {
        LOG_WARN("fail to check is key exists", K(ret), K(db), K(j), K(keys.at(j)), K(static_cast<ObRedisModel>(i)));
      } else if (is_existed) {
        ++exists_cnt;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_int(exists_cnt))) {
      LOG_WARN("fail to set fmt res", K(ret), K(exists_cnt));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int GenericCommandOperator::do_type(int64_t db, const ObString &key)
{
  int ret = OB_SUCCESS;
  ObRedisCmdCtx *cmd_ctx = redis_ctx_.cmd_ctx_;
  if (OB_ISNULL(cmd_ctx)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null ObRedisCmdCtx", K(ret));
  }
  ObStringBuffer buffer(&redis_ctx_.allocator_);
  for (int i = ObRedisModel::STRING; OB_SUCC(ret) && i < ObRedisModel::INVALID; ++i) {
    bool is_existed = false;
    redis_ctx_.cur_table_idx_ = i;
    model_ = static_cast<ObRedisModel>(i);
    if (OB_FAIL(is_key_exists(db, key, static_cast<ObRedisModel>(i), is_existed))) {
      LOG_WARN("fail to update expire", K(ret), K(db), K(key), K(static_cast<ObRedisModel>(i)));
    } else if (is_existed) {
      if (!buffer.empty()) {
        if (OB_FAIL(buffer.append(", "))) {
          LOG_WARN("fail to append buffer", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ObString model_str;
        if (OB_FAIL(ObRedisHelper::model_to_string(static_cast<ObRedisModel>(i), model_str))) {
          LOG_WARN("fail to get table name by model", K(ret));
        } else if (OB_FAIL(buffer.append(model_str))) {
          LOG_WARN("fail to append buffer", K(ret), K(model_str));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (buffer.empty()) {
      if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_simple_string("none"))) {
        LOG_WARN("fail to set fmt res", K(ret));
      }
    } else {
      ObString res_str;
      if (OB_FAIL(buffer.get_result_string(res_str))) {
        LOG_WARN("fail to get result string", K(ret));
      } else if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_simple_string(res_str))) {
        LOG_WARN("fail to set fmt res", K(ret), K(res_str));
      }
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int GenericCommandOperator::do_persist(int64_t db, const ObString &key)
{
  int ret = OB_SUCCESS;
  ObRedisCmdCtx *cmd_ctx = redis_ctx_.cmd_ctx_;
  if (OB_ISNULL(cmd_ctx)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null ObRedisCmdCtx", K(ret));
  }
  bool is_existed = false;
  for (int i = ObRedisModel::STRING; OB_SUCC(ret) && i < ObRedisModel::INVALID; ++i) {
    redis_ctx_.cur_table_idx_ = i;
    model_ = static_cast<ObRedisModel>(i);
    ObRedisModel model = static_cast<ObRedisModel>(i);
    ExpireStatus cur_status = ExpireStatus::INVALID;
    if (OB_FAIL(update_model_expire(db,
                                    key,
                                    model == ObRedisModel::STRING ? OB_INVALID_TIMESTAMP : INT64_MAX /*expire_ts*/,
                                    model,
                                    cur_status))) {
      LOG_WARN("fail to update expire", K(ret), K(db), K(key), K(model));
    } else if (cur_status == ExpireStatus::NOT_EXPIRED) {
      is_existed = true;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_int(is_existed ? 1 : 0))) {
      LOG_WARN("fail to set fmt res", K(ret));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int GenericCommandOperator::del_key(int64_t db, const ObString &key, ObRedisModel model,
                                          bool &exists)
{
  int ret = OB_SUCCESS;
  switch (model) {
    case ObRedisModel::STRING: {
      StringCommandOperator string_op(redis_ctx_);
      ret = string_op.del_key(db, key, exists);
      break;
    }
    case ObRedisModel::LIST: {
      ListCommandOperator list_op(redis_ctx_);
      ret = list_op.do_del(db, key, exists);
      break;
    }
    case ObRedisModel::ZSET:
    case ObRedisModel::SET:
    case ObRedisModel::HASH: {
      ret = del_complex_key(model, db, key, true/*del_meta*/, exists);
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid redis model", K(ret), K(model));
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to del key", K(ret), K(model), K(db), K(key));
  }
  return ret;
}

int GenericCommandOperator::do_del(int64_t db, const common::ObIArray<common::ObString> &keys)
{
  int ret = OB_SUCCESS;
  int del_cnt = 0;
  ObRedisCmdCtx *cmd_ctx = redis_ctx_.cmd_ctx_;
  hash::ObHashSet<ObString, hash::NoPthreadDefendMode> key_set;
  if (OB_ISNULL(cmd_ctx)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null ObRedisCmdCtx", K(ret));
  } else if (OB_FAIL(key_set.create(keys.count(), ObMemAttr(MTL_ID(), "RedisDel")))) {
    LOG_WARN("fail to create key set", K(ret), K(keys.count()));
  }

  for (int j = 0; OB_SUCC(ret) && j < keys.count(); ++j) {
    redis_ctx_.cur_rowkey_idx_ = j;
    bool is_existed = false;
    if (OB_FAIL(key_set.set_refactored(keys.at(j), 0/*not cover exists object*/))) {
      if (ret != OB_HASH_EXIST) {
        LOG_WARN("fail to add key to set", K(ret), K(j), K(keys.at(j)));
      }
    }
    for (int i = ObRedisModel::STRING; OB_SUCC(ret) && i < ObRedisModel::INVALID; ++i) {
      redis_ctx_.cur_table_idx_ = i;
      model_ = static_cast<ObRedisModel>(i);
      if (OB_FAIL(del_key(db, keys.at(j), static_cast<ObRedisModel>(i), is_existed))) {
        LOG_WARN("fail to check is key exists", K(ret), K(db), K(j), K(keys.at(j)), K(static_cast<ObRedisModel>(i)));
      } else if (is_existed) {
        ++del_cnt;
      }
    }
    if (ret == OB_HASH_EXIST) {
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_int(del_cnt))) {
      LOG_WARN("fail to set fmt res", K(ret), K(del_cnt));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

}  // namespace table
}  // namespace oceanbase
