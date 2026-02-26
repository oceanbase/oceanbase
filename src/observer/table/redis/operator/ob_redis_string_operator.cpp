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
#include "ob_redis_string_operator.h"
#include "lib/utility/ob_fast_convert.h"
#include "lib/time/ob_hrtime.h"
#include "share/table/redis/ob_redis_error.h"

namespace oceanbase
{
using namespace observer;
using namespace common;
using namespace share;
using namespace sql;
namespace table
{
int StringCommandOperator::build_rowkey_entity(int64_t db, const ObString &key, ObITableEntity *&entity)
{
  int ret = OB_SUCCESS;
  ObObj *obj_ptr = nullptr;
  ObRowkey rowkey;
  if (OB_ISNULL(redis_ctx_.entity_factory_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null entity_factory_", K(ret));
  } else if (OB_ISNULL(entity = redis_ctx_.entity_factory_->alloc())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null entity_factory_", K(ret));
  } else if (OB_ISNULL(
                 obj_ptr = static_cast<ObObj *>(redis_ctx_.allocator_.alloc(sizeof(ObObj) * ObRedisUtil::STRING_ROWKEY_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else if (OB_FAIL(ObRedisCtx::reset_objects(obj_ptr, ObRedisUtil::STRING_ROWKEY_SIZE))) {
    LOG_WARN("fail to init object", K(ret));
  } else {
    obj_ptr[0].set_int(db);
    obj_ptr[1].set_varbinary(key);
    rowkey.assign(obj_ptr, ObRedisUtil::STRING_ROWKEY_SIZE);
    if (OB_FAIL(entity->set_rowkey(rowkey))) {
      LOG_WARN("fail to set rowkey", K(ret));
    }
  }
  return ret;
}

int StringCommandOperator::get_value_from_entity(const ObITableEntity &entity, ObString &value)
{
  int ret = OB_SUCCESS;
  ObObj value_obj;
  if (OB_FAIL(entity.get_property(ObRedisUtil::VALUE_PROPERTY_NAME, value_obj))) {
    LOG_WARN("fail to get value obj", K(ret), K(entity));
  } else if (OB_FAIL(value_obj.get_varbinary(value))) {
    LOG_WARN("fail to get value from obj", K(ret), K(value_obj));
  }
  return ret;
}

int StringCommandOperator::get_expire_from_entity(const ObITableEntity &entity, int64_t &expire_ts)
{
  int ret = OB_SUCCESS;
  ObObj expire_obj;
  if (OB_FAIL(entity.get_property(ObRedisUtil::EXPIRE_TS_PROPERTY_NAME, expire_obj))) {
    LOG_WARN("fail to get value obj", K(ret), K(entity));
  } else if (expire_obj.is_null()) {
    expire_ts = OB_INVALID_TIMESTAMP;
  } else if (OB_FAIL(expire_obj.get_timestamp(expire_ts))) {
    LOG_WARN("fail to get value from obj", K(ret), K(expire_obj));
  }
  return ret;
}

int StringCommandOperator::build_key_value_entity(
    int64_t db,
    const ObString &key,
    const ObString &value,
    ObITableEntity *&entity)
{
  int ret = OB_SUCCESS;
  ObObj value_obj;
  value_obj.set_varbinary(value);
  if (OB_FAIL(build_string_rowkey_entity(db, key, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key));
  } else if (OB_FAIL(entity->set_property(ObRedisUtil::VALUE_PROPERTY_NAME, value_obj))) {
    LOG_WARN("fail to set value", K(ret), K(value_obj), KPC(entity));
  }
  return ret;
}

int StringCommandOperator::is_key_exists(int64_t db, const ObString &key, bool &exists)
{
  int ret = OB_SUCCESS;
  ObITableEntity *entity = nullptr;
  exists = false;
  if (OB_FAIL(build_string_rowkey_entity(db, key, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key));
  } else {
    ObTableOperation op = ObTableOperation::retrieve(*entity);
    ObTableOperationResult result;
    int64_t expire_ts = OB_INVALID_TIMESTAMP;
    ObITableEntity *res_entity = nullptr;
    if (OB_FAIL(process_table_single_op(op, result))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to process table get", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(result.get_entity(res_entity))) {
      LOG_WARN("fail to get entity", K(ret), K(result));
    } else if (OB_FAIL(get_expire_from_entity(*res_entity, expire_ts))) {
      LOG_WARN("fail to get old value from result entity", K(ret), KPC(res_entity));
    } else if (expire_ts == OB_INVALID_TIMESTAMP || ObTimeUtility::fast_current_time() < expire_ts) {
      exists = true;
    }
    if (OB_SUCC(ret)) {
      redis_ctx_.entity_factory_->free(entity);
    }
  }
  return ret;
}
int StringCommandOperator::build_key_value_expire_entity(
    int64_t db,
    const ObString &key,
    const ObString &value,
    int64_t expire_tus,
    ObITableEntity *&entity)
{
  int ret = OB_SUCCESS;
  ObObj expire_obj;
  if (expire_tus == OB_INVALID_TIMESTAMP) {
    expire_obj.set_null();
  } else {
    expire_obj.set_timestamp(expire_tus);
  }
  if (OB_FAIL(build_key_value_entity(db, key, value, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key));
  } else if (OB_FAIL(entity->set_property(ObRedisUtil::EXPIRE_TS_PROPERTY_NAME, expire_obj))) {
    LOG_WARN("fail to set expire property", K(ret), K(expire_obj));
  }
  return ret;
}

// return empty string if key not exists
int StringCommandOperator::get_value(
    int64_t db,
    const ObString &key,
    ObString &value,
    int64_t &expire_ts,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObITableEntity *entity = nullptr;
  if (OB_FAIL(build_string_rowkey_entity(db, key, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key));
  } else {
    ObTableOperation op = ObTableOperation::retrieve(*entity);
    ObTableOperationResult result;
    ObITableEntity *res_entity = nullptr;
    if (OB_FAIL(process_table_single_op(op, result))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to process table get", K(ret));
      } else {
        is_exist = false;
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(result.get_entity(res_entity))) {
      LOG_WARN("fail to get entity", K(ret), K(result));
    } else if (OB_FAIL(get_varbinary_from_entity(*res_entity, ObRedisUtil::VALUE_PROPERTY_NAME, value))) {
      LOG_WARN("fail to get old value from result entity", K(ret), KPC(res_entity));
    } else if (OB_FAIL(get_expire_from_entity(*res_entity, expire_ts))) {
      LOG_WARN("fail to get old value from result entity", K(ret), KPC(res_entity));
    }
    if (OB_SUCC(ret)) {
      redis_ctx_.entity_factory_->free(entity);
    }
  }
  return ret;
}

int StringCommandOperator::do_get_set(int64_t db, const ObString &key, const ObString &new_value)
{
  int ret = OB_SUCCESS;
  // 1. get old value
  // 2. update new value
  ObITableEntity *entity = nullptr;
  ObString old_value;
  int64_t expire_ts = OB_INVALID_TIMESTAMP;  // unused
  bool is_exist = true;
  int64_t reset_expire_ts = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(get_value(db, key, old_value, expire_ts, is_exist))) {
    LOG_WARN("fail to get old value", K(ret), K(db), K(key));
  } else if (OB_FAIL(build_key_value_expire_entity(db, key, new_value, reset_expire_ts, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key));
  } else {
    ObTableOperation op = put_or_insup(*entity);
    ObTableOperationResult result;
    if (OB_FAIL(process_table_single_op(op, result))) {
      LOG_WARN("fail to process table put", K(ret));
    }
  }

  // 3. return old value if exist
  if (OB_SUCC(ret)) {
    if (is_exist) {
      reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_bulk_string(old_value);
    } else {
      reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_fmt_res(ObRedisFmt::NULL_BULK_STRING);
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int StringCommandOperator::set_bit(int32_t offset, char bit_value, char *new_value, char &old_bit_value)
{
  int ret = OB_SUCCESS;
  if (bit_value != 0 && bit_value != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("bit value should be 0 or 1", K(ret), K(bit_value));
  } else {
    int32_t byte_idx = offset / 8;
    int32_t bit_idx = offset % 8;
    old_bit_value = (new_value[byte_idx] >> (7 - bit_idx)) & 1;
    if (old_bit_value == bit_value) {
      // do nothing
    } else if (bit_value == 1) {
      new_value[byte_idx] = new_value[byte_idx] | (1 << (7 - bit_idx));
    } else {
      new_value[byte_idx] = new_value[byte_idx] & (~(1 << (7 - bit_idx)));
    }
  }
  return ret;
}

int StringCommandOperator::do_set_bit(int64_t db, const ObString &key, int32_t offset, char value)
{
  int ret = OB_SUCCESS;
  // 1. get old value
  ObITableEntity *entity = nullptr;
  ObString old_value;
  char old_bit_value;
  int64_t expire_ts = OB_INVALID_TIMESTAMP;
  bool is_exist = true;
  const int64_t len = offset / 8 + 1;
  if (offset < 0) {
    RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::BIT_OFFSET_INTEGER_ERR);
  } else if (OB_FAIL(get_value(db, key, old_value, expire_ts, is_exist))) {
    LOG_WARN("fail to get old value", K(ret), K(db), K(key));
  } else if (old_value.length() < len) {
    char *empty_value = nullptr;
    if (OB_ISNULL(empty_value = static_cast<char *>(redis_ctx_.allocator_.alloc(sizeof(char) * len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc empty value", K(ret));
    } else {
      MEMSET(empty_value, 0, len);
      MEMCPY(empty_value, old_value.ptr(), old_value.length());
      old_value = ObString(len, empty_value);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(set_bit(offset, value, old_value.ptr(), old_bit_value))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key));
  } else if (OB_FAIL(build_key_value_expire_entity(db, key, old_value, expire_ts, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key));
  } else {
    ObTableOperation op = put_or_insup(*entity);
    ObTableOperationResult result;
    if (OB_FAIL(process_table_single_op(op, result))) {
      LOG_WARN("fail to process table put", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(old_bit_value);
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int StringCommandOperator::convert_incr_str_to_int(const common::ObString &incr, bool is_incr, int64_t &incr_num)
{
  int ret = OB_SUCCESS;
  if (is_incr) {
    if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(incr, incr_num))) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
      LOG_WARN("invalid int from str", K(ret), K(incr));
    }
  } else if (incr.length() < 1) {
    LOG_WARN("invalid old value", K(ret), K(incr));
    RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
  } else if (incr[0] == '-') {
    const int SKIP_NEG_FLAG = 1;
    ObString incr_without_symbol(incr.length() - SKIP_NEG_FLAG, incr.ptr() + SKIP_NEG_FLAG);
    if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(incr_without_symbol, incr_num))) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
      LOG_WARN("invalid int from str", K(ret), K(incr));
    }
  } else {
    if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(incr, incr_num))) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
      LOG_WARN("invalid int from str", K(ret), K(incr));
    } else {
      incr_num = -incr_num;
    }
  }
  return ret;
}

int StringCommandOperator::do_incrby(int64_t db, const ObString &key, const ObString &incr, bool is_incr)
{
  int ret = OB_SUCCESS;
  ObString old_str;
  int64_t expire_ts = OB_INVALID_TIMESTAMP;
  int64_t old_value = 0;
  int64_t incr_num = 0;
  bool is_exist = true;
  if (OB_FAIL(get_value(db, key, old_str, expire_ts, is_exist))) {
    LOG_WARN("fail to get old value", K(ret), K(db), K(key));
  } else {
    // convert old value
    if (is_exist && OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(old_str, old_value))) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
      LOG_WARN("invalid int from str", K(ret), K(old_str));
    } else if (OB_FAIL(convert_incr_str_to_int(incr, is_incr, incr_num))) {
      LOG_WARN("fail to convert incr str to int", K(ret), K(incr), K(is_incr));
    } else if (is_incr_out_of_range(old_value, incr_num)) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INCR_OVERFLOW);
      LOG_WARN("increment or decrement would overflow", K(ret), K(old_value), K(incr_num));
    }
  }

  ObFastFormatInt ffi(incr_num);
  ObString real_incr(ffi.length(), ffi.ptr());
  ObITableEntity *entity = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(build_key_value_entity(db, key, real_incr, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key));
  } else {
    ObTableOperation op = ObTableOperation::increment(*entity);
    ObTableOperationResult result;
    ObITableEntity *res_entity = nullptr;
    ObString new_value;
    if (OB_FAIL(process_table_single_op(op, result, nullptr/*meta*/, RedisOpFlags::RETURN_AFFECTED_ENTITY))) {
      LOG_WARN("fail to process table get", K(ret));
    } else if (OB_FAIL(result.get_entity(res_entity))) {
      LOG_WARN("fail to get entity", K(ret), K(result));
    } else if (OB_FAIL(get_varbinary_from_entity(*res_entity, ObRedisUtil::VALUE_PROPERTY_NAME, new_value))) {
      LOG_WARN("fail to get old value from result entity", K(ret), KPC(res_entity));
    } else {
      bool is_res_valid = false;
      int64_t res_num = 0;
      if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(new_value, res_num))) {
        RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
        LOG_WARN("fail to convert result to integer", K(ret), K(new_value));
      } else {
        reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(res_num);
      }
    }
  }
  if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int StringCommandOperator::do_append(int64_t db, const common::ObString &key, const common::ObString &val)
{
  int ret = OB_SUCCESS;

  ObString new_value;
  ObITableEntity *entity = nullptr;
  if (OB_FAIL(build_key_value_entity(db, key, val, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key));
  } else {
    ObTableOperation op = ObTableOperation::append(*entity);
    ObTableOperationResult result;
    ObITableEntity *res_entity = nullptr;
    if (OB_FAIL(process_table_single_op(op, result, nullptr/*meta*/, RedisOpFlags::RETURN_AFFECTED_ROWS))) {
      LOG_WARN("fail to process table append", K(ret));
    } else if (OB_FAIL(result.get_entity(res_entity))) {
      LOG_WARN("fail to get entity", K(ret), K(result));
    } else if (OB_FAIL(get_value_from_entity(*res_entity, new_value))) {
      LOG_WARN("fail to get new_value from result entity", K(ret), KPC(res_entity));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(new_value.length()))) {
      LOG_WARN("fail to set result int", K(ret), K(new_value.length()));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int get_bit_count(const ObString &val, int64_t start, int64_t end, int64_t &bit_count)
{
  int ret = OB_SUCCESS;
  int val_len = val.length();
  bit_count = 0;
  if (end < 0) {
    end += val_len;
  }
  if (start < 0) {
    start += val_len;
  }

  if (start > end || start >= val_len) {
    return ret;
  }

  if (start < 0) {
    start = 0;
  }

  if (end < 0) {
    end = 0;
  } else if (end >= val_len) {
    end = val_len - 1;
  }

  for (int64_t i = start; i <= end; ++i) {
    for (int64_t j = 0; j < 8; ++j) {
      if ((val[i] >> j) & 1) {
        bit_count++;
      }
    }
  }
  return ret;
}

int StringCommandOperator::do_bit_count(
    int64_t db,
    const common::ObString &key,
    const common::ObString &start_str,
    const common::ObString &end_str)
{
  int ret = OB_SUCCESS;
  int64_t res_bit_count = 0;

  int64_t start = 0;
  int64_t end = -1;
  if (!start_str.empty() && OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(start_str, start))) {
    RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
    LOG_WARN("start_str is not an integer or out of range", K(ret), K(start_str));
  } else if (!end_str.empty() && OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(end_str, end))) {
    RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
    LOG_WARN("end_str is not an integer or out of range", K(ret), K(end_str));
  } else {
    ObITableEntity *entity = nullptr;
    ObString value;
    int64_t expire_ts = OB_INVALID_TIMESTAMP;  // unused
    bool is_exist = true;
    if (OB_FAIL(get_value(db, key, value, expire_ts, is_exist))) {
      LOG_WARN("fail to get value", K(ret), K(db), K(key));
    } else if (OB_FAIL(get_bit_count(value, start, end, res_bit_count))) {
      LOG_WARN("fail to get bit count", K(ret), K(value));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(res_bit_count))) {
      LOG_WARN("fail to set res", K(ret), K(res_bit_count));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}
int StringCommandOperator::do_get(int64_t db, const common::ObString &key)
{
  int ret = OB_SUCCESS;

  ObString value;
  int64_t expire_ts = OB_INVALID_TIMESTAMP;  // unused
  bool is_exist = true;
  if (OB_FAIL(get_value(db, key, value, expire_ts, is_exist))) {
    if (ObRedisErr::is_redis_error(ret)) {
      RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
    }
    LOG_WARN("fail to get value", K(ret), K(db), K(key));
  } else if (is_exist) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_bulk_string(value))) {
      LOG_WARN("fail to set result", K(ret), K(value));
    }
  } else {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_fmt_res(ObRedisFmt::NULL_BULK_STRING))) {
      LOG_WARN("fail to set result", K(ret), K(value));
    }
  }
  return ret;
}

int StringCommandOperator::get_bit_at_pos(common::ObString &value, int64_t offset, int64_t &res_bit)
{
  int ret = OB_SUCCESS;
  int64_t byte_index = offset / 8;
  int64_t bit_index = offset % 8;
  if (byte_index >= value.length()) {
    res_bit = 0;
  } else {
    res_bit = (value[byte_index] >> (7 - bit_index)) & 1;
  }

  return ret;
}

int StringCommandOperator::do_get_bit(int64_t db, const common::ObString &key, const common::ObString &offset_str)
{
  int ret = OB_SUCCESS;

  int64_t res_bit = 0;
  int64_t offset = 0;
  if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(offset_str, offset) || offset < 0)) {
    RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::BIT_OFFSET_INTEGER_ERR);
    LOG_WARN("offset_str is not an integer or out of range", K(ret), K(offset_str));
  } else {
    ObITableEntity *entity = nullptr;
    ObString value;
    int64_t expire_ts = OB_INVALID_TIMESTAMP;  // unused
    bool is_exist = true;
    if (OB_FAIL(get_value(db, key, value, expire_ts, is_exist))) {
      LOG_WARN("fail to get value", K(ret), K(db), K(key));
    } else if (is_exist && OB_FAIL(get_bit_at_pos(value, offset, res_bit))) {
      LOG_WARN("fail to get bit at pos", K(ret), K(value), K(offset));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(res_bit))) {
      LOG_WARN("fail to set res", K(ret), K(res_bit));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int StringCommandOperator::get_range_value(
    const common::ObString &value,
    int64_t start,
    int64_t end,
    common::ObString &res_val)
{
  int ret = OB_SUCCESS;

  if (start < 0 && end < 0 && start > end) {
    return ret;
  }

  int64_t len = value.length();

  if (end < 0) {
    end += len;
  }

  if (start < 0) {
    start += len;
  }

  if (start < 0) {
    start = 0;
  }

  if (end < 0) {
    end = 0;
  }

  if (end >= len) {
    end = len - 1;
  }

  if (start > end || len == 0) {
    return ret;
  }
  if (OB_FAIL(common::ob_sub_str(op_temp_allocator_, value, start, end, res_val))) {
    LOG_WARN("failed to sub string from str", K(ret), K(value), K(start), K(end));
  }

  return ret;
}

int StringCommandOperator::do_get_range(
    int64_t db,
    const common::ObString &key,
    const common::ObString &start_str,
    const common::ObString &end_str)
{
  int ret = OB_SUCCESS;

  ObString res_val;
  int64_t start = 0;
  int64_t end = 0;
  if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(start_str, start))) {
    RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
    LOG_WARN("start_str is not an integer or out of range", K(ret), K(start_str));
  } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(end_str, end))) {
    RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
    LOG_WARN("end_str is not an integer or out of range", K(ret), K(end_str));
  } else {
    ObITableEntity *entity = nullptr;
    ObString value;
    int64_t expire_ts = OB_INVALID_TIMESTAMP;  // unused
    bool is_exist = true;
    if (OB_FAIL(get_value(db, key, value, expire_ts, is_exist))) {
      LOG_WARN("fail to get value", K(ret), K(db), K(key));
    } else if (is_exist && OB_FAIL(get_range_value(value, start, end, res_val))) {
      LOG_WARN("fail to get range value", K(ret), K(value), K(value));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_bulk_string(res_val))) {
      LOG_WARN("fail to set res", K(ret), K(res_val));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int StringCommandOperator::do_incr_by_float(int64_t db, const common::ObString &key, const common::ObString &incr_str)
{
  int ret = OB_SUCCESS;

  ObString res_val;
  long double incr = 0.0;
  long double old_val = 0.0;
  ObITableEntity *entity = nullptr;
  int64_t expire_ts = OB_INVALID_TIMESTAMP;
  ObString old_val_str;
  bool is_exist = true;
  if (OB_FAIL(ObRedisHelper::string_to_long_double(incr_str, incr))) {
    RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::FLOAT_ERR);
    LOG_WARN("incr_str is not a float", K(ret), K(incr_str));
  } else if (OB_FAIL(get_value(db, key, old_val_str, expire_ts, is_exist))) {
    LOG_WARN("fail to get old value", K(ret), K(db), K(key));
  } else if (OB_FAIL(ObRedisHelper::string_to_long_double(old_val_str, old_val))) {
    RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::FLOAT_ERR);
    LOG_WARN("old_val_str is not a float", K(ret), K(old_val_str));
  }

  if (OB_SUCC(ret)) {
    long double new_val = old_val + incr;
    if (OB_FAIL(ObRedisHelper::long_double_to_string(op_temp_allocator_, new_val, res_val))) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::FLOAT_ERR);
      LOG_WARN("new_val_str is not a float", K(ret), K(res_val));
    } else if (OB_FAIL(build_key_value_expire_entity(db, key, res_val, expire_ts, entity))) {
      LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key));
    } else {
      ObTableOperation op = put_or_insup(*entity);
      ObTableOperationResult result;
      ObITableEntity *res_entity = nullptr;
      if (OB_FAIL(process_table_single_op(op, result, nullptr/*meta*/, RedisOpFlags::RETURN_AFFECTED_ROWS))) {
        LOG_WARN("fail to process table increment", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_bulk_string(res_val))) {
      LOG_WARN("fail to set response int", K(ret), K(res_val));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}
int StringCommandOperator::do_mget(int64_t db, const common::ObIArray<common::ObString> &keys)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> res_values(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(op_temp_allocator_, "RedisMget"));
  ObTableBatchOperation batch_op;
  if (OB_FAIL(res_values.reserve(keys.count()))) {
    LOG_WARN("fail to reserve array", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
      const ObString &key = keys.at(i);
      ObITableEntity *entity = nullptr;
      if (OB_FAIL(build_rowkey_entity(db, key, entity))) {
        LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key));
      } else if (OB_FAIL(batch_op.retrieve(*entity))) {
        LOG_WARN("fail to add get op", K(ret), KPC(entity));
      }
    }
  }

  ResultFixedArray batch_res(op_temp_allocator_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(process_table_batch_op(batch_op, batch_res))) {
      LOG_WARN("fail to process table batch op", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < batch_res.count(); ++i) {
        ObString val;
        if (batch_res[i].get_return_rows() > 0) {
          ObITableEntity *res_entity = nullptr;
          if (OB_FAIL(batch_res[i].get_entity(res_entity))) {
            LOG_WARN("fail to get entity", K(ret), K(batch_res[i]));
          } else if (OB_FAIL(get_value_from_entity(*res_entity, val))) {
            LOG_WARN("fail to get value from entity", K(ret), KPC(res_entity));
          }
        } else {
          // push back empty ObString, RedisEncoder will encodes to '(nil)'
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(res_values.push_back(val))) {
            LOG_WARN("fail to push back value", K(ret), K(val));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_array(res_values))) {
      LOG_WARN("fail to set response int", K(ret), K(res_values));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int StringCommandOperator::del_key(int64_t db, const common::ObString &key, bool& is_exist)
{
  int ret = OB_SUCCESS;
  ObITableEntity *entity = nullptr;
  if (OB_FAIL(build_string_rowkey_entity(db, key, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key));
  } else {
    ObTableOperation op = ObTableOperation::del(*entity);
    ObTableOperationResult result;
    ObITableEntity *res_entity = nullptr;
    if (OB_FAIL(process_table_single_op(op, result))) {
      LOG_WARN("fail to process table get", K(ret));
    } else {
      is_exist = result.get_affected_rows() > 0;
      redis_ctx_.entity_factory_->free(entity);
    }
  }
  return ret;
}

int StringCommandOperator::do_mset(int64_t db, const RedisCommand::FieldValMap &field_val_map)
{
  int ret = OB_SUCCESS;
  ObTableBatchOperation batch_op;
  int64_t expire_ts = OB_INVALID_TIMESTAMP;

  for (RedisCommand::FieldValMap::const_iterator iter = field_val_map.begin();
        OB_SUCC(ret) && iter != field_val_map.end();
        ++iter) {
    ObITableEntity *entity = nullptr;
    if (OB_FAIL(build_key_value_expire_entity(db, iter->first, iter->second, expire_ts, entity))) {
      LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(iter->first), K(iter->second));
    } else if (OB_FAIL(put_or_insup(*entity, batch_op))) {
      LOG_WARN("fail to add insup op", K(ret), KPC(entity));
    }
  }

  ResultFixedArray batch_res(op_temp_allocator_);
  if (OB_SUCC(ret) && OB_FAIL(process_table_batch_op(batch_op, batch_res))) {
    LOG_WARN("fail to process table batch op", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_fmt_res(ObRedisFmt::OK))) {
      LOG_WARN("fail to set response int", K(ret));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int StringCommandOperator::do_set(int64_t db, const common::ObString &key, const SetArg &set_arg)
{
  int ret = OB_SUCCESS;

  int res_val = 0;
  int64_t now_tus = ObTimeUtility::current_time();
  int64_t expire_tm = OB_INVALID_TIMESTAMP;
  if (!set_arg.expire_str_.empty()) {
    if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(set_arg.expire_str_, expire_tm))) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
      LOG_WARN("invalid expire time", K(ret), K(set_arg.expire_str_));
    } else if (expire_tm <= 0) {
      const ObString &cmd_name = reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).request_.get_cmd_name();
      SPRINTF_REDIS_ERROR(
          redis_ctx_.allocator_,
          fmt_redis_msg_,
          ObRedisErr::EXPIRE_TIME_CMD_ERR,
          cmd_name.length(),
          cmd_name.ptr());
      LOG_WARN("invalid expire time", K(ret), K(set_arg.expire_str_));
    } else if (set_arg.is_ms_) {
      // ms to us
      expire_tm = now_tus + common::msec_to_usec(expire_tm);
    } else {
      // s to us
      expire_tm = now_tus + common::sec_to_usec(expire_tm);
    }
  }

  if (OB_SUCC(ret)) {
    ObITableEntity *entity = nullptr;
    if (OB_FAIL(build_key_value_expire_entity(db, key, set_arg.value_, expire_tm, entity))) {
      LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key));
    } else {
      ObTableOperation op;
      if (set_arg.nx_) {
        op = ObTableOperation::insert(*entity);
      } else {
        op = put_or_insup(*entity);
      }
      ObTableOperationResult result;
      if (OB_FAIL(process_table_single_op(op, result))) {
        if (set_arg.nx_ && OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to process table insert", K(ret));
        }
      } else {
        res_val = 1;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (set_arg.nx_) {
      if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(res_val))) {
        LOG_WARN("fail to set response int", K(ret), K(res_val));
      }
    } else if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_fmt_res(ObRedisFmt::OK))) {
      LOG_WARN("fail to set response res", K(ret), K(res_val));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int StringCommandOperator::set_range(
    const common::ObString &old_val, int64_t offset, const common::ObString &value, common::ObString &new_val)
{
  int ret = OB_SUCCESS;

  if (old_val.length() < offset + value.length()) {
    int64_t new_len = offset + value.length();
    char *new_buf = nullptr;
    if (OB_FAIL(ObRedisHelper::check_string_length(offset, value.length()))) {
      LOG_WARN("fail to check string length", K(ret), K(offset), K(value.length()));
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::MAXIMUM_ERR);
    } else if (OB_ISNULL(new_buf = static_cast<char *>(redis_ctx_.allocator_.alloc(sizeof(char) * new_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc empty value", K(ret));
    } else {
      MEMSET(new_buf, 0, new_len);
      if (offset < old_val.length()) {
        MEMCPY(new_buf, old_val.ptr(), offset);
      } else {
        MEMCPY(new_buf, old_val.ptr(), old_val.length());
      }
      MEMCPY(new_buf + offset, value.ptr(), value.length());
      new_val = ObString(new_len, new_buf);
    }
  } else {
    new_val = old_val;
    MEMCPY(new_val.ptr() + offset, value.ptr(), value.length());
  }

  return ret;
}

int StringCommandOperator::do_setrange(
    int64_t db, const common::ObString &key, const common::ObString &offset_str, const common::ObString &value)
{
  int ret = OB_SUCCESS;

  ObString new_val;
  int64_t offset = 0;
  if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(offset_str, offset))) {
    RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
    LOG_WARN("invalid offset str", K(ret), K(offset_str));
  } else if (offset < 0) {
    RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::OFFSET_OUT_RANGE_ERR);
  } else if (offset > MAX_RANGE) {
    RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::MAXIMUM_ERR);
  } else {
    ObString old_val;
    int64_t expire_tm = OB_INVALID_TIMESTAMP;  // unused
    bool is_exist = true;
    if (OB_FAIL(get_value(db, key, old_val, expire_tm, is_exist))) {
      LOG_WARN("fail to get value", K(ret), K(db), K(key));
    } else if (value.empty()) {
      new_val = old_val;
    } else if (OB_FAIL(set_range(old_val, offset, value, new_val))) {
      LOG_WARN("fail to set range", K(ret), K(old_val), K(offset), K(value));
    } else {
      ObITableEntity *entity = nullptr;
      if (OB_FAIL(build_key_value_expire_entity(db, key, new_val, expire_tm, entity))) {
        LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key), K(new_val), K(expire_tm));
      } else {
        ObTableOperation op = put_or_insup(*entity);
        ObTableOperationResult result;
        if (OB_FAIL(process_table_single_op(op, result))) {
          LOG_WARN("fail to process table put", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(new_val.length()))) {
      LOG_WARN("fail to set response", K(ret), K(new_val));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}
int StringCommandOperator::do_strlen(int64_t db, const common::ObString &key)
{
  int ret = OB_SUCCESS;

  ObString val;
  int64_t expire_tm = OB_INVALID_TIMESTAMP;  // unused
  bool is_exist = true;
  if (OB_FAIL(get_value(db, key, val, expire_tm, is_exist))) {
    if (ObRedisErr::is_redis_error(ret)) {
      RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
    }
    LOG_WARN("fail to get value", K(ret), K(db), K(key));
    reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.return_table_error(ret);
  } else if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(val.length()))) {
    LOG_WARN("fail to set response", K(ret), K(val));
  }

  return ret;
}

int StringCommandOperator::inner_do_group_get(const ObString &prop_name, ResultFixedArray &batch_res)
{
  int ret = OB_SUCCESS;
  ObRedisBatchCtx &group_ctx = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_);
  ObTableBatchOperation batch_op;
  group_ctx.entity_factory_ = &op_entity_factory_;

  for (int i = 0; OB_SUCC(ret) && i < group_ctx.ops().count(); ++i) {
    ObRedisOp *op = reinterpret_cast<ObRedisOp*>(group_ctx.ops().at(i));
    if (OB_ISNULL(op) || OB_ISNULL(op->cmd())) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null op", K(ret), KP(op), KP(op->cmd()));
    } else {
      RedisCommand *cmd = reinterpret_cast<RedisCommand*>(op->cmd());
      ObITableEntity *entity = nullptr;
      redis_ctx_.entity_factory_ = &op->default_entity_factory_;
      if (OB_FAIL(build_string_rowkey_entity(op->db(), cmd->key(), entity))) {
        LOG_WARN("fail to build rowkey entity", K(ret), K(op->db()), K(cmd->key()));
      } else if (!prop_name.empty()) {
        if(OB_FAIL(entity->add_retrieve_property(prop_name))) {
          LOG_WARN("fail to add retrive property", K(ret), KPC(entity));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(batch_op.retrieve(*entity))) {
        LOG_WARN("fail to add get op", K(ret), KPC(entity));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObRedisBatchCtx &group_ctx = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_);
    if (OB_FAIL(init_tablet_ids_by_ops(group_ctx.ops()))) {
      LOG_WARN("fail to init tablet ids by ops", K(ret));
    } else if (OB_FAIL(process_table_batch_op(
            batch_op, batch_res, nullptr, RedisOpFlags::NONE, &op_temp_allocator_, &op_entity_factory_, &tablet_ids_))) {
      LOG_WARN("fail to process table batch op", K(ret));
    }
  }
  return ret;
}

int StringCommandOperator::do_group_get()
{
  int ret = OB_SUCCESS;
  ResultFixedArray batch_res(op_temp_allocator_);
  if (OB_FAIL(inner_do_group_get(ObRedisUtil::VALUE_PROPERTY_NAME, batch_res))) {
    LOG_WARN("fail to do inner group get", K(ret));
  } else if (OB_FAIL(reply_batch_res(batch_res))) {
    LOG_WARN("fail to reply batch res", K(ret));
  }
  return ret;
}

int StringCommandOperator::do_group_set()
{
  int ret = OB_SUCCESS;
  int64_t expire_tm = OB_INVALID_TIMESTAMP;
  ObRedisBatchCtx &group_ctx = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_);
  ObTableBatchOperation batch_op;
  group_ctx.entity_factory_ = &op_entity_factory_;
  tablet_ids_.reuse();
  if (OB_FAIL(tablet_ids_.reserve(group_ctx.ops().count()))) {
    LOG_WARN("fail to reserve tablet ids", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < group_ctx.ops().count(); ++i) {
    ObRedisOp *op = reinterpret_cast<ObRedisOp*>(group_ctx.ops().at(i));
    if (OB_ISNULL(op) || OB_ISNULL(op->cmd())) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null op", K(ret), KP(op), KP(op->cmd()));
    } else {
      redis_ctx_.entity_factory_ = &op->default_entity_factory_;
      Set *set = reinterpret_cast<Set*>(op->cmd());
      ObITableEntity *entity = nullptr;
      if (OB_FAIL(build_key_value_expire_entity(op->db(), set->key(), set->value(), expire_tm, entity))) {
        LOG_WARN("fail to build rowkey entity", K(ret), K(op->db()), K(set->key()), K(set->value()));
      } else if (OB_FAIL(put_or_insup(*entity, batch_op))) {
        LOG_WARN("fail to add get op", K(ret), KPC(entity));
      } else if (OB_FAIL(tablet_ids_.push_back(op->tablet_id()))) {
        LOG_WARN("fail to push back tablet_id", K(ret), K(op->tablet_id()));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ResultFixedArray batch_res(op_temp_allocator_);
    if (OB_FAIL(process_table_batch_op(
          batch_op, batch_res, nullptr, RedisOpFlags::NONE, &op_temp_allocator_, &op_entity_factory_, &tablet_ids_))) {
      LOG_WARN("fail to process table batch op", K(ret));
    }
  }

  for (int i = 0; OB_SUCC(ret) && i < group_ctx.ops().count(); ++i) {
    ObRedisOp *op = reinterpret_cast<ObRedisOp*>(group_ctx.ops().at(i));
    if (OB_FAIL(op->response().set_fmt_res(ObRedisFmt::OK))) {
      LOG_WARN("fail to set bulk string", K(ret));
    }
  }
  return ret;
}

int StringCommandOperator::do_group_incr()
{
  int ret = OB_SUCCESS;
  // 0. init
  ObRedisBatchCtx &group_ctx = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_);
  using ValExpPair = std::pair<int64_t, int64_t>; // <string value, string expire ts>
  using KVMap = hash::ObHashMap<RedisKeyNode, ValExpPair, common::hash::NoPthreadDefendMode>;
  KVMap kv_map;
  ResultFixedArray batch_res(op_temp_allocator_);
  if (OB_FAIL(kv_map.create(group_ctx.ops().count(), "RedisGpIncr"))) {
    LOG_WARN("failed to create fd hash map", K(ret));
  } else if (OB_FAIL(inner_do_group_get("", batch_res))) {
    LOG_WARN("fail to do inner group get", K(ret));
  } else if (batch_res.count() != group_ctx.ops().count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch res count should be equal to ops count",
      K(ret), K(batch_res.count()), K(group_ctx.ops().count()));
  }

  // 1. calc new_val and reply
  ObString old_str;
  int64_t old_value = 0;
  int64_t expire_ts = OB_INVALID_TIMESTAMP;
  ValExpPair old_pair;
  for (int i = 0; OB_SUCC(ret) && i < group_ctx.ops().count(); ++i) {
    old_value = 0;
    ObRedisOp *op = reinterpret_cast<ObRedisOp*>(group_ctx.ops().at(i));
    if (OB_ISNULL(op) || OB_ISNULL(op->cmd())) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null op", K(ret), KP(op), KP(op->cmd()));
    } else {
      IncrBy *cmd = reinterpret_cast<IncrBy*>(op->cmd());
      RedisKeyNode node(op->db(), cmd->key(), op->tablet_id());
      // 1.1 get old val
      int tmp_ret = kv_map.get_refactored(node, old_pair);
      old_value = old_pair.first;
      if (tmp_ret == OB_HASH_NOT_EXIST) {
        if (batch_res[i].get_return_rows() == 0) {
          // not exists, old_value = 0
        } else {
          old_str.reset();
          const ObITableEntity *res_entity = nullptr;
          if (OB_FAIL(batch_res[i].get_entity(res_entity))) {
            LOG_WARN("fail to get entity", K(ret), K(batch_res[i]));
          } else if (OB_FAIL(get_varbinary_from_entity(
                        *res_entity, ObRedisUtil::VALUE_PROPERTY_NAME, old_str))) {
            LOG_WARN("fail to get value from entity", K(ret), KPC(res_entity));
          } else if (OB_FAIL(get_expire_from_entity(*res_entity, expire_ts))) {
            LOG_WARN("fail to get old value from result entity", K(ret), KPC(res_entity));
          } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(old_str, old_value))) {
            RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
            LOG_WARN("invalid int from str", K(ret), K(old_str));
          }
        }
      } else if (tmp_ret != OB_SUCCESS) {
        ret = tmp_ret;
        LOG_WARN("fail to get map", K(ret), K(node));
      }

      // 1.2 get new val
      int64_t new_val = old_value;
      if (OB_SUCC(ret)) {
        int64_t cur_incr_num = 0;
        if (OB_FAIL(convert_incr_str_to_int(cmd->incr(), cmd->is_incr(), cur_incr_num))) {
          LOG_WARN("fail to convert incr str to int", K(ret), K(cmd->incr()), K(cmd->is_incr()));
        } else if (is_incr_out_of_range(old_value, cur_incr_num)) {
          RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INCR_OVERFLOW);
          LOG_WARN("increment or decrement would overflow", K(ret), K(old_value), K(cur_incr_num));
        } else {
          new_val += cur_incr_num;
        }
      }

      // 1.3 set new_val
      if (OB_SUCC(ret)) {
        ValExpPair new_pair(new_val, expire_ts);
        if (OB_FAIL(kv_map.set_refactored(node, new_pair, 1 /*conver exists key*/))) {
          LOG_WARN("fail to do set refactored", K(ret), K(cmd->key()));
        } else if (OB_FAIL(op->response().set_res_int(new_val))) {
          LOG_WARN("fail to set bulk string", K(ret), K(new_val));
        }
      } else if (ObRedisErr::is_redis_error(ret)) {
        RESPONSE_REDIS_ERROR(op->response(), fmt_redis_msg_.ptr());
        ret = COVER_REDIS_ERROR(ret);
      }
    }
  }

  // 2. do batch insup
  ObTableBatchOperation batch_op;
  tablet_ids_.reuse();
  char *ptr = nullptr;
  int64_t size = ObFastFormatInt::MAX_DIGITS10_STR_SIZE * group_ctx.ops().count();
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ptr = static_cast<char*>(op_temp_allocator_.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(size));
  }

  int64_t idx = 0;
  for (KVMap::const_iterator i = kv_map.begin(); OB_SUCC(ret) && i != kv_map.end(); ++i, ++idx) {
    RedisKeyNode node = i->first;
    ObITableEntity *entity = nullptr;
    char *addr = ptr + idx * ObFastFormatInt::MAX_DIGITS10_STR_SIZE;
    int64_t len = ObFastFormatInt::format_signed(i->second.first, addr);
    ObString val_str(len, addr);
    if (OB_FAIL(build_key_value_expire_entity(node.db_, node.key_, val_str, i->second.second, entity))) {
      LOG_WARN("fail to build rowkey entity", K(ret), K(node), K(val_str));
    } else if (OB_FAIL(put_or_insup(*entity, batch_op))) {
      LOG_WARN("fail to add get op", K(ret), KPC(entity));
    } else if (OB_FAIL(tablet_ids_.push_back(node.tablet_id_))) {
      LOG_WARN("fail to push back tablet_id", K(ret), K(node.tablet_id_));
    }
  }

  if (OB_SUCC(ret)) {
    ResultFixedArray batch_res(op_temp_allocator_);
    if (OB_FAIL(process_table_batch_op(
          batch_op, batch_res, nullptr, RedisOpFlags::NONE, &op_temp_allocator_, &op_entity_factory_, &tablet_ids_))) {
      LOG_WARN("fail to process table batch op", K(ret));
    }
  }
  return ret;
}

int StringCommandOperator::do_group_incrbyfloat()
{
  int ret = OB_SUCCESS;
  // 0. init
  ObRedisBatchCtx &group_ctx = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_);
  using ValExpPair = std::pair<long double, int64_t>; // <string value, string expire ts>
  using KVMap = hash::ObHashMap<RedisKeyNode, ValExpPair, common::hash::NoPthreadDefendMode>;
  KVMap kv_map;
  ResultFixedArray batch_res(op_temp_allocator_);
  if (OB_FAIL(kv_map.create(group_ctx.ops().count(), "RedisGpIncr"))) {
    LOG_WARN("failed to create fd hash map", K(ret));
  } else if (OB_FAIL(inner_do_group_get("", batch_res))) {
    LOG_WARN("fail to do inner group get", K(ret));
  } else if (batch_res.count() != group_ctx.ops().count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch res count should be equal to ops count",
      K(ret), K(batch_res.count()), K(group_ctx.ops().count()));
  }

  // 1. calc new_val and reply
  ObString old_str;
  long double old_value = 0;
  int64_t expire_ts = OB_INVALID_TIMESTAMP;
  ValExpPair old_pair;
  for (int i = 0; OB_SUCC(ret) && i < group_ctx.ops().count(); ++i) {
    old_value = 0;
    ObRedisOp *op = reinterpret_cast<ObRedisOp*>(group_ctx.ops().at(i));
    if (OB_ISNULL(op) || OB_ISNULL(op->cmd())) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null hget op", K(ret), KP(op), KP(op->cmd()));
    } else {
      IncrBy *cmd = reinterpret_cast<IncrBy*>(op->cmd());
      RedisKeyNode node(op->db(), cmd->key(), op->tablet_id_);
      // 1.1 get old val
      int tmp_ret = kv_map.get_refactored(node, old_pair);
      old_value = old_pair.first;
      if (tmp_ret == OB_HASH_NOT_EXIST) {
        if (batch_res[i].get_return_rows() == 0) {
          // not exists, old_value = 0
        } else {
          old_str.reset();
          const ObITableEntity *res_entity = nullptr;
          if (OB_FAIL(batch_res[i].get_entity(res_entity))) {
            LOG_WARN("fail to get entity", K(ret), K(batch_res[i]));
          } else if (OB_FAIL(get_varbinary_from_entity(
                        *res_entity, ObRedisUtil::VALUE_PROPERTY_NAME, old_str))) {
            LOG_WARN("fail to get value from entity", K(ret), KPC(res_entity));
          } else if (OB_FAIL(get_expire_from_entity(*res_entity, expire_ts))) {
            LOG_WARN("fail to get old value from result entity", K(ret), KPC(res_entity));
          } else if (OB_FAIL(ObRedisHelper::string_to_long_double(old_str, old_value))) {
            RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::FLOAT_ERR);
            LOG_WARN("invalid int from str", K(ret), K(old_str));
          }
        }
      } else if (tmp_ret != OB_SUCCESS) {
        ret = tmp_ret;
        LOG_WARN("fail to get map", K(ret), K(node));
      }

      // 1.2 get new val
      long double new_val = old_value;
      if (OB_SUCC(ret)) {
        long double cur_incr_num = 0;
        if (OB_FAIL(ObRedisHelper::string_to_long_double(cmd->incr(), cur_incr_num))) {
          LOG_WARN("fail to convert incr str to int", K(ret), K(cmd->incr()), K(cmd->is_incr()));
        } else if (is_incrby_out_of_range(old_value, cur_incr_num)) {
          RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::FLOAT_ERR);
          LOG_WARN("increment or decrement would overflow", K(ret));
        } else {
          new_val += cur_incr_num;
        }
      }

      // 1.3 set new_val
      if (OB_SUCC(ret)) {
        ObString reply_str;
        ValExpPair new_pair(new_val, expire_ts);
        if (OB_FAIL(kv_map.set_refactored(node, new_pair, 1 /*conver exists key*/))) {
          LOG_WARN("fail to do set refactored", K(ret), K(cmd->key()));
        } else if (OB_FAIL(ObRedisHelper::long_double_to_string(op_temp_allocator_, new_val, reply_str))) {
          RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::FLOAT_ERR);
          LOG_WARN("new_val_str is not a float", K(ret), K(reply_str));
        } else if (OB_FAIL(op->response().set_res_bulk_string(reply_str))) {
          LOG_WARN("fail to set bulk string", K(ret));
        }
      } else if (ObRedisErr::is_redis_error(ret)) {
        RESPONSE_REDIS_ERROR(op->response(), fmt_redis_msg_.ptr());
        ret = COVER_REDIS_ERROR(ret);
      }
    }
  }

  // 2. do batch insup
  ObTableBatchOperation batch_op;
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(tablet_ids_.reuse())) {
  } else if (OB_FAIL(tablet_ids_.reserve(group_ctx.ops().count()))) {
    LOG_WARN("fail to reserve tablet ids", K(ret));
  }
  int64_t idx = 0;
  for (KVMap::const_iterator i = kv_map.begin(); OB_SUCC(ret) && i != kv_map.end(); ++i, ++idx) {
    RedisKeyNode node = i->first;
    ObITableEntity *entity = nullptr;
    ObString val_str;
    if (OB_FAIL(ObRedisHelper::long_double_to_string(op_temp_allocator_, i->second.first, val_str))) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::FLOAT_ERR);
      LOG_WARN("new_val_str is not a float", K(ret), K(val_str));
    } else if (OB_FAIL(build_key_value_expire_entity(node.db_, node.key_, val_str, i->second.second, entity))) {
      LOG_WARN("fail to build rowkey entity", K(ret), K(node), K(val_str));
    } else if (OB_FAIL(put_or_insup(*entity, batch_op))) {
      LOG_WARN("fail to add get op", K(ret), KPC(entity));
    } else if (OB_FAIL(tablet_ids_.push_back(node.tablet_id_))) {
      LOG_WARN("fail to push back tablet_id", K(ret), K(node.tablet_id_));
    }
  }

  if (OB_SUCC(ret)) {
    ResultFixedArray batch_res(op_temp_allocator_);
    if (OB_FAIL(process_table_batch_op(
          batch_op, batch_res, nullptr, RedisOpFlags::NONE, &op_temp_allocator_, &op_entity_factory_, &tablet_ids_))) {
      LOG_WARN("fail to process table batch op", K(ret));
    }
  }
  return ret;
}

int StringCommandOperator::do_group_setnx()
{
  int ret = OB_SUCCESS;
  ObRedisBatchCtx &group_ctx = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_);
  ObTableBatchOperation batch_op;
  group_ctx.entity_factory_ = &op_entity_factory_;

  for (int i = 0; OB_SUCC(ret) && i < group_ctx.ops().count(); ++i) {
    ObRedisOp *op = reinterpret_cast<ObRedisOp*>(group_ctx.ops().at(i));
    if (OB_ISNULL(op) || OB_ISNULL(op->cmd())) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null hget op", K(ret), KP(op), KP(op->cmd()));
    } else {
      Set *cmd = reinterpret_cast<Set*>(op->cmd());
      ObITableEntity *entity = nullptr;
      int64_t expire_tm = OB_INVALID_TIMESTAMP;
      if (OB_FAIL(build_key_value_expire_entity(op->db(), cmd->key(), cmd->value(), expire_tm, entity))) {
        LOG_WARN("fail to build rowkey entity", K(ret), K(op->db()), K(cmd->key()));
      } else if (OB_FAIL(batch_op.insert(*entity))) {
        LOG_WARN("fail to insert entity", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ResultFixedArray batch_res(op_temp_allocator_);
    if (OB_FAIL(init_tablet_ids_by_ops(group_ctx.ops()))) {
      LOG_WARN("fail to init tablet ids by ops", K(ret));
    } else if (OB_FAIL(process_table_batch_op(
            batch_op,
            batch_res,
            nullptr,
            RedisOpFlags::BATCH_NOT_ATOMIC,
            &op_temp_allocator_,
            &op_entity_factory_,
            &tablet_ids_))) {
      LOG_WARN("fail to process table batch op", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < group_ctx.ops().count(); ++i) {
      ObRedisOp *op = reinterpret_cast<ObRedisOp*>(group_ctx.ops().at(i));
      int batch_ret = batch_res.at(i).get_errno();
      int reply = 0;
      if (batch_ret == OB_SUCCESS) {
        reply = 1;
      } else if (batch_ret == OB_ERR_PRIMARY_KEY_DUPLICATE) {
        reply = 0;
      } else {
        ret = batch_ret;
        LOG_WARN("fail to do batch insert", K(ret));
      }
      if (OB_SUCC(ret) && OB_FAIL(op->response().set_res_int(reply))) {
        LOG_WARN("fail to set bulk string", K(ret), K(reply));
      }
    }
  }
  return ret;
}

int StringCommandOperator::do_group_append()
{
  int ret = OB_SUCCESS;
  ObRedisBatchCtx &group_ctx = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_);
  ObTableBatchOperation batch_op;
  group_ctx.entity_factory_ = &op_entity_factory_;
  tablet_ids_.reuse();
  if (OB_FAIL(tablet_ids_.reserve(group_ctx.ops().count()))) {
    LOG_WARN("fail to reserve tablet ids", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < group_ctx.ops().count(); ++i) {
    ObRedisOp *op = reinterpret_cast<ObRedisOp*>(group_ctx.ops().at(i));
    if (OB_ISNULL(op) || OB_ISNULL(op->cmd())) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null hget op", K(ret), KP(op), KP(op->cmd()));
    } else {
      Append *cmd = reinterpret_cast<Append*>(op->cmd());
      ObITableEntity *entity = nullptr;
      if (OB_FAIL(build_key_value_entity(op->db(), cmd->key(), cmd->val_, entity))) {
        LOG_WARN("fail to build rowkey entity", K(ret), K(op->db()), K(cmd->key()), K(cmd->val_));
      } else if (OB_FAIL(batch_op.append(*entity))) {
        LOG_WARN("fail to add get op", K(ret), KPC(entity));
      } else if (OB_FAIL(tablet_ids_.push_back(op->tablet_id_))) {
        LOG_WARN("fail to push back tablet_id", K(ret), K(op->tablet_id_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ResultFixedArray batch_res(op_temp_allocator_);
    if (OB_FAIL(process_table_batch_op(
          batch_op, batch_res, nullptr, RedisOpFlags::RETURN_AFFECTED_ENTITY, &op_temp_allocator_, &op_entity_factory_, &tablet_ids_))) {
      LOG_WARN("fail to process table batch op", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < group_ctx.ops().count(); ++i) {
      ObRedisOp *op = reinterpret_cast<ObRedisOp*>(group_ctx.ops().at(i));
      ObITableEntity *res_entity = nullptr;
      ObString new_value;
      if (OB_FAIL(batch_res.at(i).get_entity(res_entity))) {
        LOG_WARN("fail to get entity", K(ret), K(batch_res.at(i)));
      } else if (OB_FAIL(get_value_from_entity(*res_entity, new_value))) {
        LOG_WARN("fail to get new_value from result entity", K(ret), KPC(res_entity));
      } else if (OB_FAIL(op->response().set_res_int(new_value.length()))) {
        LOG_WARN("fail to set bulk string", K(ret));
      }
    }
  }

  return ret;
}

int StringCommandOperator::analyze_getbit(ObRedisOp *op, ObTableOperationResult &res)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op) || OB_ISNULL(op->cmd())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null hget op", K(ret), KP(op), KP(op->cmd()));
  } else {
    GetBit *cmd = reinterpret_cast<GetBit*>(op->cmd());
    int64_t offset = 0;
    int64_t res_bit = 0;
    if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(cmd->offset_, offset) || offset < 0)) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::BIT_OFFSET_INTEGER_ERR);
      LOG_WARN("offset_str is not an integer or out of range", K(ret), K(cmd->offset_));
    } else if (res.get_return_rows() > 0) {
      ObITableEntity *res_entity = nullptr;
      ObString value;
      if (OB_FAIL(res.get_entity(res_entity))) {
        LOG_WARN("fail to get entity", K(ret), K(res));
      } else if (OB_FAIL(get_value_from_entity(*res_entity, value))) {
        LOG_WARN("fail to get new_value from result entity", K(ret), KPC(res_entity));
      } else if (OB_FAIL(get_bit_at_pos(value, offset, res_bit))) {
        LOG_WARN("fail to get bit at pos", K(ret), K(value), K(offset));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(op->response().set_res_int(res_bit))) {
        LOG_WARN("fail to set bulk string", K(ret));
      }
    }
  }
  return ret;
}

int StringCommandOperator::analyze_getrange(ObRedisOp *op, ObTableOperationResult &res)
{
  int ret = OB_SUCCESS;
  int64_t start = 0;
  int64_t end = -1;
  int64_t res_bit_count = 0;
  if (OB_ISNULL(op) || OB_ISNULL(op->cmd())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null hget op", K(ret), KP(op), KP(op->cmd()));
  } else {
    GetRange *cmd = reinterpret_cast<GetRange*>(op->cmd());
    ObITableEntity *res_entity = nullptr;
    ObString value;
    ObString res_val;
    if (!cmd->start_.empty() && OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(cmd->start_, start))) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
      LOG_WARN("start_str is not an integer or out of range", K(ret), K(cmd->start_));
    } else if (!cmd->end_.empty() && OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(cmd->end_, end))) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
      LOG_WARN("end_str is not an integer or out of range", K(ret), K(cmd->end_));
    } else if (res.get_return_rows() > 0) {
      if (OB_FAIL(res.get_entity(res_entity))) {
        LOG_WARN("fail to get entity", K(ret), K(res));
      } else if (OB_FAIL(get_value_from_entity(*res_entity, value))) {
        LOG_WARN("fail to get new_value from result entity", K(ret), KPC(res_entity));
      } else if (OB_FAIL(get_range_value(value, start, end, res_val))) {
        LOG_WARN("fail to get range value", K(ret), K(value), K(value));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(op->response().set_res_bulk_string(res_val))) {
        LOG_WARN("fail to set bulk string", K(ret));
      }
    }
  }
  return ret;
}

int StringCommandOperator::analyze_strlen(ObRedisOp *op, ObTableOperationResult &res)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op) || OB_ISNULL(op->cmd())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null hget op", K(ret), KP(op), KP(op->cmd()));
  } else {
    StrLen *cmd = reinterpret_cast<StrLen*>(op->cmd());
    ObString value;
    if (res.get_return_rows() > 0) {
      ObITableEntity *res_entity = nullptr;
      int64_t res_bit = 0;
      if (OB_FAIL(res.get_entity(res_entity))) {
        LOG_WARN("fail to get entity", K(ret), K(res));
      } else if (OB_FAIL(get_value_from_entity(*res_entity, value))) {
        LOG_WARN("fail to get new_value from result entity", K(ret), KPC(res_entity));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(op->response().set_res_int(value.length()))) {
        LOG_WARN("fail to set bulk string", K(ret));
      }
    }
  }
  return ret;
}

int StringCommandOperator::analyze_bitcount(ObRedisOp *op, ObTableOperationResult &res)
{
  int ret = OB_SUCCESS;
  int64_t start = 0;
  int64_t end = -1;
  int64_t res_bit_count = 0;
  if (OB_ISNULL(op) || OB_ISNULL(op->cmd())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null hget op", K(ret), KP(op), KP(op->cmd()));
  } else {
    BitCount *cmd = reinterpret_cast<BitCount*>(op->cmd());
    ObITableEntity *res_entity = nullptr;
    ObString value;
    if (!cmd->start_.empty() && OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(cmd->start_, start))) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
      LOG_WARN("start_str is not an integer or out of range", K(ret), K(cmd->start_));
    } else if (!cmd->end_.empty() && OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(cmd->end_, end))) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
      LOG_WARN("end_str is not an integer or out of range", K(ret), K(cmd->end_));
    } else if (res.get_return_rows() > 0) {
      if (OB_FAIL(res.get_entity(res_entity))) {
        LOG_WARN("fail to get entity", K(ret), K(res));
      } else if (OB_FAIL(get_value_from_entity(*res_entity, value))) {
        LOG_WARN("fail to get new_value from result entity", K(ret), KPC(res_entity));
      } else if (OB_FAIL(get_bit_count(value, start, end, res_bit_count))) {
        LOG_WARN("fail to get bit count", K(ret), K(value));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(op->response().set_res_int(res_bit_count))) {
        LOG_WARN("fail to set bulk string", K(ret));
      }
    }
  }
  return ret;
}

int StringCommandOperator::do_group_analyze(int (StringCommandOperator::*analyze_func)(ObRedisOp *op, ObTableOperationResult &),
                                            StringCommandOperator *obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(analyze_func) || OB_ISNULL(obj)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null analyze func or obj", K(ret), KP(obj));
  } else {
    ObRedisBatchCtx &group_ctx = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_);
    ResultFixedArray batch_res(op_temp_allocator_);
    if (OB_FAIL(inner_do_group_get(ObRedisUtil::VALUE_PROPERTY_NAME, batch_res))) {
      LOG_WARN("fail to do inner group get", K(ret));
    } else if (batch_res.count() != group_ctx.ops().count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("batch res count should be equal to ops count",
        K(ret), K(batch_res.count()), K(group_ctx.ops().count()));
    }

    // 1. calc new_val and reply
    for (int i = 0; OB_SUCC(ret) && i < batch_res.count(); ++i) {
      ObRedisOp *op = reinterpret_cast<ObRedisOp*>(group_ctx.ops().at(i));
      if (OB_FAIL((obj->*analyze_func)(op, batch_res.at(i)))) {
        LOG_WARN("fail to do analyze func", K(ret), K(i), KPC(op));
      }
    }
  }
  return ret;
}

}  // namespace table
}  // namespace oceanbase
