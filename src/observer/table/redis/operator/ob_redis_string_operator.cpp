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

namespace oceanbase
{
using namespace observer;
using namespace common;
using namespace share;
using namespace sql;
namespace table
{
const ObString StringCommandOperator::VALUE_PROPERTY_NAME = ObString::make_string("value");

int StringCommandOperator::build_rowkey_entity(int64_t db, const ObString &key,
                                               ObITableEntity *&entity)
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
  } else if (FALSE_IT(entity->set_allocator(&op_temp_allocator_))) {
  } else if (OB_ISNULL(obj_ptr =
                           static_cast<ObObj *>(redis_ctx_.allocator_.alloc(sizeof(ObObj) * STRING_ROWKEY_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else {
    obj_ptr[0].set_int(db);
    obj_ptr[1].set_varbinary(key);
    rowkey.assign(obj_ptr, STRING_ROWKEY_SIZE);
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
  if (OB_FAIL(entity.get_property(VALUE_PROPERTY_NAME, value_obj))) {
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
  if (OB_FAIL(entity.get_property(ObRedisUtil::REDIS_EXPIRE_NAME, expire_obj))) {
    LOG_WARN("fail to get value obj", K(ret), K(entity));
  } else if (expire_obj.is_null()) {
    expire_ts = -1;
  } else if (OB_FAIL(expire_obj.get_timestamp(expire_ts))) {
    LOG_WARN("fail to get value from obj", K(ret), K(expire_obj));
  }
  return ret;
}

int StringCommandOperator::build_key_value_entity(int64_t db, const ObString &key,
                                                  const ObString &value, ObITableEntity *&entity)
{
  int ret = OB_SUCCESS;
  ObObj value_obj;
  value_obj.set_varbinary(value);
  if (OB_FAIL(build_rowkey_entity(db, key, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key));
  } else if (OB_FAIL(entity->set_property(VALUE_PROPERTY_NAME, value_obj))) {
    LOG_WARN("fail to set value", K(ret), K(value_obj), KPC(entity));
  }
  return ret;
}

// return empty string if key not exists
int StringCommandOperator::get_value(int64_t db, const ObString &key, ObString &value,
                                     int64_t &expire_ts)
{
  int ret = OB_SUCCESS;
  // get old value
  ObITableEntity *entity = nullptr;
  if (OB_FAIL(build_rowkey_entity(db, key, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key));
  } else {
    ObTableOperation op = ObTableOperation::retrieve(*entity);
    ObTableOperationResult result;
    ObITableEntity *res_entity = nullptr;
    if (OB_FAIL(process_table_single_op(op, result))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to process table get", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(result.get_entity(res_entity))) {
      LOG_WARN("fail to get entity", K(ret), K(result));
    } else if (OB_FAIL(get_value_from_entity(*res_entity, value))) {
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
  ObObj null_obj;
  null_obj.set_null();
  int64_t expire_ts = -1;  // unused
  if (OB_FAIL(get_value(db, key, old_value, expire_ts))) {
    LOG_WARN("fail to get old value", K(ret), K(db), K(key));
  } else if (OB_FAIL(build_key_value_entity(db, key, new_value, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key));
  } else if (OB_FAIL(entity->set_property(ObRedisUtil::REDIS_EXPIRE_NAME, null_obj))) {
    LOG_WARN("fail to set expire property", K(ret), K(null_obj));
  } else {
    ObTableOperation op = ObTableOperation::insert_or_update(*entity);
    ObTableOperationResult result;
    if (OB_FAIL(process_table_single_op(op, result))) {
      LOG_WARN("fail to process table get", K(ret));
    }
  }

  // 3. return old value if exist
  if (OB_SUCC(ret)) {
    if (!old_value.empty()) {
      redis_ctx_.response_.set_res_bulk_string(old_value);
    } else {
      redis_ctx_.response_.set_fmt_res(ObRedisUtil::NULL_BULK_STRING);
    }
  } else {
    redis_ctx_.response_.return_table_error(ret);
  }
  return ret;
}

int StringCommandOperator::set_bit(int32_t offset, char bit_value, char *new_value,
                                   char &old_bit_value)
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
  int64_t expire_ts = -1;
  const int64_t len = offset / 8 + 1;
  if (offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid offset", K(ret), K(offset));
  } else if (OB_FAIL(get_value(db, key, old_value, expire_ts))) {
    LOG_WARN("fail to get old value", K(ret), K(db), K(key));
  } else if (old_value.length() < len) {
    char *empty_value = nullptr;
    if (OB_ISNULL(empty_value =
                      static_cast<char *>(redis_ctx_.allocator_.alloc(sizeof(char) * len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc empty value", K(ret));
    } else {
      MEMSET(empty_value, 0, len);
      MEMCPY(old_value.ptr(), empty_value, old_value.length());
      old_value = ObString(len, empty_value);
    }
  }

  ObObj expire_obj;
  expire_ts == -1 ? expire_obj.set_null() : expire_obj.set_timestamp(expire_ts);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(set_bit(offset, value, old_value.ptr(), old_bit_value))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key));
  } else if (OB_FAIL(build_key_value_entity(db, key, old_value, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key));
  } else if (OB_FAIL(entity->set_property(ObRedisUtil::REDIS_EXPIRE_NAME, expire_obj))) {
    LOG_WARN("fail to set expire property", K(ret), K(expire_obj));
  } else {
    ObTableOperation op = ObTableOperation::insert_or_update(*entity);
    ObTableOperationResult result;
    if (OB_FAIL(process_table_single_op(op, result))) {
      LOG_WARN("fail to process table get", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    redis_ctx_.response_.set_res_int(old_bit_value);
  } else {
    redis_ctx_.response_.return_table_error(ret);
  }
  return ret;
}

bool StringCommandOperator::is_incr_out_of_range(int64_t old_val, int64_t incr)
{
  return (incr > 0) ? (old_val > INT64_MAX - incr) : (old_val < INT64_MIN - incr);
}

int StringCommandOperator::do_incrby(int64_t db, const ObString &key, const ObString &incr, bool is_incr)
{
  int ret = OB_SUCCESS;
  ObString old_str;
  int64_t expire_ts = -1;
  int64_t old_value = 0;
  ObString err_msg;
  int64_t incr_num = 0;
  if (OB_FAIL(get_value(db, key, old_str, expire_ts))) {
    LOG_WARN("fail to get old value", K(ret), K(db), K(key));
  } else {
    bool is_valid_val = false;
    if (old_str.empty()) {
      is_valid_val = true;
    } else {
      old_value =
          ObFastAtoi<int64_t>::atoi(old_str.ptr(), old_str.ptr() + old_str.length(), is_valid_val);
    }
    bool is_valid_incr = false;
    if (is_incr) {
      incr_num = ObFastAtoi<int64_t>::atoi(incr.ptr(), incr.ptr() + incr.length(), is_valid_incr);
    } else if (incr.length() < 1) {
      // send redis err_msg instead of observer error
      LOG_WARN("invalid old value", K(ret), K(old_str));
      err_msg = "ERR value is not an integer or out of range";
    } else if (incr[0] == '-') {
      const int SKIP_NEG_FLAG = 1;
      incr_num = ObFastAtoi<int64_t>::atoi(incr.ptr() + SKIP_NEG_FLAG, incr.ptr() + incr.length(), is_valid_incr);
    } else {
      incr_num = ObFastAtoi<int64_t>::atoi(incr.ptr(), incr.ptr() + incr.length(), is_valid_incr);
      incr_num = -incr_num;
    }
    // send redis err_msg instead of observer error
    if (!is_valid_val || !is_valid_incr) {
      LOG_WARN("invalid value", K(ret), K(old_str), K(incr));
      err_msg = "ERR value is not an integer or out of range";
    } else if (is_incr_out_of_range(old_value, incr_num)) {
      LOG_WARN("increment or decrement would overflow", K(ret), K(old_value), K(incr_num));
      err_msg = "ERR increment or decrement would overflow";
    }
  }

  ObFastFormatInt ffi(incr_num);
  ObString real_incr(ffi.length(), ffi.ptr());
  ObITableEntity *entity = nullptr;
  if (OB_FAIL(ret) || !err_msg.empty()) {
  } else if (OB_FAIL(build_key_value_entity(db, key, real_incr, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(db), K(key));
  } else {
    ObTableOperation op = ObTableOperation::increment(*entity);
    ObTableOperationResult result;
    ObITableEntity *res_entity = nullptr;
    ObString new_value;
    if (OB_FAIL(process_table_single_op(op, result, true))) {
      LOG_WARN("fail to process table get", K(ret));
    } else if (OB_FAIL(result.get_entity(res_entity))) {
      LOG_WARN("fail to get entity", K(ret), K(result));
    } else if (OB_FAIL(get_value_from_entity(*res_entity, new_value))) {
      LOG_WARN("fail to get old value from result entity", K(ret), KPC(res_entity));
    } else {
      bool is_res_valid = false;
      int64_t res_num = ObFastAtoi<int64_t>::atoi(new_value.ptr(), new_value.ptr() + new_value.length(), is_res_valid);
      if (!is_res_valid) {
        ret = OB_CONVERT_ERROR;
        LOG_WARN("fail to convert result to integer", K(ret), K(new_value));
      } else {
        redis_ctx_.response_.set_res_int(res_num);
      }
    }
  }

  // 3. return old value if exist
  if (OB_FAIL(ret)) {
    redis_ctx_.response_.return_table_error(ret);
  } else if (!err_msg.empty()) {
    redis_ctx_.response_.set_res_error(err_msg);
  }
  return ret;
}

}  // namespace table
}  // namespace oceanbase
