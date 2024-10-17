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

#include "share/table/ob_redis_common.h"
#include "share/table/ob_redis_parser.h"
#include "observer/table/redis/ob_redis_command_factory.h"
#include "lib/utility/ob_fast_convert.h"

#define USING_LOG_PREFIX SERVER

namespace oceanbase
{
namespace table
{
const ObString ObRedisUtil::FMT_OK = "+OK\r\n";
const ObString ObRedisUtil::FMT_SYNTAX_ERR = "-ERR syntax error\r\n";
const ObString ObRedisUtil::FMT_FLOAT_ERR = "-ERR value is not a valid float\r\n";
const char *ObRedisUtil::REDIS_CRLF = "\r\n";
const char *ObRedisUtil::NULL_BULK_STRING = "$-1\r\n";
const char *ObRedisUtil::FMT_MINUS_ONE = ":-1\r\n";
const char *ObRedisUtil::NULL_ARRAY = "*-1\r\n";
const char *const ObRedisUtil::FMT_ZERO = ":0\r\n";
const char *const ObRedisUtil::FMT_EMPTY_ARRAY = "*0\r\n";

const ObString ObRedisUtil::REDIS_INDEX_NAME = "index";
const char *const ObRedisUtil::REDIS_VALUE_CNAME = "value";
const ObString ObRedisUtil::REDIS_VALUE_NAME = REDIS_VALUE_CNAME;
const char *const ObRedisUtil::REDIS_EXPIRE_CNAME = "EXPIRE_TS";
const ObString ObRedisUtil::REDIS_EXPIRE_NAME = REDIS_EXPIRE_CNAME;
const char *const ObRedisUtil::REDIS_CODE_PROPERTY_CNAME = "REDIS_CODE_STR";
const ObString ObRedisUtil::REDIS_PROPERTY_NAME = REDIS_CODE_PROPERTY_CNAME;
const ObString ObRedisUtil::DB_PROPERTY_NAME = "db";
const ObString ObRedisUtil::RKEY_PROPERTY_NAME = "rkey";
const ObString ObRedisUtil::INSERT_TS_PROPERTY_NAME = "insert_ts";
const ObString ObRedisUtil::SCORE_PROPERTY_NAME = "score";

///////////////////////////////////////////////////////////////////////////////////////////////////////

int RedisOperationHelper::get_decoded_msg_from_entity(ObIAllocator &allocator,
                                                      const ObITableEntity &entity,
                                                      ObString &cmd_name,
                                                      ObArray<ObString> &args)
{
  int ret = OB_SUCCESS;

  ObObj prop_value;
  ObString cmd_buffer;
  if (OB_FAIL(entity.get_property(ObRedisUtil::REDIS_PROPERTY_NAME, prop_value))) {
    LOG_WARN("fail to get redis property", K(ret), K(entity));
  } else if (OB_FAIL(prop_value.get_string(cmd_buffer))) {
    LOG_WARN("fail to get string from redis prop_value", K(ret), K(prop_value));
  } else if (OB_FAIL(ObRedisParser::decode(allocator, cmd_buffer, cmd_name, args))) {
    LOG_WARN("fail to decode redis row str", K(ret), K(entity), K(prop_value), K(cmd_buffer));
  }

  return ret;
}

RedisCommandType RedisOperationHelper::get_command_type(const ObString &cmd_name)
{
  RedisCommandType type = REDIS_COMMAND_INVALID;
  ObRedisCommandFactory::cmd_to_type(cmd_name, type);
  return type;
}

observer::ObTableProccessType RedisOperationHelper::redis_cmd_to_proccess_type(const RedisCommandType &redis_cmd_type)
{
  return static_cast<observer::ObTableProccessType>(redis_cmd_type +
                                                    observer::ObTableProccessType::TABLE_API_REDIS_TYPE_OFFSET);
}

int RedisOperationHelper::get_lock_key(ObIAllocator &allocator, const ObRedisRequest &redis_req, ObString &lock_key)
{
  int ret = OB_SUCCESS;

  ObStringBuffer lock_key_buffer(&allocator);
  int64_t db_num = 0;
  ObRowkey row_key = redis_req.get_entity().get_rowkey();
  // row_key: db + key + [else]
  const ObObj *obj_ptr = row_key.get_obj_ptr();
  if (redis_req.get_args().empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid redis args", K(ret), K(redis_req));
  } else if (row_key.length() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rowkey", K(ret), K(row_key));
  } else if (OB_FAIL(obj_ptr[0].get_int(db_num))) {
    LOG_WARN("fail to get db num", K(ret), K(row_key));
  } else {
    common::ObFastFormatInt ffi(db_num);
    ObString db_str(ffi.length(), ffi.ptr());
    const char sep = ObRedisUtil::SIMPLE_ERR_FLAG;
    if (OB_FAIL(lock_key_buffer.reserve(db_str.length() + redis_req.get_args()[0].length() + 1))) {
      LOG_WARN("fail to reserve memory", K(ret), K(db_str), K(redis_req.get_args()[0]));
    } else if (OB_FAIL(lock_key_buffer.append(db_str))) {
      LOG_WARN("fail to append db str", K(ret), K(db_str));
    } else if (OB_FAIL(lock_key_buffer.append(&sep, ObRedisUtil::FLAG_LEN))) {
      LOG_WARN("fail to append err flag", K(ret));
    } else if (OB_FAIL(lock_key_buffer.append(redis_req.get_args()[0]))) {
      LOG_WARN("fail to append key", K(ret), K(redis_req.get_args()[0]));
    } else if (OB_FAIL(lock_key_buffer.get_result_string(lock_key))) {
      LOG_WARN("fail to get result string", K(ret), K(lock_key_buffer));
    }
  }

  return ret;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
int ObRedisRequest::decode()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(RedisOperationHelper::get_decoded_msg_from_entity(
          allocator_, request_.table_operation_.entity(), cmd_name_, args_))) {
    LOG_WARN("fail to get decoded msg from entity", K(ret), K(request_));
  } else if (cmd_name_.empty()) {
    ret = OB_KV_REDIS_PARSE_ERROR;
    LOG_WARN("redis cmd is invalid", K(ret), K(request_));
  } else {
    ObRowkey row_key = get_entity().get_rowkey();
    const ObObj *obj_ptr = row_key.get_obj_ptr();
    if (!row_key.is_valid() || OB_UNLIKELY(row_key.length() < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid rowkey", K(ret), K(row_key), KPC(obj_ptr));
    } else if (OB_FAIL(obj_ptr[0].get_int(db_))) {
      LOG_WARN("fail to get db num", K(ret), K(row_key));
    }
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////

int ObRedisResponse::set_res_array(const ObIArray<ObString> &res)
{
  int ret = OB_SUCCESS;
  ObString encoded;
  ObTableEntity *res_entity = static_cast<ObTableEntity *>(result_.get_entity());
  if (OB_ISNULL(res_entity)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null result entity", K(ret));
  } else if (OB_FAIL(ObRedisParser::encode_array(allocator_, res, encoded))) {
    LOG_WARN("fail to encode array", K(ret));
  } else {
    ObObj obj;
    obj.set_varchar(encoded);
    res_entity->set_property(ObRedisUtil::REDIS_PROPERTY_NAME, obj);
  }

  result_.set_type(ObTableOperationType::Type::REDIS);
  result_.set_err(ret);

  return ret;
}

int ObRedisResponse::set_res_error(const ObString &err_msg)
{
  int ret = OB_SUCCESS;
  ObTableEntity *res_entity = static_cast<ObTableEntity *>(result_.get_entity());
  ObObj obj;
  ObString encoded_msg;
  if (OB_ISNULL(res_entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("res_entity is null", K(ret));
  } else if (OB_FAIL(ObRedisParser::encode_error(allocator_, err_msg, encoded_msg))) {
    LOG_WARN("fail to encode error", K(ret), K(err_msg));
  } else if (OB_FALSE_IT(obj.set_varchar(encoded_msg))) {
  } else if (OB_FAIL(res_entity->set_property(ObRedisUtil::REDIS_PROPERTY_NAME, obj))) {
    LOG_WARN("fail to set property", K(ret), K(encoded_msg));
  }

  result_.set_err(ret);
  result_.set_type(ObTableOperationType::REDIS);

  return ret;
}

int ObRedisResponse::set_res_bulk_string(const ObString &res)
{
  int ret = OB_SUCCESS;
  ObString encoded;
  ObTableEntity *res_entity = static_cast<ObTableEntity *>(result_.get_entity());
  if (OB_ISNULL(res_entity)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null result entity", K(ret));
  } else if (OB_FAIL(ObRedisParser::encode_bulk_string(allocator_, res, encoded))) {
    LOG_WARN("fail to encode array", K(ret));
  } else {
    ObObj obj;
    obj.set_varchar(encoded);
    res_entity->set_property(ObRedisUtil::REDIS_PROPERTY_NAME, obj);
  }

  result_.set_type(ObTableOperationType::Type::REDIS);
  result_.set_err(ret);
  return ret;
}

int ObRedisResponse::set_res_int(int64_t res)
{
  int ret = OB_SUCCESS;
  ObTableEntity *res_entity = static_cast<ObTableEntity *>(result_.get_entity());
  ObObj obj;
  ObString res_msg;
  if (OB_ISNULL(res_entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("res_entity is null", K(ret));
  } else if (OB_FAIL(ObRedisParser::encode_integer(allocator_, res, res_msg))) {
    LOG_WARN("fail to encode error", K(ret), K(res));
  } else if (OB_FALSE_IT(obj.set_varchar(res_msg))) {
  } else if (OB_FAIL(res_entity->set_property(ObRedisUtil::REDIS_PROPERTY_NAME, obj))) {
    LOG_WARN("fail to set property", K(ret), K(res));
  }

  result_.set_err(ret);
  result_.set_type(ObTableOperationType::REDIS);
  result_.set_entity(res_entity);

  return ret;
}

int ObRedisResponse::set_fmt_res(const ObString &fmt_res)
{
  int ret = OB_SUCCESS;
  ObTableEntity *res_entity = static_cast<ObTableEntity *>(result_.get_entity());
  ObObj obj;
  if (OB_ISNULL(res_entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("res_entity is null", K(ret));
  } else if (OB_FALSE_IT(obj.set_varchar(fmt_res))) {
  } else if (OB_FAIL(res_entity->set_property(ObRedisUtil::REDIS_PROPERTY_NAME, obj))) {
    LOG_WARN("fail to set property", K(ret), K(fmt_res));
  }

  result_.set_err(ret);
  result_.set_type(ObTableOperationType::REDIS);

  return ret;
}

void ObRedisResponse::return_table_error(int ret)
{
  result_.set_err(ret);
  result_.set_type(ObTableOperationType::REDIS);
}


}  // end namespace table
}  // namespace oceanbase
