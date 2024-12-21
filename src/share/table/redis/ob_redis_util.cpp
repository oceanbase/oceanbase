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

#include "share/table/redis/ob_redis_util.h"
#include "share/table/redis/ob_redis_parser.h"
#include "observer/table/redis/ob_redis_command_factory.h"
#include "lib/utility/ob_fast_convert.h"

#define USING_LOG_PREFIX SERVER

namespace oceanbase
{
namespace table
{
int ObRedisHelper::get_lock_key(ObIAllocator &allocator, const ObRedisRequest &redis_req, ObString &lock_key)
{
  int ret = OB_SUCCESS;

  ObStringBuffer lock_key_buffer(&allocator);
  int64_t db_num = redis_req.get_db();
  if (redis_req.get_args().empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid redis args", K(ret), K(redis_req));
  } else {
    common::ObFastFormatInt ffi(db_num);
    ObString db_str(ffi.length(), ffi.ptr());
    const char sep = ObRedisUtil::SIMPLE_ERR_FLAG;
    const ObString &first_arg = redis_req.get_args().at(0);
    if (OB_FAIL(lock_key_buffer.reserve(db_str.length() + first_arg.length() + 1))) {
      LOG_WARN("fail to reserve memory", K(ret), K(db_str), K(first_arg));
    } else if (OB_FAIL(lock_key_buffer.append(db_str))) {
      LOG_WARN("fail to append db str", K(ret), K(db_str));
    } else if (OB_FAIL(lock_key_buffer.append(&sep, ObRedisUtil::FLAG_LEN))) {
      LOG_WARN("fail to append err flag", K(ret));
    } else if (OB_FAIL(lock_key_buffer.append(first_arg))) {
      LOG_WARN("fail to append key", K(ret), K(first_arg));
    } else if (OB_FAIL(lock_key_buffer.get_result_string(lock_key))) {
      LOG_WARN("fail to get result string", K(ret), K(lock_key_buffer));
    }
  }

  return ret;
}

int ObRedisHelper::get_table_name_by_model(ObRedisModel model, ObString &table_name)
{
  int ret = OB_SUCCESS;
  switch (model) {
    case ObRedisModel::HASH : {
      table_name = ObRedisInfoV1::HASH_TABLE_NAME;
      break;
    }
    case ObRedisModel::LIST : {
      table_name = ObRedisInfoV1::LIST_TABLE_NAME;
      break;
    }
    case ObRedisModel::ZSET : {
      table_name = ObRedisInfoV1::ZSET_TABLE_NAME;
      break;
    }
    case ObRedisModel::SET : {
      table_name = ObRedisInfoV1::SET_TABLE_NAME;
      break;
    }
    case ObRedisModel::STRING : {
      table_name = ObRedisInfoV1::STRING_TABLE_NAME;
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid model value", K(ret));
    }
  }
  return ret;
}

int ObRedisHelper::model_to_string(ObRedisModel model, ObString &str)
{
  int ret = OB_SUCCESS;
  switch (model) {
    case ObRedisModel::STRING : {
      str = "string";
      break;
    }
    case ObRedisModel::HASH : {
      str = "hash";
      break;
    }
    case ObRedisModel::LIST : {
      str = "list";
      break;
    }
    case ObRedisModel::ZSET : {
      str = "zset";
      break;
    }
    case ObRedisModel::SET : {
      str = "set";
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid model value", K(ret));
    }
  }
  return ret;
}

int ObRedisHelper::check_redis_ttl_schema(const ObTableSchema &table_schema, const table::ObRedisModel redis_model)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *rowkey_schema = NULL;
  if (redis_model == ObRedisModel::STRING) {
    if (OB_ISNULL(rowkey_schema = table_schema.get_column_schema_by_idx(ObRedisUtil::COL_IDX_DB))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("can't get rowkey column schema", K(ret));
    } else if (ObRedisUtil::DB_PROPERTY_NAME.case_compare(rowkey_schema->get_column_name()) != 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("the first column should be DB", K(ret), K(rowkey_schema->get_column_name()));
    } else if (OB_ISNULL(rowkey_schema = table_schema.get_column_schema_by_idx(ObRedisUtil::COL_IDX_RKEY))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("can't get rowkey column schema", K(ret));
    } else if (ObRedisUtil::RKEY_PROPERTY_NAME.case_compare(rowkey_schema->get_column_name()) != 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("the second column should be rkey", K(ret), K(rowkey_schema->get_column_name()));
    }
  } else {
    if (OB_ISNULL(rowkey_schema = table_schema.get_column_schema_by_idx(ObRedisUtil::COL_IDX_DB))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("can't get rowkey column schema", K(ret));
    } else if (ObRedisUtil::DB_PROPERTY_NAME.case_compare(rowkey_schema->get_column_name()) != 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("the first column should be DB", K(ret), K(rowkey_schema->get_column_name()));
    } else if (OB_ISNULL(rowkey_schema = table_schema.get_column_schema_by_idx(ObRedisUtil::COL_IDX_RKEY))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("can't get rowkey column schema", K(ret));
    } else if (ObRedisUtil::RKEY_PROPERTY_NAME.case_compare(rowkey_schema->get_column_name()) != 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("the second column should be rkey", K(ret), K(rowkey_schema->get_column_name()));
    } else if (OB_ISNULL(rowkey_schema = table_schema.get_column_schema_by_idx(ObRedisUtil::COL_IDX_EXPIRE_TS))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("can't get rowkey column schema", K(ret));
    } else if (ObRedisUtil::EXPIRE_TS_PROPERTY_NAME.case_compare(rowkey_schema->get_column_name()) != 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("the first column should be K", K(ret), K(rowkey_schema->get_column_name()));
    } else if (OB_ISNULL(rowkey_schema = table_schema.get_column_schema_by_idx(ObRedisUtil::COL_IDX_INSERT_TS))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("can't get rowkey column schema", K(ret));
    } else if (ObRedisUtil::INSERT_TS_PROPERTY_NAME.case_compare(rowkey_schema->get_column_name()) != 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("the first column should be K", K(ret), K(rowkey_schema->get_column_name()));
    }
  }

  return ret;
}

int ObRedisHelper::string_to_double(const ObString &str, double &d)
{
  int ret = OB_SUCCESS;
  if (str.empty() || isspace(str[0])) {
    ret = OB_DATA_OUT_OF_RANGE;
    LOG_WARN("invalid empty string or started with space", K(ret), K(str));
  } else if ((str.case_compare("+INFINITY") == 0)
          || (str.case_compare("INFINITY") == 0)
          || (str.case_compare("INF") == 0)
          || (str.case_compare("+INF") == 0)) {
    d = INFINITY;
  } else if ((str.case_compare("-INFINITY") == 0)
          || (str.case_compare("-INF") == 0)) {
    d = -INFINITY;
  } else {
    char *endptr = nullptr;
    int err = 0;
    d = ObCharset::strntod(str.ptr(), str.length(), &endptr, &err);
    if (err != 0 || (str.ptr() + str.length()) != endptr) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("fail to cast string to double", K(ret), K(str), K(err), KP(endptr));
    }
  }
  return ret;
}

int ObRedisHelper::init_str2d(ObIAllocator &allocator, double d, char *&buf, size_t &length)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    MEMSET(buf, '\0', DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE);
    length = 0;
    if (isinf(d)) {
      if (d > 0) {
        length = 3;
        MEMCPY(buf, "inf", length);
      } else {
        length = 4;
        MEMCPY(buf, "-inf", length);
      }
    } else if (isnan(d)) {
      length = 3;
      MEMCPY(buf, "nan", length);
    }
  }
  return ret;
}

int ObRedisHelper::double_to_string(ObIAllocator &allocator, double d, ObString &str)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  size_t length = 0;
  if (OB_FAIL(init_str2d(allocator, d, buf, length))) {
    LOG_WARN("fail to init str2d", K(ret), K(d));
  } else if (length == 0) {
    if (d == 0) {
      if (1.0 / d < 0) {
        length = 2;
        MEMCPY(buf, "-0", length);
      } else {
        length = 1;
        MEMCPY(buf, "0", length);
      }
    } else {
      length =
        ob_gcvt(d, OB_GCVT_ARG_DOUBLE, static_cast<int32_t>(OB_CAST_TO_VARCHAR_MAX_LENGTH - 1), buf, NULL);
      ObString val(OB_CAST_TO_VARCHAR_MAX_LENGTH, static_cast<int32_t>(length), buf);
      if (length == 0) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("fail to convert double to string", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    str = ObString(DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE, static_cast<ObString::obstr_size_t>(length), buf);
  }
  return ret;
}

int ObRedisHelper::string_to_long_double(const ObString &str, long double &ld)
{
  int ret = OB_SUCCESS;
  if (str.empty() || isspace(str[0]) || str.length() >= DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE) {
    ret = OB_DATA_OUT_OF_RANGE;
    LOG_WARN("invalid empty string or started with space", K(ret), K(str));
  } else {
    // convert to C style string
    char buf[DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE];
    MEMCPY(buf, str.ptr(), str.length());
    buf[str.length()] = '\0';
    char *endptr = nullptr;
    int err = 0;
    ld = strtold(buf, &endptr);
    if ((buf + str.length()) != endptr ||
        (err == ERANGE && (ld == HUGE_VAL || ld == -HUGE_VAL || fpclassify(ld) == FP_ZERO))) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("fail to cast string to double", K(ret), KP(str.ptr()), K(str.length()), K(err), KP(endptr));
    }
  }
  return ret;
}

int ObRedisHelper::long_double_to_string(ObIAllocator &allocator, long double ld, ObString &str)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  size_t length = 0;
  if (OB_FAIL(init_str2d(allocator, ld, buf, length))) {
    LOG_WARN("fail to init str2d", K(ret));
  } else if (length == 0) {
    if (ld == 0) {
      if (1.0 / ld < 0) {
        length = 2;
        MEMCPY(buf, "-0", length);
      } else {
        length = 1;
        MEMCPY(buf, "0", length);
      }
    } else {
      length = snprintf(buf, DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE, "%.17Lf", ld);
      if (length == 0) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("fail to convert double to string", K(ret));
      } else {
        // remove last '0' and '.'
        if (OB_NOT_NULL(strchr(buf, '.'))) {
          char *p = buf + length - 1;
          while (*p == '0') {
            --p;
            --length;
          }
          if (*p == '.') {
            --length;
          }
        }
        // convert '-0' to '0'
        if (length == 2 && buf[0] == '-' && buf[1] == '0') {
          buf[0] = '0';
          --length;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    str = ObString(DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE, static_cast<ObString::obstr_size_t>(length), buf);
  }
  return ret;
}

int ObRedisHelper::gen_meta_scan_range(ObIAllocator &allocator,
                                       const ObTableCtx &tb_ctx,
                                       ObIArray<ObNewRange> &scan_ranges)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObNewRange> &src_key_ranges = tb_ctx.get_key_ranges();
  int64_t last_db = -1;
  ObString last_rkey;
  for (int i = 0; OB_SUCC(ret) && i < src_key_ranges.count(); ++i) {
    const ObNewRange &data_range = src_key_ranges.at(i);
    ObNewRange meta_range = data_range;
    int64_t start_obj_cnt = data_range.start_key_.get_obj_cnt();
    int64_t end_obj_cnt = data_range.end_key_.get_obj_cnt();
    ObRedisModel model = tb_ctx.redis_ttl_ctx()->get_model();
    int64_t expected_cnt = (model == ObRedisModel::LIST) ?
                            ObRedisUtil::LIST_ROWKEY_NUM : ObRedisUtil::COMPLEX_ROWKEY_NUM;
    // copy first range and replace is_data with false
    if (start_obj_cnt == 1 || end_obj_cnt == 1) {
      // do nothing, full scan may have one obj
      LOG_INFO("full scan redis ttl table", K(ret), K(start_obj_cnt), K(end_obj_cnt));
    } else if (start_obj_cnt != expected_cnt || end_obj_cnt != expected_cnt) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("redis rowkey should have 4 ObObj", K(ret), K(start_obj_cnt), K(end_obj_cnt), K(src_key_ranges));
    } else {
      int64_t db = 0;
      ObString rkey;
      if (OB_FAIL(data_range.start_key_.get_obj_ptr()[ObRedisUtil::COL_IDX_DB].get_int(db))) {
        LOG_WARN("fail to get db", K(ret));
      } else if (OB_FAIL(data_range.start_key_.get_obj_ptr()[ObRedisUtil::COL_IDX_RKEY].get_varbinary(rkey))) {
        LOG_WARN("fail to get rkey", K(ret));
      } else if (db != last_db || rkey != last_rkey) {
        last_db = db;
        last_rkey = rkey;
        if (OB_FAIL(ObRedisMetaUtil::build_meta_rowkey_by_model(allocator,
          tb_ctx.redis_ttl_ctx()->get_model(), db, rkey, meta_range.start_key_))) {
          LOG_WARN("fail to build meta rowkey by model", K(ret));
        } else if (OB_FALSE_IT(meta_range.end_key_.assign(
                        meta_range.start_key_.get_obj_ptr(), data_range.start_key_.get_obj_cnt()))) {
        } else {
          meta_range.border_flag_.set_inclusive_start();
          meta_range.border_flag_.set_inclusive_end();
          if (OB_FAIL(scan_ranges.push_back(meta_range))) {
            LOG_WARN("fail to push back meta range", K(ret), K(meta_range));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(scan_ranges.push_back(data_range))) {
          LOG_WARN("fail to push back data range", K(ret), K(meta_range));
        }
      }
    }
  }
  return ret;
}

int ObRedisHelper::check_string_length(int64_t old_len, int64_t append_len)
{
  int ret = OB_SUCCESS;
  uint64_t new_len = static_cast<uint64_t>(old_len) + static_cast<uint64_t>(append_len);
  if (new_len > MAX_BULK_LEN || new_len < old_len || new_len < append_len) {
    ret = OB_ERR_DATA_TOO_LONG;
    LOG_WARN("string exceeds maximum allowed size 1M", K(ret), K(old_len), K(append_len), K(new_len));
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////

int ObRedisRequest::decode()
{
  int ret = OB_SUCCESS;
  ObString cmd_buffer;
  if (request_.get_type() == ObTableRequsetType::TABLE_REDIS_REQUEST) {
    cmd_buffer = reinterpret_cast<const ObRedisRpcRequest &>(request_).resp_str_;
    db_ = reinterpret_cast<const ObRedisRpcRequest &>(request_).redis_db_;
  } else {
    ObObj prop_value;
    const ObITableEntity &entity =
      reinterpret_cast<const ObTableOperationRequest &>(request_).table_operation_.entity();
    const ObRowkey &row_key = entity.get_rowkey();
    const ObObj *obj_ptr = row_key.get_obj_ptr();
    if (OB_FAIL(entity.get_property(ObRedisUtil::REDIS_PROPERTY_NAME, prop_value))) {
      LOG_WARN("fail to get redis property", K(ret), K(entity));
    } else if (OB_FAIL(prop_value.get_string(cmd_buffer))) {
      LOG_WARN("fail to get string from redis prop_value", K(ret), K(prop_value));
    } else if (row_key.length() < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid rowkey", K(ret), K(row_key));
    } else if (OB_FAIL(obj_ptr[0].get_int(db_))) {
      LOG_WARN("fail to get db num", K(ret), K(row_key));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObRedisParser::decode(cmd_buffer, cmd_name_, args_))) {
    LOG_WARN("fail to get decoded msg from entity", K(ret));
  } else if (cmd_name_.empty()) {
    ret = OB_KV_REDIS_PARSE_ERROR;
    LOG_WARN("redis cmd is invalid", K(ret));
  }
  return ret;
}

int ObRedisHelper::str_to_lower(ObIAllocator &allocator, const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  const ObString::obstr_size_t src_len = src.length();
  const char *src_ptr = src.ptr();
  char *dst_ptr = NULL;
  void *ptr = NULL;
  char letter = '\0';
  if (OB_ISNULL(src_ptr) || OB_UNLIKELY(0 >= src_len)) {
    dst.assign(NULL, 0);
  } else if (NULL == (ptr = allocator.alloc(src_len + 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), "size", src_len);
  } else {
    memset(ptr, '\0', src_len + 1);
    dst_ptr = static_cast<char *>(ptr);
    for (ObString::obstr_size_t i = 0; i < src_len; ++i) {
      letter = src_ptr[i];
      if (letter >= 'A' && letter <= 'Z') {
        dst_ptr[i] = static_cast<char>(letter + 32);
      } else {
        dst_ptr[i] = letter;
      }
    }
    dst.assign_ptr(dst_ptr, src_len);
  }
  return ret;
}

// todo: add more
bool ObRedisHelper::is_read_only_command(RedisCommandType type)
{
  return type == REDIS_COMMAND_HGET ||
         type == REDIS_COMMAND_GET;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
int ObRedisResponse::set_res_array(const ObIArray<ObString> &res)
{
  int ret = OB_SUCCESS;
  ObString encoded;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null allocator_", K(ret));
  } else if (OB_FAIL(ObRedisParser::encode_array(*allocator_, res, encoded))) {
    LOG_WARN("fail to encode array", K(ret));
  }
  result_.set_ret(ret, encoded);
  return ret;
}

int ObRedisResponse::set_res_error(const ObString &err_msg)
{
  int ret = OB_SUCCESS;
  ObString encoded_msg;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null allocator_", K(ret));
  } else if (OB_FAIL(ObRedisParser::encode_error(*allocator_, err_msg, encoded_msg))) {
    LOG_WARN("fail to encode error", K(ret), K(err_msg));
  }
  result_.set_ret(ret, encoded_msg);
  return ret;
}

int ObRedisResponse::set_res_bulk_string(const ObString &res)
{
  int ret = OB_SUCCESS;
  ObString encoded;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null allocator_", K(ret));
  } else if (OB_FAIL(ObRedisParser::encode_bulk_string(*allocator_, res, encoded))) {
    LOG_WARN("fail to encode array", K(ret));
  }
  result_.set_ret(ret, encoded);
  return ret;
}

int ObRedisResponse::set_res_simple_string(const ObString &res)
{
  int ret = OB_SUCCESS;
  ObString encoded;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null allocator_", K(ret));
  } else if (OB_FAIL(ObRedisParser::encode_simple_string(*allocator_, res, encoded))) {
    LOG_WARN("fail to encode array", K(ret));
  }
  result_.set_ret(ret, encoded);
  return ret;
}

int ObRedisResponse::set_res_int(int64_t res)
{
  int ret = OB_SUCCESS;
  ObString res_msg;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null allocator_", K(ret));
  } else if (OB_FAIL(ObRedisParser::encode_integer(*allocator_, res, res_msg))) {
    LOG_WARN("fail to encode error", K(ret), K(res));
  }
  result_.set_ret(ret, res_msg);
  return ret;
}

int ObRedisResponse::set_fmt_res(const ObString &fmt_res)
{
  int ret = OB_SUCCESS;
  result_.set_ret(ret, fmt_res);
  return ret;
}

void ObRedisResponse::return_table_error(int ret)
{
  result_.set_err(ret);
}

int ObRedisResponse::get_result(ObITableResult *&result) const
{
  int ret = OB_SUCCESS;
  if (req_type_ == ObTableRequsetType::TABLE_REDIS_REQUEST) {
    result = &result_;
  } else if (req_type_ == ObTableRequsetType::TABLE_OPERATION_REQUEST) {
    ObTableOperationResult *tmp_res = nullptr;
    ObTableEntity *entity = nullptr;
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null allocator_", K(ret));
    } else if (OB_ISNULL(tmp_res = OB_NEWx(ObTableOperationResult, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    } else if (OB_ISNULL(entity = OB_NEWx(ObTableEntity, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    } else if (FALSE_IT(tmp_res->set_entity(entity))) {
    } else if (OB_FAIL(result_.convert_to_table_op_result(*tmp_res))) {
      LOG_WARN("fail to convert redis result to table operation result", K(ret), K(result_));
    } else {
      result = tmp_res;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid request type", K(ret), K(req_type_));
  }
  return ret;
}
}  // end namespace table
}  // namespace oceanbase
