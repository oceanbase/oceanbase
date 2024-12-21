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

#include "ob_redis_meta.h"
#include "lib/utility/ob_fast_convert.h"
#include "lib/string/ob_string_buffer.h"
#include "observer/table/redis/operator/ob_redis_list_operator.h"
#include "observer/table/redis/ob_redis_service.h"
#include "observer/table/redis/operator/ob_redis_set_operator.h"
#include "observer/table/redis/ob_redis_rkey.h"

namespace oceanbase
{
namespace table
{
OB_SERIALIZE_MEMBER(ObRedisMeta, ttl_, reserved_);

OB_SERIALIZE_MEMBER(
    ObRedisListMeta,  // FARM COMPAT WHITELIST
    count_,
    left_idx_,
    right_idx_,
    ins_region_left_,
    ins_region_right_);

int ObRedisMeta::decode(const ObString &encoded_content)
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("should not call ObRedisMeta::encode", K(ret));
  return ret;
}

int ObRedisMeta::encode(ObIAllocator &allocator, ObString &encoded_content) const
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("should not call ObRedisMeta::encode", K(ret));

  return ret;
}

int ObRedisListMeta::decode(const ObString &encoded_content)
{
  int ret = OB_SUCCESS;
#ifndef NDEBUG
  // debug mode
  ObString meta_value = encoded_content;
  // meta_value: count:left_idx:right_idx:ttl
  ObString left_idx_str;
  ObString right_idx_str;
  ObString ins_region_left_str;
  ObString ins_region_right_str;
  ObString count_str = meta_value.split_on(META_SPLIT_FLAG);
  if (count_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid meta value", K(ret), K(meta_value));
  } else if (OB_FALSE_IT(left_idx_str = meta_value.split_on(META_SPLIT_FLAG))) {
  } else if (left_idx_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid meta value", K(ret), K(meta_value));
  } else if (OB_FALSE_IT(right_idx_str = meta_value.split_on(META_SPLIT_FLAG))) {
  } else if (right_idx_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid meta value", K(ret), K(meta_value));
  } else if (OB_FALSE_IT(ins_region_left_str = meta_value.split_on(META_SPLIT_FLAG))) {
  } else if (ins_region_left_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid meta value", K(ret), K(meta_value));
  } else if (OB_FALSE_IT(ins_region_right_str = meta_value)) {
  } else if (ins_region_right_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid meta value", K(ret), K(meta_value));
  }  else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(count_str, count_))) {
    LOG_WARN("fail to get int from str", K(ret), K(count_str));
  } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(left_idx_str, left_idx_))) {
    LOG_WARN("fail to get int from str", K(ret), K(left_idx_str));
  } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(right_idx_str, right_idx_))) {
    LOG_WARN("fail to get int from str", K(ret), K(right_idx_str));
  } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(ins_region_left_str, ins_region_left_))) {
    LOG_WARN("fail to get int from str", K(ret), K(ins_region_left_));
  } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(ins_region_right_str, ins_region_right_))) {
    LOG_WARN("fail to get int from str", K(ret), K(ins_region_right_));
  }

#else
  // release mode
  int64_t pos = 0;
  if (OB_FAIL(deserialize(encoded_content.ptr(), encoded_content.length(), pos))) {
    LOG_WARN("fail to deserialize meta value", K(ret), K(encoded_content));
  }
#endif
  return ret;
}

int ObRedisListMeta::encode(ObIAllocator &allocator, ObString &encoded_content) const
{
  int ret = OB_SUCCESS;
#ifndef NDEBUG
  // debug mode
  ObFastFormatInt count_i(count_);
  ObString count_str(count_i.length(), count_i.ptr());

  ObFastFormatInt left_idx_i(left_idx_);
  ObString left_idx_str(left_idx_i.length(), left_idx_i.ptr());

  ObFastFormatInt right_idx_i(right_idx_);
  ObString right_idx_str(right_idx_i.length(), right_idx_i.ptr());

  ObFastFormatInt ins_region_left_i(ins_region_left_);
  ObString ins_region_left_str(ins_region_left_i.length(), ins_region_left_i.ptr());

  ObFastFormatInt ins_region_right_i(ins_region_right_);
  ObString ins_region_right_str(ins_region_right_i.length(), ins_region_right_i.ptr());

  const char flag = META_SPLIT_FLAG;

  ObStringBuffer buffer(&allocator);
  int split_flag_count = ObRedisListMeta::ELE_COUNT - 1;
  if (OB_FAIL(buffer.reserve(split_flag_count + count_i.length() + left_idx_i.length()
                             + right_idx_i.length() + ins_region_left_i.length() + ins_region_right_i.length()))) {
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
  } else if (OB_FAIL(buffer.append(&flag, ObRedisUtil::FLAG_LEN))) {
    LOG_WARN("fail to append flag", K(ret), K(flag));
  } else if (OB_FAIL(buffer.append(ins_region_left_str))) {
    LOG_WARN("fail to append ins_region_left_str", K(ret), K(ins_region_left_));
  } else if (OB_FAIL(buffer.append(&flag, ObRedisUtil::FLAG_LEN))) {
    LOG_WARN("fail to append flag", K(ret), K(flag));
  } else if (OB_FAIL(buffer.append(ins_region_right_str))) {
    LOG_WARN("fail to append ins_region_right_str", K(ret), K(ins_region_right_str));
  } else if (OB_FAIL(buffer.get_result_string(encoded_content))) {
    LOG_WARN("fail to get result string", K(ret), KP(this));
  }
#else
  // release mode
  const int64_t size = get_serialize_size();
  if (size > 0) {
    char *buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(size));
    } else if (OB_FAIL(serialize(buf, size, pos))) {
      LOG_WARN("fail to serialize meta value", K(ret), KP(this));
    } else {
      encoded_content.assign_ptr(buf, size);
    }
  }
#endif

  return ret;
}

int ObRedisListMeta::build_meta_rowkey(int64_t db, const ObString &key, ObRedisCtx &redis_ctx, ObITableEntity *&entity) const
{
  ListCommandOperator op(redis_ctx);
  return op.gen_entity_with_rowkey(db, key, ObRedisListMeta::META_INDEX, false/*is_data*/, entity, redis_ctx.entity_factory_);
}

int ObRedisHashMeta::build_meta_rowkey(int64_t db, const ObString &key, ObRedisCtx &redis_ctx, ObITableEntity *&entity) const
{
  CommandOperator op(redis_ctx);
  return op.build_complex_type_rowkey_entity(db, key, false/*is_data*/, ObRedisUtil::HASH_SET_META_MEMBER, entity);
}

int ObRedisSetMeta::build_meta_rowkey(int64_t db, const ObString &key, ObRedisCtx &redis_ctx, ObITableEntity *&entity) const
{
  int ret = OB_SUCCESS;
  CommandOperator op(redis_ctx);
  return op.build_complex_type_rowkey_entity(db, key, false/*is_data*/, ObRedisUtil::HASH_SET_META_MEMBER, entity);
}

int ObRedisZSetMeta::put_meta_into_entity(ObIAllocator &allocator, ObITableEntity &meta_entity) const
{
  int ret = OB_SUCCESS;
  ObObj score_obj;
  score_obj.set_null();
  if (OB_FAIL(ObRedisMeta::put_meta_into_entity(allocator, meta_entity))) {
    LOG_WARN("fail to put meta into entity", K(ret));
  } else if (OB_FAIL(meta_entity.set_property(ObRedisUtil::SCORE_PROPERTY_NAME, score_obj))) {
    LOG_WARN("fail to set meta value", K(ret), K(score_obj));
  }
  return ret;
}

int ObRedisHashMeta::put_meta_into_entity(ObIAllocator &allocator, ObITableEntity &meta_entity) const
{
  int ret = OB_SUCCESS;
  ObObj value_obj;
  value_obj.set_null();
  if (OB_FAIL(ObRedisMeta::put_meta_into_entity(allocator, meta_entity))) {
    LOG_WARN("fail to put meta into entity", K(ret));
  }
  return ret;
}

int ObRedisMeta::put_meta_into_entity(ObIAllocator &allocator, ObITableEntity &meta_entity) const
{
  int ret = OB_SUCCESS;

  ObObj meta_value_obj;
  ObObj insert_tm_obj;
  insert_tm_obj.set_timestamp(get_insert_ts());
  ObObj expire_ts_obj;
  expire_ts_obj.set_timestamp(get_expire_ts());
  if (OB_FAIL(meta_entity.set_property(ObRedisUtil::EXPIRE_TS_PROPERTY_NAME, expire_ts_obj))) {
    LOG_WARN("fail to set meta value", K(ret), K(expire_ts_obj));
  } else if (OB_FAIL(meta_entity.set_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, insert_tm_obj))) {
    LOG_WARN("fail to convert datetime to timestamp", K(ret), K(insert_tm_obj));
  } else if (has_meta_value_) {
    if (OB_FAIL(encode_meta_value(allocator, meta_value_obj))) {
      LOG_WARN("fail to encode meta value", K(ret));
    } else if (OB_FAIL(meta_entity.set_property(meta_col_name_, meta_value_obj))) {
      LOG_WARN("fail to set value property", K(ret), K(meta_value_obj));
    }
  }

  return ret;
}


int ObRedisMeta::encode_meta_value(ObIAllocator &allocator, ObObj &meta_value_obj) const
{
  int ret = OB_SUCCESS;

  ObString meta_value_content;
  if (OB_FAIL(encode(allocator, meta_value_content))) {
    LOG_WARN("fail to encode meta value", K(ret));
  } else {
    meta_value_obj.set_varbinary(meta_value_content);
  }
  return ret;
}


int ObRedisMeta::decode_meta_value(const ObObj &meta_value_obj)
{
  int ret = OB_SUCCESS;

  ObString meta_value;
  if (OB_FAIL(meta_value_obj.get_varbinary(meta_value))) {
    LOG_WARN("fail to get meta from obj", K(ret), K(meta_value_obj));
  } else if (OB_FAIL(decode(meta_value))) {
    LOG_WARN("fail to decode meta value", K(ret), K(meta_value));
  }

  return ret;
}


int ObRedisMeta::get_meta_from_entity(const ObITableEntity &meta_entity)
{
  int ret = OB_SUCCESS;

  ObObj meta_value_obj;
  ObObj insert_tm_obj;
  ObObj expire_ts_obj;
  int64_t expire_ts = 0;
  if (has_meta_value_) {
    if (OB_FAIL(meta_entity.get_property(meta_col_name_, meta_value_obj))) {
      LOG_WARN("fail to get meta value", K(ret), K(meta_entity));
    } else if (OB_FAIL(decode_meta_value(meta_value_obj))) {
      LOG_WARN("fail to decode meta value", K(ret), K(meta_value_obj));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(meta_entity.get_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, insert_tm_obj))) {
    LOG_WARN("fail to get expire ts", K(ret), K(meta_entity));
  } else if (OB_FAIL(insert_tm_obj.get_timestamp(insert_ts_))) {
    LOG_WARN("fail to get ttl from obj", K(ret), K(insert_tm_obj));
  } else if (OB_FAIL(meta_entity.get_property(ObRedisUtil::EXPIRE_TS_PROPERTY_NAME, expire_ts_obj))) {
    LOG_WARN("fail to get expire ts", K(ret), K(meta_entity));
  } else if (OB_FAIL(expire_ts_obj.get_timestamp(expire_ts))) {
    LOG_WARN("fail to get ttl from obj", K(ret), K(expire_ts_obj));
  } else {
    set_expire_ts(expire_ts);
  }

  return ret;
}

/*******************/
/* ObRedisMetaUtil */
/*******************/

int ObRedisMetaUtil::create_redis_meta_by_model(ObIAllocator &allocator,
                                          ObRedisModel model,
                                          ObRedisMeta *&meta)
{
  int ret = OB_SUCCESS;
  meta = nullptr;
  switch (model) {
    case ObRedisModel::HASH: {
      meta = OB_NEWx(ObRedisHashMeta, &allocator);
      break;
    }
    case ObRedisModel::LIST: {
      meta = OB_NEWx(ObRedisListMeta, &allocator);
      break;
    }
    case ObRedisModel::ZSET: {
      meta = OB_NEWx(ObRedisZSetMeta, &allocator);
      break;
    }
    case ObRedisModel::SET: {
      meta = OB_NEWx(ObRedisSetMeta, &allocator);
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid model type or model do not have meta", K(ret), K(model));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(meta)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc special meta type", K(ret), K(model));
  }
  return ret;
}

int ObRedisMetaUtil::build_meta_rowkey_by_model(
    ObIAllocator &allocator,
    ObRedisModel model,
    int64_t db,
    const ObString &key,
    ObRowkey &meta_rowkey)
{
  int ret = OB_SUCCESS;
  switch (model) {
    case ObRedisModel::HASH:
    case ObRedisModel::ZSET:
    case ObRedisModel::SET: {
      ObString decode_key;
      if (OB_FAIL(ObRedisRKeyUtil::decode_key(key, decode_key))) {
        LOG_WARN("fail to decode key", K(ret), K(key));
      } else {
        ret = CommandOperator::build_complex_type_rowkey(
            allocator, db, decode_key, false /*is_data*/, ObRedisUtil::HASH_SET_META_MEMBER, meta_rowkey);
      }
      break;
    }
    case ObRedisModel::LIST: {
      ret = ListCommandOperator::build_list_type_rowkey(
          allocator, db, key, false /*is_data*/, ObRedisListMeta::META_INDEX, meta_rowkey);
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid model type or model do not have meta", K(ret), K(model));
    }
  }

  return ret;
}

}  // namespace table
}  // namespace oceanbase
