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

namespace oceanbase
{
using namespace observer;
using namespace common;
using namespace share;
using namespace sql;
namespace table
{
const ObString HashCommandOperator::VALUE_PROPERTY_NAME = ObString::make_string("value");
const ObString HashCommandOperator::FIELD_PROPERTY_NAME = ObString::make_string("field");

int HashCommandOperator::do_hset_inner(int64_t db, const ObString &key,
                                 const HashCommand::FieldValMap &field_val_map, int64_t &insert_num)
{
  int ret = OB_SUCCESS;
  const ObITableEntity &req_entity = redis_ctx_.get_entity();
  ObTableBatchOperation ops;
  ops.set_entity_factory(redis_ctx_.entity_factory_);
  int64_t cur_ts = ObTimeUtility::current_time();

  for (HashCommand::FieldValMap::const_iterator iter = field_val_map.begin();
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
    if (!results[i].get_is_insertup_do_update()) {
      ++insert_num;
    }
  }

  return ret;
}

int HashCommandOperator::do_hset(int64_t db, const ObString &key,
                                 const HashCommand::FieldValMap &field_val_map)
{
  int ret = OB_SUCCESS;
  int64_t insert_num = 0;
  if (OB_FAIL(do_hset_inner(db, key, field_val_map, insert_num))) {
    LOG_WARN("fail to do inner hset", K(ret), K(db), K(key));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(redis_ctx_.response_.set_res_int(insert_num))) {
      LOG_WARN("fail to set response int", K(ret), K(insert_num));
    }
  } else {
    redis_ctx_.response_.return_table_error(ret);
  }

  return ret;
}

int HashCommandOperator::do_hmset(int64_t db, const ObString &key,
                                 const HashCommand::FieldValMap &field_val_map)
{
  int ret = OB_SUCCESS;
  int64_t insert_num = 0;
  if (OB_FAIL(do_hset_inner(db, key, field_val_map, insert_num))) {
    LOG_WARN("fail to do inner hset", K(ret), K(db), K(key));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(redis_ctx_.response_.set_fmt_res(ObRedisUtil::FMT_OK))) {
      LOG_WARN("fail to set OK response", K(ret));
    }
  } else {
    redis_ctx_.response_.return_table_error(ret);
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
  if (OB_FAIL(build_hash_set_rowkey_entity(db, key, true /*not meta*/, field, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(field), K(db), K(key));
  } else if (OB_FAIL(entity->set_property(VALUE_PROPERTY_NAME, value_obj))) {
    LOG_WARN("fail to set row key value", K(ret), K(value_obj));
  } else if (OB_FAIL(entity->set_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, ts_obj))) {
    LOG_WARN("fail to set row key value", K(ret), K(ts_obj));
  }
  return ret;
}

}  // namespace table
}  // namespace oceanbase
