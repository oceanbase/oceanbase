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
#include "ob_redis_zset_operator.h"
#include "lib/utility/ob_fast_convert.h"
#include "observer/table/redis/ob_redis_rkey.h"
#include "share/table/redis/ob_redis_error.h"

namespace oceanbase
{
using namespace common;
namespace table
{
const ObString ZSetCommandOperator::SCORE_INDEX_NAME = ObString::make_string("index_score");

void ZSetScoreUpdater::operator()(common::hash::HashMapPair<common::ObString, double> &entry)
{
  aggregate_scores(agg_type_, entry.second, new_score_, weight_, entry.second);
}

int ZSetScoreUpdater::aggregate_scores(ZSetAggCommand::AggType agg_type, double old_score,
                                       double new_score, double new_score_weight, double &res_score)
{
  int ret = OB_SUCCESS;
  switch (agg_type) {
    case ZSetAggCommand::AggType::MAX: {
      res_score = OB_MAX(new_score * new_score_weight, old_score);
      break;
    }
    case ZSetAggCommand::AggType::MIN: {
      res_score = OB_MIN(new_score * new_score_weight, old_score);
      break;
    }
    case ZSetAggCommand::AggType::SUM: {
      res_score = new_score * new_score_weight + old_score;
      break;
    }
    default: {
      // should not enter here
      int ret = OB_ERR_INVALID_INPUT_ARGUMENT;
      LOG_WARN("invalid agg type", K(ret), K(agg_type), K(new_score), K(old_score));
    }
  }
  return ret;
}

int ZSetCommandOperator::do_zadd(int64_t db, const ObString &key,
                                 const ZSetCommand::MemberScoreMap &mem_score_map)
{
  int ret = OB_SUCCESS;
  // add meta, zadd do not update expire time
  bool is_new_meta = false;
  ObRedisMeta *meta = nullptr;
  if (OB_FAIL(check_and_insup_meta(db, key, ObRedisModel::ZSET, is_new_meta, meta))) {
    LOG_WARN("fail to check and insup meta", K(ret));
  } else if (OB_FAIL(do_zadd_data(db, key, mem_score_map, is_new_meta))) {
    LOG_WARN("fail to do zadd data", K(ret), K(db), K(key), K(is_new_meta));
  }
  return ret;
}

int ZSetCommandOperator::do_zadd_data(
    int64_t db,
    const ObString &key,
    const ZSetCommand::MemberScoreMap &mem_score_map,
    bool is_new_meta)
{
  int ret = OB_SUCCESS;
  ObTableBatchOperation ops;
  ops.set_entity_factory(redis_ctx_.entity_factory_);
  int64_t cur_time = ObTimeUtility::current_time();

  // add data
  for (ZSetCommand::MemberScoreMap::const_iterator iter = mem_score_map.begin();
       OB_SUCC(ret) && iter != mem_score_map.end();
       ++iter) {
    ObITableEntity *value_entity = nullptr;
    if (OB_FAIL(build_score_entity(db, key, iter->first, iter->second, cur_time, value_entity))) {
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

  int64_t insert_num = 0;
  for (int i = 0; OB_SUCC(ret) && i < results.count(); ++i) {
    if (is_new_meta || !results[i].get_is_insertup_do_update()) {
      ++insert_num;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(insert_num))) {
      LOG_WARN("fail to set response int", K(ret), K(insert_num));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int ZSetCommandOperator::do_zcard_inner(int64_t db, const ObString &key, int64_t &card)
{
  int ret = OB_SUCCESS;
  ObTableQuery query;
  ObTableAggregation cnt_agg(ObTableAggregationType::COUNT, "*");
  if (OB_FAIL(add_complex_type_subkey_scan_range(db, key, query))) {
    LOG_WARN("fail to add scan query", K(ret), K(key), K(db));
  } else if (OB_FAIL(query.add_aggregation(cnt_agg))) {
    LOG_WARN("fail to add count aggregation", K(ret), K(cnt_agg));
  } else if (OB_FAIL(query.add_select_column(COUNT_STAR_PROPERTY))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else {
    SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
    {
      ObRedisZSetMeta *null_meta = nullptr;
      QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, null_meta)
      ObTableQueryResult *one_result = nullptr;
      const ObITableEntity *result_entity = nullptr;
      // aggregate result only one row
      if (OB_FAIL(iter->get_next_result(one_result))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next result", K(ret));
        }
      } else {
        one_result->rewind();
        ObObj cnt_obj;
        bool is_member = false;
        if (OB_FAIL(one_result->get_next_entity(result_entity))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next result", K(ret));
          }
        } else if (OB_FAIL(result_entity->get_property(COUNT_STAR_PROPERTY, cnt_obj))) {
          LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
        } else if (OB_FAIL(cnt_obj.get_int(card))) {
          LOG_WARN("fail to get db num", K(ret));
        }
      }
      QUERY_ITER_END(iter)
    }
  }

  return ret;
}

int ZSetCommandOperator::do_zcard(int64_t db, const ObString &key)
{
  int ret = OB_SUCCESS;
  int64_t cnt = -1;
  if (OB_FAIL(do_zcard_inner(db, key, cnt))) {
    if (ObRedisErr::is_redis_error(ret)) {
      RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
    }
    LOG_WARN("fail to do zcard inner", K(ret), K(db), K(key));
  } else if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(cnt))) {
    LOG_WARN("fail to set diff member", K(ret));
  }
  return ret;
}

int ZSetCommandOperator::do_zincrby(int64_t db, const ObString &key, const ObString &member,
                                    double increment)
{
  int ret = OB_SUCCESS;
  bool is_meta_exists = true;
  ObRedisMeta *meta = nullptr;  // unused
  double new_val = increment;
  ObTableBatchOperation ops;
  if (OB_FAIL(get_meta(db, key, ObRedisModel::ZSET, meta))) {
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
    ObITableEntity *put_meta_entity = nullptr;
    if (OB_FAIL(gen_meta_entity(db, key, ObRedisModel::HASH, *meta, put_meta_entity))) {
      LOG_WARN("fail to generate meta entity", K(ret), K(db), K(key), KPC(meta));
    } else if (OB_FAIL(ops.insert_or_update(*put_meta_entity))) {
      LOG_WARN("fail to put meta entity", K(ret));
    }
  } else {
    bool is_data_exists = true;
    double old_val = 0.0;
    if (OB_FAIL(do_zscore_inner(db, key, member, old_val))) {
      if (ret == OB_ITER_END) {
        is_data_exists = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get member score", K(ret), K(member), K(db), K(key));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (is_data_exists) {
      if (is_incrby_out_of_range(old_val, increment)) {
        RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::FLOAT_ERR);
        LOG_WARN("increment or decrement would overflow", K(ret), K(old_val), K(increment));
      } else {
        new_val += old_val;
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObITableEntity *value_entity = nullptr;
    ResultFixedArray results(op_temp_allocator_);
    if (OB_FAIL(build_score_entity(db, key, member, new_val, ObTimeUtility::current_time(), value_entity))) {
      LOG_WARN(
          "fail to build score entity", K(ret), K(member), K(new_val), K(db), K(key));
    } else if (OB_FAIL(ops.insert_or_update(*value_entity))) {
      LOG_WARN("fail to push back insert or update op", K(ret), KPC(value_entity));
    } else if (OB_FAIL(process_table_batch_op(ops, results))) {
      LOG_WARN("fail to process table batch op", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObString new_value;
    if (OB_FAIL(ObRedisHelper::double_to_string(op_temp_allocator_, new_val, new_value))) {
      LOG_WARN("fail to do double to string", K(ret), K(new_val));
    } else if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_bulk_string(new_value))) {
      LOG_WARN("fail to set int", K(ret), K(new_value));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

// return OB_ITER_END if score not exist
int ZSetCommandOperator::do_zscore_inner(int64_t db, const ObString &key, const ObString &member,
                                         double &score)
{
  int ret = OB_SUCCESS;
  ObTableOperationResult result;
  ObITableEntity *req_entity = nullptr;
  if (OB_FAIL(build_complex_type_rowkey_entity(db, key, true /*not meta*/, member, req_entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(member), K(db), K(key));
  } else if (OB_FAIL(process_table_single_op(ObTableOperation::retrieve(*req_entity), result))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to process table get", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObITableEntity *res_entity = nullptr;
    if (OB_FAIL(result.get_entity(res_entity))) {
      LOG_WARN("fail to get entity", K(ret), K(result));
    } else if (OB_FAIL(get_score_from_entity(*res_entity, score))) {
      LOG_WARN("fail to get score from entity", K(ret), KPC(res_entity));
    }
  }
  return ret;
}

int ZSetCommandOperator::do_zscore(int64_t db, const ObString &key, const ObString &member)
{
  int ret = OB_SUCCESS;
  double score = 0.0;
  if (OB_FAIL(do_zscore_inner(db, key, member, score))) {
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_fmt_res(ObRedisFmt::NULL_BULK_STRING))) {
        LOG_WARN("fail to set response null bulk string", K(ret));
      }
    } else {
      LOG_WARN("fail to find score", K(ret), K(db), K(key), K(member));
    }
  } else {
    ObString str;
    if (OB_FAIL(ObRedisHelper::double_to_string(redis_ctx_.allocator_, score, str))) {
      LOG_WARN("fail to convert double to string", K(ret), K(score));
    } else if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_bulk_string(str))) {
      LOG_WARN("fail to set response bulk string", K(ret), K(str));
    }
  }

  if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int ZSetCommandOperator::get_rank_in_same_score(
  int64_t db,
  ZRangeCtx &zrange_ctx,
  const ObString &member,
  int64_t score,
  ObRedisZSetMeta *set_meta,
  int64_t &rank)
{
  int ret = OB_SUCCESS;
  zrange_ctx.min_ = score;
  zrange_ctx.min_inclusive_ = true;
  zrange_ctx.max_ = score;
  zrange_ctx.max_inclusive_ = true;
  ObTableQuery query;
  rank = 0;
  ObTableAggregation cnt_agg(ObTableAggregationType::COUNT, "*");
  ObRedisRKey rkey(op_temp_allocator_, zrange_ctx.key_, true/*is_data*/, member);
  ObString rkey_str;
  ObObj *start_ptr = nullptr;
  ObObj *end_ptr = nullptr;
  ObNewRange *range = nullptr;
  int meta_rank_delta = 0;
  if (OB_ISNULL(start_ptr =
                    static_cast<ObObj *>(op_temp_allocator_.alloc(sizeof(ObObj) * 3)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else if (OB_FAIL(ObRedisCtx::reset_objects(start_ptr, 3))) {
    LOG_WARN("fail to init object", K(ret));
  } else if (OB_ISNULL(end_ptr = static_cast<ObObj *>(
                           op_temp_allocator_.alloc(sizeof(ObObj) * 3)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else if (OB_FAIL(ObRedisCtx::reset_objects(end_ptr, 3))) {
    LOG_WARN("fail to init object", K(ret));
  } else if (OB_FAIL(rkey.encode(rkey_str))) {
    LOG_WARN("fail to encode rkey", K(ret), K(rkey));
  } else {
    // index: db, key, score, rkey
    start_ptr[0].set_int(zrange_ctx.db_);
    start_ptr[1].set_varbinary(zrange_ctx.key_);
    start_ptr[2].set_double(score);

    end_ptr[0].set_int(zrange_ctx.db_);
    end_ptr[1].set_varbinary(zrange_ctx.key_);
    end_ptr[2].set_double(score);

    ObRowkey start_key(start_ptr, 3);
    ObRowkey end_key(end_ptr, 3);
    if (OB_FAIL(build_range(
            start_key, end_key, range, zrange_ctx.min_inclusive_, zrange_ctx.max_inclusive_))) {
      LOG_WARN("fail to build range", K(ret));
    } else if (OB_FAIL(query.add_scan_range(*range))) {
      LOG_WARN("fail to add scan range", K(ret), KPC(range));
    }
  }

  ObQueryFlag::ScanOrder scan_order =
      zrange_ctx.is_rev_ ? ObQueryFlag::ScanOrder::Reverse : ObQueryFlag::ScanOrder::Forward;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(query.set_scan_index(SCORE_INDEX_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.set_scan_order(scan_order))) {
    LOG_WARN("fail to set scan order", K(ret), K(scan_order));
  } else if (OB_FAIL(query.add_aggregation(cnt_agg))) {
    LOG_WARN("fail to add count aggregation", K(ret), K(cnt_agg));
  } else if (OB_FAIL(query.add_select_column(COUNT_STAR_PROPERTY))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  }

  if (OB_SUCC(ret)) {
    SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
    {
      QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, set_meta)
      ObTableQueryResult *one_result = nullptr;
      const ObITableEntity *result_entity = nullptr;
      // aggregate result only one row
      if (OB_FAIL(iter->get_next_result(one_result))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next result", K(ret));
        }
      } else {
        one_result->rewind();
        ObObj cnt_obj;
        if (OB_FAIL(one_result->get_next_entity(result_entity))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next result", K(ret));
          }
        } else if (OB_FAIL(result_entity->get_property(COUNT_STAR_PROPERTY, cnt_obj))) {
          LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
        } else if (OB_FAIL(cnt_obj.get_int(rank))) {
          LOG_WARN("fail to get count", K(ret), K(cnt_obj));
        }
      }
      QUERY_ITER_END(iter)
    }
  }
  return ret;
}

int ZSetCommandOperator::do_zrank(int64_t db, const ObString &member, ZRangeCtx &zrange_ctx)
{
  int ret = OB_SUCCESS;
  // 1. get score of <db, key, member>
  double score = 0.0;
  const ObString &key = zrange_ctx.key_;
  ObString score_str;
  if (OB_FAIL(do_zscore_inner(db, key, member, score))) {
    if (ret == OB_ITER_END) {
      // 2.1 score not fund, return null bulk string
      ret = OB_SUCCESS;
      if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_fmt_res(ObRedisFmt::NULL_BULK_STRING))) {
        LOG_WARN("fail to set response null bulk string", K(ret));
      }
    } else {
      LOG_WARN("fail to find score", K(ret), K(db), K(key), K(member));
    }
  } else if (OB_FAIL(ObRedisHelper::double_to_string(op_temp_allocator_, score, score_str))) {
    LOG_WARN("fail to convert double to string", K(ret), K(score));
  } else {
    // 2.2 score fund, continue to found the number of records less/greater than score
    ObTableQuery query;
    ObTableAggregation cnt_agg(ObTableAggregationType::COUNT, "*");
    int64_t cnt = 0;
    if (zrange_ctx.is_rev_) {
      zrange_ctx.min_ = score;
      zrange_ctx.min_inclusive_ = false;
      zrange_ctx.max_ = INFINITY;
      zrange_ctx.max_inclusive_ = true;
    } else {
      zrange_ctx.min_ = -INFINITY;
      zrange_ctx.min_inclusive_ = true;
      zrange_ctx.max_ = score;
      zrange_ctx.max_inclusive_ = false;
    }
    ObRedisZSetMeta *set_meta = nullptr;
    ObRedisMeta *meta = nullptr;
    if (OB_FAIL(get_meta(db, zrange_ctx.key_, ObRedisModel::ZSET, meta))) {
      LOG_WARN("fail to get meta", K(ret), K(zrange_ctx));
    } else if (FALSE_IT(set_meta = reinterpret_cast<ObRedisZSetMeta*>(meta))) {
    } else if (OB_FAIL(build_score_scan_query(op_temp_allocator_, zrange_ctx, query))) {
      LOG_WARN("fail to build scan query", K(ret), K(score));
    } else if (OB_FAIL(query.add_aggregation(cnt_agg))) {
      LOG_WARN("fail to add count aggregation", K(ret), K(cnt_agg));
    } else if (OB_FAIL(query.add_select_column(COUNT_STAR_PROPERTY))) {
      LOG_WARN("fail to add select member column", K(ret), K(query));
    } else {
      SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
      {
        QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, set_meta)
        ObTableQueryResult *one_result = nullptr;
        const ObITableEntity *result_entity = nullptr;
        // aggregate result only one row
        if (OB_FAIL(iter->get_next_result(one_result))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next result", K(ret));
          }
        } else {
          one_result->rewind();
          ObObj cnt_obj;
          if (OB_FAIL(one_result->get_next_entity(result_entity))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", K(ret));
            }
          } else if (OB_FAIL(result_entity->get_property(COUNT_STAR_PROPERTY, cnt_obj))) {
            LOG_WARN("fail to get member from result enrity", K(ret), KPC(result_entity));
          } else if (OB_FAIL(cnt_obj.get_int(cnt))) {
            LOG_WARN("fail to get count", K(ret), K(cnt_obj));
          }
        }
        QUERY_ITER_END(iter)
      }
    }

    // 3. exclusive member with same score
    int64_t same_member_rank = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_rank_in_same_score(db, zrange_ctx, member, score, set_meta, same_member_rank))) {
      LOG_WARN("fail to get rank in same score", K(ret));
    } else if (same_member_rank > 1) {
      cnt += (same_member_rank - 1); // "-1" means not include itself
    }

    // 4. count(*) is the rank of score, check if withscore is needed
    if (OB_FAIL(ret)) {
      if (ObRedisErr::is_redis_error(ret)) {
        RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
      }
    } else if (zrange_ctx.with_scores_) {
      ObFastFormatInt ffi(cnt);
      ObString cnt_str(ffi.length(), ffi.ptr());
      ObSEArray<ObString, 2> ret_arr;
      if (OB_FAIL(ret_arr.push_back(cnt_str))) {
        LOG_WARN("fail to push back count string", K(ret), K(cnt_str));
      } else if (OB_FAIL(ret_arr.push_back(score_str))) {
        LOG_WARN("fail to push back score string", K(ret), K(score_str));
      } else if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_array(ret_arr))) {
        LOG_WARN("fail to set diff member", K(ret));
      }
    } else {
      if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(cnt))) {
        LOG_WARN("fail to set diff member", K(ret));
      }
    }
  }

  return ret;
}

int ZSetCommandOperator::build_score_entity(int64_t db, const ObString &key, const ObString &member,
                                            double score, int64_t time, ObITableEntity *&entity)
{
  int ret = OB_SUCCESS;
  ObObj score_obj;
  score_obj.set_double(score);
  ObObj insert_ts_obj;
  insert_ts_obj.set_timestamp(time);
  ObObj expire_obj;
  expire_obj.set_null();
  if (OB_FAIL(build_complex_type_rowkey_entity(db, key, true /*not meta*/, member, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(member), K(db), K(key));
  } else if (OB_FAIL(entity->set_property(ObRedisUtil::SCORE_PROPERTY_NAME, score_obj))) {
    LOG_WARN("fail to set row key value", K(ret), K(score_obj));
  } else if (OB_FAIL(entity->set_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, insert_ts_obj))) {
    LOG_WARN("fail to set row key value", K(ret), K(score_obj));
  } else if (OB_FAIL(entity->set_property(ObRedisUtil::EXPIRE_TS_PROPERTY_NAME, expire_obj))) {
    LOG_WARN("fail to set row key value", K(ret), K(score_obj));
  }
  return ret;
}

int ZSetCommandOperator::get_score_from_entity(const ObITableEntity &entity, double &score)
{
  int ret = OB_SUCCESS;
  ObObj score_obj;
  if (OB_FAIL(entity.get_property(ObRedisUtil::SCORE_PROPERTY_NAME, score_obj))) {
    LOG_WARN("fail to get score obj", K(ret), K(entity));
  } else if (OB_FAIL(score_obj.get_double(score))) {
    LOG_WARN("fail to get score from obj", K(ret), K(score_obj));
  }
  return ret;
}

int ZSetCommandOperator::get_score_str_from_entity(ObIAllocator &allocator,
                                                   const ObITableEntity &entity,
                                                   ObString &score_str)
{
  int ret = OB_SUCCESS;
  double score_num = 0.0;
  if (OB_FAIL(get_score_from_entity(entity, score_num))) {
    LOG_WARN("fail to get score from entity", K(ret), K(entity));
  } else if (OB_FAIL(ObRedisHelper::double_to_string(allocator, score_num, score_str))) {
    LOG_WARN("fail to convert double to string", K(ret), K(score_str));
  }
  return ret;
}

// offset = start, limit = (end - start + 1), start and end should be >= 0
int ZSetCommandOperator::build_rank_scan_query(ObIAllocator &allocator, int64_t start, int64_t end,
                                               const ZRangeCtx &zrange_ctx, ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  ObObj *start_ptr = nullptr;
  ObObj *end_ptr = nullptr;
  ObNewRange *range = nullptr;
  if (OB_ISNULL(start_ptr =
                    static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * SCORE_INDEX_COL_NUM)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else if (OB_FAIL(ObRedisCtx::reset_objects(start_ptr, SCORE_INDEX_COL_NUM))) {
    LOG_WARN("fail to init object", K(ret));
  } else if (OB_ISNULL(end_ptr = static_cast<ObObj *>(
                           allocator.alloc(sizeof(ObObj) * SCORE_INDEX_COL_NUM)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else if (OB_FAIL(ObRedisCtx::reset_objects(end_ptr, SCORE_INDEX_COL_NUM))) {
    LOG_WARN("fail to init object", K(ret));
  } else {
    // index: <db, vk, score>
    start_ptr[0].set_int(zrange_ctx.db_);
    start_ptr[1].set_varbinary(zrange_ctx.key_);
    start_ptr[2].set_min_value();
    ObRowkey start_key(start_ptr, SCORE_INDEX_COL_NUM);
    end_ptr[0].set_int(zrange_ctx.db_);
    end_ptr[1].set_varbinary(zrange_ctx.key_);
    end_ptr[2].set_max_value();
    ObRowkey end_key(end_ptr, SCORE_INDEX_COL_NUM);
    if (OB_FAIL(build_range(start_key, end_key, range))) {
      LOG_WARN("fail to build range", K(ret));
    } else if (OB_FAIL(query.add_scan_range(*range))) {
      LOG_WARN("fail to add scan range", K(ret), KPC(range));
    }
  }

  ObQueryFlag::ScanOrder scan_order =
      zrange_ctx.is_rev_ ? ObQueryFlag::ScanOrder::Reverse : ObQueryFlag::ScanOrder::Forward;
  int meta_rank_delta = zrange_ctx.is_rev_ ? 0 : 1;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(query.add_select_column(ObRedisUtil::RKEY_PROPERTY_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.add_select_column(ObRedisUtil::SCORE_PROPERTY_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.set_scan_index(SCORE_INDEX_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.set_offset(start + meta_rank_delta))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.set_limit(end - start + 1))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.set_scan_order(scan_order))) {
    LOG_WARN("fail to set scan order", K(ret), K(scan_order));
  }
  return ret;
}

int ZSetCommandOperator::exec_member_score_query(const ObTableQuery &query, bool with_scores,
                                                 ObIArray<ObString> &ret_arr,
                                                 ObRedisZSetMeta *meta/* = nullptr*/)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
  {
    QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, meta)
    ObTableQueryResult *one_result = nullptr;
    const ObITableEntity *result_entity = nullptr;
    // 2. add <member, score> into ret_arr
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next_result(one_result))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next result", K(ret));
        }
      }
      one_result->rewind();
      while (OB_SUCC(ret)) {
        ObObj member_obj;
        ObString member;
        ObString score;
        if (OB_FAIL(one_result->get_next_entity(result_entity))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next result", K(ret));
          }
        } else if (OB_FAIL(get_subkey_from_entity(op_temp_allocator_, *result_entity, member))) {
          LOG_WARN("fail to get member from entity", K(ret), KPC(result_entity));
        } else if (OB_FAIL(ret_arr.push_back(member))) {
          LOG_WARN("fail to push back member", K(ret), K(member));
        } else if (with_scores) {
          if (OB_FAIL(get_score_str_from_entity(op_temp_allocator_, *result_entity, score))) {
            LOG_WARN("fail to get score str from entity", K(ret));
          } else if (OB_FAIL(ret_arr.push_back(score))) {
            LOG_WARN("fail to push back score", K(ret), K(score));
          }
        }
      }
    }
    QUERY_ITER_END(iter)
  }
  return ret;
}

int ZSetCommandOperator::do_zrange_inner(int64_t db, const ZRangeCtx &zrange_ctx,
                                         ObIArray<ObString> &ret_arr)
{
  int ret = OB_SUCCESS;
  // 0. process negtive start and end, -1 being the last element of the sorted set
  int64_t card = -1;
  int64_t start = zrange_ctx.start_;
  int64_t end = zrange_ctx.end_;
  if (OB_FAIL(do_zcard_inner(db, zrange_ctx.key_, card))) {
    LOG_WARN("fail to do zcard inner", K(ret), K(db), K(zrange_ctx.key_));
  } else if (card == 0) {
    // do nothing
  } else {
    start = start < 0 ? (start + card) : start;
    end = end < 0 ? (end + card) : end;
    start = start < 0 ? 0 : start;
  }

  // 1. build scan range with table.max >= member >= table.min
  //    do not need specify db and key cause partition by <db, key> and it is a local das
  ObTableQuery query;
  int64_t fixed_size = zrange_ctx.with_scores_ ? 2 * (end - start + 1) : end - start + 1;
  ObRedisZSetMeta *set_meta = nullptr;
  ObRedisMeta *meta = nullptr;
  if (OB_FAIL(ret) || card == 0 || start > end) {
  } else if (OB_FAIL(get_meta(db, zrange_ctx.key_, ObRedisModel::ZSET, meta))) {
    if (ret == OB_ITER_END) {
      // not exists key, return empty array
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get set meta", K(ret), K(zrange_ctx));
    }
  } else if (FALSE_IT(set_meta = reinterpret_cast<ObRedisZSetMeta*>(meta))) {
  } else if (OB_FAIL(ret_arr.reserve(fixed_size))) {
    LOG_WARN("fail to reserve space for ret_arr", K(ret), K(fixed_size));
  } else if (OB_FAIL(build_rank_scan_query(op_temp_allocator_, start, end, zrange_ctx, query))) {
    LOG_WARN("fail to build scan query", K(ret), K(start), K(end), K(zrange_ctx));
  } else if (OB_FAIL(exec_member_score_query(query, zrange_ctx.with_scores_, ret_arr, set_meta))) {
    LOG_WARN("fail to execute query", K(ret));
  }
  return ret;
}

int ZSetCommandOperator::do_zrange(int64_t db, const ZRangeCtx &zrange_ctx)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> ret_arr(OB_MALLOC_NORMAL_BLOCK_SIZE,
                            ModulePageAllocator(op_temp_allocator_, "RedisZRange"));
  if (OB_FAIL(do_zrange_inner(db, zrange_ctx, ret_arr))) {
    if (ObRedisErr::is_redis_error(ret)) {
      RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
    }
    LOG_WARN("fail to do zrange inner", K(ret), K(zrange_ctx));
  } else if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_array(ret_arr))) {
    LOG_WARN("fail to set ret member", K(ret));
  }
  return ret;
}

int ZSetCommandOperator::do_zrem_range_by_rank(int64_t db, const ZRangeCtx &zrange_ctx)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> members(OB_MALLOC_NORMAL_BLOCK_SIZE,
                            ModulePageAllocator(op_temp_allocator_, "RedisZRange"));
  if (OB_FAIL(do_zrange_inner(db, zrange_ctx, members))) {
    LOG_WARN("fail to do zrange inner", K(ret), K(db), K(zrange_ctx));
  } else if (OB_FAIL(do_srem(db, zrange_ctx.key_, members))) {
    LOG_WARN("fail to do zrem inner", K(ret), K(db), K(zrange_ctx.key_));
  }
  if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int ZSetCommandOperator::build_score_scan_query(ObIAllocator &allocator,
                                                const ZRangeCtx &zrange_ctx, ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  ObObj *start_ptr = nullptr;
  ObObj *end_ptr = nullptr;
  ObNewRange *range = nullptr;
  int meta_rank_delta = 0;
  if (OB_ISNULL(start_ptr =
                    static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * SCORE_INDEX_COL_NUM)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else if (OB_FAIL(ObRedisCtx::reset_objects(start_ptr, SCORE_INDEX_COL_NUM))) {
    LOG_WARN("fail to init object", K(ret));
  } else if (OB_ISNULL(end_ptr = static_cast<ObObj *>(
                           allocator.alloc(sizeof(ObObj) * SCORE_INDEX_COL_NUM)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else if (OB_FAIL(ObRedisCtx::reset_objects(end_ptr, SCORE_INDEX_COL_NUM))) {
    LOG_WARN("fail to init object", K(ret));
  } else {
    // index: db, key, score
    start_ptr[0].set_int(zrange_ctx.db_);
    start_ptr[1].set_varbinary(zrange_ctx.key_);
    if (zrange_ctx.min_ == -INFINITY) {
      start_ptr[2].set_min_value();
      // -INFINITY will include meta
      if (!zrange_ctx.is_rev_) {
        meta_rank_delta = 1;
      }
    } else {
      start_ptr[2].set_double(zrange_ctx.min_);
    }
    end_ptr[0].set_int(zrange_ctx.db_);
    end_ptr[1].set_varbinary(zrange_ctx.key_);
    if (zrange_ctx.max_ == INFINITY) {
      end_ptr[2].set_max_value();
    } else {
      end_ptr[2].set_double(zrange_ctx.max_);
    }

    ObRowkey start_key(start_ptr, SCORE_INDEX_COL_NUM);
    ObRowkey end_key(end_ptr, SCORE_INDEX_COL_NUM);
    if (OB_FAIL(build_range(
            start_key, end_key, range, zrange_ctx.min_inclusive_, zrange_ctx.max_inclusive_))) {
      LOG_WARN("fail to build range", K(ret));
    } else if (OB_FAIL(query.add_scan_range(*range))) {
      LOG_WARN("fail to add scan range", K(ret), KPC(range));
    }
  }

  ObQueryFlag::ScanOrder scan_order =
      zrange_ctx.is_rev_ ? ObQueryFlag::ScanOrder::Reverse : ObQueryFlag::ScanOrder::Forward;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(query.set_scan_index(SCORE_INDEX_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.set_offset(zrange_ctx.offset_ + meta_rank_delta))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.set_limit(zrange_ctx.limit_))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.set_scan_order(scan_order))) {
    LOG_WARN("fail to set scan order", K(ret), K(scan_order));
  }
  return ret;
}

int ZSetCommandOperator::do_zrange_by_score_inner(int64_t db, const ZRangeCtx &zrange_ctx,
                                                  ObIArray<ObString> &ret_arr)
{
  int ret = OB_SUCCESS;
  bool is_max_min_equal = fabs(zrange_ctx.min_ - zrange_ctx.max_) < OB_DOUBLE_EPSINON;
  ObTableQuery query;
  ObRedisZSetMeta *set_meta = nullptr;
  ObRedisMeta *meta = nullptr;

  if ((is_max_min_equal && (!zrange_ctx.max_inclusive_ || !zrange_ctx.min_inclusive_))
      || (zrange_ctx.max_ < zrange_ctx.min_) || (zrange_ctx.offset_ < 0)) {
    // do nothing, return empty array
  } else if (OB_FAIL(get_meta(db, zrange_ctx.key_, ObRedisModel::ZSET, meta))) {
    if (ret == OB_ITER_END) {
      // not exists key, return empty array
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get set meta", K(ret), K(zrange_ctx));
    }
  } else if (FALSE_IT(set_meta = reinterpret_cast<ObRedisZSetMeta*>(meta))) {
  } else if (OB_FAIL(build_score_scan_query(op_temp_allocator_, zrange_ctx, query))) {
    LOG_WARN("fail to build score scan query", K(ret), K(zrange_ctx));
  } else if (OB_FAIL(query.add_select_column(ObRedisUtil::RKEY_PROPERTY_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.add_select_column(ObRedisUtil::SCORE_PROPERTY_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(exec_member_score_query(query, zrange_ctx.with_scores_, ret_arr, set_meta))) {
    LOG_WARN("fail to execute query", K(ret), K(zrange_ctx));
  }
  return ret;
}

int ZSetCommandOperator::do_zrange_by_score(int64_t db, const ZRangeCtx &zrange_ctx)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> ret_arr(OB_MALLOC_NORMAL_BLOCK_SIZE,
                            ModulePageAllocator(op_temp_allocator_, "RedisZRange"));
  if (OB_FAIL(do_zrange_by_score_inner(db, zrange_ctx, ret_arr))) {
    if (ObRedisErr::is_redis_error(ret)) {
      RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
    }
    LOG_WARN("fail to do_zrange_by_score_inner", K(ret), K(zrange_ctx));
  } else if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_array(ret_arr))) {
    LOG_WARN("fail to set ret member", K(ret));
  }
  return ret;
}

int ZSetCommandOperator::do_zrem_range_by_score(int64_t db, const ZRangeCtx &zrange_ctx)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> members(OB_MALLOC_NORMAL_BLOCK_SIZE,
                            ModulePageAllocator(op_temp_allocator_, "RedisZRange"));
  if (OB_FAIL(do_zrange_by_score_inner(db, zrange_ctx, members))) {
    LOG_WARN("fail to do zrange inner", K(ret), K(db), K(zrange_ctx));
  } else if (OB_FAIL(do_srem(db, zrange_ctx.key_, members))) {
    LOG_WARN("fail to do zrem inner", K(ret), K(db), K(zrange_ctx.key_));
  }
  if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int ZSetCommandOperator::do_zcount(int64_t db, const ObString &key, double min, bool min_inclusive,
                                   double max, bool max_inclusive)
{
  int ret = OB_SUCCESS;
  bool is_max_min_equal = fabs(min - max) < OB_DOUBLE_EPSINON;
  ObTableQuery query;
  int64_t count = 0;
  ZRangeCtx range_ctx;
  range_ctx.db_ = db;
  range_ctx.key_ = key;
  range_ctx.min_ = min;
  range_ctx.max_ = max;
  range_ctx.min_inclusive_ = min_inclusive;
  range_ctx.max_inclusive_ = max_inclusive;
  ObRedisZSetMeta *set_meta = nullptr;
  ObRedisMeta *meta = nullptr;
  if (OB_FAIL(get_meta(db, key, ObRedisModel::ZSET, meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get meta", K(ret), K(db), K(key));
    }
  } else if (FALSE_IT(set_meta = reinterpret_cast<ObRedisZSetMeta*>(meta))) {
  }

  if (OB_FAIL(ret)) {
    if (ret == OB_ITER_END) {
      // do nothing, 0
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get meta", K(ret), K(db), K(key));
    }
  } else if ((is_max_min_equal && (!max_inclusive || !min_inclusive)) || (max < min)) {
    // do nothing, 0
  } else if (OB_FAIL(build_score_scan_query(op_temp_allocator_, range_ctx, query))) {
    LOG_WARN("fail to build score scan query",
             K(ret),
             K(min),
             K(min_inclusive),
             K(max),
             K(max_inclusive));
  } else if (OB_FAIL(query.add_select_column(ObRedisUtil::RKEY_PROPERTY_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.add_select_column(ObRedisUtil::SCORE_PROPERTY_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(process_table_query_count(op_temp_allocator_, query, set_meta, count))) {
    LOG_WARN("fail to process table query count", K(ret), K(query), K(meta), K(set_meta));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(count))) {
      LOG_WARN("fail to set response int", K(ret), K(count));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int ZSetCommandOperator::union_zset(int64_t db, const ObString &key, double weight,
                                    ZSetAggCommand::AggType agg_type,
                                    ZSetCommand::MemberScoreMap &ms_map)
{
  int ret = OB_SUCCESS;
  ObTableQuery query;
  if (OB_FAIL(add_complex_type_subkey_scan_range(db, key, query))) {
    LOG_WARN("fail to build scan query", K(ret), K(key), K(db));
  } else if (OB_FAIL(query.add_select_column(ObRedisUtil::RKEY_PROPERTY_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.add_select_column(ObRedisUtil::SCORE_PROPERTY_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else {
    SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
    {
      ObRedisZSetMeta *null_meta = nullptr;
      QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, null_meta)
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
          double new_score;
          if (OB_FAIL(one_result->get_next_entity(result_entity))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", K(ret));
            }
          } else if (OB_FAIL(get_subkey_from_entity(op_temp_allocator_, *result_entity, member))) {
            LOG_WARN("fail to get member from entity", K(ret), KPC(result_entity));
          } else if (OB_FAIL(get_score_from_entity(*result_entity, new_score))) {
            LOG_WARN("fail to get score from entity", K(ret), KPC(result_entity));
          } else {
            ZSetScoreUpdater updater(new_score, weight, agg_type);
            if (OB_FAIL(ms_map.set_or_update(member, new_score * weight, updater))) {
              LOG_WARN("fail to update or set score in set", K(ret), K(member), K(new_score));
            }
          }
        }
      }
      QUERY_ITER_END(iter)
    }
  }
  return ret;
}

int ZSetCommandOperator::do_zunion_store(int64_t db, const ObString &dest,
                                         const ObIArray<ObString> &keys,
                                         const ObIArray<double> &weights,
                                         ZSetAggCommand::AggType agg_type)
{
  int ret = OB_SUCCESS;
  // 1. aggregate <member, score> in ms_map
  ZSetCommand::MemberScoreMap ms_map;
  if (keys.empty() || keys.count() != weights.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(keys.count()), K(weights.count()), K(agg_type));
  } else if (OB_FAIL(ms_map.create(RedisCommand::DEFAULT_BUCKET_NUM,
                                   ObMemAttr(MTL_ID(), "RedisZUnion")))) {
    LOG_WARN("fail to create member-score map", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
    if (OB_FAIL(union_zset(db, keys.at(i), weights.at(i), agg_type, ms_map))) {
      LOG_WARN(
          "fail to union zset", K(ret), K(i), K(db), K(keys.at(i)), K(weights.at(i)), K(agg_type));
    }
  }

  // 2. clear dest zset and store ms_map to dest zset
  redis_ctx_.need_dist_das_ = false;
  bool is_exists = false; // unused
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(del_complex_key(ObRedisModel::ZSET, db, dest, false/*del_meta*/, is_exists))) {
    LOG_WARN("fail to delete dest", K(ret), K(db), K(dest));
  } else if (!ms_map.empty()) {
    if (OB_FAIL(insup_meta(db, dest, ObRedisModel::ZSET))) {
      LOG_WARN("fail to insert up meta", K(ret), K(db), K(dest));
    } else if (OB_FAIL(do_zadd_data(db, dest, ms_map, true/*is_new_meta*/))) {
      LOG_WARN("fail to do zadd", K(ret), K(db), K(dest), K(ms_map.size()));
    }
  } else {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(0))) {
      LOG_WARN("fail to set response int", K(ret));
    }
  }

  int tmp_ret = ms_map.destroy();
  if (tmp_ret != OB_SUCCESS) {
    LOG_WARN("fail to destroy map", K(ret));
    ret = COVER_SUCC(tmp_ret);
  }
  if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int ZSetCommandOperator::aggregate_scores_in_all_keys(int64_t db, const ObIArray<ObString> &keys,
                                                      const ObIArray<double> &weights,
                                                      ZSetAggCommand::AggType agg_type,
                                                      int start_idx, int end_idx,
                                                      const ObString &member, bool &is_member,
                                                      double &res_score)
{
  int ret = OB_SUCCESS;
  is_member = true;
  if (start_idx < 0 || end_idx > keys.count() || weights.count() != keys.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("start index or end index is invalid",
             K(ret),
             K(start_idx),
             K(end_idx),
             K(keys.count()),
             K(weights.count()));
  }
  for (int i = start_idx; OB_SUCC(ret) && is_member && i < end_idx; ++i) {
    ObTableOperation get_op;
    ObITableEntity *entity = nullptr;
    ObRowkey rowkey;
    if (OB_FAIL(build_complex_type_rowkey_entity(db, keys.at(i), true /*not meta*/, member, entity))) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null entity_factory_", K(ret), K(db), K(keys.at(i)));
    } else {
      ObTableOperation get_op = ObTableOperation::retrieve(*entity);
      ObTableOperationResult result;
      ObITableEntity *res_entity = nullptr;
      double score = 0.0;
      if (OB_FAIL(process_table_single_op(get_op, result))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to process table get", K(ret));
        } else {
          is_member = false;
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(result.get_entity(res_entity))) {
        LOG_WARN("fail to get entity from result", K(ret));
      } else if (OB_FAIL(get_score_from_entity(*res_entity, score))) {
        LOG_WARN("fail to get score from entity", K(ret), KPC(res_entity));
      } else if (OB_FAIL(ZSetScoreUpdater::aggregate_scores(
                     agg_type, res_score, score, weights.at(i), res_score))) {
        LOG_WARN("fail to aggregate score",
                 K(ret),
                 K(agg_type),
                 K(res_score),
                 K(score),
                 K(weights.at(i)));
      }
    }
  }
  return ret;
}

int ZSetCommandOperator::do_zinter_store(int64_t db, const ObString &dest,
                                         const ObIArray<ObString> &keys,
                                         const ObIArray<double> &weights,
                                         ZSetAggCommand::AggType agg_type)
{
  int ret = OB_SUCCESS;
  // 1. aggregate <member, score> in ms_map
  ZSetCommand::MemberScoreMap ms_map;
  if (keys.empty() || keys.count() != weights.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(keys.count()), K(weights.count()), K(agg_type));
  } else if (OB_FAIL(ms_map.create(RedisCommand::DEFAULT_BUCKET_NUM,
                                   ObMemAttr(MTL_ID(), "RedisZUnion")))) {
    LOG_WARN("fail to create member-score map", K(ret));
  } else {
    // use smallest key set better than use first key set
    // but getting the size of each key has a certain cost in ob-redis currently
    ObString first_key = keys.at(0);
    double first_weight = weights.at(0);
    ObTableQuery query;
    if (OB_FAIL(add_complex_type_subkey_scan_range(db, first_key, query))) {
      LOG_WARN("fail to build scan query", K(ret), K(db), K(first_key));
    } else if (OB_FAIL(query.add_select_column(ObRedisUtil::RKEY_PROPERTY_NAME))) {
      LOG_WARN("fail to add select member column", K(ret), K(query));
    } else if (OB_FAIL(query.add_select_column(ObRedisUtil::SCORE_PROPERTY_NAME))) {
      LOG_WARN("fail to add select member column", K(ret), K(query));
    } else {
      SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
      {
        ObRedisZSetMeta *null_meta = nullptr;
        QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, null_meta)
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
            bool is_member = false;
            double score = 0;
            if (OB_FAIL(one_result->get_next_entity(result_entity))) {
              if (OB_ITER_END != ret) {
                LOG_WARN("fail to get next result", K(ret));
              }
            } else if (OB_FAIL(
                           get_subkey_from_entity(op_temp_allocator_, *result_entity, member))) {
              LOG_WARN("fail to get member from entity", K(ret), K(*result_entity));
            } else if (OB_FAIL(get_score_from_entity(*result_entity, score))) {
              LOG_WARN("fail to get member from entity", K(ret), K(*result_entity));
            } else {
              double res_score = score * first_weight;
              if (OB_FAIL(aggregate_scores_in_all_keys(db,
                                                       keys,
                                                       weights,
                                                       agg_type,
                                                       1,
                                                       keys.count(),
                                                       member,
                                                       is_member,
                                                       res_score))) {
                LOG_WARN("fail to check is member in set", K(ret), K(keys.count()), K(db));
              } else if (is_member) {
                if (OB_FAIL(ms_map.set_refactored(member, res_score))) {
                  LOG_WARN("fail to update or set score in set", K(ret), K(member), K(res_score));
                }
              }
            }
          }
        }
        QUERY_ITER_END(iter)
      }
    }
  }

  // 2. store ms_map to dest zset
  redis_ctx_.need_dist_das_ = false;
  bool is_exists = false; // unused
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(del_complex_key(ObRedisModel::ZSET, db, dest, false/*del_meta*/, is_exists))) {
    LOG_WARN("fail to delete dest", K(ret), K(db), K(dest));
  } else if (!ms_map.empty()) {
    if (OB_FAIL(insup_meta(db, dest, ObRedisModel::ZSET))) {
      LOG_WARN("fail to insert up meta", K(ret), K(db), K(dest));
    } else if (OB_FAIL(do_zadd_data(db, dest, ms_map, true/*is_new_meta*/))) {
      LOG_WARN("fail to do zadd", K(ret), K(db), K(dest), K(ms_map.size()));
    }
  } else {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(0))) {
      LOG_WARN("fail to set response int", K(ret));
    }
  }

  int tmp_ret = ms_map.destroy();
  if (tmp_ret != OB_SUCCESS) {
    LOG_WARN("fail to destroy map", K(ret));
    ret = COVER_SUCC(tmp_ret);
  }
  if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int ZSetCommandOperator::fill_set_batch_op(const ObRedisOp &op,
                                           ObIArray<ObTabletID> &tablet_ids,
                                           ObTableBatchOperation &batch_op)
{
  int ret = OB_SUCCESS;
  const ZAdd *zadd = reinterpret_cast<const ZAdd*>(op.cmd());
  if (OB_ISNULL(zadd)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null zadd op", K(ret));
  } else if (OB_FAIL(tablet_ids.push_back(op.tablet_id()))) {
    LOG_WARN("fail to push back tablet id", K(ret));
  }
  int64_t cur_ts = ObTimeUtility::fast_current_time();
  const ZAdd::MemberScoreMap &mem_score_map = zadd->member_score_map();
  ObITableEntity *value_entity = nullptr;
  ObString key;
  ObObj insert_obj;
  ObObj expire_obj;
  for (ZSetCommand::MemberScoreMap::const_iterator iter = mem_score_map.begin();
      OB_SUCC(ret) && iter != mem_score_map.end(); ++iter) {
    insert_obj.set_timestamp(cur_ts);
    expire_obj.set_null();
    if (OB_FAIL(op.get_key(key))) {
      LOG_WARN("fail to get key", K(ret), K(op));
    } else if (OB_FAIL(build_score_entity(op.db(), key, iter->first, iter->second, cur_ts, value_entity))) {
      LOG_WARN(
          "fail to build score entity", K(ret), K(iter->first), K(iter->second), K(op.db()), K(key));
    } else {
      if (OB_FAIL(batch_op.insert_or_update(*value_entity))) {
        LOG_WARN("fail to push back insert or update op", K(ret), KPC(value_entity));
      }
    }
  }
  return ret;
}

int ZSetCommandOperator::do_group_zscore()
{
  int ret = OB_SUCCESS;
  ResultFixedArray batch_res(op_temp_allocator_);
  if (OB_FAIL(group_get_complex_type_data(ObRedisUtil::SCORE_PROPERTY_NAME, batch_res))) {
    LOG_WARN("fail to group get complex type data", K(ret), K(ObRedisUtil::SCORE_PROPERTY_NAME));
  }

  // reply
  ObRedisBatchCtx &group_ctx = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_);
  for (int i = 0; i < group_ctx.ops().count() && OB_SUCC(ret); ++i) {
    ObRedisOp *op = reinterpret_cast<ObRedisOp*>(group_ctx.ops().at(i));
    if (batch_res.at(i).get_return_rows() == 0) {
      if (OB_FAIL(op->response().set_fmt_res(ObRedisFmt::NULL_BULK_STRING))) {
        LOG_WARN("fail to set response null bulk string", K(ret));
      }
    } else {
      double score = 0.0;
      ObITableEntity *res_entity = nullptr;
      ObString score_str;
      if (OB_FAIL(batch_res.at(i).get_entity(res_entity))) {
        LOG_WARN("fail to get entity", K(ret), K(batch_res.at(i)));
      } else if (OB_FAIL(get_score_from_entity(*res_entity, score))) {
        LOG_WARN("fail to get score from entity", K(ret), KPC(res_entity));
      } else if (OB_FAIL(ObRedisHelper::double_to_string(redis_ctx_.allocator_, score, score_str))) {
        LOG_WARN("fail to convert double to string", K(ret), K(score));
      } else if (OB_FAIL(op->response().set_res_bulk_string(score_str))) {
        LOG_WARN("fail to set response bulk string", K(ret), K(score_str));
      }
    }
  }
  return ret;
}
}  // namespace table
}  // namespace oceanbase
