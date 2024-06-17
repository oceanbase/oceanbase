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

namespace oceanbase
{
using namespace common;
namespace table
{
const ObString ZSetCommandOperator::SCORE_PROPERTY_NAME = ObString::make_string("score");
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
  const ObITableEntity &req_entity = redis_ctx_.get_entity();
  ObTableBatchOperation ops;
  ops.set_entity_factory(redis_ctx_.entity_factory_);

  for (ZSetCommand::MemberScoreMap::const_iterator iter = mem_score_map.begin();
       OB_SUCC(ret) && iter != mem_score_map.end();
       ++iter) {
    ObITableEntity *value_entity = nullptr;
    if (OB_FAIL(build_score_entity(db, key, iter->first, iter->second, value_entity))) {
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
    if (!results[i].get_is_insertup_do_update()) {
      ++insert_num;
    }
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

int ZSetCommandOperator::do_zcard_inner(int64_t db, const ObString &key, int64_t &card)
{
  int ret = OB_SUCCESS;
  ObTableQuery query;
  ObTableAggregation cnt_agg(ObTableAggregationType::COUNT, "*");
  if (OB_FAIL(add_member_scan_range(db, key, query))) {
    LOG_WARN("fail to add scan query", K(ret));
  } else if (OB_FAIL(query.add_aggregation(cnt_agg))) {
    LOG_WARN("fail to add count aggregation", K(ret), K(cnt_agg));
  } else if (OB_FAIL(query.add_select_column(COUNT_STAR_PROPERTY))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else {
    SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
    {
      QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter)
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
    LOG_WARN("fail to do zcard inner", K(ret), K(db), K(key));
  } else if (OB_FAIL(redis_ctx_.response_.set_res_int(cnt))) {
    LOG_WARN("fail to set diff member", K(ret));
  } else {
    redis_ctx_.response_.return_table_error(ret);
  }
  return ret;
}

int ZSetCommandOperator::do_zrem_inner(int64_t db, const ObString &key,
                                       const ObIArray<ObString> &members, int64_t &del_num)
{
  int ret = OB_SUCCESS;
  const ObITableEntity &req_entity = redis_ctx_.get_entity();
  ObTableBatchOperation ops;
  ops.set_entity_factory(redis_ctx_.entity_factory_);

  for (int i = 0; OB_SUCC(ret) && i < members.count(); ++i) {
    ObITableEntity *value_entity = nullptr;
    if (OB_FAIL(build_hash_set_rowkey_entity(db, key, members.at(i), value_entity))) {
      LOG_WARN("fail to get rowkey entity", K(ret), K(db), K(key), K(i), K(members.at(i)));
    } else if (OB_FAIL(ops.del(*value_entity))) {
      LOG_WARN("fail to push back insert or update op", K(ret), KPC(value_entity));
    }
  }

  ResultFixedArray results(op_temp_allocator_);
  if (OB_SUCC(ret) && OB_FAIL(process_table_batch_op(ops, results))) {
    LOG_WARN("fail to process table batch op", K(ret));
  }

  del_num = 0;
  for (int i = 0; OB_SUCC(ret) && i < results.count(); ++i) {
    del_num += results[i].get_affected_rows();
  }

  return ret;
}

int ZSetCommandOperator::do_zrem(int64_t db, const ObString &key, const ObIArray<ObString> &members)
{
  int ret = OB_SUCCESS;
  int64_t del_num = 0;
  if (OB_FAIL(do_zrem_inner(db, key, members, del_num))) {
    LOG_WARN("fail to do zrem inner", K(ret), K(db), K(key));
  } else if (OB_FAIL(redis_ctx_.response_.set_res_int(del_num))) {
    LOG_WARN("fail to set response int", K(ret), K(del_num));
  }
  if (OB_FAIL(ret)) {
    redis_ctx_.response_.return_table_error(ret);
  }

  return ret;
}

int ZSetCommandOperator::do_zincrby(int64_t db, const ObString &key, const ObString &member,
                                    double increment)
{
  int ret = OB_SUCCESS;
  ObTableOperation get_op;
  ObITableEntity *entity = nullptr;
  // db rkey member
  ObRowkey rowkey = redis_ctx_.get_entity().get_rowkey();
  ObObj score_obj;
  score_obj.set_double(increment);
  double ret_score = 0.0;
  if (OB_FAIL(build_score_entity(db, key, member, increment, entity))) {
    LOG_WARN("fail to build score entity", K(ret), K(member), K(increment), K(db), K(key));
  } else {
    ObTableOperation op = ObTableOperation::increment(*entity);
    ObTableOperationResult result;
    ObITableEntity *res_entity = nullptr;
    ObObj res_score;
    if (OB_FAIL(process_table_single_op(op, result, true))) {
      LOG_WARN("fail to process table get", K(ret));
    } else if (OB_FAIL(result.get_entity(res_entity))) {
      LOG_WARN("fail to get entity", K(ret), K(result));
    } else if (OB_FAIL(get_score_from_entity(*res_entity, ret_score))) {
      LOG_WARN("fail to get score from entity", K(ret), KPC(entity));
    }
  }

  if (OB_SUCC(ret)) {
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t length =
        ob_gcvt(ret_score, OB_GCVT_ARG_DOUBLE, static_cast<int32_t>(sizeof(buf) - 1), buf, NULL);
    ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
    if (length == 0) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("fail to convert double to string", K(ret));
    } else if (OB_FAIL(redis_ctx_.response_.set_res_bulk_string(str))) {
      LOG_WARN("fail to set response int", K(ret), K(str));
    }
  }
  if (OB_FAIL(ret)) {
    redis_ctx_.response_.return_table_error(ret);
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
  if (OB_FAIL(build_hash_set_rowkey_entity(db, key, member, req_entity))) {
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
      if (OB_FAIL(redis_ctx_.response_.set_fmt_res(ObRedisUtil::NULL_BULK_STRING))) {
        LOG_WARN("fail to set response null bulk string", K(ret));
      }
    } else {
      LOG_WARN("fail to find score", K(ret), K(db), K(key), K(member));
    }
  } else {
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t length =
        ob_gcvt(score, OB_GCVT_ARG_DOUBLE, static_cast<int32_t>(sizeof(buf) - 1), buf, NULL);
    ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
    if (length == 0) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("fail to convert double to string", K(ret));
    } else if (OB_FAIL(redis_ctx_.response_.set_res_bulk_string(str))) {
      LOG_WARN("fail to set response bulk string", K(ret), K(str));
    }
  }

  if (OB_FAIL(ret)) {
    redis_ctx_.response_.return_table_error(ret);
  }
  return ret;
}

int ZSetCommandOperator::do_zrank(int64_t db, const ObString &member, ZRangeCtx &zrange_ctx)
{
  int ret = OB_SUCCESS;
  // 1. get score of <db, key, member>
  double score = 0.0;
  const ObString &key = zrange_ctx.key_;
  if (OB_FAIL(do_zscore_inner(db, key, member, score))) {
    if (ret == OB_ITER_END) {
      // 2.1 score not fund, return null bulk string
      ret = OB_SUCCESS;
      if (OB_FAIL(redis_ctx_.response_.set_fmt_res(ObRedisUtil::NULL_BULK_STRING))) {
        LOG_WARN("fail to set response null bulk string", K(ret));
      }
    } else {
      LOG_WARN("fail to find score", K(ret), K(db), K(key), K(member));
    }
  } else {
    // 2.2 score fund, continue to found the number of records less/greater than score
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t length =
        ob_gcvt(score, OB_GCVT_ARG_DOUBLE, static_cast<int32_t>(sizeof(buf) - 1), buf, NULL);
    ObString score_str(sizeof(buf), static_cast<int32_t>(length), buf);
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
    if (OB_FAIL(build_score_scan_query(op_temp_allocator_, zrange_ctx, query))) {
      LOG_WARN("fail to build scan query", K(ret), K(score));
    } else if (OB_FAIL(query.add_aggregation(cnt_agg))) {
      LOG_WARN("fail to add count aggregation", K(ret), K(cnt_agg));
    } else if (OB_FAIL(query.add_select_column(COUNT_STAR_PROPERTY))) {
      LOG_WARN("fail to add select member column", K(ret), K(query));
    } else {
      SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
      {
        QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter)
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
          } else if (OB_FAIL(cnt_obj.get_int(cnt))) {
            LOG_WARN("fail to get count", K(ret), K(cnt_obj));
          }
        }
        QUERY_ITER_END(iter)
      }
    }

    // 3. count(*) is the rank of score, check if withscore is needed
    if (OB_FAIL(ret)) {
    } else if (zrange_ctx.with_scores_) {
      ObFastFormatInt ffi(cnt);
      ObString cnt_str(ffi.length(), ffi.ptr());
      ObSEArray<ObString, 2> ret_arr;
      if (OB_FAIL(ret_arr.push_back(cnt_str))) {
        LOG_WARN("fail to push back count string", K(ret), K(cnt_str));
      } else if (OB_FAIL(ret_arr.push_back(score_str))) {
        LOG_WARN("fail to push back score string", K(ret), K(score_str));
      } else if (OB_FAIL(redis_ctx_.response_.set_res_array(ret_arr))) {
        LOG_WARN("fail to set diff member", K(ret));
      }
    } else {
      if (OB_FAIL(redis_ctx_.response_.set_res_int(cnt))) {
        LOG_WARN("fail to set diff member", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    redis_ctx_.response_.return_table_error(ret);
  }

  return ret;
}

int ZSetCommandOperator::build_score_entity(int64_t db, const ObString &key, const ObString &member,
                                            double score, ObITableEntity *&entity)
{
  int ret = OB_SUCCESS;
  ObObj score_obj;
  score_obj.set_double(score);
  if (OB_FAIL(build_hash_set_rowkey_entity(db, key, member, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(member), K(db), K(key));
  } else if (OB_FAIL(entity->set_property(SCORE_PROPERTY_NAME, score_obj))) {
    LOG_WARN("fail to set row key value", K(ret), K(score_obj));
  }
  return ret;
}

int ZSetCommandOperator::get_score_from_entity(const ObITableEntity &entity, double &score)
{
  int ret = OB_SUCCESS;
  ObObj score_obj;
  if (OB_FAIL(entity.get_property(SCORE_PROPERTY_NAME, score_obj))) {
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
  } else {
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t length =
        ob_gcvt(score_num, OB_GCVT_ARG_DOUBLE, static_cast<int32_t>(sizeof(buf) - 1), buf, NULL);
    ObString tmp_str(sizeof(buf), static_cast<int32_t>(length), buf);
    if (length == 0) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("fail to convert double to string", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, tmp_str, score_str))) {
      LOG_WARN("fail to write string", K(ret));
    }
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
  ObQueryFlag::ScanOrder scan_order =
      zrange_ctx.is_rev_ ? ObQueryFlag::ScanOrder::Reverse : ObQueryFlag::ScanOrder::Forward;
  if (OB_ISNULL(start_ptr =
                    static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * SCORE_INDEX_COL_NUM)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else if (OB_ISNULL(end_ptr = static_cast<ObObj *>(
                           allocator.alloc(sizeof(ObObj) * SCORE_INDEX_COL_NUM)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else {
    // index: <db, rkey, score>
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

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(query.add_select_column(MEMBER_PROPERTY_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.add_select_column(SCORE_PROPERTY_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.set_scan_index(SCORE_INDEX_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.set_offset(start))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.set_limit(end - start + 1))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.set_scan_order(scan_order))) {
    LOG_WARN("fail to set scan order", K(ret), K(scan_order));
  }
  return ret;
}

int ZSetCommandOperator::exec_member_score_query(const ObTableQuery &query, bool with_scores,
                                                 ObIArray<ObString> &ret_arr)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
  {
    QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter)
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
        } else if (OB_FAIL(get_member_from_entity(op_temp_allocator_, *result_entity, member))) {
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
  if (OB_FAIL(ret) || card == 0 || start > end) {
  } else if (OB_FAIL(ret_arr.reserve(fixed_size))) {
    LOG_WARN("fail to reserve space for ret_arr", K(ret), K(fixed_size));
  } else if (OB_FAIL(build_rank_scan_query(op_temp_allocator_, start, end, zrange_ctx, query))) {
    LOG_WARN("fail to build scan query", K(ret), K(start), K(end), K(zrange_ctx));
  } else if (OB_FAIL(exec_member_score_query(query, zrange_ctx.with_scores_, ret_arr))) {
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
    LOG_WARN("fail to do zrange inner", K(ret), K(zrange_ctx));
  } else if (OB_FAIL(redis_ctx_.response_.set_res_array(ret_arr))) {
    LOG_WARN("fail to set ret member", K(ret));
  } else {
    redis_ctx_.response_.return_table_error(ret);
  }
  return ret;
}

int ZSetCommandOperator::delete_key(int64_t db, const ObString &key)
{
  int ret = OB_SUCCESS;
  int64_t del_num = 0;
  ZRangeCtx zrange_ctx;
  zrange_ctx.key_ = key;
  zrange_ctx.start_ = 0;
  zrange_ctx.end_ = -1;
  ObArray<ObString> members(OB_MALLOC_NORMAL_BLOCK_SIZE,
                            ModulePageAllocator(op_temp_allocator_, "RedisZRange"));
  if (OB_FAIL(do_zrange_inner(db, zrange_ctx, members))) {
    LOG_WARN("fail to do zrange inner", K(ret), K(db), K(zrange_ctx));
  } else if (OB_FAIL(do_zrem_inner(db, key, members, del_num))) {
    LOG_WARN("fail to do zrem inner", K(ret), K(db), K(zrange_ctx.key_));
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
  } else if (OB_FAIL(do_zrem(db, zrange_ctx.key_, members))) {
    LOG_WARN("fail to do zrem inner", K(ret), K(db), K(zrange_ctx.key_));
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
  ObQueryFlag::ScanOrder scan_order =
      zrange_ctx.is_rev_ ? ObQueryFlag::ScanOrder::Reverse : ObQueryFlag::ScanOrder::Forward;
  if (OB_ISNULL(start_ptr =
                    static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * SCORE_INDEX_COL_NUM)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else if (OB_ISNULL(end_ptr = static_cast<ObObj *>(
                           allocator.alloc(sizeof(ObObj) * SCORE_INDEX_COL_NUM)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else {
    // index: db, key, score
    start_ptr[0].set_int(zrange_ctx.db_);
    start_ptr[1].set_varbinary(zrange_ctx.key_);
    if (zrange_ctx.min_ == -INFINITY) {
      start_ptr[2].set_min_value();
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

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(query.set_scan_index(SCORE_INDEX_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.set_offset(zrange_ctx.offset_))) {
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

  if ((is_max_min_equal && (!zrange_ctx.max_inclusive_ || !zrange_ctx.min_inclusive_))
      || (zrange_ctx.max_ < zrange_ctx.min_) || (zrange_ctx.offset_ < 0)) {
    // do nothing, return empty array
  } else if (OB_FAIL(build_score_scan_query(op_temp_allocator_, zrange_ctx, query))) {
    LOG_WARN("fail to build score scan query", K(ret), K(zrange_ctx));
  } else if (OB_FAIL(query.add_select_column(MEMBER_PROPERTY_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.add_select_column(SCORE_PROPERTY_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(exec_member_score_query(query, zrange_ctx.with_scores_, ret_arr))) {
    LOG_WARN("fail to execute query", K(ret));
  }
  return ret;
}

int ZSetCommandOperator::do_zrange_by_score(int64_t db, const ZRangeCtx &zrange_ctx)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> ret_arr(OB_MALLOC_NORMAL_BLOCK_SIZE,
                            ModulePageAllocator(op_temp_allocator_, "RedisZRange"));
  if (OB_FAIL(do_zrange_by_score_inner(db, zrange_ctx, ret_arr))) {
    LOG_WARN("fail to do_zrange_by_score_inner", K(ret), K(zrange_ctx));
  } else if (OB_FAIL(redis_ctx_.response_.set_res_array(ret_arr))) {
    LOG_WARN("fail to set ret member", K(ret));
  } else {
    redis_ctx_.response_.return_table_error(ret);
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
  } else if (OB_FAIL(do_zrem(db, zrange_ctx.key_, members))) {
    LOG_WARN("fail to do zrem inner", K(ret), K(db), K(zrange_ctx.key_));
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

  if ((is_max_min_equal && (!max_inclusive || !min_inclusive)) || (max < min)) {
    // do nothing, 0
  } else if (OB_FAIL(build_score_scan_query(op_temp_allocator_, range_ctx, query))) {
    LOG_WARN("fail to build score scan query",
             K(ret),
             K(min),
             K(min_inclusive),
             K(max),
             K(max_inclusive));
  } else if (OB_FAIL(query.add_select_column(MEMBER_PROPERTY_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.add_select_column(SCORE_PROPERTY_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(process_table_query_count(op_temp_allocator_, query, count))) {
    LOG_WARN("fail to process table query count", K(ret), K(query));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(redis_ctx_.response_.set_res_int(count))) {
      LOG_WARN("fail to set response int", K(ret), K(count));
    }
  } else {
    redis_ctx_.response_.return_table_error(ret);
  }
  return ret;
}

int ZSetCommandOperator::union_zset(int64_t db, const ObString &key, double weight,
                                    ZSetAggCommand::AggType agg_type,
                                    ZSetCommand::MemberScoreMap &ms_map)
{
  int ret = OB_SUCCESS;
  ObTableQuery query;
  if (OB_FAIL(add_member_scan_range(db, key, query))) {
    LOG_WARN("fail to build scan query", K(ret));
  } else if (OB_FAIL(query.add_select_column(MEMBER_PROPERTY_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else if (OB_FAIL(query.add_select_column(SCORE_PROPERTY_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
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
          ObString member;
          double new_score;
          if (OB_FAIL(one_result->get_next_entity(result_entity))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", K(ret));
            }
          } else if (OB_FAIL(get_member_from_entity(op_temp_allocator_, *result_entity, member))) {
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
  redis_ctx_.tb_ctx_.set_need_dist_das(false);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(delete_key(db, dest))) {
    LOG_WARN("fail to delete dest", K(ret), K(db), K(dest));
  } else if (OB_FAIL(do_zadd(db, dest, ms_map))) {
    LOG_WARN("fail to do zadd", K(ret), K(db), K(dest), K(ms_map.size()));
  }

  int tmp_ret = ms_map.destroy();
  if (tmp_ret != OB_SUCCESS) {
    LOG_WARN("fail to destroy map", K(ret));
    ret = COVER_SUCC(tmp_ret);
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
    if (OB_FAIL(build_hash_set_rowkey_entity(db, keys.at(i), member, entity))) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null entity_factory_", K(ret));
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
    if (OB_FAIL(add_member_scan_range(db, first_key, query))) {
      LOG_WARN("fail to build scan query", K(ret));
    } else if (OB_FAIL(query.add_select_column(MEMBER_PROPERTY_NAME))) {
      LOG_WARN("fail to add select member column", K(ret), K(query));
    } else if (OB_FAIL(query.add_select_column(SCORE_PROPERTY_NAME))) {
      LOG_WARN("fail to add select member column", K(ret), K(query));
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
            ObString member;
            bool is_member = false;
            double score = 0;
            if (OB_FAIL(one_result->get_next_entity(result_entity))) {
              if (OB_ITER_END != ret) {
                LOG_WARN("fail to get next result", K(ret));
              }
            } else if (OB_FAIL(
                           get_member_from_entity(op_temp_allocator_, *result_entity, member))) {
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
  redis_ctx_.tb_ctx_.set_need_dist_das(false);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(delete_key(db, dest))) {
    LOG_WARN("fail to delete dest", K(ret), K(db), K(dest));
  } else if (OB_FAIL(do_zadd(db, dest, ms_map))) {
    LOG_WARN("fail to do zadd", K(ret), K(db), K(dest));
  }

  int tmp_ret = ms_map.destroy();
  if (tmp_ret != OB_SUCCESS) {
    LOG_WARN("fail to destroy map", K(ret));
    ret = COVER_SUCC(tmp_ret);
  }
  return ret;
}

int ZSetCommandOperator::add_db_rkey_filter(int64_t db, const ObString &key, ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  ObFastFormatInt ffi(db);
  ObString db_str(ffi.length(), ffi.ptr());
  FilterStrBuffer buffer(op_temp_allocator_);
  ObString filter_str;
  if (OB_FAIL(buffer.add_value_compare(
          hfilter::CompareOperator::EQUAL, ObRedisUtil::DB_PROPERTY_NAME, db_str))) {
    LOG_WARN("fail to gen compare filter string", K(ret), K(db_str));
  } else if (OB_FAIL(buffer.add_conjunction(true))) {
    LOG_WARN("fail to add conjunction", K(ret));
  } else if (OB_FAIL(buffer.add_value_compare(
                 hfilter::CompareOperator::EQUAL, ObRedisUtil::RKEY_PROPERTY_NAME, key))) {
    LOG_WARN("fail to gen compare filter string", K(ret), K(db_str));
  } else if (OB_FAIL(buffer.get_filter_str(filter_str))) {
    LOG_WARN("fail to get filter str");
  } else if (OB_FAIL(query.set_filter(filter_str))) {
    LOG_WARN("fail to set query filter", K(ret), K(filter_str));
  }
  return ret;
}

}  // namespace table
}  // namespace oceanbase
