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
#include "ob_redis_set_operator.h"
#include "share/table/redis/ob_redis_error.h"
#include "src/observer/table/redis/ob_redis_rkey.h"

namespace oceanbase
{
using namespace observer;
using namespace common;
using namespace share;
using namespace sql;
namespace table
{
int SetCommandOperator::agg_need_push_member(
    int64_t db,
    const ObIArray<ObString> &keys,
    const ObString &member,
    SetCommand::AggFunc agg_func,
    bool &is_needed)
{
  int ret = OB_SUCCESS;
  is_needed = false;
  switch (agg_func) {
    case SetCommand::AggFunc::DIFF: {
      bool is_member = false;
      if (OB_FAIL(is_member_in_one_key(db, keys, 1 /*start_idx*/, keys.count(), member, is_member))) {
        LOG_WARN("fail to check is member in set", K(ret), K(keys.count()), K(db));
      } else {
        is_needed = !is_member;
      }
      break;
    }
    case SetCommand::AggFunc::UNION: {
      is_needed = true;
      break;
    }
    case SetCommand::AggFunc::INTER: {
      if (OB_FAIL(is_member_in_all_keys(db, keys, 1 /*start_idx*/, keys.count(), member, is_needed))) {
        LOG_WARN("fail to check is member in set", K(ret), K(keys.count()), K(db));
      }
      break;
    }
  }
  return ret;
}

int SetCommandOperator::do_union(int64_t db, const ObString &key, SetCommand::MemberSet &members)
{
  int ret = OB_SUCCESS;

  ObTableQuery query;
  if (OB_FAIL(add_complex_type_subkey_scan_range(db, key, query))) {
    LOG_WARN("fail to build scan query", K(ret), K(key), K(db));
  } else if (OB_FAIL(query.add_select_column(ObRedisUtil::RKEY_PROPERTY_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else {
    SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
    {
      ObRedisSetMeta *null_meta = nullptr;
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
          bool is_needed = false;
          if (OB_FAIL(one_result->get_next_entity(result_entity))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", K(ret));
            }
          } else if (OB_FAIL(get_subkey_from_entity(op_temp_allocator_, *result_entity, member))) {
            LOG_WARN("fail to get member from entity", K(ret), KPC(result_entity), K(member));
          } else if (OB_FAIL(members.set_refactored(member))) {
            if (ret == OB_HASH_EXIST) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to push diff member", K(ret));
            }
          }
        }
      }
      QUERY_ITER_END(iter)
    }
  }
  return ret;
}

int SetCommandOperator::do_aggregate_inner(
    int64_t db,
    const ObIArray<ObString> &keys,
    SetCommand::AggFunc agg_func,
    SetCommand::MemberSet &members)
{
  int ret = OB_SUCCESS;

  ObString first_key = keys.at(0);
  ObTableQuery query;
  if (OB_FAIL(add_complex_type_subkey_scan_range(db, first_key, query))) {
    LOG_WARN("fail to build scan query", K(ret), K(first_key), K(db));
  } else if (OB_FAIL(query.add_select_column(ObRedisUtil::RKEY_PROPERTY_NAME))) {
    LOG_WARN("fail to add select member column", K(ret), K(query));
  } else {
    SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
    {
      ObRedisSetMeta *null_meta = nullptr;
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
          bool is_needed = false;
          if (OB_FAIL(one_result->get_next_entity(result_entity))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", K(ret));
            }
          } else if (OB_FAIL(get_subkey_from_entity(op_temp_allocator_, *result_entity, member))) {
            LOG_WARN("fail to get member from entity", K(ret), KPC(result_entity), K(member));
          } else if (agg_need_push_member(db, keys, member, agg_func, is_needed)) {
            LOG_WARN("fail to cal if member is needed to push", K(ret));
          } else if (is_needed) {
            if (OB_FAIL(members.set_refactored(member))) {
              if (ret == OB_HASH_EXIST) {
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("fail to push diff member", K(ret));
              }
            }
          }
        }
      }
      QUERY_ITER_END(iter)
    }
  }
  return ret;
}

int SetCommandOperator::do_aggregate(int64_t db, const ObIArray<ObString> &keys, SetCommand::AggFunc agg_func)
{
  int ret = OB_SUCCESS;
  SetCommand::MemberSet members;
  ObArray<ObString> member_arr(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(op_temp_allocator_, "RedisSAgg"));
  if (OB_FAIL(members.create(RedisCommand::DEFAULT_BUCKET_NUM, ObMemAttr(MTL_ID(), "RedisSAgg")))) {
    LOG_WARN("fail to create hash set", K(ret));
  } else if (agg_func == SetCommand::AggFunc::UNION) {
    for (int i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
      if (OB_FAIL(do_union(db, keys.at(i), members))) {
        LOG_WARN("fail to do union inner", K(ret), K(db));
      }
    }
  } else if (OB_FAIL(do_aggregate_inner(db, keys, agg_func, members))) {
    LOG_WARN("fail to do aggregate inner", K(ret), K(db));
  }

  if (OB_FAIL(ret)) {
    if (ObRedisErr::is_redis_error(ret)) {
      RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
    }
  } else if (OB_FAIL(hashset_to_array(members, member_arr))) {
    LOG_WARN("fail to conver hashset to array", K(ret));
  } else if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_array(member_arr))) {
    LOG_WARN("fail to set diff member", K(ret));
  }

  int tmp_ret = members.destroy();
  if (tmp_ret != OB_SUCCESS) {
    LOG_WARN("fail to destroy members", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }
  return ret;
}

// range [start_idx, end_idx)
// return true if input member in one of keys
int SetCommandOperator::is_member_in_one_key(
    int64_t db,
    const ObIArray<ObString> &keys,
    int start_idx,
    int end_idx,
    const ObString &member,
    bool &is_member)
{
  int ret = OB_SUCCESS;
  is_member = false;
  if (start_idx < 0 || end_idx > keys.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("start index or end index is invalid", K(ret), K(start_idx), K(end_idx), K(keys.count()));
  }
  for (int i = start_idx; OB_SUCC(ret) && !is_member && i < end_idx; ++i) {
    if (OB_FAIL(is_key_member(db, keys.at(i), member, is_member))) {
      LOG_WARN("fail to check is memeber in key", K(ret), K(i));
    }
  }
  return ret;
}

// range [start_idx, end_idx)
// return true if input member in one of keys
int SetCommandOperator::is_member_in_all_keys(
    int64_t db,
    const ObIArray<ObString> &keys,
    int start_idx,
    int end_idx,
    const ObString &member,
    bool &is_member)
{
  int ret = OB_SUCCESS;
  is_member = true;
  if (start_idx < 0 || end_idx > keys.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("start index or end index is invalid", K(ret), K(start_idx), K(end_idx), K(keys.count()));
  }
  for (int i = start_idx; OB_SUCC(ret) && is_member && i < end_idx; ++i) {
    if (OB_FAIL(is_key_member(db, keys.at(i), member, is_member))) {
      LOG_WARN("fail to check is memeber in key", K(ret), K(i));
    }
  }
  return ret;
}

int SetCommandOperator::is_key_member(int64_t db, const ObString &key, const ObString &member, bool &is_member)
{
  int ret = OB_SUCCESS;
  ObTableOperation get_op;
  ObITableEntity *entity = nullptr;
  ObRowkey rowkey;
  is_member = false;
  if (OB_FAIL(build_complex_type_rowkey_entity(db, key, true /*not meta*/, member, entity))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null entity_factory_", K(ret), K(key), K(member));
  } else {
    ObTableOperation get_op = ObTableOperation::retrieve(*entity);
    ObTableOperationResult result;
    if (OB_FAIL(process_table_single_op(get_op, result))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to process table get", K(ret));
      } else {
        is_member = false;
        ret = OB_SUCCESS;
      }
    } else {
      is_member = true;
    }
  }
  return ret;
}

int SetCommandOperator::insert_single_data(int64_t db, const ObString &key,
                                           const ObString &member, bool &is_duplicated)
{
  int ret = OB_SUCCESS;
  is_duplicated = false;
  // add meta, sadd do not update expire time
  bool is_insup = false;
  ObRedisMeta *meta = nullptr;
  if (OB_FAIL(check_and_insup_meta(db, key, ObRedisModel::SET, is_insup, meta))) {
    LOG_WARN("fail to check and insup meta", K(ret), K(key), K(db));
  }
  ObITableEntity *value_entity = nullptr;
  ObObj insert_obj;
  insert_obj.set_timestamp(ObTimeUtility::fast_current_time());
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(build_complex_type_rowkey_entity(db, key, true /*not meta*/, member, value_entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(member), K(db), K(key));
  } else if (OB_FAIL(value_entity->set_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, insert_obj))) {
    LOG_WARN("fail to set member property", K(ret), K(insert_obj));
  } else {
    ObTableOperation op = ObTableOperation::insert(*value_entity);
    ObTableOperationResult op_res;
    if (OB_FAIL(process_table_single_op(op, op_res, meta))) {
      if (ret == OB_ERR_PRIMARY_KEY_DUPLICATE) {
        ret = OB_SUCCESS;
        is_duplicated = true;
      } else {
        LOG_WARN("fail to process table single op", K(op));
      }
    }
  }
  return ret;
}

int SetCommandOperator::do_sadd_data(int64_t db, const ObString &key, const SetCommand::MemberSet &members,
                    bool is_new_meta, int64_t &insert_num)
{
  int ret = OB_SUCCESS;
  ObTableBatchOperation ops;
  ops.set_entity_factory(redis_ctx_.entity_factory_);
  int64_t cur_time = ObTimeUtility::current_time();

  SetCommand::MemberSet::const_iterator iter = members.begin();
  for (; OB_SUCC(ret) && iter != members.end(); ++iter) {
    ObITableEntity *value_entity = nullptr;
    ObObj insert_obj;
    insert_obj.set_timestamp(cur_time);
    if (OB_FAIL(build_complex_type_rowkey_entity(db, key, true /*not meta*/, iter->first, value_entity))) {
      LOG_WARN("fail to build rowkey entity", K(ret), K(iter->first), K(db), K(key));
    } else if (OB_FAIL(value_entity->set_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, insert_obj))) {
      LOG_WARN("fail to set member property", K(ret), K(insert_obj));
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
    if (is_new_meta || !results[i].get_is_insertup_do_update()) {
      ++insert_num;
    }
  }
  return ret;
}

int SetCommandOperator::do_sadd_inner(
    int64_t db,
    const ObString &key,
    const SetCommand::MemberSet &members,
    int64_t &insert_num)
{
  int ret = OB_SUCCESS;
  // add meta, sadd do not update expire time
  bool is_new_meta = false;
  ObRedisMeta *meta = nullptr;
  if (OB_FAIL(check_and_insup_meta(db, key, ObRedisModel::SET, is_new_meta, meta))) {
    LOG_WARN("fail to check and insup meta", K(ret), K(key), K(db));
  } else if (OB_FAIL(do_sadd_data(db, key, members, is_new_meta, insert_num))) {
    LOG_WARN("fail to do sadd data", K(ret), K(db), K(key), K(is_new_meta));
  }
  return ret;
}

int SetCommandOperator::do_sadd(int64_t db, const ObString &key, const SetCommand::MemberSet &members)
{
  int ret = OB_SUCCESS;
  int64_t insert_num = 0;
  if (OB_FAIL(do_sadd_inner(db, key, members, insert_num))) {
    LOG_WARN("fail to do sadd inner", K(ret), K(db), K(key));
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

int SetCommandOperator::do_aggregate_store(
    int64_t db,
    const ObString &dest,
    const ObIArray<ObString> &keys,
    SetCommand::AggFunc agg_func)
{
  int ret = OB_SUCCESS;
  SetCommand::MemberSet members;
  ObArray<ObString> member_arr(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(op_temp_allocator_, "RedisSAgg"));
  if (OB_FAIL(members.create(RedisCommand::DEFAULT_BUCKET_NUM, ObMemAttr(MTL_ID(), "RedisSAgg")))) {
    LOG_WARN("fail to create hash set", K(ret));
  } else if (agg_func == SetCommand::AggFunc::UNION) {
    for (int i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
      if (OB_FAIL(do_union(db, keys.at(i), members))) {
        LOG_WARN("fail to do union inner", K(ret), K(db));
      }
    }
  } else if (OB_FAIL(do_aggregate_inner(db, keys, agg_func, members))) {
    LOG_WARN("fail to do aggregate inner", K(ret), K(db));
  }

  bool is_exist = false; // unused
  int64_t insert_num = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(del_complex_key(ObRedisModel::SET, db, dest, false/*del_meta*/, is_exist))) {
    LOG_WARN("fail to delete set", K(ret), K(db), K(dest));
  } else if (!members.empty()) {
    if (OB_FAIL(insup_meta(db, dest, ObRedisModel::SET))) {
      LOG_WARN("fail to insert up meta", K(ret), K(db), K(dest));
    } else if (OB_FAIL(do_sadd_data(db, dest, members, true/*is_new_meta*/, insert_num))) {
      LOG_WARN("fail to add diff members", K(ret), K(db), K(dest));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_int(insert_num))) {
      LOG_WARN("fail to set response int", K(ret), K(insert_num));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  int tmp_ret = members.destroy();
  if (tmp_ret != OB_SUCCESS) {
    LOG_WARN("fail to destroy members", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }
  return ret;
}

int SetCommandOperator::do_sismember(int64_t db, const ObString &key,
                                     const ObString &member)
{
  int ret = OB_SUCCESS;
  ObString value;  // unused
  bool exists = true;
  ObITableEntity *entity = nullptr;
  if (OB_FAIL(build_complex_type_rowkey_entity(db, key, true /*not meta*/, member, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(member), K(db), K(key));
  } else {
    ObTableOperation op = ObTableOperation::retrieve(*entity);
    ObTableOperationResult op_res;
    ObITableEntity *res_entity = nullptr;
    if (OB_FAIL(process_table_single_op(op, op_res, nullptr/*meta*/))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to process table single op", K(op));
      } else {
        exists = false;
        ret = OB_SUCCESS;
      }
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

int SetCommandOperator::do_scard(int64_t db, const ObString &key)
{
  int ret = OB_SUCCESS;
  int64_t row_cnt = 0;
  if (OB_FAIL(get_complex_type_count(db, key, row_cnt))) {
    LOG_WARN("fail to get set count", K(ret), K(db), K(key));
  } else if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(row_cnt))) {
    LOG_WARN("fail to set result", K(ret), K(row_cnt));
  }

  if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int SetCommandOperator::do_smembers_inner(
    int64_t db,
    const common::ObString &key,
    bool get_insert_ts,
    SrandResult &srand_result)
{
  int ret = OB_SUCCESS;
  ObTableQuery query;
  if (OB_FAIL(add_complex_type_subkey_scan_range(db, key, query))) {
    LOG_WARN("fail to add hash set scan range", K(ret), K(db), K(key));
  } else if (OB_FAIL(query.add_select_column(ObRedisUtil::RKEY_PROPERTY_NAME))) {
    LOG_WARN("fail to add select column", K(ret));
  } else if (get_insert_ts && OB_FAIL(query.add_select_column(ObRedisUtil::INSERT_TS_PROPERTY_NAME))) {
    LOG_WARN("fail to add select insert ts column", K(ret), K(query));
  } else {
    SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
    {
      ObRedisSetMeta *null_meta = nullptr;
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
          if (OB_FAIL(one_result->get_next_entity(result_entity))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", K(ret));
            }
          } else if (OB_FAIL(get_subkey_from_entity(op_temp_allocator_, *result_entity, member))) {
            LOG_WARN("fail to get member from entity", K(ret), KPC(result_entity));
          } else if (OB_FAIL(srand_result.res_members_.push_back(member))) {
            LOG_WARN("fail to push back", K(ret));
          } else if (get_insert_ts) {
            int64_t insert_ts = 0;
            if (OB_FAIL(get_insert_ts_from_entity(*result_entity, insert_ts))) {
              LOG_WARN("fail to get insert ts from entity", K(ret), KPC(result_entity));
            } else if (OB_FAIL(srand_result.res_insert_ts_.push_back(insert_ts))) {
              LOG_WARN("fail to push back insert_ts", K(ret), K(insert_ts));
            }
          }
        }
      }
      QUERY_ITER_END(iter)
    }
  }

  return ret;
}

int SetCommandOperator::query_member_at_indexes(
    bool get_insert_ts,
    const ObTableQuery &query,
    const ObArray<int64_t> &target_idxs,
    SrandResult& srand_result)
{
  int ret = OB_SUCCESS;

  int64_t count = target_idxs.count();

  SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_)
  {
    ObRedisSetMeta *null_meta = nullptr;
    QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, null_meta)
    ObTableQueryResult *one_result = nullptr;
    const ObITableEntity *result_entity = nullptr;

    int64_t cur_seq = 0;
    while (OB_SUCC(ret) && cur_seq < count) {
      tb_ctx.set_limit(1);
      // offset 0 is meta
      tb_ctx.set_offset(target_idxs[cur_seq++] + 1);
      ObNewRow *tmp_next_row = NULL;
      if (OB_FAIL(row_iter->get_scan_executor()->rescan())) {
        LOG_WARN("failed to rescan executor", K(ret));
      } else if (OB_FAIL(row_iter->get_next_row(tmp_next_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else {
        ObString member;
        ObString encoded;
        ObString tmp_subkey;
        const ObObj &rkey_obj = tmp_next_row->get_cell(ObRedisUtil::COL_IDX_RKEY);
        if (OB_FAIL(rkey_obj.get_varbinary(encoded))) {
          LOG_WARN("fail to get db num", K(ret), K(rkey_obj));
        } else if (ObRedisRKeyUtil::decode_subkey(encoded, tmp_subkey)) {
          LOG_WARN("fail to decode subkey", K(ret), K(encoded));
        } else if (OB_FAIL(ob_write_string(op_temp_allocator_, tmp_subkey, member))) {
          LOG_WARN("fail to write string", K(ret), K(tmp_subkey));
        } else if (OB_FAIL(srand_result.res_members_.push_back(member))) {
          LOG_WARN("fail to push back member", K(ret), K(member));
        } else if (get_insert_ts) {
          int64_t insert_ts = 0;
          const ObObj &insert_ts_obj = tmp_next_row->get_cell(ObRedisUtil::COL_IDX_INSERT_TS);
          if (OB_FAIL(insert_ts_obj.get_timestamp(insert_ts))) {
            LOG_WARN("fail to get insert ts", K(ret), K(insert_ts_obj));
          } else if (OB_FAIL(srand_result.res_insert_ts_.push_back(insert_ts))) {
            LOG_WARN("fail to push back insert_ts", K(ret), K(insert_ts));
          }
        }
      }
    }

    QUERY_ITER_END(iter)
  }
  return ret;
}

int SetCommandOperator::do_srand_mem_repeat_inner(
    int64_t db,
    const common::ObString &key,
    int64_t count,
    SrandResult& srand_result)
{
  int ret = OB_SUCCESS;
  int64_t total_count = 0;
  if (count < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("count need > 0", K(ret), K(db), K(key), K(count));
  } else if (OB_FAIL(get_complex_type_count(db, key, total_count))) {
    LOG_WARN("fail to get set count", K(ret), K(db), K(key));
  } else if (total_count == 0 || count == 0) {
    LOG_INFO("set is empty", K(ret), K(db), K(key));
  } else if (OB_FAIL(srand_result.res_members_.reserve(count))) {
    LOG_WARN("fail to reserve", K(ret), K(count));
  } else {
    ObRandom random;
    ObArray<int64_t> target_idxs(
        OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(op_temp_allocator_, "RedisSRandMem"));
    if (OB_FAIL(target_idxs.reserve(count))) {
      LOG_WARN("fail to create hash set", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        int64_t rand_idx = random.get(0, total_count - 1);
        if (OB_FAIL(target_idxs.push_back(rand_idx))) {
          LOG_WARN("fail to set refactored", K(ret), K(rand_idx));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t cur_index = 0;
        int64_t idx = 0;
        ObTableQuery query;
        if (OB_FAIL(add_complex_type_subkey_scan_range(db, key, query))) {
          LOG_WARN("fail to build scan query", K(ret));
        } else if (OB_FAIL(query.add_select_column(ObRedisUtil::RKEY_PROPERTY_NAME))) {
          LOG_WARN("fail to add select member column", K(ret), K(query));
        } else if (OB_FAIL(query_member_at_indexes(false /* not get_insert_ts */, query, target_idxs, srand_result))) {
          LOG_WARN("fail to do query idxs member", K(ret), K(query), K(target_idxs));
        }
        if (OB_SUCC(ret)) {
          if (srand_result.res_members_.count() != count) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN(
                "fail to get enough members",
                K(ret),
                K(count),
                K(total_count),
                K(srand_result.res_members_),
                K(cur_index),
                K(target_idxs));
          }
        }
      }
    }
  }

  return ret;
}

int SetCommandOperator::do_srand_mem_inner(
    int64_t db,
    const common::ObString &key,
    bool get_insert_ts,
    int64_t count,
    SrandResult& srand_result)
{
  int ret = OB_SUCCESS;
  srand_result.is_get_all_ = false;
  int64_t total_count = 0;
  if (count < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("count need > 0", K(ret), K(db), K(key), K(count));
  } else if (OB_FAIL(get_complex_type_count(db, key, total_count))) {
    LOG_WARN("fail to get set count", K(ret), K(db), K(key));
  } else if (total_count == 0) {
    LOG_INFO("set is empty", K(ret), K(db), K(key));
  } else if (count >= total_count) {
    srand_result.is_get_all_ = true;
    if (OB_FAIL(srand_result.res_members_.reserve(total_count))) {
      LOG_WARN("fail to reserve", K(ret), K(total_count));
    } else if (OB_FAIL(do_smembers_inner(db, key, get_insert_ts, srand_result))) {
      LOG_WARN("fail to do smembers", K(ret), K(db), K(key));
    }
  } else if (OB_FAIL(srand_result.res_members_.reserve(count))) {
    LOG_WARN("fail to reserve", K(ret), K(count));
  } else if (get_insert_ts && OB_FAIL(srand_result.res_insert_ts_.reserve(count))) {
    LOG_WARN("fail to reserve", K(ret), K(count));
  } else {
    ObRandom random;
    ObArray<int64_t> target_idxs(
        OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(op_temp_allocator_, "RedisSRandMem"));
    hash::ObHashMap<int64_t, int64_t, common::hash::NoPthreadDefendMode> idx_seq;
    if (OB_FAIL(idx_seq.create(count, ObMemAttr(MTL_ID(), "RedisSRandMem")))) {
      LOG_WARN("fail to create hash set", K(ret));
    } else if (OB_FAIL(target_idxs.reserve(count))) {
      LOG_WARN("fail to reserve", K(ret), K(count));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < count;) {
        int64_t rand_idx = random.get(0, total_count - 1);
        if (OB_FAIL(idx_seq.set_refactored(rand_idx, i))) {
          if (OB_HASH_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to set refactored", K(ret), K(rand_idx));
          }
        } else if (OB_FAIL(target_idxs.push_back(rand_idx))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
          // if idx not exist in hash, ++i
          ++i;
        }
      }

      if (OB_SUCC(ret)) {
        ObTableQuery query;
        if (OB_FAIL(add_complex_type_subkey_scan_range(db, key, query))) {
          LOG_WARN("fail to build scan query", K(ret));
        } else if (OB_FAIL(query.add_select_column(ObRedisUtil::RKEY_PROPERTY_NAME))) {
          LOG_WARN("fail to add select member column", K(ret), K(query));
        } else if (get_insert_ts && OB_FAIL(query.add_select_column(ObRedisUtil::INSERT_TS_PROPERTY_NAME))) {
          LOG_WARN("fail to add select insert ts column", K(ret), K(query));
        } else if (OB_FAIL(query_member_at_indexes(get_insert_ts, query, target_idxs, srand_result))) {
          LOG_WARN("fail to do query idxs member", K(ret), K(query), K(target_idxs));
        }
      }
    }
    int tmp_ret = idx_seq.destroy();
    if (tmp_ret != OB_SUCCESS) {
      LOG_WARN("fail to destroy sets_idx", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  return ret;
}

int SetCommandOperator::do_smembers(int64_t db, const ObString &key)
{
  int ret = OB_SUCCESS;

  SrandResult srand_result(op_temp_allocator_);
  if (OB_FAIL(do_smembers_inner(db, key, false /*not get_insert_ts*/, srand_result))) {
    LOG_WARN("fail to do smembers inner", K(db), K(key));
  } else if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_.set_res_array(srand_result.res_members_))) {
    LOG_WARN("fail to set result array", K(ret));
  }

  if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int SetCommandOperator::del_member_after_spop(
    int64_t db,
    const common::ObString &key,
    const ObArray<ObString> &members,
    const ObArray<int64_t> &res_insert_ts)
{
  int ret = OB_SUCCESS;
  ObTableBatchOperation del_ops;
  ObITableEntity *value_entity = nullptr;
  ObObj insert_ts_obj;
  ObObj expire_ts_obj;
  expire_ts_obj.set_null();
  for (int64_t i = 0; OB_SUCC(ret) && i < members.count(); ++i) {
    if (OB_FAIL(build_complex_type_rowkey_entity(db, key, true /*not meta*/, members.at(i), value_entity))) {
      LOG_WARN("fail to build rowkey entity", K(ret), K(members.at(i)), K(db), K(key));
    } else if (OB_FALSE_IT(insert_ts_obj.set_timestamp(res_insert_ts.at(i)))) {
    } else if (OB_FAIL(value_entity->set_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, insert_ts_obj))) {
      LOG_WARN("fail to set insert_ts", K(ret), K(res_insert_ts.at(i)));
    } else if (OB_FAIL(value_entity->set_property(ObRedisUtil::EXPIRE_TS_PROPERTY_NAME, expire_ts_obj))) {
      LOG_WARN("fail to set expire_ts", K(ret));
    } else {
      del_ops.del(*value_entity);
    }
  }
  if (OB_SUCC(ret)) {
    ResultFixedArray results(op_temp_allocator_);
    if (OB_FAIL(process_table_batch_op(del_ops, results, nullptr /*meta*/, RedisOpFlags::DEL_SKIP_SCAN))) {
      LOG_WARN("fail to del data", K(ret), K(del_ops));
    }
  }
  return ret;
}
int SetCommandOperator::do_spop(int64_t db, const common::ObString &key, const common::ObString &count_str)
{
  int ret = OB_SUCCESS;

  int64_t count = 1;
  SrandResult srand_result(op_temp_allocator_);
  if (!count_str.empty()) {
    bool is_valid = false;
    count = 0;
    if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(count_str, count))) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
      LOG_WARN("fail to get int from str", K(ret), K(count_str));
    } else if (count < 0) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::NOT_POSITIVE_ERR);
    }
  }

  if (OB_SUCC(ret) && count != 0) {
    if (OB_FAIL(do_srand_mem_inner(db, key, true /*need get insert_ts*/, count, srand_result))) {
      LOG_WARN("fail to do srandmem inner", K(ret), K(db), K(key));
    } else if (srand_result.res_members_.count() != srand_result.res_insert_ts_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("res_members count not equal to res_insert_ts", K(ret), K(srand_result));
    } else if (OB_FAIL(del_member_after_spop(db, key, srand_result.res_members_, srand_result.res_insert_ts_))) {
      LOG_WARN("fail to del member after spop", K(ret));
    } else if (
        srand_result.is_get_all_ && OB_FAIL(fake_del_empty_key_meta(is_zset_ ? ObRedisModel::ZSET : ObRedisModel::SET, db, key))) {
      LOG_WARN("fail to delete empty key meta", K(ret), K(db), K(key));
    }
  }

  if (OB_SUCC(ret)) {
    if (count_str.empty()) {
      if (srand_result.res_members_.empty()) {
        if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_fmt_res(ObString::make_string(ObRedisFmt::NULL_BULK_STRING)))) {
          LOG_WARN("fail to set fmt res", K(ret));
        }
      } else if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_bulk_string(srand_result.res_members_.at(0)))) {
        LOG_WARN("fail to set res string", K(ret), K(srand_result.res_members_));
      }
    } else if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_array(srand_result.res_members_))) {
      LOG_WARN("fail to set result int", K(ret), K(srand_result.res_members_));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}
int SetCommandOperator::do_srand_member(int64_t db, const common::ObString &key, const common::ObString &count_str)
{
  int ret = OB_SUCCESS;

  SrandResult srand_result(op_temp_allocator_);

  int64_t count = 1;
  if (!count_str.empty()) {
    if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(count_str, count))) {
      RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
      LOG_WARN("fail to get int from str", K(ret), K(count_str));
    }
  }

  if (OB_SUCC(ret)) {
    if (count == 0) {
    } else if (count < 0) {
      if (count == INT64_MIN) {
        RECORD_REDIS_ERROR(fmt_redis_msg_, ObRedisErr::INTEGER_ERR);
      } else if (OB_FAIL(do_srand_mem_repeat_inner(db, key, -count, srand_result))) {
        LOG_WARN("fail to do srandmem inner", K(ret), K(db), K(key), K(count));
      }
    } else if (OB_FAIL(do_srand_mem_inner(db, key, false /*not get insert_ts*/, count, srand_result))) {
      LOG_WARN("fail to do srandmem inner", K(ret), K(db), K(key));
    }
  }

  if (OB_SUCC(ret)) {
    if (count_str.empty()) {
      if (srand_result.res_members_.empty()) {
        if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_fmt_res(ObString::make_string(ObRedisFmt::NULL_BULK_STRING)))) {
          LOG_WARN("fail to set fmt res", K(ret));
        }
      } else if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_bulk_string(srand_result.res_members_.at(0)))) {
        LOG_WARN("fail to set res string", K(ret), K(srand_result.res_members_));
      }
    } else if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_array(srand_result.res_members_))) {
      LOG_WARN("fail to set result int", K(ret), K(srand_result.res_members_));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int SetCommandOperator::do_smove(int64_t db,
                                 const ObString &src,
                                 const ObString &dest,
                                 const ObString &member)
{
  int ret = OB_SUCCESS;
  ObITableEntity *entity = nullptr;
  bool exists = true;
  if (OB_FAIL(build_complex_type_rowkey_entity(db, src, true /*not meta*/, member, entity))) {
    LOG_WARN("fail to build rowkey entity", K(ret), K(member), K(db), K(src));
  } else {
    ObTableOperation op = ObTableOperation::retrieve(*entity);
    ObTableOperationResult op_res;
    if (OB_FAIL(process_table_single_op(op, op_res, nullptr/*meta*/))) {
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
        exists = false;
      } else {
        LOG_WARN("fail to process table single op", K(op));
      }
    } else {
      ObITableEntity *del_entity = op_res.get_entity();
      ObTableOperation del_op;
      ObTableOperationResult del_result;
      ObRedisMeta *meta = nullptr;
      if (OB_FAIL(del_entity->set_rowkey(entity->get_rowkey()))) {
        LOG_WARN("fail to set rowkey", K(ret), K(entity->get_rowkey()));
      } else if (FALSE_IT(del_op = ObTableOperation::del(*del_entity))) {
      } else if (OB_FAIL(process_table_single_op(
              del_op,
              del_result,
              meta,
              RedisOpFlags::DEL_SKIP_SCAN))) {
        LOG_WARN("fail to del data", K(ret), K(del_op));
      }
    }

    if (OB_SUCC(ret) && exists) {
      bool is_duplicated = false; /*unused*/
      if (OB_FAIL(insert_single_data(db, dest, member, is_duplicated))) {
        LOG_WARN("fail to build rowkey entity", K(ret), K(member), K(db), K(dest));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(exists ? 1 : 0))) {
      LOG_WARN("fail to set response int", K(ret), K(exists));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int SetCommandOperator::do_srem_inner(
    int64_t db,
    const common::ObString &key,
    const SetCommand::MemberSet &members,
    int64_t &del_num)
{
  int ret = OB_SUCCESS;
  ObTableBatchOperation ops;
  ops.set_entity_factory(redis_ctx_.entity_factory_);
  ObArray<ObRowkey> rowkeys(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(op_temp_allocator_, "RedisSRem"));
  if (OB_FAIL(rowkeys.reserve(members.size()))) {
    LOG_WARN("fail to reserve count", K(ret), K(members.size()));
  }
  for (SetCommand::MemberSet::const_iterator iter = members.begin();
      OB_SUCC(ret) && iter != members.end();
      ++iter) {
    ObITableEntity *value_entity = nullptr;
    if (OB_FAIL(build_complex_type_rowkey_entity(db, key, true /*not meta*/, iter->first, value_entity))) {
      LOG_WARN("fail to build rowkey entity", K(ret), K(iter->first), K(db), K(key));
    } else if (OB_FAIL(ops.retrieve(*value_entity))) {
      LOG_WARN("fail to push back op", K(ret), KPC(value_entity));
    } else if (rowkeys.push_back(value_entity->get_rowkey())) {
      LOG_WARN("fail to push back rowkey", K(ret), K(value_entity->get_rowkey()));
    }
  }

  ObRedisModel model = is_zset_ ? ObRedisModel::ZSET : ObRedisModel::SET;
  ResultFixedArray results(op_temp_allocator_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(process_table_batch_op(ops, results))) {
    LOG_WARN("fail to process table batch op", K(ret));
  } else if (OB_FAIL(delete_results(results, rowkeys, del_num))) {
    LOG_WARN("fail to delete results", K(ret), K(results));
  } else if (OB_FAIL(fake_del_empty_key_meta(model, db, key))) {
    LOG_WARN("fail to delete empty key meta", K(ret), K(db), K(key));
  }
  return ret;
}

int SetCommandOperator::do_srem_inner(
    int64_t db,
    const common::ObString &key,
    const common::ObIArray<ObString> &members,
    int64_t &del_num)
{
  int ret = OB_SUCCESS;
  ObTableBatchOperation ops;
  ops.set_entity_factory(redis_ctx_.entity_factory_);
  ObArray<ObRowkey> rowkeys(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(op_temp_allocator_, "RedisSRem"));
  if (OB_FAIL(rowkeys.reserve(members.count()))) {
    LOG_WARN("fail to reserve count", K(ret), K(members.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < members.count(); ++i) {
    ObITableEntity *value_entity = nullptr;
    if (OB_FAIL(build_complex_type_rowkey_entity(db, key, true /*not meta*/, members.at(i), value_entity))) {
      LOG_WARN("fail to build rowkey entity", K(ret), K(members.at(i)), K(db), K(key));
    } else if (OB_FAIL(ops.retrieve(*value_entity))) {
      LOG_WARN("fail to push back op", K(ret), KPC(value_entity));
    } else if (rowkeys.push_back(value_entity->get_rowkey())) {
      LOG_WARN("fail to push back rowkey", K(ret), K(i));
    }
  }

  ObRedisModel model = is_zset_ ? ObRedisModel::ZSET : ObRedisModel::SET;
  ResultFixedArray results(op_temp_allocator_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(process_table_batch_op(ops, results))) {
    LOG_WARN("fail to process table batch op", K(ret));
  } else if (OB_FAIL(delete_results(results, rowkeys, del_num))) {
    LOG_WARN("fail to delete results", K(ret), K(results));
  } else if (OB_FAIL(fake_del_empty_key_meta(model, db, key))) {
    LOG_WARN("fail to delete empty key meta", K(ret), K(db), K(key));
  }
  return ret;
}

int SetCommandOperator::do_srem(int64_t db, const common::ObString &key, const SetCommand::MemberSet &members)
{
  int ret = OB_SUCCESS;
  int64_t del_num = 0;
  if (OB_FAIL(do_srem_inner(db, key, members, del_num))) {
    LOG_WARN("fail to do srem inner", K(ret), K(db), K(key));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(del_num))) {
      LOG_WARN("fail to set response int", K(ret), K(del_num));
    }
  } else if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }
  return ret;
}

int SetCommandOperator::do_srem(int64_t db, const ObString &key, const common::ObIArray<ObString> &members)
{
  int ret = OB_SUCCESS;
  int64_t del_num = 0;
  if (OB_FAIL(do_srem_inner(db, key, members, del_num))) {
    LOG_WARN("fail to do zrem inner", K(ret), K(db), K(key));
  } else if (OB_FAIL(reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.set_res_int(del_num))) {
    LOG_WARN("fail to set response int", K(ret), K(del_num));
  }
  if (ObRedisErr::is_redis_error(ret)) {
    RESPONSE_REDIS_ERROR(reinterpret_cast<ObRedisSingleCtx &>(redis_ctx_).response_, fmt_redis_msg_.ptr());
  }

  return ret;
}

int SetCommandOperator::fill_set_batch_op(const ObRedisOp &op,
                                           ObIArray<ObTabletID> &tablet_ids,
                                           ObTableBatchOperation &batch_op)
{
  int ret = OB_SUCCESS;
  const SAdd *sadd = reinterpret_cast<const SAdd*>(op.cmd());
  if (OB_ISNULL(sadd)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null sadd op", K(ret));
  } else if (OB_FAIL(tablet_ids.push_back(op.tablet_id()))) {
    LOG_WARN("fail to push back tablet id", K(ret));
  }
  int64_t cur_ts = ObTimeUtility::fast_current_time();
  const SAdd::MemberSet &mem_set = sadd->members();
  ObITableEntity *value_entity = nullptr;
  ObString key;
  ObObj insert_obj;
  ObObj expire_obj;
  for (SAdd::MemberSet::const_iterator iter = mem_set.begin();
      OB_SUCC(ret) && iter != mem_set.end(); ++iter) {
    insert_obj.set_timestamp(cur_ts);
    expire_obj.set_null();
    if (OB_FAIL(op.get_key(key))) {
      LOG_WARN("fail to get key", K(ret), K(op));
    } else if (OB_FAIL(build_complex_type_rowkey_entity(op.db(), key, true /*not meta*/, iter->first, value_entity))) {
      LOG_WARN("fail to build rowkey entity", K(ret), K(iter->first), K(op.db()), K(key));
    } else if (OB_FAIL(value_entity->set_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, insert_obj))) {
      LOG_WARN("fail to set member property", K(ret), K(insert_obj));
    } else if (OB_FAIL(value_entity->set_property(ObRedisUtil::EXPIRE_TS_PROPERTY_NAME, expire_obj))) {
      LOG_WARN("fail to set expire ts property", K(ret), K(expire_obj));
    } else {
      if (OB_FAIL(batch_op.insert_or_update(*value_entity))) {
        LOG_WARN("fail to push back insert or update op", K(ret), KPC(value_entity));
      }
    }
  }
  return ret;
}

int SetCommandOperator::hashset_to_array(const SetCommand::MemberSet &hash_set,
                                      ObIArray<ObString> &ret_arr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ret_arr.reserve(hash_set.size()))) {
    LOG_WARN("fail to reserve space", K(ret), K(hash_set.size()));
  }

  for (SetCommand::MemberSet::const_iterator iter = hash_set.begin();
       OB_SUCC(ret) && iter != hash_set.end();
       iter++) {
    if (OB_FAIL(ret_arr.push_back(iter->first))) {
      LOG_WARN("fail to push back string", K(ret), K(iter->first));
    }
  }
  return ret;
}

}  // namespace table
}  // namespace oceanbase
