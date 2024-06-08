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

namespace oceanbase
{
using namespace observer;
using namespace common;
using namespace share;
using namespace sql;
namespace table
{
const ObString SetCommandOperator::MEMBER_PROPERTY_NAME = ObString::make_string("member");
// with deep copy
int SetCommandOperator::get_member_from_entity(ObIAllocator &allocator,
                                               const ObITableEntity &entity, ObString &member_str)
{
  int ret = OB_SUCCESS;
  ObObj member_obj;
  ObString tmp_member;
  if (OB_FAIL(entity.get_property(MEMBER_PROPERTY_NAME, member_obj))) {
    LOG_WARN("fail to get member from result enrity", K(ret), K(entity));
  } else if (OB_FAIL(member_obj.get_string(tmp_member))) {
    LOG_WARN("fail to get db num", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, tmp_member, member_str))) {
    LOG_WARN("fail to write string", K(ret));
  }
  return ret;
}

int SetCommandOperator::agg_need_push_member(int64_t db, const ObIArray<ObString> &keys,
                                             const ObString &member, SetCommand::AggFunc agg_func,
                                             bool &is_needed)
{
  int ret = OB_SUCCESS;
  is_needed = false;
  switch (agg_func) {
    case SetCommand::AggFunc::DIFF: {
      bool is_member = false;
      if (OB_FAIL(is_member_in_one_key(db, keys, 1/*start_idx*/, keys.count(), member, is_member))) {
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
      if (OB_FAIL(is_member_in_all_keys(db, keys, 1/*start_idx*/, keys.count(), member, is_needed))) {
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
  if (OB_FAIL(add_member_scan_range(db, key, query))) {
    LOG_WARN("fail to build scan query", K(ret));
  } else if (OB_FAIL(query.add_select_column(MEMBER_PROPERTY_NAME))) {
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
          bool is_needed = false;
          if (OB_FAIL(one_result->get_next_entity(result_entity))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", K(ret));
            }
          } else if (OB_FAIL(get_member_from_entity(op_temp_allocator_, *result_entity, member))) {
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

int SetCommandOperator::do_aggregate_inner(int64_t db, const ObIArray<ObString> &keys,
                                           SetCommand::AggFunc agg_func,
                                           SetCommand::MemberSet &members)
{
  int ret = OB_SUCCESS;

  ObString first_key = keys.at(0);
  ObTableQuery query;
  if (OB_FAIL(add_member_scan_range(db, first_key, query))) {
    LOG_WARN("fail to build scan query", K(ret));
  } else if (OB_FAIL(query.add_select_column(MEMBER_PROPERTY_NAME))) {
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
          bool is_needed = false;
          if (OB_FAIL(one_result->get_next_entity(result_entity))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", K(ret));
            }
          } else if (OB_FAIL(get_member_from_entity(op_temp_allocator_, *result_entity, member))) {
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

int SetCommandOperator::do_aggregate(int64_t db, const ObIArray<ObString> &keys,
                                     SetCommand::AggFunc agg_func)
{
  int ret = OB_SUCCESS;
  SetCommand::MemberSet members;
  ObArray<ObString> member_arr(OB_MALLOC_NORMAL_BLOCK_SIZE,
                               ModulePageAllocator(op_temp_allocator_, "RedisSDiff"));
  if (OB_FAIL(
          members.create(RedisCommand::DEFAULT_BUCKET_NUM, ObMemAttr(MTL_ID(), "RedisSDiff")))) {
    LOG_WARN("fail to create hash set", K(ret));
  } else if (agg_func == SetCommand::AggFunc::UNION) {
    for (int i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
      if (OB_FAIL(do_union(db, keys.at(i), members))) {
        LOG_WARN("fail to do sdiff inner", K(ret), K(db));
      }
    }
  } else if (OB_FAIL(do_aggregate_inner(db, keys, agg_func, members))) {
    LOG_WARN("fail to do sdiff inner", K(ret), K(db));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(hashset_to_array(members, member_arr))) {
    LOG_WARN("fail to conver hashset to array", K(ret));
  } else if (OB_FAIL(redis_ctx_.response_.set_res_array(member_arr))) {
    LOG_WARN("fail to set diff member", K(ret));
  }

  if (OB_FAIL(ret)) {
    redis_ctx_.response_.return_table_error(ret);
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
int SetCommandOperator::is_member_in_one_key(int64_t db, const ObIArray<ObString> &keys,
                                             int start_idx, int end_idx, const ObString &member,
                                             bool &is_member)
{
  int ret = OB_SUCCESS;
  is_member = false;
  if (start_idx < 0 || end_idx > keys.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "start index or end index is invalid", K(ret), K(start_idx), K(end_idx), K(keys.count()));
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
int SetCommandOperator::is_member_in_all_keys(int64_t db, const ObIArray<ObString> &keys,
                                              int start_idx, int end_idx, const ObString &member,
                                              bool &is_member)
{
  int ret = OB_SUCCESS;
  is_member = true;
  if (start_idx < 0 || end_idx > keys.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "start index or end index is invalid", K(ret), K(start_idx), K(end_idx), K(keys.count()));
  }
  for (int i = start_idx; OB_SUCC(ret) && is_member && i < end_idx; ++i) {
    if (OB_FAIL(is_key_member(db, keys.at(i), member, is_member))) {
      LOG_WARN("fail to check is memeber in key", K(ret), K(i));
    }
  }
  return ret;
}

int SetCommandOperator::is_key_member(int64_t db, const ObString &key, const ObString &member,
                                      bool &is_member)
{
  int ret = OB_SUCCESS;
  ObTableOperation get_op;
  ObITableEntity *entity = nullptr;
  ObRowkey rowkey;
  is_member = false;
  if (OB_FAIL(build_hash_set_rowkey_entity(db, key, member, entity))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null entity_factory_", K(ret));
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

int SetCommandOperator::add_member_scan_range(int64_t db, const ObString &key, ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  ObRowkey start_key;
  ObRowkey end_key;
  ObNewRange *range = nullptr;
  if (OB_FAIL(build_hash_set_rowkey(db, key, true, start_key))) {
    LOG_WARN("fail to build start key", K(ret), K(db), K(key));
  } else if (OB_FAIL(build_hash_set_rowkey(db, key, false, end_key))) {
    LOG_WARN("fail to build start key", K(ret), K(db), K(key));
  } else if (OB_FAIL(build_range(start_key, end_key, range))) {
    LOG_WARN("fail to build range", K(ret), K(start_key), K(end_key));
  } else if (OB_FAIL(query.add_scan_range(*range))) {
    LOG_WARN("fail to add scan range", K(ret));
  }

  return ret;
}

int SetCommandOperator::build_range(const common::ObRowkey &start_key,
                                    const common::ObRowkey &end_key, ObNewRange *&range,
                                    bool inclusive_start /* = true */,
                                    bool inclusive_end /* = true */)
{
  int ret = OB_SUCCESS;
  range = nullptr;
  if (OB_ISNULL(range = OB_NEWx(ObNewRange, &op_temp_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObNewRange", K(ret));
  } else {
    range->start_key_ = start_key;
    range->end_key_ = end_key;
    if (inclusive_start) {
      range->border_flag_.set_inclusive_start();
    }
    if (inclusive_end) {
      range->border_flag_.set_inclusive_end();
    }
  }
  return ret;
}

int SetCommandOperator::do_sadd_inner(int64_t db, const ObString &key,
                                      const SetCommand::MemberSet &members, int64_t &insert_num)
{
  int ret = OB_SUCCESS;
  const ObITableEntity &req_entity = redis_ctx_.get_entity();
  ObTableBatchOperation ops;
  ops.set_entity_factory(redis_ctx_.entity_factory_);

  for (SetCommand::MemberSet::const_iterator iter = members.begin();
       OB_SUCC(ret) && iter != members.end();
       ++iter) {
    ObITableEntity *value_entity = nullptr;
    ObObj expire_obj;
    expire_obj.set_null();
    if (OB_FAIL(build_hash_set_rowkey_entity(db, key, iter->first, value_entity))) {
      LOG_WARN("fail to build rowkey entity", K(ret), K(iter->first), K(db), K(key));
    } else if (OB_FAIL(value_entity->set_property(ObRedisUtil::REDIS_EXPIRE_NAME, expire_obj))) {
      LOG_WARN("fail to set member property", K(ret), K(expire_obj));
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

int SetCommandOperator::do_sadd(int64_t db, const ObString &key,
                                const SetCommand::MemberSet &members)
{
  int ret = OB_SUCCESS;
  int64_t insert_num = 0;
  if (OB_FAIL(do_sadd_inner(db, key, members, insert_num))) {
    LOG_WARN("fail to do sadd inner", K(ret), K(db), K(key));
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

int SetCommandOperator::do_aggregate_store(int64_t db, const ObString &dest,
                                           const ObIArray<ObString> &keys,
                                           SetCommand::AggFunc agg_func)
{
  int ret = OB_SUCCESS;
  SetCommand::MemberSet res_members;
  ObArray<ObString> diff_arr(OB_MALLOC_NORMAL_BLOCK_SIZE,
                             ModulePageAllocator(op_temp_allocator_, "RedisSDiff"));
  if (OB_FAIL(res_members.create(RedisCommand::DEFAULT_BUCKET_NUM,
                                        ObMemAttr(MTL_ID(), "RedisSDiff")))) {
    LOG_WARN("fail to create hash set", K(ret));
  } else if (agg_func == SetCommand::AggFunc::UNION) {
    for (int i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
      if (OB_FAIL(do_union(db, keys.at(i), res_members))) {
        LOG_WARN("fail to do sdiff inner", K(ret), K(db));
      }
    }
  } else if (OB_FAIL(do_aggregate_inner(db, keys, agg_func, res_members))) {
    LOG_WARN("fail to do sdiff inner", K(ret), K(db));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(delete_set(db, dest))) {
    LOG_WARN("fail to delete set", K(ret), K(db), K(dest));
  } else if (OB_FAIL(do_sadd(db, dest, res_members))) {
    LOG_WARN("fail to add diff members", K(ret), K(db), K(dest));
  }

  int tmp_ret = res_members.destroy();
  if (tmp_ret != OB_SUCCESS) {
    LOG_WARN("fail to destroy members", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }
  return ret;
}

int SetCommandOperator::delete_set(int db, const ObString &key)
{
  int ret = OB_SUCCESS;
  ObTableQuery query;
  ObTableBatchOperation ops;
  ops.set_entity_factory(redis_ctx_.entity_factory_);
  if (OB_FAIL(add_member_scan_range(db, key, query))) {
    LOG_WARN("fail to build scan query", K(ret));
  } else if (OB_FAIL(query.add_select_column(MEMBER_PROPERTY_NAME))) {
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
          ObITableEntity *value_entity = nullptr;
          if (OB_FAIL(one_result->get_next_entity(result_entity))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", K(ret));
            }
          } else if (OB_FAIL(get_member_from_entity(op_temp_allocator_, *result_entity, member))) {
            LOG_WARN("fail to get member from entity", K(ret), KPC(result_entity));
          } else if (OB_FAIL(build_hash_set_rowkey_entity(db, key, member, value_entity))) {
            LOG_WARN("fail to build rowkey entity", K(ret), K(member), K(db), K(key));
          } else if (OB_FAIL(ops.del(*value_entity))) {
            LOG_WARN("fail to push back insert or update op", K(ret), KPC(value_entity));
          }
        }
      }
      QUERY_ITER_END(iter)
    }
  }

  ResultFixedArray results(op_temp_allocator_);
  if (OB_SUCC(ret) && OB_FAIL(process_table_batch_op(ops, results))) {
    LOG_WARN("fail to process table batch op", K(ret));
  }
  return ret;
}

}  // namespace table
}  // namespace oceanbase
