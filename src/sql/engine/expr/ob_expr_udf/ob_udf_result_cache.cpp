/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX PL_UDF_RESULT_CACHE

#include "ob_udf_result_cache.h"  //MTL
#include "lib/oblog/ob_log_module.h"
#include "pl/ob_pl_user_type.h"

namespace oceanbase
{
namespace pl
{

bool UDFArgRow::operator==(const UDFArgRow &other) const
{
  bool ret = false;
  if (default_param_bitmap_ == other.default_param_bitmap_) {
    ret = DatumRow::operator==(other);
  }
  return ret;
}

int UDFArgRow::hash(uint64_t &hash_val, uint64_t seed) const
{
  int ret = OB_SUCCESS;
  ret = DatumRow::hash(hash_val, seed);
  return ret;
}

void ObPLUDFResultCacheKey::reset()
{
  db_id_ = common::OB_INVALID_ID;
  package_id_ = common::OB_INVALID_ID;
  routine_id_ = common::OB_INVALID_ID;
  namespace_ = ObLibCacheNameSpace::NS_INVALID;
  sys_vars_str_.reset();
  config_vars_str_.reset();
}

int ObPLUDFResultCacheKey::deep_copy(ObIAllocator &allocator, const ObILibCacheKey &other)
{
  int ret = OB_SUCCESS;
  const ObPLUDFResultCacheKey &key = static_cast<const ObPLUDFResultCacheKey&>(other);
  if (OB_FAIL(common::ob_write_string(allocator, key.sys_vars_str_, sys_vars_str_))) {
    LOG_WARN("failed to deep copy config vars", K(ret), K(sys_vars_str_));
  } else if (OB_FAIL(common::ob_write_string(allocator, key.config_vars_str_, config_vars_str_))) {
    LOG_WARN("failed to deep copy sys vars", K(ret), K(config_vars_str_));
  } else {
    db_id_ = key.db_id_;
    package_id_ = key.package_id_;
    routine_id_ = key.routine_id_;
    namespace_ = key.namespace_;
  }
  return ret;
}

void ObPLUDFResultCacheKey::destory(common::ObIAllocator &allocator)
{
  if (nullptr != sys_vars_str_.ptr()) {
    allocator.free(const_cast<char *>(sys_vars_str_.ptr()));
  }
  if (nullptr != config_vars_str_.ptr()) {
    allocator.free(const_cast<char *>(config_vars_str_.ptr()));
  }
}

uint64_t ObPLUDFResultCacheKey::hash() const
{
  uint64_t hash_ret = murmurhash(&db_id_, sizeof(uint64_t), 0);
  hash_ret = murmurhash(&package_id_, sizeof(uint64_t), hash_ret);
  hash_ret = murmurhash(&routine_id_, sizeof(uint64_t), hash_ret);
  hash_ret = murmurhash(&namespace_, sizeof(ObLibCacheNameSpace), hash_ret);
  hash_ret = sys_vars_str_.hash(hash_ret);
  hash_ret = config_vars_str_.hash(hash_ret);
  return hash_ret;
}

bool ObPLUDFResultCacheKey::is_equal(const ObILibCacheKey &other) const
{
  const ObPLUDFResultCacheKey &key = static_cast<const ObPLUDFResultCacheKey&>(other);
  bool cmp_ret = db_id_ == key.db_id_ &&
                 package_id_ == key.package_id_ &&
                 routine_id_ == key.routine_id_ &&
                 namespace_ == key.namespace_ &&
                 sys_vars_str_ == key.sys_vars_str_ &&
                 config_vars_str_ == key.config_vars_str_;
  return cmp_ret;
}

int ObPLUDFResultCacheObject::update_cache_obj_stat(sql::ObILibCacheCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObPLUDFResultCacheCtx &rc_ctx = static_cast<ObPLUDFResultCacheCtx&>(ctx);
  PLUDFResultCacheObjStat &stat = get_stat_for_update();

  stat.pl_schema_id_ = (rc_ctx.key_.package_id_ != OB_INVALID_ID) ? rc_ctx.key_.package_id_ : rc_ctx.key_.routine_id_;
  stat.db_id_ = rc_ctx.key_.db_id_;
  stat.gen_time_ = ObTimeUtility::current_time();
  stat.build_time_ = rc_ctx.execute_time_;
  stat.hit_count_ = 0;
  stat.hash_value_ = rc_ctx.cache_obj_hash_value_;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ob_write_string(get_allocator(),
                                rc_ctx.name_,
                                stat.name_))) {
      LOG_WARN("failed to write sql", K(ret));
    } else if (OB_FAIL(ob_write_string(get_allocator(),
                                rc_ctx.key_.sys_vars_str_,
                                stat_.sys_vars_str_))) {
      LOG_WARN("failed to write sql", K(ret));
    } else {
      stat.sql_cs_type_ = rc_ctx.session_info_->get_local_collation_connection();
    }
  }

  if (OB_SUCC(ret)) {
    // Update last_active_time_ last, because last_active_time_ is used to
    // indicate whether the cache stat has been updated.
    stat.last_active_time_ = ObTimeUtility::current_time();
  }
  return ret;
}

int ObPLUDFResultCacheObject::deep_copy_result(ObIAllocator &alloc, ObObj &src, ObObj &dst)
{
  int ret = OB_SUCCESS;

  if (src.is_pl_extend()) {
    OZ (ObUserDefinedType::deep_copy_obj(alloc, src, dst));
  } else {
    OZ (deep_copy_obj(alloc, src, dst));
  }

  return ret;
}

void ObPLUDFResultCacheObject::reset()
{
  ObILibCacheObject::reset();
  if (result_.is_pl_extend()) {
    (void)ObUserDefinedType::destruct_obj(result_);
  }
}

int ObPLUDFResultCacheObject::check_need_add_cache_obj_stat(ObILibCacheCtx &ctx, bool &need_real_add)
{
  int ret = OB_SUCCESS;

  ObPLUDFResultCacheCtx &rc_ctx = static_cast<ObPLUDFResultCacheCtx&>(ctx);
  need_real_add = rc_ctx.need_add_obj_stat_;

  return ret;
}

int ObPLUDFResultCacheSet::init(ObILibCacheCtx &ctx, const ObILibCacheObject *obj)
{
  int ret = OB_SUCCESS;
  ObPLUDFResultCacheCtx& rc_ctx = static_cast<ObPLUDFResultCacheCtx&>(ctx);

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(rc_ctx.dependency_tables_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dependency table is null", K(ret));
  } else if (OB_FAIL(hashmap_.create(64 * 1024,
                                      ObModIds::OB_HASH_BUCKET_PLAN_CACHE,
                                      ObModIds::OB_HASH_NODE_PLAN_CACHE,
                                      MTL_ID()))) {
    LOG_WARN("fail to create hash map", K(ret));
  } else if (OB_FAIL(key_.deep_copy(allocator_, rc_ctx.key_))) {
    LOG_WARN("fail to init plan cache key in pcv set", K(ret));
  } else if (OB_FAIL(set_stored_schema_objs(*rc_ctx.dependency_tables_,
                                            rc_ctx.schema_guard_))) {
    LOG_WARN("failed to set stored schema objs", K(ret));
  } else {
    sys_schema_version_ = rc_ctx.sys_schema_version_;
    tenant_schema_version_ = rc_ctx.tenant_schema_version_;
    is_inited_ = true;
  }
  return ret;
}

int ObPLUDFResultCacheSet::create_new_cache_key(ObPLUDFResultCacheCtx &rc_ctx,
                                                UDFArgRow &cache_key)
{
  int ret = OB_SUCCESS;

  cache_key.cnt_ = rc_ctx.argument_params_.cnt_;
  if (cache_key.cnt_ > 0) {
    if (OB_ISNULL(cache_key.elems_
              = static_cast<ObDatum *> (allocator_.alloc(sizeof(ObDatum) * cache_key.cnt_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for row key", K(ret),  K(cache_key.cnt_));
    } else if (OB_FAIL(cache_key.default_param_bitmap_.assign(rc_ctx.argument_params_.default_param_bitmap_))) {
      LOG_WARN("fail to assign bitmap", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < cache_key.cnt_; ++i) {
        if (OB_FAIL(cache_key.elems_[i].deep_copy(rc_ctx.argument_params_.elems_[i], allocator_))) {
          LOG_WARN("failed to copy probe row", K(ret));
        }
      }
    }
  }

  return ret;
}

void ObPLUDFResultCacheSet::destroy()
{
  if (is_inited_) {
    if (hashmap_.created()) {
      hashmap_.destroy();
    }

    key_.destory(allocator_);
    key_.reset();

    is_inited_ = false;
  }
}

int ObPLUDFResultCacheSet::inner_get_cache_obj(ObILibCacheCtx &ctx,
                                                ObILibCacheKey *key,
                                                ObILibCacheObject *&cache_obj)
{
  int ret = OB_SUCCESS;

  ObPLUDFResultCacheCtx& rc_ctx = static_cast<ObPLUDFResultCacheCtx&>(ctx);
  rc_ctx.schema_guard_->set_session_id(rc_ctx.session_info_->get_sessid_for_table());
  ObSEArray<PCVPlSchemaObj, 4> schema_array;
  ObSEArray<PCVPlSchemaObj, 4> sys_schema_array;
  bool has_old_version_err = false;

  schema_array.reset();
  sys_schema_array.reset();
  int64_t new_tenant_schema_version = OB_INVALID_VERSION;
  int64_t new_sys_schema_version = OB_INVALID_VERSION;
  bool need_check_schema = true;
  bool need_check_sys_obj = true;
  bool is_old_version = false;
  bool is_same = true;
  pl::ObPLUDFResultCacheObject *cache_object = nullptr;
  if (OB_FAIL(get_all_dep_schema(rc_ctx,
                                new_tenant_schema_version,
                                new_sys_schema_version,
                                schema_array,
                                sys_schema_array))) {
    LOG_WARN("failed to get all table schema", K(ret));
  } else if (schema_array.count() != 0 && OB_FAIL(match_dep_schema(rc_ctx, stored_schema_objs_, schema_array, is_same))) {
    LOG_WARN("failed to match_dep_schema", K(ret));
  } else if (!is_same) {
    ret = OB_OLD_SCHEMA_VERSION;
    LOG_WARN("failed to get all table schema", K(ret));
  } else if (sys_schema_array.count() != 0 && OB_FAIL(match_dep_schema(rc_ctx, stored_sys_schema_objs_, sys_schema_array, is_same))) {
    LOG_WARN("failed to match_dep_schema", K(ret));
  } else if (!is_same) {
    ret = OB_OLD_SCHEMA_VERSION;
    LOG_WARN("failed to get all table schema", K(ret));
  } else if (OB_FAIL(check_value_version(rc_ctx.schema_guard_,
                                          stored_schema_objs_,
                                          schema_array,
                                          is_old_version))) {
    LOG_WARN("fail to check table version", K(ret));
  } else if (true == is_old_version) {
    ret = OB_OLD_SCHEMA_VERSION;
    LOG_WARN("failed to get all table schema", K(ret));
  } else if (OB_FAIL(check_value_version(rc_ctx.schema_guard_,
                                          stored_sys_schema_objs_,
                                          sys_schema_array,
                                          is_old_version))) {
    LOG_WARN("fail to check table version", K(ret));
  } else if (true == is_old_version) {
    ret = OB_OLD_SCHEMA_VERSION;
    LOG_WARN("failed to get all table schema", K(ret));
  } else if (OB_FAIL(hashmap_.get_refactored(rc_ctx.argument_params_, cache_object))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to find in hash map", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(cache_object)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache obj is null", K(ret));
  } else {
    cache_obj = cache_object;
    cache_obj->set_dynamic_ref_handle(rc_ctx.handle_id_);
    if (OB_FAIL(lift_schema_version(new_tenant_schema_version, new_sys_schema_version))) {
      LOG_WARN("failed to lift pcv's tenant schema version", K(ret));
    }
  }
  if (OB_SUCC(ret) && nullptr == cache_obj) {
    ret = OB_SQL_PC_NOT_EXIST;
    LOG_WARN("failed to get cache obj in pl cache", K(ret), K(rc_ctx.key_));
  }
  return ret;
}

int ObPLUDFResultCacheSet::inner_add_cache_obj(ObILibCacheCtx &ctx,
                                                ObILibCacheKey *key,
                                                ObILibCacheObject *cache_obj)
{
  int ret = OB_SUCCESS;

  ObPLUDFResultCacheCtx& rc_ctx = static_cast<ObPLUDFResultCacheCtx&>(ctx);
  pl::ObPLUDFResultCacheObject *cache_object = static_cast<pl::ObPLUDFResultCacheObject *>(cache_obj);
  ObSEArray<PCVPlSchemaObj, 4> schema_array;
  ObSEArray<PCVPlSchemaObj, 4> sys_schema_array;

  if (OB_ISNULL(rc_ctx.dependency_tables_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null dependency tables", K(ret));
  } else if (OB_FAIL(get_all_dep_schema(*rc_ctx.schema_guard_,
                                        *rc_ctx.dependency_tables_,
                                        schema_array,
                                        sys_schema_array))) {
    LOG_WARN("failed to get all dep schema", K(ret));
  } else {
    bool is_same = true;
    bool is_old_version = false;
    if (schema_array.count() != 0) {
      if (OB_FAIL(match_dep_schema(rc_ctx, stored_schema_objs_, schema_array, is_same))) {
        LOG_WARN("failed to match_dep_schema", K(ret));
      } else if (!is_same) {
        ret = OB_OLD_SCHEMA_VERSION;
        LOG_WARN("old schema version, to be delete", K(ret), K(schema_array));
      } else if (check_value_version(rc_ctx.schema_guard_,
                                      stored_schema_objs_,
                                      schema_array,
                                      is_old_version)) {
        LOG_WARN("fail to check table version", K(ret));
      } else if (true == is_old_version) {
        ret = OB_OLD_SCHEMA_VERSION;
        LOG_WARN("old schema version, to be delete", K(ret), K(schema_array));
      }
    }
    if (OB_SUCC(ret) && sys_schema_array.count() != 0) {
      if (OB_FAIL(match_dep_schema(rc_ctx, stored_sys_schema_objs_, sys_schema_array, is_same))) {
        LOG_WARN("failed to match_dep_schema", K(ret));
      } else if (!is_same) {
        ret = OB_OLD_SCHEMA_VERSION;
        LOG_WARN("old schema version, to be delete", K(ret), K(sys_schema_array));
      } else if (check_value_version(rc_ctx.schema_guard_,
                                      stored_sys_schema_objs_,
                                      sys_schema_array,
                                      is_old_version)) {
        LOG_WARN("fail to check table version", K(ret));
      } else if (true == is_old_version) {
        ret = OB_OLD_SCHEMA_VERSION;
        LOG_WARN("old schema version, to be delete", K(ret), K(sys_schema_array));
      }
    }
  }

  /* if cache node has a value which has different schema and schema version,
     it must report an error.
     so, if ret is 0, need to create new pl object value. */
  if (OB_SUCC(ret)) {
    UDFArgRow cache_key;
    if (OB_FAIL(create_new_cache_key(rc_ctx, cache_key))) {
      LOG_WARN("fail to create cache key", K(ret));
    } else {
      cache_object->set_dynamic_ref_handle(PC_REF_UDF_RESULT_HANDLE);
      if (OB_FAIL(hashmap_.set_refactored(cache_key, cache_object))) {
        LOG_WARN("fail to add obj", K(ret), K(rc_ctx.key_));
        if (OB_HASH_EXIST == ret) {
          ret = OB_SQL_PC_PLAN_DUPLICATE;
        }
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

int64_t ObPLUDFResultCacheSet::get_mem_size()
{
  int64_t value_mem_size = 0;

  for (hash::ObHashMap<UDFArgRow, ObPLUDFResultCacheObject*, common::hash::NoPthreadDefendMode>::iterator iter = hashmap_.begin();
        iter != hashmap_.end();
        iter++) {
    if (OB_ISNULL(iter->second)) {
      BACKTRACE_RET(ERROR, OB_ERR_UNEXPECTED, true, "invalid pl_object_value");
    } else {
      value_mem_size += iter->second->get_mem_size();
    }
  } // end for
  return value_mem_size;
}

}
}