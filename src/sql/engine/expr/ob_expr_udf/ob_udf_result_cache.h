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

#ifndef OCEANBASE_UDF_RESULT_CACHE_H_
#define OCEANBASE_UDF_RESULT_CACHE_H_
#include "share/ob_define.h"
#include "lib/hash/ob_hashmap.h"
#include "sql/ob_sql_define.h"
#include "sql/plan_cache/ob_i_lib_cache_key.h"
#include "sql/plan_cache/ob_i_lib_cache_object.h"
#include "sql/plan_cache/ob_i_lib_cache_node.h"
#include "sql/plan_cache/ob_i_lib_cache_context.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "sql/plan_cache/ob_lib_cache_register.h"
#include "pl/pl_cache/ob_pl_cache.h"
#include "sql/engine/subquery/ob_subplan_filter_op.h"

namespace oceanbase
{

namespace pl
{

class UDFArgRow : public DatumRow
{
public:
  UDFArgRow() :
    DatumRow(),
    default_param_bitmap_()
  {}

  bool operator==(const UDFArgRow &other) const;
  int hash(uint64_t &hash_val, uint64_t seed=0) const;

  ObBitSet<> default_param_bitmap_;
};

struct ObPLUDFResultCacheKey : public ObILibCacheKey
{
  ObPLUDFResultCacheKey()
  : ObILibCacheKey(ObLibCacheNameSpace::NS_INVALID),
    db_id_(common::OB_INVALID_ID),
    package_id_(common::OB_INVALID_ID),
    routine_id_(common::OB_INVALID_ID),
    sys_vars_str_(),
    config_vars_str_() {}
  ObPLUDFResultCacheKey(uint64_t db_id, uint64_t key_id)
  : ObILibCacheKey(ObLibCacheNameSpace::NS_INVALID),
    db_id_(db_id),
    package_id_(key_id),
    routine_id_(common::OB_INVALID_ID),
    sys_vars_str_(),
    config_vars_str_() {}

  void reset();
  virtual int deep_copy(common::ObIAllocator &allocator, const ObILibCacheKey &other) override;
  void destory(common::ObIAllocator &allocator);
  virtual uint64_t hash() const override;
  virtual bool is_equal(const ObILibCacheKey &other) const;

  TO_STRING_KV(K_(db_id),
               K_(package_id),
               K_(routine_id),
               K_(namespace),
               K_(sys_vars_str),
               K_(config_vars_str));

  uint64_t db_id_;
  uint64_t package_id_;
  uint64_t routine_id_;
  common::ObString sys_vars_str_;
  common::ObString config_vars_str_;
};

struct ObPLUDFResultCacheCtx : public ObILibCacheCtx, public ObPLCacheBasicCtx
{
  ObPLUDFResultCacheCtx()
    : ObILibCacheCtx(),
      ObPLCacheBasicCtx(),
      handle_id_(MAX_HANDLE),
      key_(),
      need_add_obj_stat_(true),
      dependency_tables_(nullptr),
      sys_schema_version_(OB_INVALID_VERSION),
      tenant_schema_version_(OB_INVALID_VERSION),
      argument_params_(),
      name_(),
      execute_time_(0),
      cache_obj_hash_value_(0),
      result_cache_max_result_(0),
      result_cache_max_size_(0)
  {
  }

  CacheRefHandleID handle_id_;
  ObPLUDFResultCacheKey key_;
  bool need_add_obj_stat_;
  sql::DependenyTableStore *dependency_tables_;
  uint64_t sys_schema_version_;
  uint64_t tenant_schema_version_;
  UDFArgRow argument_params_;
  ObString name_;
  int64_t execute_time_;
  uint64_t cache_obj_hash_value_;
  int64_t result_cache_max_result_;
  int64_t result_cache_max_size_;
};

struct PLUDFResultCacheObjStat
{
  int64_t pl_schema_id_;
  uint64_t db_id_;
  ObString name_;
  common::ObCollationType sql_cs_type_;
  int64_t gen_time_;
  int64_t last_active_time_;
  int64_t build_time_;
  uint64_t hit_count_;
  common::ObString sys_vars_str_;
  common::ObString config_vars_str_;
  ObPLCacheObjectType type_;
  uint64_t hash_value_;

  PLUDFResultCacheObjStat()
    : pl_schema_id_(OB_INVALID_ID),
      db_id_(OB_INVALID_ID),
      name_(),
      sql_cs_type_(common::CS_TYPE_INVALID),
      gen_time_(0),
      last_active_time_(0),
      build_time_(0),
      hit_count_(0),
      sys_vars_str_(),
      config_vars_str_(),
      type_(ObPLCacheObjectType::UDF_RESULT_TYPE),
      hash_value_(0)
  {
  }

  inline bool is_updated() const
  {
    return last_active_time_ != 0;
  }

  void reset()
  {

    pl_schema_id_ = OB_INVALID_ID;
    db_id_ = OB_INVALID_ID;
    sql_cs_type_ = common::CS_TYPE_INVALID;
    gen_time_ = 0;
    last_active_time_ = 0;
    hit_count_ = 0;
    name_.reset();
    sys_vars_str_.reset();
  }

  TO_STRING_KV(K_(pl_schema_id),
               K_(db_id),
               K_(name),
               K_(gen_time),
               K_(last_active_time),
               K_(hit_count),
               K_(build_time),
               K_(sys_vars_str));
};

class ObPLUDFResultCacheObject : public sql::ObILibCacheObject
{
public:
  ObPLUDFResultCacheObject(lib::MemoryContext &mem_context)
  : ObILibCacheObject(ObLibCacheNameSpace::NS_UDF_RESULT_CACHE, mem_context),
    stat_(),
    result_()
    {}

  virtual ~ObPLUDFResultCacheObject() { reset(); }

  static int deep_copy_result(ObIAllocator &alloc, ObObj &src, ObObj &dst);

  ObObj &get_result() { return result_; }

  inline const PLUDFResultCacheObjStat get_stat() const { return stat_; }
  inline PLUDFResultCacheObjStat &get_stat_for_update() { return stat_; }

  virtual int check_need_add_cache_obj_stat(ObILibCacheCtx &ctx, bool &need_real_add);
  virtual void dump_deleted_log_info(const bool is_debug_log = true) const {}

  virtual int update_cache_obj_stat(sql::ObILibCacheCtx &ctx);

  virtual void reset();

  TO_STRING_KV(K_(stat), K_(result));

protected:
  PLUDFResultCacheObjStat stat_;
  ObObj result_;
};

class ObPLUDFResultCacheSet : public ObILibCacheNode, public ObPLDependencyCheck
{
public:
  ObPLUDFResultCacheSet(ObPlanCache *lib_cache, lib::MemoryContext &mem_context)
    : ObILibCacheNode(lib_cache, mem_context),
      ObPLDependencyCheck(allocator_),
      is_inited_(false),
      key_()
  {
  }
  virtual ~ObPLUDFResultCacheSet()
  {
    destroy();
  };
  virtual int init(ObILibCacheCtx &ctx, const ObILibCacheObject *cache_obj) override;
  virtual int inner_get_cache_obj(ObILibCacheCtx &ctx,
                                  ObILibCacheKey *key,
                                  ObILibCacheObject *&cache_obj) override;
  virtual int inner_add_cache_obj(ObILibCacheCtx &ctx,
                                  ObILibCacheKey *key,
                                  ObILibCacheObject *cache_obj) override;

  virtual int before_cache_evicted() { return OB_SUCCESS; }
  void destroy();

  common::ObString &get_sql_id() { return sql_id_; }
  int create_new_cache_key(ObPLUDFResultCacheCtx &rc_ctx,
                            UDFArgRow &cache_key);

  int64_t get_mem_size();

  TO_STRING_KV(K_(is_inited));
private:
  bool is_inited_;
  ObPLUDFResultCacheKey key_;  //used for manager key memory
  common::ObString sql_id_;
  common::hash::ObHashMap<UDFArgRow, ObPLUDFResultCacheObject*, common::hash::NoPthreadDefendMode> hashmap_;
};

} // namespace pl end
} // namespace oceanbase end

#endif