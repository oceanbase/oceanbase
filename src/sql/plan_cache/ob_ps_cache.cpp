/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_PC
#include "ob_ps_cache.h"
#include "sql/plan_cache/ob_ps_sql_utils.h"
#include "sql/plan_cache/ob_ps_cache_callback.h"
#include "sql/resolver/cmd/ob_call_procedure_stmt.h"
#include "sql/udr/ob_udr_mgr.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "lib/rc/ob_rc.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;

namespace sql
{

ObPsCache::ObPsCache()
  : next_ps_stmt_id_(0),
    inited_(false),
    tenant_id_(OB_INVALID_ID),
    host_(),
    stmt_id_map_(),
    stmt_info_map_(),
    mem_limit_pct_(0),
    mem_high_pct_(0),
    mem_low_pct_(0),
    hit_count_(0),
    access_count_(0),
    mutex_(common::ObLatchIds::PS_CACHE_EVICT_MUTEX_LOCK),
    mem_context_(NULL),
    inner_allocator_(NULL),
    tg_id_(-1)
{
}

void ObPsCache::destroy()
{
  if (inited_) {
    // ps_stmt_id和ps_stmt_info创建时，会给其增加引用计数
    // 现在PsCache要析构了，对所有内部对象减去1,如果引用计数到0，会显式free内存
    TG_DESTROY(tg_id_);
    cache_evict_all_ps();

    if (NULL != mem_context_) {
      DESTROY_CONTEXT(mem_context_);
      mem_context_ = NULL;
    }
    inited_ = false;
  }
}

ObPsCache::~ObPsCache()
{
  int ret = OB_SUCCESS;
  destroy();
  LOG_INFO("release ps plan cache", "bt", lbt(), K(tenant_id_), K(ret));
}

int ObPsCache::mtl_init(ObPsCache* &ps_cache)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = lib::current_resource_owner_id();
  int64_t mem_limit = lib::get_tenant_memory_limit(tenant_id);
  ps_cache->inited_ = false;
  return ret;
}

void ObPsCache::mtl_stop(ObPsCache * &ps_cache)
{
  if (OB_LIKELY(nullptr != ps_cache && ps_cache->is_inited())) {
    TG_CANCEL(ps_cache->tg_id_, ps_cache->evict_task_);
    TG_STOP(ps_cache->tg_id_);
  }
}

int ObPsCache::init(const int64_t hash_bucket,
                    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (!inited_) {
    ObMemAttr attr;
    attr.label_ = ObModIds::OB_SQL_PS_CACHE;
    attr.tenant_id_ = tenant_id;
    attr.ctx_id_ = ObCtxIds::PS_CACHE_CTX_ID;
    lib::ContextParam param;
    param.set_properties(lib::ALLOC_THREAD_SAFE |
                        lib::RETURN_MALLOC_DEFAULT)
    .set_parallel(4)
    .set_mem_attr(attr);
    if (OB_FAIL(stmt_id_map_.create(hash::cal_next_prime(hash_bucket),
                                    ObModIds::OB_HASH_BUCKET_PS_CACHE,
                                    ObModIds::OB_HASH_NODE_PS_CACHE,
                                    tenant_id))) {
      LOG_WARN("failed to init sql_id_map", K(ret));
    } else if (OB_FAIL(stmt_info_map_.create(hash::cal_next_prime(hash_bucket),
                                             ObModIds::OB_HASH_BUCKET_PS_INFO,
                                             ObModIds::OB_HASH_NODE_PS_INFO,
                                             tenant_id))) {
      LOG_WARN("FAILED TO INIT sql_plan_map", K(ret));
    } else if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(
        mem_context_,
        param))) {
      LOG_WARN("create memory entity failed", K(ret));
    } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::PsCacheEvict, tg_id_))) {
      LOG_WARN("failed to create tg", K(ret));
    } else if (OB_FAIL(TG_START(tg_id_))) {
      LOG_WARN("failed to start tg", K(ret));
    } else if (OB_FAIL(TG_SCHEDULE(tg_id_, evict_task_, GCONF.plan_cache_evict_interval, true))) {
      LOG_WARN("failed to schedule refresh task", K(ret));

    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL memory entity returned", K(ret));
    } else {
      evict_task_.ps_cache_ = this;
      inner_allocator_ = &mem_context_->get_allocator();
      tenant_id_ = tenant_id;
      host_ = const_cast<ObAddr &>(GCTX.self_addr());
      inited_ = true;
      LOG_INFO("init ps cache success", K(GCTX.self_addr()), K(tenant_id), K(hash_bucket));
    }
  }
  return ret;
}

//for close a session explicitly
int ObPsCache::deref_ps_stmt(const ObPsStmtId stmt_id, bool erase_item/*=false*/)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("close ps stmt", K(stmt_id));
  ObPsStmtInfoGuard guard;
  if (OB_FAIL(get_stmt_info_guard(stmt_id, guard))) {
     LOG_WARN("get stmt info guard failed", K(ret), K(stmt_id));
  } else {
    ObPsStmtInfo *ps_info = guard.get_stmt_info();
    const ObPsSqlKey ps_sql_key = ps_info->get_sql_key();
    int tmp_ret = OB_SUCCESS;
    if (erase_item) { // dec cached ref
      if (OB_FAIL(erase_stmt_item(stmt_id, ps_sql_key))) {
        LOG_WARN("fail to erase stmt", K(ret));
      }
    } else { // dec session ref
      if (OB_ISNULL(ps_info->get_ps_item())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(*ps_info));
      } else {
        ps_info->get_ps_item()->dec_ref_count_check_erase();
      }
    }

    if (OB_SUCCESS != (tmp_ret = deref_stmt_info(stmt_id))) {
      ret = tmp_ret; //previous ret ignore
      LOG_WARN("deref stmt info failed", K(ret), K(stmt_id), K(ps_sql_key));
    } else {
      LOG_TRACE("deref stmt info success", K(stmt_id), K(ps_sql_key), K(ret));
    }
  }
  return ret;
}

// for ~ObPLFunction
int ObPsCache::deref_all_ps_stmt(const ObIArray<ObPsStmtId> &ps_stmt_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ps_stmt_ids.count(); ++i) {
    const ObPsStmtId stmt_id = ps_stmt_ids.at(i);
    if (OB_FAIL(deref_ps_stmt(stmt_id))) {
      LOG_WARN("failed to close ps stmt", K(ret), K(stmt_id));
    }
  }
  return ret;
}

int ObPsCache::set_mem_conf(const ObPCMemPctConf &conf)
{
  int ret = OB_SUCCESS;
  if (conf.limit_pct_ != get_mem_limit_pct()) {
    set_mem_limit_pct(conf.limit_pct_);
    LOG_INFO("update ob_plan_cache_percentage",
             "new value", conf.limit_pct_,
             "old value", get_mem_limit_pct());
  }
  if (conf.high_pct_ != get_mem_high_pct()) {
    set_mem_high_pct(conf.high_pct_);
    LOG_INFO("update ob_plan_cache_evict_high_percentage",
             "new value", conf.high_pct_,
             "old value", get_mem_high_pct());
  }
  if (conf.low_pct_ != get_mem_low_pct()) {
    set_mem_low_pct(conf.low_pct_);
    LOG_INFO("update ob_plan_cache_evict_low_percentage",
             "new value", conf.low_pct_,
             "old value", get_mem_low_pct());
  }

  return ret;
}

int ObPsCache::get_stmt_info_guard(const ObPsStmtId ps_stmt_id,
                                   ObPsStmtInfoGuard &guard)
{
  int ret = OB_SUCCESS;
  ObPsStmtInfo *stmt_info = NULL;
  if (OB_FAIL(ref_stmt_info(ps_stmt_id, stmt_info))) {
    LOG_WARN("ref stmt info failed", K(ret), K(ps_stmt_id));
  } else if (OB_ISNULL(stmt_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_info should not be null", K(ps_stmt_id));
  } else {
    guard.set_ps_cache(*this);
    guard.set_stmt_info(*stmt_info);
    guard.set_ps_stmt_id(ps_stmt_id);
    LOG_TRACE("success to get stmt info guard", K(ps_stmt_id), K(*stmt_info));
  }
  return ret;
}

//1.set stmt_id_map_成功，创建新的stmt_item
//2.set stmt_id_map_返回OB_HASH_EXIST时，尝试从stmt_id_map_中获取stmt_item
//  1）获取成功，返回stmt_item
//  2) 报OB_HASH_NOT_EXIST, 则递归调get_or_add_stmt_item，尝试重新创建
int ObPsCache::get_or_add_stmt_item(uint64_t db_id,
                                    const ObString &ps_sql,
                                    const bool is_contain_tmp_tbl,
                                    ObPsStmtItem *&ps_item_value)
{
  int ret = OB_SUCCESS;
  ObPsStmtId new_stmt_id = gen_new_ps_stmt_id();
  ObPsStmtItem tmp_item_value(new_stmt_id);
  tmp_item_value.assign_sql_key(ObPsSqlKey(db_id,
                                           is_contain_tmp_tbl ? new_stmt_id : OB_INVALID_ID,
                                           ps_sql));
  //will deep copy
  ObPsStmtItem *new_item_value = NULL;
  //由于stmt_id_map_中的value是ObPsStmtItem的指针，因此这里需要copy整个内存
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    WITH_CONTEXT(mem_context_) {
      if (OB_FAIL(ObPsSqlUtils::alloc_new_var(*inner_allocator_, tmp_item_value, new_item_value))) {
        LOG_WARN("alloc new var failed", K(ret));
      } else if (OB_ISNULL(new_item_value)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new_ps_item_value should not be null");
      }
    }
  }
  if (OB_SUCC(ret)) {
    const ObPsSqlKey ps_sql_key = new_item_value->get_sql_key();
    new_item_value->check_erase_inc_ref_count(); //inc ref count for ps cache, ignore ret;
    ret = stmt_id_map_.set_refactored(ps_sql_key, new_item_value);
    if (OB_SUCC(ret)) {
      //do nothing
      LOG_INFO("add stmt item", K(ps_sql_key), K(*new_item_value));
      ps_item_value = new_item_value;
    } else if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
      //may be other session has set
      //inc ref count
      ObPsStmtItem *tmp_item_value = NULL;
      if (OB_FAIL(ref_stmt_item(ps_sql_key, tmp_item_value))) {
        LOG_WARN("get stmt item failed", K(ret));
        if (OB_HASH_NOT_EXIST == ret) {//stmt item被删除，需要重新创建
          if (OB_FAIL(get_or_add_stmt_item(db_id, ps_sql, is_contain_tmp_tbl, ps_item_value))) {
            LOG_WARN("fail to get or add stmt item", K(ret));
          }
        } else {
          LOG_WARN("unexpected error", K(ret), K(ps_sql_key));
        }
      } else {
        if (OB_ISNULL(tmp_item_value)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ps_item value should not be null", K(ret));
        } else {
          ps_item_value = tmp_item_value;
        }
      }
      //no matter succ or not release
      new_item_value->~ObPsStmtItem();
      inner_allocator_->free(new_item_value);
    } else {
      LOG_WARN("unexpected error", K(ret), K(new_stmt_id));
      new_item_value->~ObPsStmtItem();
      inner_allocator_->free(new_item_value);
    }
  }
  return ret;
}

// 通过plan sql key, 从key -> PsStmtIdMap中获取Item
//will increase ref count
#define LOG_WARN_IGNORE_PS_NOTFOUND(ret, fmt, args...) \
  do {\
    if (common::OB_HASH_NOT_EXIST == ret) {\
      LOG_DEBUG(fmt, ##args);\
    } else {\
      LOG_WARN(fmt, ##args);\
    }\
  } while(0);

//从stmt_id_map_获取指定ps_sql_key对应的ps_stmt_item
//1. 若call_back_ret == OB_TRY_EGAIN，说明当前item的ref_cout == 0, 已经准备淘汰；
//   此时，重试直到报OB_HASH_NOT_EXIST,说明其他session已经将item从stmt_id_map_中删除；
//   将OB_HASH_NOT_EXIST错误码返回给外层，期待创建新的ps_stmt_item
//2. 该接口只有在prepare阶段使用，execute和close阶段不应该调用
//3. 该接口会返回三类错误码：
//   1) OB_SUCCESS: 成功获取ps_stmt_item
//   2) OB_HASH_NOT_EXIST: a.stmt_id_map_中本身就不存在当前key b.开始key对应的引用计数为0，重试若干次后变为该错误码
//   3) OB_EGAIN: 尝试了MAX_RETRY_CNT，ps_stmt_item的ref_count仍旧为0
int ObPsCache::ref_stmt_item(const ObPsSqlKey &ps_sql_key,
                             ObPsStmtItem *&ps_stmt_item)
{
  int ret = OB_SUCCESS;
  int callback_ret = OB_SUCCESS;
  ObPsStmtItemRefAtomicOp op;
  int64_t MAX_RETRY_CNT = 2000;
  int64_t retry_cnt = 0;
  //get stmt_item and inc ref count
  do {
    ps_stmt_item = NULL;
    ret = stmt_id_map_.read_atomic(ps_sql_key, op);
    switch(ret) {
      case OB_SUCCESS: {
        if (OB_SUCCESS != (callback_ret = op.get_callback_ret())) {
          ret = callback_ret;
          if (OB_EAGAIN == ret) {
            LOG_INFO("try again", K(ret), "stmt_id", ps_sql_key, K(retry_cnt));
            ob_usleep(static_cast<uint32_t>(500)); //sleep 500us
          }
        } else if (OB_FAIL(op.get_value(ps_stmt_item))) {
          LOG_WARN("failed to get ps stmt item", K(ret), K(ps_sql_key));
        }
        break;
      }
      case OB_HASH_NOT_EXIST: {
        LOG_WARN_IGNORE_PS_NOTFOUND(ret, "entry not exist", K(ret), K(ps_sql_key));
        break;
      }
      case OB_EAGAIN: {
        LOG_WARN("try again", K(ret), "stmt_id", ps_sql_key, K(retry_cnt));
        ob_usleep(static_cast<uint32_t>(500)); //sleep 500us
        break;
      }
      default: {
        LOG_WARN("failed to get ps stmt item", K(ret), K(ps_sql_key));
      }
    }
    retry_cnt++;
  } while(OB_EAGAIN == ret && retry_cnt < MAX_RETRY_CNT);
  return ret;
}

int ObPsCache::deref_stmt_item(const ObPsSqlKey &ps_sql_key)
{
  int ret = OB_SUCCESS;
  ObPsStmtItemDerefAtomicOp op;
  if (OB_FAIL(stmt_id_map_.read_atomic(ps_sql_key, op))) {
    LOG_WARN("deref stmt item failed", K(ps_sql_key), K(ret));
  } else if (op.get_ret() != OB_SUCCESS) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObPsCache::ref_stmt_item(const uint64_t db_id,
                             const ObString &ps_sql,
                             ObPsStmtItem *&stmt_item)
{
  int ret = OB_SUCCESS;
  stmt_item = NULL;
  if (ps_sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty sql", K(ret), K(ps_sql));
  } else {
    ObPsSqlKey ps_sql_key(db_id, ps_sql);
    if (OB_FAIL(ref_stmt_item(ps_sql_key, stmt_item))) {
      LOG_WARN_IGNORE_PS_NOTFOUND(ret, "ps item value not exist", K(ret), K(ps_sql_key));
    } else if (OB_ISNULL(stmt_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get stmt id failed", K(ret));
    }
  }
  return ret;
}

int ObPsCache::get_or_add_stmt_info(const PsCacheInfoCtx &info_ctx,
                                    const ObResultSet &result,
                                    ObSchemaGetterGuard &schema_guard,
                                    ObPsStmtItem *ps_item,
                                    ObPsStmtInfo *&ref_ps_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ps_item)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ps_item));
  } else if (OB_ISNULL(info_ctx.normalized_sql_.ptr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("normalized sql is null", K(ret), K(ps_item));
  } else if (OB_ISNULL(info_ctx.raw_params_) || OB_ISNULL(info_ctx.fixed_param_idx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null", K(ret));
  } else if (OB_FAIL(ref_stmt_info(ps_item->get_ps_stmt_id(), ref_ps_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      int64_t tenant_version = OB_INVALID_VERSION;
      ObArenaAllocator allocator;
      ObPsStmtInfo tmp_stmt_info(&allocator);
      tmp_stmt_info.assign_sql_key(*ps_item);
      tmp_stmt_info.set_stmt_type(info_ctx.stmt_type_);
      tmp_stmt_info.set_ps_item(ps_item);
      // calc check_sum with normalized sql
      uint64_t ps_stmt_checksum = ob_crc64(info_ctx.normalized_sql_.ptr(),
                                           info_ctx.normalized_sql_.length()); // actual is crc32
      tmp_stmt_info.set_ps_stmt_checksum(ps_stmt_checksum);
      if (OB_FAIL(schema_guard.get_schema_version(tenant_id_, tenant_version))) {
        LOG_WARN("fail to get tenant version", K(ret), K(tenant_id_));
      } else if (FALSE_IT(tmp_stmt_info.set_tenant_version(tenant_version))) {
        // do nothing
      } else if (OB_FAIL(tmp_stmt_info.assign_no_param_sql(info_ctx.no_param_sql_))) {
        LOG_WARN("fail to assign no param sql", K(ret), K(info_ctx.no_param_sql_));
      } else if (OB_FAIL(tmp_stmt_info.assign_raw_sql(info_ctx.raw_sql_))) {
        LOG_WARN("fail to assign rule name", K(ret), K(info_ctx.raw_sql_));
      } else if (OB_FAIL(tmp_stmt_info.assign_fixed_raw_params(*info_ctx.fixed_param_idx_,
                                                               *info_ctx.raw_params_))) {
        LOG_WARN("fail to assign raw params failed", KPC(info_ctx.fixed_param_idx_), K(ret));
      } else if (OB_FAIL(fill_ps_stmt_info(result, info_ctx.param_cnt_,
                                           tmp_stmt_info, info_ctx.num_of_returning_into_))) {
        LOG_WARN("fill ps stmt info failed", K(ret));
      } else if (OB_FAIL(add_stmt_info(*ps_item, tmp_stmt_info, ref_ps_info))) {
        LOG_WARN("add stmt info failed", K(ret), K(*ps_item), K(tmp_stmt_info));
      }
    } else {
      LOG_WARN("fail to get stmt info", K(ret), K(*ps_item));
    }
  }

  return ret;
}

// may parallel execute by multi_thread
int ObPsCache::erase_stmt_item(ObPsStmtId stmt_id, const ObPsSqlKey &ps_key)
{
  int ret = OB_SUCCESS;
  ObPsStmtItem *ps_item = NULL;
/*
 *              Thread A                                            Thread B
 *     Get stmt_item successfully                           Get stmt_item successfully
 *
 *      Check stmt_info expired
 *                                                            Check stmt_info expired
 *
 *  Delete stmt_item by key(db_id, ps_sql)
 *
 *
 *
 * Add stmt_item with (db_id, ps_sql) as key
 *
 *                                                        Delete stmt_item by key(db_id, ps_sql)
 *                                               The item added in the previous step of thread A is deleted
 *
 *
 */
  ObPsStmtItemEraseAtomicOp op(stmt_id);
  if (OB_FAIL(stmt_id_map_.read_atomic(ps_key, op))) {
    if (OB_HASH_NOT_EXIST == ret) {
      LOG_INFO("erased by others", K(ret), K(ps_key));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get ps stmt item", K(ps_key), K(ret));
    }
  } else if (op.need_erase()) {
    if (OB_FAIL(stmt_id_map_.erase_refactored(ps_key, &ps_item))) {
      if (OB_HASH_NOT_EXIST == ret) {
        LOG_INFO("erased by others", K(ret), K(ps_key));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to erase stmt info", K(ps_key), K(ret));
      }
    } else {
      ps_item->dec_ref_count_check_erase();
    }
  }
  return ret;
}

int ObPsCache::get_all_stmt_id(ObIArray<ObPsStmtId> *id_array)
{
  int ret = OB_SUCCESS;
  ObGetAllStmtIdOp op(id_array);
  if (OB_ISNULL(id_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("id_array is NULL", K(ret));
  } else if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ps_cache is not init yet", K(ret));
  } else if (OB_FAIL(stmt_info_map_.foreach_refactored(op))) {
    LOG_WARN("traverse stmt_info_map_ failed", K(ret));
  } else if (OB_FAIL(op.get_callback_ret())) {
    LOG_WARN("traverse stmt_info_map_ failed", K(ret));
  }
  return ret;
}

int ObPsCache::fill_ps_stmt_info(const ObResultSet &result,
                                 int64_t param_cnt,
                                 ObPsStmtInfo &ps_stmt_info,
                                 int32_t returning_into_parm_num) const
{
  int ret = OB_SUCCESS;
  const ParamsFieldIArray *params = result.get_param_fields();
  const ColumnsFieldIArray *columns = result.get_field_columns();
  const ObSqlCtx *sql_ctx = result.get_exec_context().get_sql_ctx();
  if (OB_ISNULL(params) || OB_ISNULL(columns) || OB_ISNULL(sql_ctx)
      || OB_ISNULL(ps_stmt_info.get_inner_allocator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(params), K(columns), K(sql_ctx), K(ret));
  } else if (OB_FAIL(ps_stmt_info.reserve_ps_meta_fields(params->count(),
                                                         columns->count()))) {
    LOG_WARN("fail to reserve ps meta field", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < params->count(); ++i) {
    if (OB_FAIL(ps_stmt_info.add_param_field(params->at(i)))) {
      LOG_WARN("add param field failed", K(ret), K(i));
    }
  }
  for (int i = 0; OB_SUCC(ret) && i < columns->count(); ++i) {
    if (OB_FAIL(ps_stmt_info.add_column_field(columns->at(i)))) {
      LOG_WARN("add filed column failed", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && result.get_ref_objects().count() > 0) {
    int64_t size = result.get_ref_objects().count() * sizeof(ObSchemaObjVersion);
    char *buf = NULL;
    if (NULL == (buf = (char*)ps_stmt_info.get_inner_allocator()->alloc(size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(size));
    } else {
      for (int64_t i = 0; i < result.get_ref_objects().count(); i++) {
        ObSchemaObjVersion *obj = new(buf+i * sizeof(ObSchemaObjVersion))ObSchemaObjVersion();
        *obj = result.get_ref_objects().at(i);
      }
      ps_stmt_info.set_dep_objs(reinterpret_cast<ObSchemaObjVersion*>(buf),
                                result.get_ref_objects().count());
    }
  }
  if (OB_SUCC(ret)) {
    ps_stmt_info.set_question_mark_count(param_cnt);
    // only used when returning into
    ps_stmt_info.set_num_of_returning_into(returning_into_parm_num);
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(ps_stmt_info.get_ps_sql().ptr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql should already be init", K(ret), K(ps_stmt_info));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t info_size = 0;
    if (OB_FAIL(ps_stmt_info.get_convert_size(info_size))) {
      LOG_WARN("fail to get convert size", K(ret));
    } else {
      int64_t item_size = ps_stmt_info.get_ps_sql().length() + sizeof(ObPsStmtItem) + 1
                          + ps_stmt_info.get_no_param_sql().length() + 1;
      ps_stmt_info.set_item_and_info_size(item_size + info_size);
    }
  }
  LOG_INFO("fill ps stmt info", KPC(columns), KPC(params), K(result), K(ps_stmt_info), K(param_cnt));
  return ret;
}

int ObPsCache::add_stmt_info(const ObPsStmtItem &ps_item,
                             const ObPsStmtInfo &ps_info,
                             ObPsStmtInfo *&ref_ps_info)
{
  int ret = OB_SUCCESS;
  ObPsStmtInfo *new_info_value = NULL;
  if (!ps_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt_id is invalid", K(ret), K(ps_item), K(ps_info));
  } else if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    //will deep copy
    WITH_CONTEXT(mem_context_) {
      if (OB_FAIL(ObPsSqlUtils::alloc_new_var(*inner_allocator_, ps_info, new_info_value))) { // ref_count == 1
        LOG_WARN("alloc new var failed", K(ret));
      } else if (OB_ISNULL(new_info_value)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new_ps_item_value should not be null");
      }
    }
  }
  if (OB_SUCC(ret)) {
    const ObPsStmtId stmt_id = ps_item.get_ps_stmt_id();
    new_info_value->check_erase_inc_ref_count(); //inc ref count for ps cache, ignore ret;
    ret = stmt_info_map_.set_refactored(stmt_id, new_info_value);
    if (OB_SUCC(ret)) {
      ref_ps_info = new_info_value;
      LOG_INFO("succ to add stmt info", K(ps_item), K(*new_info_value), K(ret));
    } else if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
      ObPsStmtInfo *tmp_stmt_info = NULL;
      //may be other session has set
      if (OB_FAIL(ref_stmt_info(ps_item.get_ps_stmt_id(), tmp_stmt_info))) {
        LOG_INFO("fail to ref stmt info, set again", K(ret), K(ps_item));
        if (OB_HASH_NOT_EXIST == ret) {
          if (OB_FAIL(add_stmt_info(ps_item, ps_info, ref_ps_info))) {
            LOG_WARN("fail to add stmt info", K(ret));
          }
        } else {
          LOG_WARN("fail to add stmt info", K(ret), K(ps_item), K(ps_info));
        }
      } else {
        if (OB_ISNULL(tmp_stmt_info)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("stmt info should not be null", K(ret), K(ps_item));
        } else {
          ref_ps_info = tmp_stmt_info;
          LOG_INFO("succ to ref stmt info", K(ps_item), K(*tmp_stmt_info), K(ret));
        }
      }
      new_info_value->~ObPsStmtInfo();
      inner_allocator_->free(new_info_value);
    } else {
      LOG_WARN("add new stmt info failed", K(ret), K(ps_item), K(*new_info_value));
      new_info_value->~ObPsStmtInfo();
      inner_allocator_->free(new_info_value);
    }
  }
  return ret;
}

int ObPsCache::ref_stmt_info(const ObPsStmtId stmt_id, ObPsStmtInfo *&ps_stmt_info)
{
  int ret = OB_SUCCESS;
  int callback_ret = OB_SUCCESS;
  ObPsStmtInfoRefAtomicOp op;
  int64_t MAX_RETRY_CNT = 2000;
  int64_t retry_cnt = 0;
  do {
    ps_stmt_info = NULL;
    ret = stmt_info_map_.read_atomic(stmt_id, op);
    switch(ret) {
      case OB_SUCCESS: {
        if (OB_SUCCESS != (callback_ret = op.get_callback_ret())) {
          ret = callback_ret;
          if (OB_EAGAIN == ret) {
            LOG_INFO("try again", K(ret), K(stmt_id), K(retry_cnt));
            ob_usleep(static_cast<uint32_t>(500)); //sleep 500us
          }
        } else if (OB_FAIL(op.get_value(ps_stmt_info))) {
          LOG_WARN("failed to get ps_stmt_info", K(ret), K(ps_stmt_info));
        }
        break;
      }
      case OB_HASH_NOT_EXIST: {
        LOG_WARN_IGNORE_PS_NOTFOUND(ret, "entry not exist", K(ret), K(stmt_id));
        break;
      }
      case OB_EAGAIN: {
        LOG_INFO("try again", K(ret), K(stmt_id), K(retry_cnt));
        ob_usleep(static_cast<uint32_t>(500)); //sleep 500us
        break;
      }
      default: {
        LOG_WARN("failed to get ps stmt info", K(ret), K(stmt_id));
      }
    }
    retry_cnt++;
  } while(OB_EAGAIN == ret && retry_cnt < MAX_RETRY_CNT);
  return ret;
}

int ObPsCache::deref_stmt_info(const ObPsStmtId stmt_id)
{
  int ret = OB_SUCCESS;
  ObPsStmtInfoDerefAtomicOp op;
  ObPsStmtInfo *ps_info = NULL;
  ObIAllocator *allocator = NULL;
  if (OB_FAIL(stmt_info_map_.read_atomic(stmt_id, op))) {
    LOG_WARN("deref stmt info failed", K(stmt_id), K(ret));
  } else if (op.get_ret() != OB_SUCCESS) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deref stmt info failed", K(ret));
  } else if (op.is_erase()) {
    if (OB_FAIL(stmt_info_map_.erase_refactored(stmt_id, &ps_info))) {
      LOG_WARN("fail to erase stmt info", K(stmt_id), K(ret));
    } else if (OB_ISNULL(ps_info)) {
      LOG_INFO("ps info is null in map", K(stmt_id));
      // do nothing
    } else if (OB_ISNULL(allocator = ps_info->get_external_allocator())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(allocator), K(ret));
    } else {
      LOG_INFO("free ps info", K(*ps_info), K(stmt_id));
      ps_info->~ObPsStmtInfo();
      allocator->free(ps_info);
    }
    LOG_INFO("erase stmt info", K(stmt_id), K(ret));
  }
  return ret;
}

struct ObDumpPsItem
{
  int operator()(common::hash::HashMapPair<ObPsSqlKey, ObPsStmtItem *>&entry)
  {
    int ret = common::OB_SUCCESS;

    if (OB_ISNULL(entry.second)) {
      LOG_INFO("ps item is null", "stmt_key", entry.first);
    } else {
      LOG_INFO("dump ps item", "ps_item", *entry.second);
    }

    return ret;
  }
};

struct ObDumpPsInfo
{
  int operator()(common::hash::HashMapPair<ObPsStmtId, ObPsStmtInfo *>&entry)
  {
    int ret = common::OB_SUCCESS;

    if (OB_ISNULL(entry.second)) {
      LOG_INFO("ps info is null", "stmt_id", entry.first);
    } else {
      LOG_INFO("dump ps info", "stmt_id", entry.first, "ps_info", *entry.second);
    }

    return ret;
  }
};

class PsTimeCmp
{
public:
  bool operator() (std::pair<ObPsStmtId, int64_t> left, std::pair<ObPsStmtId, int64_t> right)
  {
    return left.second < right.second;
  }
};

int ObPsCache::cache_evict()
{
  return inner_cache_evict(false/*is_evict_all*/);
}

int ObPsCache::cache_evict_all_ps()
{
  return inner_cache_evict(true/*is_evict_all*/);
}

int ObPsCache::inner_cache_evict(bool is_evict_all)
{
  int ret = OB_SUCCESS;
  // There is a concurrency problem between the regularly triggered ps cache evict task and
  // the manually triggered flush ps cache evict task. There is also a concurrency problem between
  // the flush ps cache evict tasks triggered at the same time in different sessions.
  // The same ps stmt may be added to the closed list by different ps cache evict tasks
  // at the same time, so mutex is used here to make all flush ps cache evict tasks execute serially
  lib::ObMutexGuard guard(mutex_);
  SMART_VARS_2((PsIdClosedTimePairs, expired_stmt_ids),
               (PsIdClosedTimePairs, closed_stmt_ids)) {
    ObGetClosedStmtIdOp op(&expired_stmt_ids, &closed_stmt_ids);
    if (!is_inited()) {
      ret = OB_NOT_INIT;
      LOG_WARN("ps_cache is not init yet", K(ret));
    } else if (OB_FAIL(stmt_info_map_.foreach_refactored(op))) {
      LOG_WARN("traverse stmt_info_map_ failed", K(ret));
    } else if (OB_FAIL(op.get_callback_ret())) {
      LOG_WARN("traverse stmt_info_map_ failed", K(ret));
    } else {
      LOG_INFO("ps cache evict", K_(tenant_id), K(stmt_id_map_.size()), K(stmt_info_map_.size()),
                                K(op.get_used_size()), K(get_mem_high()), K(expired_stmt_ids.count()),
                                K(closed_stmt_ids.count()));
      // evict expired ps
      for (int64_t i = 0; i < expired_stmt_ids.count(); ++i) { //ignore ret
        LOG_TRACE("ps close time", K(i), K(expired_stmt_ids.at(i).first), K(expired_stmt_ids.at(i).second));
        if (OB_FAIL(deref_ps_stmt(expired_stmt_ids.at(i).first, true/*erase_stmt*/))) {
          LOG_WARN("fail to evict ps stmt", K(ret),
                  K(expired_stmt_ids.at(i).first), K(expired_stmt_ids.count()));
        }
      }
      if (is_evict_all) {
        for (int64_t i = 0; i < closed_stmt_ids.count(); ++i) { //ignore ret
            LOG_TRACE("ps close time", K(i), K(closed_stmt_ids.at(i).first), K(closed_stmt_ids.at(i).second));
            if (OB_FAIL(deref_ps_stmt(closed_stmt_ids.at(i).first, true/*erase_stmt*/))) {
              LOG_WARN("fail to evict ps stmt", K(ret),
                      K(closed_stmt_ids.at(i).first), K(closed_stmt_ids.count()));
            }
          }
      } else {
        if (op.get_used_size() > get_mem_high()) {
          std::sort(closed_stmt_ids.begin(), closed_stmt_ids.end(), PsTimeCmp());
          for (int64_t i = 0; i < closed_stmt_ids.count() / 2; ++i) { //ignore ret
            LOG_TRACE("ps close time", K(i), K(closed_stmt_ids.at(i).first), K(closed_stmt_ids.at(i).second));
            if (OB_FAIL(deref_ps_stmt(closed_stmt_ids.at(i).first, true/*erase_stmt*/))) {
              LOG_WARN("fail to evict ps stmt", K(ret),
                      K(closed_stmt_ids.at(i).first), K(closed_stmt_ids.count()));
            }
          }
        }
      }
      LOG_TRACE("ps cache evict end");
    }
  }
  return ret;
}


void ObPsCache::dump_ps_cache()
{
  int ret = OB_SUCCESS;
  LOG_INFO("================= ps cache info ========", K_(tenant_id), K(stmt_id_map_.size()), K(stmt_info_map_.size()));
  ObDumpPsItem dump_ps_item_op;
  ObDumpPsInfo dump_ps_info_op;
  if (OB_FAIL(stmt_id_map_.foreach_refactored(dump_ps_item_op))) {
    LOG_WARN("fail to dump ps item");
  } // ignore ret
  if (OB_FAIL(stmt_info_map_.foreach_refactored(dump_ps_info_op))) {
    LOG_WARN("fail to dump ps info", K(ret));
  } // ignore ret
}

int ObPsCache::mem_total(int64_t &mem_total) const
{
  int ret = OB_SUCCESS;
  mem_total = 0;
  if (true == is_inited()) {
    if (OB_ISNULL(inner_allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner_allocator_ is NULL", K(ret));
    } else {
      mem_total = inner_allocator_->total();
    }
  } else {
    LOG_DEBUG("ps cache is not init", K(ret), K(is_inited()));
  }
  return ret;
}

int ObPsCache::check_schema_version(ObSchemaGetterGuard &schema_guard,
                                    ObPsStmtInfo &stmt_info,
                                    bool &is_expired)
{
  int ret = OB_SUCCESS;
  is_expired = false;
  int64_t new_tenant_version = OB_INVALID_VERSION;
  if (OB_FAIL(schema_guard.get_schema_version(tenant_id_, new_tenant_version))) {
    LOG_WARN("fail to get tenant version", K(ret), K(tenant_id_));
  } else if (new_tenant_version != stmt_info.get_tenant_version()) {
    LOG_TRACE("tenant version change", K(stmt_info), K(new_tenant_version), K(tenant_id_));
    for (int64_t i = 0; OB_SUCC(ret) && !is_expired && i < stmt_info.get_dep_objs_cnt(); i++) {
      ObSchemaObjVersion &obj_version = stmt_info.get_dep_objs()[i];
      int64_t new_version = OB_INVALID_VERSION;
      if (OB_FAIL(schema_guard.get_schema_version(obj_version.get_schema_type(),
                                                  tenant_id_,
                                                  obj_version.object_id_,
                                                  new_version))) {
        LOG_WARN("fail to get schema version", K(ret), K(tenant_id_), K(obj_version));
      } else if (new_version != obj_version.version_) {
        LOG_INFO("ps cache is expired", K(ret), K(stmt_info), K(tenant_id_),
                 K(new_version), K(obj_version), KP(&stmt_info));
        is_expired = true;
      }
    }
  }

  return ret;
}

void ObPsCacheEliminationTask::runTimerTask()
{
  ps_cache_->update_memory_conf();
  ps_cache_->cache_evict();
}

int ObPsCache::update_memory_conf()
{
  int ret = OB_SUCCESS;
  ObPCMemPctConf pc_mem_conf;
  const char* conf_names[3] =
      {
        "ob_plan_cache_percentage",
        "ob_plan_cache_evict_low_percentage",
        "ob_plan_cache_evict_high_percentage"
      };
  int64_t *conf_values[3] =
      {
        &pc_mem_conf.limit_pct_,
        &pc_mem_conf.low_pct_,
        &pc_mem_conf.high_pct_
      };
  ObArenaAllocator alloc;
  ObObj obj_val;

  if (tenant_id_ > OB_SYS_TENANT_ID && tenant_id_ <= OB_MAX_RESERVED_TENANT_ID) {
    // tenant id between (OB_SYS_TENANT_ID, OB_MAX_RESERVED_TENANT_ID) is a virtual tenant,
    // virtual tenants do not have schema
    // do nothing
  } else {
    for (int32_t i = 0; i < 3 && OB_SUCC(ret); ++i) {
    if (OB_FAIL(ObBasicSessionInfo::get_global_sys_variable(tenant_id_,
                                                            alloc,
                                                            ObDataTypeCastParams(),
                                                            ObString(conf_names[i]), obj_val))) {
      } else if (OB_FAIL(obj_val.get_int(*conf_values[i]))) {
        LOG_WARN("failed to get int", K(ret), K(obj_val));
      } else if (*conf_values[i] < 0 || *conf_values[i] > 100) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid value of plan cache conf", K(obj_val));
      }
    }  // end for

    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_mem_conf(pc_mem_conf))) {
        SQL_PC_LOG(WARN, "fail to update plan cache mem conf", K(ret));
      }
    }
  }

  LOG_TRACE("update plan cache memory config",
            "ob_plan_cache_percentage", pc_mem_conf.limit_pct_,
            "ob_plan_cache_evict_high_percentage", pc_mem_conf.high_pct_,
            "ob_plan_cache_evict_low_percentage", pc_mem_conf.low_pct_,
            "tenant_id", tenant_id_);

  return ret;
}

} //end of sql
} //end of oceanbase
