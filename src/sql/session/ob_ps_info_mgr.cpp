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

#define USING_LOG_PREFIX SQL_SESSION
#include "sql/session/ob_ps_info_mgr.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObPsInfoMgr::ObPsInfoMgr()
    :last_stmt_id_(0),
     block_allocator_(SMALL_BLOCK_SIZE, common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                      ObMalloc(ObModIds::OB_SQL_SESSION_SBLOCK)),
     bucket_allocator_wrapper_(&block_allocator_),
     id_psinfo_map_allocer_(SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_)),
     name_psinfo_map_allocer_(SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_)),
     id_psinfo_map_(),
     name_psinfo_map_(),
     ps_session_info_pool_(SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_))
{
}

ObPsInfoMgr::~ObPsInfoMgr()
{
}

int ObPsInfoMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(id_psinfo_map_.create(64,
                                    &id_psinfo_map_allocer_,
                                    &bucket_allocator_wrapper_))) {
    LOG_WARN("init id_psinfo_map failed", K(ret));
  } else if (OB_FAIL(name_psinfo_map_.create(64,
                                             &name_psinfo_map_allocer_,
                                             &bucket_allocator_wrapper_))) {
    LOG_WARN("init name_psinfo_map failed", K(ret));
  }
  return ret;
}

void ObPsInfoMgr::reset()
{
  last_stmt_id_ = 0;
  ps_sql_store_ = NULL;
  id_psinfo_map_.clear();
  id_psinfo_map_allocer_.reset();
  name_psinfo_map_.clear();
  name_psinfo_map_allocer_.reset();
  ps_session_info_pool_.reset();
}

int ObPsInfoMgr::add_ps_info(const uint64_t sql_id,
                             const ObString &sql,
                             const ObPhysicalPlanCtx *pctx,
                             const bool is_dml)
{
  int ret = OB_SUCCESS;
  ObPsSessionInfo *info = NULL;
  uint64_t stmt_id = allocate_stmt_id();
  ret = id_psinfo_map_.get(static_cast<uint32_t>(sql_id), info);
  if (hash::HASH_NOT_EXIST == ret) {
    info = ps_session_info_pool_.alloc();
    if (OB_UNLIKELY(NULL == info)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR( "can not alloc mem for ObPsSessionInfo");
    } else {
      info->init(ObWrapperAllocator(&block_allocator_));
      info->set_sql_id(sql_id);
      info->set_params_count(pctx->get_param_store().count());
      ObString osql;
      if (OB_SUCC(ps_sql_store_->store_sql(sql, osql))) {
        info->set_prepare_sql(osql);
        //info->set_stmt_type(stmt_type);//TODO 这个有什么用
        info->set_dml(is_dml);
        ret = id_psinfo_map_.set(static_cast<uint32_t>(stmt_id), info);
        if (hash::HASH_INSERT_SUCC == ret) {
          LOG_DEBUG("insert item into id_psinfo_map success", K(stmt_id), K(info));
          ret = OB_SUCCESS;
        } else if (hash::HASH_EXIST == ret) {
          LOG_WARN("exist in id_psinfo_map", K(stmt_id));
          ps_session_info_pool_.free(info);
          ret = OB_ERR_UNEXPECTED;
        } else {
          LOG_WARN("fail to insert item into id_psinfo_map", K(stmt_id), K(ret));
          ps_session_info_pool_.free(info);
          ret = OB_ERR_UNEXPECTED;
        }
      } else {
        LOG_WARN("store prepare sql failed", K(ret));
      }
    }
  } else if (hash::HASH_EXIST == ret) {
    LOG_ERROR("stmt id exist", K(stmt_id), K(ret));
  } else {
    LOG_WARN("fail to get ps session info", K(stmt_id), K(ret));
  }
  return ret;
}

int ObPsInfoMgr::add_ps_info(const ObString &pname,
                             const uint64_t sql_id,
                             const ObString &sql,
                             const ObPhysicalPlanCtx *pctx,
                             const bool is_dml)
{
  int ret = OB_SUCCESS;
  ObPsSessionInfo *info = NULL;
  ret = name_psinfo_map_.get(pname, info);
  if (hash::HASH_NOT_EXIST == ret) {
    info  = ps_session_info_pool_.alloc();
    if (OB_UNLIKELY(NULL == info)) {
      LOG_WARN("can not alloc mem for ObPsSessionInfo");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      info->init(ObWrapperAllocator(&block_allocator_));
      info->set_sql_id(sql_id);
      info->set_params_count(pctx->get_param_store().count());
      ObString osql;
      if (OB_SUCC(ps_sql_store_->store_sql(sql, osql))) {
        info->set_prepare_sql(osql);
        //info->set_stmt_type(stmt_type);//TODO 这个有什么用
        info->set_dml(is_dml);
        ret = name_psinfo_map_.set(pname, info);
        if (hash::HASH_INSERT_SUCC == ret) {
          LOG_DEBUG("insert item into name_psinfo_map success", K(pname), K(info));
          ret = OB_SUCCESS;
        } else if (hash::HASH_EXIST == ret) {
          LOG_WARN("exist in name_psinfo_map", K(pname));
          ps_session_info_pool_.free(info);
          ret = OB_ERR_UNEXPECTED;
        } else {
          LOG_WARN("fail to insert item into name_psinfo_map", K(pname), K(ret));
          ps_session_info_pool_.free(info);
          ret = OB_ERR_UNEXPECTED;
        }
      } else {
        LOG_WARN("store prepare sql failed", K(ret));
      }
    }
  } else if (hash::HASH_EXIST == ret) {
    LOG_ERROR("stmt id exist", K(pname), K(ret));
  } else {
    LOG_WARN("fail to get ps session info", K(pname), K(ret));
  }
  return ret;
}

int ObPsInfoMgr::close_all_stmt()
{
  int ret = OB_SUCCESS;
 //int err = OB_SUCCESS;
 //IdPsInfoMap::iterator iter;
 //ObPsSessionInfo *info = NULL;
 //uint64_t sql_id = 0;
 //for (iter = id_psinfo_map_.begin(); iter != id_psinfo_map_.end(); iter++) {
 //  if (hash::HASH_EXIST != (err = id_psinfo_map_.get(iter->first, info))) {
 //    LOG_WARN("not found ObPsSessionInfo", "whose key", iter->first);
 //  } else {
 //    sql_id = info->get_sql_id();
 //    if (OB_SUCCESS != (err = ps_store_->remove_plan(sql_id))) {
 //      LOG_WARN("close prepared statement failed", K_(session_key), K(sql_id));
 //    } else {
 //      LOG_INFO("close prepared statement when session quit", K_(session_key), K(sql_id),
 //                "stmt_id", iter->first);
 //      OB_STAT_INC(SQL, OB_SYS_TENANT_ID, SQL_PS_COUNT, -1);
 //    }
 //    ps_session_info_pool_.free(info);//free ps session info when session quit
 //  }
 //}
 //id_psinfo_map_.clear(); //clear id_psinfo_map
 //
 //NamePsInfoMap::Iterator niter;
 //for (niter = name_psinfo_map_.begin(); niter != name_psinfo_map_.end(); niter++) {
 //  if (hash::HASH_EXIST != (err = name_psinfo_map_.get(niter->first, info))) {
 //    LOG_WARN("not found ObPsSessionInfo", "whose key", niter->first);
 //  } else {
 //    sql_id = info->get_sql_id();
 //    if (OB_SUCCESS != (err = ps_store_->remove_plan(sql_id))) {
 //      LOG_WARN("close prepared statement failed", K_(session_key), K(sql_id));
 //    } else {
 //      LOG_INFO("close prepared statement when session quit", K_(session_key), K(sql_id),
 //               "stmt_id", iter->first);
 //      OB_STAT_INC(SQL, OB_SYS_TENANT_ID, SQL_PS_COUNT, -1);
 //    }
 //    ps_session_info_pool_.free(info);//free ps session info when session quit
 //  }
 //}
 //name_psinfo_map_.clear(); //clear name_psinfo_map
  return ret;
}

int ObPsInfoMgr::remove_ps_info(const uint64_t stmt_id)
{
  int ret = OB_SUCCESS;
  ObPsSessionInfo *info = NULL;
  if (id_psinfo_map_.get(static_cast<uint32_t>(stmt_id), info) != hash::HASH_EXIST) {
    ret = OB_ERR_PREPARE_STMT_NOT_FOUND;
    LOG_WARN("prepare statement id not found in id_psinfo_map", K(stmt_id));
  } else {
    if (OB_SUCC(ps_sql_store_->free_sql(info->get_prepare_sql()))) {
      ps_session_info_pool_.free(info);
      if (id_psinfo_map_.erase(static_cast<uint32_t>(stmt_id)) != hash::HASH_EXIST) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("drop prepare statement error", K(stmt_id));
      } else {
        LOG_DEBUG("remove ps statement", K(stmt_id));
      }
    } else {
      LOG_WARN("remove prepared sql failed", K(ret));
    }
  }
  return ret;
}

int ObPsInfoMgr::remove_ps_info(const ObString &name)
{
  int ret = OB_SUCCESS;
  ObPsSessionInfo *info = NULL;
  if (name_psinfo_map_.get(name, info) != hash::HASH_EXIST) {
    ret = OB_ERR_PREPARE_STMT_NOT_FOUND;
    LOG_WARN("prepare statement id not found in name_psinfo_map", K(ret), K(name));
  } else {
    ps_sql_store_->free_sql(info->get_prepare_sql());
    ps_session_info_pool_.free(info);
    info = NULL;
    if (name_psinfo_map_.erase(name) != hash::HASH_EXIST) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("drop prepare statement error", K(ret), K(name));
    } else {
      LOG_DEBUG("remove ps statement", K(name));
    }
  }
  return ret;
}

ObPsSessionInfo* ObPsInfoMgr::get_psinfo(const ObString &name)
{
  ObPsSessionInfo *sinfo = NULL;
  int ret = name_psinfo_map_.get(name, sinfo);
  if (hash::HASH_EXIST != ret) {
    LOG_WARN("Get ps session info failed", K(name), K(ret));
    sinfo = NULL;
  } else {
    LOG_DEBUG("Get ps session info success", K(name));
  }
  return sinfo;
}

ObPsSessionInfo* ObPsInfoMgr::get_psinfo(const uint64_t stmt_id)
{
  ObPsSessionInfo *sinfo = NULL;
  int ret = id_psinfo_map_.get(static_cast<uint32_t>(stmt_id), sinfo);
  if (hash::HASH_EXIST != ret) {
    LOG_WARN("Get ps session info failed", K(stmt_id), K(ret));
    sinfo = NULL;
  } else {
    LOG_DEBUG("Get ps session info success", K(stmt_id));
  }
  return sinfo;
}
