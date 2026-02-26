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

#include "ob_sql_stat_manager.h"
#include "share/wr/ob_wr_task.h"
#include "share/location_cache/ob_location_service.h"
#include "share/wr/ob_wr_rpc_proxy.h"

#define USING_LOG_PREFIX SQL
namespace oceanbase {
namespace sql {
constexpr int64_t SQL_STAT_SNAPSHOT_TIME = 1 * 60 * 1000 * 1000;
constexpr int64_t INVALID_LAST_SNAPSHOT_END_TIME = -1;
constexpr int64_t SQL_STAT_SNAPSHOT_AHEAD_SNAP_ID = -2;
constexpr int64_t SQL_STAT_MAX_COUNT = 200000;
ObSqlStatManager::ObSqlStatManager(int64_t tenant_id)
    : is_inited_(false),
      tenant_id_(tenant_id),
      memory_limit_percentage_(MEMORY_LIMIT_DEFAULT),
      memory_evict_high_percentage_(MEMORY_EVICT_HIGH_DEFAULT),
      memory_evict_low_percentage_(MEMORY_EVICT_LOW_DEFAULT),
      allocator_(),
      alloc_handle_(&allocator_, SQL_STAT_MAX_COUNT),
      sql_stat_infos_(alloc_handle_)
{}

ObSqlStatManager::~ObSqlStatManager()
{}

int ObSqlStatManager::mtl_new(ObSqlStatManager *&sql_stat_manager) {
  int ret = OB_SUCCESS;
  if (is_virtual_tenant_id(MTL_ID())) {
    // do nothing
  } else {
    void *buf = nullptr;
    if (OB_ISNULL(buf = ob_malloc(sizeof(ObSqlStatManager),
                                  ObMemAttr(MTL_ID(), "SqlStatManager")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc sql stat manager", K(ret));
    } else {
      sql_stat_manager = new (buf) ObSqlStatManager(MTL_ID());
    }
  }
  return ret;
}

int ObSqlStatManager::mtl_init(ObSqlStatManager *&sql_stat_manager) {
  int ret = OB_SUCCESS;
  if (is_virtual_tenant_id(MTL_ID())) {
    // do nothing
  } else if (OB_ISNULL(sql_stat_manager)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(sql_stat_manager->init())) {
    LOG_WARN("failed to init sql stat manager", K(ret));
  } else {
    LOG_INFO("success to init sql stat manager", K(MTL_ID()));
  }
  return ret;
}

void ObSqlStatManager::mtl_destroy(ObSqlStatManager *&sql_stat_manager) {
  int ret = OB_SUCCESS;
  common::ob_delete(sql_stat_manager);
  LOG_INFO("success to destroy sql stat manager", K(MTL_ID()));
}

int ObSqlStatManager::init() {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("sql stat manager is already inited", K(ret));
  } else {
    lib::ObMemAttr mem_attr(tenant_id_, "SqlStatObj");
    int64_t total_limit = get_memory_limit();
    if (total_limit <= 0) {
      total_limit = 0;
    }
    if (OB_FAIL(allocator_.init(common::OB_MALLOC_NORMAL_BLOCK_SIZE, mem_attr, total_limit))) {
      LOG_WARN("failed to init sql stat allocator", K(ret), K(total_limit));
    } else if (OB_FAIL(sql_stat_infos_.init("sqlstat_infos", MTL_ID()))) {
      LOG_WARN("failed to init sql stat infos", K(ret));
      allocator_.destroy();
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSqlStatManager::start() {
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql stat manager is not inited", K(ret));
  } else if (is_virtual_tenant_id(MTL_ID())) {
    // do nothing
  } else if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer *)->get_tg_id(),
                                 stat_task_, ObSqlStatTask::REFRESH_INTERVAL,
                                 true))) {
    LOG_WARN("failed to schedule task", K(ret));
  } else {
    LOG_INFO("success to start sql stat manager", K(MTL_ID()));
  }
  return ret;
}

void ObSqlStatManager::stop() {
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql stat manager is not inited", K(ret));
  } else if (is_virtual_tenant_id(MTL_ID())) {
    // do nothing
  } else {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer *)->get_tg_id(), stat_task_);
    LOG_INFO("success to stop sql stat manager", K(MTL_ID()));
  }
}

void ObSqlStatManager::wait() {
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql stat manager is not inited", K(ret));
    } else if (is_virtual_tenant_id(MTL_ID())) {
    // do nothing
  } else {
    TG_WAIT_TASK(MTL(omt::ObSharedTimer *)->get_tg_id(), stat_task_);
  }
}

void ObSqlStatManager::destroy() {
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql stat manager is not inited", K(ret));
  } else {
    if (TG_TASK_EXIST(MTL(omt::ObSharedTimer *)->get_tg_id(), stat_task_,
                      is_exist) == OB_SUCCESS && is_exist) {
      TG_CANCEL_TASK(MTL(omt::ObSharedTimer *)->get_tg_id(), stat_task_);
      TG_WAIT_TASK(MTL(omt::ObSharedTimer *)->get_tg_id(), stat_task_);
    }
    sql_stat_infos_.reset();
    sql_stat_infos_.destroy();
    allocator_.destroy();
    is_inited_ = false;
  }
}

//only use in virtual table, don't refresh latest active time
int ObSqlStatManager::get_sql_stat_record(const ObSqlStatRecordKey &key,
                                          ObExecutedSqlStatRecord *&record) {
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql stat manager is not inited", K(ret));
  } else if (OB_FAIL(sql_stat_infos_.get(key, record))) {
    // LOG_WARN("failed to get sql stat record", K(ret), K(key));
  }
  return ret;
}

int ObSqlStatManager::get_or_create_sql_stat_record(
    const ObSqlStatRecordKey &key, ObExecutedSqlStatRecord *&record, bool &is_create, bool for_select)
{
  int ret = OB_SUCCESS;
  is_create = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql stat manager is not inited", K(ret));
  } else if (OB_FAIL(sql_stat_infos_.get(key, record))) {
    if (ret == OB_ENTRY_NOT_EXIST) {
      ret = OB_SUCCESS;
      is_create = true;
      if (OB_FAIL(sql_stat_infos_.create(key, record))) {
        LOG_WARN("failed to insert and get sql stat record", K(ret), K(key),
                 KP(record), K(record));
      }
    } else {
      LOG_WARN("failed to get sql stat record", K(ret), K(key));
    }
  }
  if (is_create && OB_NOT_NULL(record)) {
    record->get_sql_stat_info().set_first_load_time(ObTimeUtility::current_time());
  }
  if (OB_NOT_NULL(record) && !for_select) {
    record->set_latest_active_time(ObTimeUtility::current_time());
  }
  return ret;
}

int ObSqlStatManager::revert_sql_stat_record(ObExecutedSqlStatRecord *record) {
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql stat manager is not inited", K(ret));
  } else {
    sql_stat_infos_.revert(record);
  }
  return ret;
}

int ObSqlStatManager::sum_sql_stat_record(const ObSqlStatRecordKey &key,
                                          ObExecutingSqlStatRecord &record,const ObSQLSessionInfo &session_info, const ObString &sql, const ObPhysicalPlan *plan) {
  int ret = OB_SUCCESS;
  bool is_create = false;
  ObExecutedSqlStatRecord *sum_record = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql stat manager is not inited", K(ret));
  } else if (OB_FAIL(get_or_create_sql_stat_record(key, sum_record, is_create))) {
    LOG_WARN("failed to get or create sql stat record", K(ret), K(key));
  } else if (OB_ISNULL(sum_record)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sum record is null", K(ret), K(key));
  } else if (is_create && OB_FAIL(sum_record->get_sql_stat_info().init(key, session_info, sql, plan))) {
    LOG_WARN("failed to init sql stat info", K(ret));
  } else if (OB_FAIL(sum_record->sum_stat_value(record))) {
    LOG_WARN("failed to sum sql stat record", K(ret), K(key));
  }
  if (OB_NOT_NULL(sum_record)) {
    sql_stat_infos_.revert(sum_record);
  }
  return ret;
}

int64_t ObSqlStatManager::get_memory_used() const {
  int ret = OB_SUCCESS;
  return allocator_.allocated();
}

int64_t ObSqlStatManager::get_memory_limit() const {
  int ret = OB_SUCCESS;
  int64_t tenant_mem = lib::get_tenant_memory_limit(tenant_id_);
  int64_t error_sim_mem = -EVENT_CALL(EventTable::EN_SQLSTAT_MGR_MEM);
  if (error_sim_mem > 0) {
    tenant_mem = error_sim_mem;
  }
  int64_t mem_limit = tenant_mem * memory_limit_percentage_ / 100 > 200 * 1024 * 1024
                          ? 200 * 1024 * 1024
                          : tenant_mem * memory_limit_percentage_ / 100;
  if (mem_limit < 1 * 1024 * 1024) {
    mem_limit = 0;
  }
  return mem_limit;
}

void ObSqlStatManager::set_memory_limit(int64_t memory_limit_percentage, int64_t memory_evict_high_percentage, int64_t memory_evict_low_percentage) {
  int ret = OB_SUCCESS;
  if (memory_limit_percentage != memory_limit_percentage_) {
    LOG_INFO("set memory limit percentage", K(memory_limit_percentage), K(memory_limit_percentage_));
    memory_limit_percentage_ = memory_limit_percentage;
  }
  if (memory_evict_high_percentage != memory_evict_high_percentage_) {
    LOG_INFO("set memory evict high percentage", K(memory_evict_high_percentage), K(memory_evict_high_percentage_));
    memory_evict_high_percentage_ = memory_evict_high_percentage;
  }
  if (memory_evict_low_percentage != memory_evict_low_percentage_) {
    LOG_INFO("set memory evict low percentage", K(memory_evict_low_percentage), K(memory_evict_low_percentage_));
    memory_evict_low_percentage_ = memory_evict_low_percentage;
  }
  if (is_inited_) {
    int64_t total_limit = get_memory_limit();
    if (total_limit <= 0) {
      total_limit = 0;
    }
    allocator_.set_total_limit(total_limit);
    LOG_INFO("set memory limit", K(total_limit));
  }
}

int ObSqlStatManager::check_if_need_evict() {
  int ret = OB_SUCCESS;
  int64_t memory_used = get_memory_used();
  int64_t memory_limit = get_memory_limit();
  int64_t tmp_mem = - EVENT_CALL(EventTable::EN_SQLSTAT_MGR_MEM);
  if (memory_used > memory_limit * memory_evict_high_percentage_ / 100) {
    if (OB_FAIL(mutex_.trylock())) {
      LOG_WARN("sqlstat manager failed to try lock mutex", K(ret));
    } else if (OB_FAIL(do_evict())) {
      mutex_.unlock();
      LOG_WARN("failed to do evict", K(ret));
    } else {
      allocator_.purge();
      sql_stat_infos_.purge();
      LOG_INFO("purge allocator", K(allocator_.allocated()));
      mutex_.unlock();
    }
  }
  LOG_INFO("check if need evict", K(memory_used), K(memory_limit),
             K(memory_evict_high_percentage_), K(memory_evict_low_percentage_),
             K(ret));
  return ret;
}

int64_t ObSqlStatManager::calculate_evict_num() const {
  int ret = OB_SUCCESS;
  int64_t memory_used = get_memory_used();
  int64_t memory_limit = get_memory_limit();
  int64_t evict_num = 0;
  if (memory_used > 0) {
    evict_num = sql_stat_infos_.count() -
                1.0 * memory_limit * memory_evict_low_percentage_ / 100 / memory_used * sql_stat_infos_.count();
    if (evict_num < 0) {
      evict_num = 0;
    }
  }
  LOG_INFO("calculate evict num",
      K(memory_used),
      K(memory_limit),
      K(memory_evict_low_percentage_),
      K(memory_evict_high_percentage_),
      K(evict_num),
      K(sql_stat_infos_.count()));
  return evict_num;
}

int ObSqlStatManager::do_evict() {
  int ret = OB_SUCCESS;
  int64_t evict_num = calculate_evict_num();
  if (evict_num > 0) {
    ObExecutedSqlStatRecordArray evict_values;
    ObSqlStatEvictOp evict_op(&evict_values, evict_num);
    if (OB_FAIL(sql_stat_infos_.for_each(evict_op))) {
      //if failed, revert the evict values to avoid memory leak
      for(int i = 0; i < evict_values.count(); i++) {
        ObExecutedSqlStatRecord *value = evict_values.at(i);
        sql_stat_infos_.revert(value);
      }
      LOG_WARN("failed to foreach sql stat record", K(ret));
    } else {
      for (int i = 0; i < evict_values.count(); i++) {
        ObExecutedSqlStatRecord *value = evict_values.at(i);
        ObSqlStatRecordKey tmp_key = value->get_key();
        sql_stat_infos_.revert(value);
        //value->key may already be freed when tenant is deleting
        sql_stat_infos_.del(tmp_key);
      }
    }
  }
  return ret;
}

int ObSqlStatManager::update_last_snap_record_value() {
  int ret = OB_SUCCESS;
  ObUpdateSqlStatOp update_op;
  if (OB_FAIL(sql_stat_infos_.for_each(update_op))) {
    LOG_WARN("failed to foreach sql stat record", K(ret));
  }
  LOG_INFO("update follower's sqlstat snapshot", K(ret));
  return ret;
}

void ObSqlStatTask::runTimerTask() {
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LOG_INFO("run sql stat eviction task", K(ObSqlStatTask::REFRESH_INTERVAL));
  ObSqlStatManager *sql_stat_manager = MTL(ObSqlStatManager *);
  if (OB_NOT_NULL(sql_stat_manager)) {
    sql_stat_manager->set_memory_limit(ObSqlStatManager::MEMORY_LIMIT_DEFAULT,
        ObSqlStatManager::MEMORY_EVICT_HIGH_DEFAULT,
        ObSqlStatManager::MEMORY_EVICT_LOW_DEFAULT);
  }
  if (OB_ISNULL(sql_stat_manager)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(sql_stat_manager->check_if_need_evict())) {
    LOG_WARN("failed to check if need evict", K(ret));
  }
}

int ObUpdateSqlStatOp::operator()(ObSqlStatRecordKey &key, ObExecutedSqlStatRecord *sqlstat)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sqlstat)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sqlstat is null", K(ret));
  } else if (OB_FAIL(sqlstat->update_last_snap_record_value())) {
    LOG_WARN("failed to update last snap record value", K(ret));
  }
  return ret == OB_SUCCESS ? true : false;
}

ObSqlStatEvictOp::ObSqlStatEvictOp(ObSqlStatManager::ObExecutedSqlStatRecordArray *evict_values, int64_t evict_num)
    : evict_values_(evict_values), evict_num_(evict_num)
{
  ObSqlStatManager *sql_stat_manager = MTL(ObSqlStatManager *);
  if (OB_ISNULL(sql_stat_manager)) {
    int ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql_stat_manager is null", K(ret));
  } else {
    hashmap_ = sql_stat_manager->get_sql_stat_infos();
  }
}
int ObSqlStatEvictOp::operator()(ObSqlStatRecordKey &key, ObExecutedSqlStatRecord *sqlstat)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(evict_values_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(sqlstat)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sqlstat is null", K(ret));
  } else if (OB_ISNULL(hashmap_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hashmap is null", K(ret));
  } else {
    if (evict_values_->count() < evict_num_) {
      //inc ref
      if (OB_FAIL(hashmap_->get(key, sqlstat))) {
        LOG_WARN("failed to get sqlstat", K(ret));
      } else if (OB_FAIL(evict_values_->push_back(sqlstat))) {
        LOG_WARN("failed to push back evict value", K(evict_values_), K(sqlstat), K(ret));
        //if fail, dec ref
        hashmap_->revert(sqlstat);
      } else if (evict_values_->count() == evict_num_) {
        std::make_heap(evict_values_->begin(), evict_values_->end(), sqlstat_compare);
      }
    } else if (sqlstat_compare(sqlstat, evict_values_->at(0))) {
      std::pop_heap(evict_values_->begin(), evict_values_->end(), sqlstat_compare);
      ObExecutedSqlStatRecord *old_sqlstat = evict_values_->at(evict_values_->count() - 1);
      evict_values_->pop_back();
      // dec old_sqlstat ref
      hashmap_->revert(old_sqlstat);
      // inc new_sqlstat ref
      if (OB_FAIL(hashmap_->get(key, sqlstat))) {
        LOG_WARN("failed to get sqlstat", K(ret));
      } else if (OB_FAIL(evict_values_->push_back(sqlstat))) {
        //if fail, dec ref
        hashmap_->revert(sqlstat);
        LOG_WARN("failed to push back evict value", K(evict_values_), K(sqlstat), K(ret));
      } else {
        std::push_heap(evict_values_->begin(), evict_values_->end(), sqlstat_compare);
      }
    }
  }
  return ret == OB_SUCCESS ? true : false;
}

} // namespace sql
} // namespace oceanbase