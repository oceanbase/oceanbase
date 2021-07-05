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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/executor/ob_interm_result_manager.h"
#include "sql/executor/ob_interm_result_pool.h"
#include "share/ob_debug_sync.h"
#include "share/ob_thread_mgr.h"
#include "sql/executor/ob_interm_result_item.h"

namespace oceanbase {
namespace sql {

using namespace oceanbase::common;
using namespace common::hash;

class ObUpdateIRExpireTime {
public:
  ObUpdateIRExpireTime(const int64_t expire_time) : ret_(OB_SUCCESS), expire_time_(expire_time)
  {}

  void operator()(common::hash::HashMapPair<ObIntermResultInfo, ObIntermResult*>& entry)
  {
    if (OB_ISNULL(entry.second)) {
      ret_ = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret_));
    } else {
      entry.second->set_expire_time(expire_time_);
    }
  }

public:
  int ret_;

private:
  const int64_t expire_time_;
};

ObIntermResultManager* ObIntermResultManager::instance_ = NULL;

ObIntermResultGC::ObIntermResultGC()
    : ir_map_(NULL),
      ir_manager_(NULL),
      cur_time_(0),
      allocator_(ObModIds::OB_SQL_EXECUTOR_INTERM_RESULT_EXPIRE_IR),
      expire_irs_(allocator_),
      invalid_time_ir_count_(0)
{}

ObIntermResultGC::~ObIntermResultGC()
{}

void ObIntermResultGC::reset()
{
  ir_map_ = NULL;
  ir_manager_ = NULL;
  cur_time_ = 0;
  expire_irs_.reset();
  invalid_time_ir_count_ = 0;
  // reset allocator in the end
  allocator_.reset();
}

void ObIntermResultGC::operator()(common::hash::HashMapPair<ObIntermResultInfo, ObIntermResult*>& entry)
{
  if (OB_ISNULL(entry.second)) {
    LOG_ERROR("null ptr");
  } else {
    int ret = OB_SUCCESS;
    int64_t time_diff = entry.second->get_expire_time() - cur_time_;
    if (OB_UNLIKELY(entry.second->get_expire_time() <= 0)) {
      LOG_ERROR("invalid ir expire time. skip gc", "time", entry.second->get_expire_time());
    } else {
      if (time_diff < 0 && OB_SUCC(entry.second->try_begin_recycle())) {
        if (OB_FAIL(expire_irs_.push_back(entry))) {
          LOG_ERROR("fail to push back to expire_irs_", K(ret), K(entry), K(expire_irs_.size()));
        }
      } else if (time_diff > 1800000000) {
        invalid_time_ir_count_++;
      }
    }
  }
}

void ObIntermResultGC::runTimerTask()
{
  static int64_t gc_run_count = 0;

  int ret = common::OB_SUCCESS;
  common::hash::HashMapPair<ObIntermResultInfo, ObIntermResult*> entry;
  if (OB_ISNULL(ir_map_) || OB_ISNULL(ir_manager_)) {
    LOG_WARN("the interm result map of the GC class is NULL", K_(ir_map), K_(ir_manager));
  } else {
    expire_irs_.reset();
    allocator_.reset();
    cur_time_ = ::oceanbase::common::ObTimeUtility::current_time();
    ir_map_->foreach_refactored(*this);

    if (ir_map_->size() > 0 || expire_irs_.size() > 0 || 0 == gc_run_count % 900) {
      LOG_INFO("Interm result recycle",
          "total_interm_result_count",
          ir_map_->size(),
          "life_too_long_interm_result_count",
          invalid_time_ir_count_,
          "ready_to_free_interm_result_count",
          expire_irs_.size());
    }
    while (OB_SUCC(expire_irs_.pop_front(entry))) {
      if (OB_FAIL(ir_manager_->free_result(entry.first))) {
        if (common::OB_NEED_RETRY == ret) {
          LOG_DEBUG("free mr result failed, need retry");
        } else {
          LOG_WARN("free mr result failed", K(ret));
        }
      } else {
        LOG_DEBUG("free mr result success");
      }
    }
    invalid_time_ir_count_ = 0;
    expire_irs_.reset();
    allocator_.reset();
  }
  gc_run_count++;
}

void ObIntermResultGC::set_ir_map(common::hash::ObHashMap<ObIntermResultInfo, ObIntermResult*>* ir_map)
{
  ir_map_ = ir_map;
}

void ObIntermResultGC::set_ir_manager(ObIntermResultManager* ir_manager)
{
  ir_manager_ = ir_manager;
}

ObIntermResultManager::ObIntermResultManager()
    : inited_(false), ir_map_(), ir_gc_(), gc_delay_time_(DEFAULT_INTERM_RESULT_GC_DELAY_TIME), ir_pool_(NULL)
{}

ObIntermResultManager::~ObIntermResultManager()
{
  reset();
}

void ObIntermResultManager::reset()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ir_map_.clear())) {
    LOG_ERROR("fail to clear interm result map", K(ret));
  }
  TG_DESTROY(lib::TGDefIDs::IntermResGC);
  ir_gc_.reset();
  gc_delay_time_ = DEFAULT_INTERM_RESULT_GC_DELAY_TIME;
  ir_pool_ = NULL;
  inited_ = false;
}

int ObIntermResultManager::build_instance()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL != instance_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("instance is not NULL, build twice", K(ret));
  } else if (OB_UNLIKELY(NULL == (instance_ = OB_NEW(ObIntermResultManager, ObModIds::OB_SQL_EXECUTOR)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("instance is NULL, unexpected", K(ret));
  } else if (OB_FAIL(instance_->init())) {
    OB_DELETE(ObIntermResultManager, ObModIds::OB_SQL_EXECUTOR, instance_);
    instance_ = NULL;
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to init interm result manager", K(ret));
  }
  return ret;
}

ObIntermResultManager* ObIntermResultManager::get_instance()
{
  ObIntermResultManager* instance = NULL;
  if (OB_UNLIKELY(OB_ISNULL(instance_) || !instance_->inited_)) {
    LOG_ERROR("instance is NULL or not inited", K(instance_));
  } else {
    instance = instance_;
  }
  return instance;
}

int ObIntermResultManager::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(ir_pool_ = ObIntermResultPool::get_instance())) {
    ret = OB_ERR_UNEXPECTED;
    _OB_LOG(ERROR, "fail to get iterm result pool instance");
  } else if (OB_FAIL(ir_map_.create(INTERM_RMAP_BUCKET_SIZE, ObModIds::OB_SQL_EXECUTOR_INTERM_RESULT_MAP))) {
    LOG_WARN("fail to create ir map", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::IntermResGC))) {
    LOG_WARN("fail to init timer", K(ret));
  } else {
    ir_gc_.set_ir_map(&ir_map_);
    ir_gc_.set_ir_manager(this);
    if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::IntermResGC, ir_gc_, gc_delay_time_, true))) {
      LOG_WARN("fail to schedule timer", K(ret));
    } else {
      inited_ = true;
    }
  }
  return ret;
}

int ObIntermResultManager::update_expire_time(const ObIntermResultInfo& ir_info, const int64_t expire_time)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ir_info.is_init() || expire_time <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ir_info), K(expire_time));
  } else {
    ObUpdateIRExpireTime updater(expire_time);
    if (OB_FAIL(ir_map_.atomic_refactored(ir_info, updater))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        LOG_WARN("hash table atomic failed", K(ret), K(ir_info));
      }
    } else if (OB_SUCCESS != updater.ret_) {
      ret = updater.ret_;
      LOG_WARN("update expire time failed", K(ret));
    }
  }
  return ret;
}

int ObIntermResultManager::get_result(const ObIntermResultInfo& ir_info, ObIntermResultIterator& iter)
{
  int ret = OB_SUCCESS;

  ObIntermResultRead ir_read;
  // ir_read try increase reference count of the map result
  if (OB_SUCCESS == (ret = ir_map_.atomic_refactored(ir_info, ir_read))) {
    ret = ir_read.get_ret();
    if (OB_SUCC(ret)) {
      // notify the iter that the reference count of the interm result is increased
      if (OB_FAIL(iter.set_interm_result(ir_info, ir_read.get_value(), true))) {
        LOG_ERROR("fail to set interm result", K(ret));
      } else {
        LOG_DEBUG("get interm result");
      }
    } else {
      // fail to increase the reference count, the interm result must be recycling
      // ret = common::OB_ENTRY_NOT_EXIST;
      LOG_WARN("can not increase the cnt, the interm result should be recycling.", K(ret));
    }
  } else if (OB_HASH_NOT_EXIST == ret) {
    ret = common::OB_ENTRY_NOT_EXIST;
    LOG_DEBUG("the required interm result is not exist.", K(ret));
  } else {
    LOG_WARN("cannot get the interm result", K(ret));
  }

  return ret;
}

int ObIntermResultManager::get_result_item(
    const ObIntermResultInfo& ir_info, const int64_t index, ObIntermResultItem& result_item, int64_t& total_cnt)
{
  int ret = OB_SUCCESS;
  ObIntermResultRead ir_read;
  if (OB_FAIL(ir_map_.atomic_refactored(ir_info, ir_read))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_DEBUG("the request item result is not exist", K(ret));
    } else {
      LOG_WARN("get interm result failed", K(ret), K(ir_info));
    }
  } else if (OB_FAIL(ir_read.get_ret())) {
    LOG_WARN("interm result read failed", K(ret), K(ir_info));
  } else if (OB_ISNULL(ir_read.get_value())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL item result", K(ret));
  } else {
    ObIntermResult* ir = ir_read.get_value();
    total_cnt = ir->get_scanner_count();
    ObIIntermResultItem* item = NULL;
    if (total_cnt > 0) {
      if (OB_FAIL(ir->get_item(index, item))) {
        LOG_WARN("get item failed", K(ret));
      } else if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("item is NULL", K(ret));
      } else {
        if (item->in_memory()) {
          if (OB_FAIL(result_item.assign(*static_cast<ObIntermResultItem*>(item)))) {
            LOG_WARN("interm result item assign failed", K(ret));
          }
        } else {
          if (OB_FAIL(result_item.from_disk_ir_item(*static_cast<ObDiskIntermResultItem*>(item)))) {
            LOG_WARN("from disk interm result item failed", K(ret));
          }
        }
      }
    }
    int tmp_ret = ir->try_dec_cnt();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("try decrease reference count failed", K(tmp_ret));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

int ObIntermResultManager::add_result(
    const ObIntermResultInfo& ir_info, ObIntermResult* interm_result, int64_t expire_time)
{
  int ret = OB_SUCCESS;
  interm_result->set_expire_time(expire_time);
  if (OB_UNLIKELY(!interm_result->rows_is_completed())) {
    LOG_WARN("rows in this interm result is not completed", K(ret));
  } else if (OB_FAIL(ir_map_.set_refactored(ir_info, interm_result))) {
    LOG_WARN("fail to set interm result to map", K(ret));
  }
  return ret;
}

int ObIntermResultManager::delete_result(const ObIntermResultInfo& ir_info)
{
  DEBUG_SYNC(BEFORE_RECYCLE_INTERM_RESULT);
  int ret = OB_SUCCESS;
  ObIntermResultRecycle ir_recycle;
  // ir_recycle try begin recycle for the map result
  if (OB_SUCCESS == (ret = ir_map_.atomic_refactored(ir_info, ir_recycle))) {
    ret = ir_recycle.get_ret();
    if (OB_SUCC(ret)) {
      // has begin recycle
      free_result(ir_info);
      LOG_DEBUG("free interm result");
    } else {
      // fail to begin recycle
      // ret = common::OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to recycle, maybe it has being recycled or some iterator is reading it", K(ret));
    }
  } else if (OB_HASH_NOT_EXIST == ret) {
    ret = common::OB_ENTRY_NOT_EXIST;
    LOG_DEBUG("the required interm result is not exist.");
  } else {
    LOG_WARN("cannot recycle the interm result", K(ret));
  }
  return ret;
}

int ObIntermResultManager::delete_result(ObIntermResultIterator& iter)
{
  int ret = OB_SUCCESS;
  ObIntermResultInfo ir_info;
  ObIntermResult* ir = iter.ir_;

  if (OB_ISNULL(ir)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(iter.get_interm_result_info(ir_info))) {
    LOG_WARN("fail to get interm result info from iterator", K(ret));
  } else {
    iter.reset();
    if (OB_FAIL(delete_result(ir_info))) {
      LOG_WARN("fail to delete result", K(ret), K(ir_info));
    }
  }

  return ret;
}

int ObIntermResultManager::alloc_result(ObIntermResult*& interm_result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ir_pool_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null ptr", K_(ir_pool), K(ret));
  } else {
    ret = ir_pool_->alloc_interm_result(interm_result);
  }
  return ret;
}

int ObIntermResultManager::free_result(ObIntermResult* interm_result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ir_pool_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null ptr", K_(ir_pool), K(ret));
  } else {
    interm_result->reset();
    ret = ir_pool_->free_interm_result(interm_result);
  }
  return ret;
}

int ObIntermResultManager::free_result(const ObIntermResultInfo& ir_info)
{
  int ret = OB_SUCCESS;
  ObIntermResult* ir = NULL;

  if (OB_ISNULL(ir_pool_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null ptr", K_(ir_pool), K(ret));
  } else if (OB_FAIL(ir_map_.erase_refactored(ir_info, &ir))) {
    LOG_WARN("erase interm result from map failed", K(ret));
  } else if (OB_ISNULL(ir)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("erase success but the interm result pointer is NULL", K(ret));
  } else {
    ir->reset();
    ret = ir_pool_->free_interm_result(ir);
    LOG_DEBUG("free interm result", "slice_id", ir_info.slice_id_);
  }
  return ret;
}

const common::hash::ObHashMap<ObIntermResultInfo, ObIntermResult*>& ObIntermResultManager::get_ir_map() const
{
  return ir_map_;
}

} /* namespace sql */
} /* namespace oceanbase */
