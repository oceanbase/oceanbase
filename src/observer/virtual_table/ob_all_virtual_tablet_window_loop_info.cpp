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
#include "ob_all_virtual_tablet_window_loop_info.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
using namespace compaction;
namespace observer
{

/*-------------------------------- ObWindowLoopInfoIterator --------------------------------*/
ObWindowLoopInfoIterator::ObWindowLoopInfoIterator()
  : allocator_(),
    queue_iter_(nullptr),
    list_iter_(nullptr),
    iter_step_(ITER_NOT_INITED),
    is_inited_(false)
{
}

int ObWindowLoopInfoIterator::init(ObWindowLoop &window_loop)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "ObWindowLoopInfoIterator has been inited, ", K(ret));
  } else if (OB_UNLIKELY(!window_loop.is_active())) {
    // TODO: more detailed error message to user
    ret = OB_ITER_END;
    SERVER_LOG(INFO, "window loop is not active, skip this tenant", K(ret), "tenant_id", MTL_ID());
  } else {
    ObWindowCompactionPriorityQueue &priority_queue = window_loop.get_score_prio_queue();
    ObWindowCompactionReadyList &ready_list = window_loop.get_ready_list();
    void *buf = nullptr;
    const int64_t alloc_size = sizeof(ObWindowCompactionPriorityQueueIterator) + sizeof(ObWindowCompactionReadyListIterator);
    if (OB_ISNULL(buf = allocator_.alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WARN, "fail to alloc memory, ", K(ret));
    } else {
      MEMSET(buf, 0, alloc_size);
      queue_iter_ = new(buf) ObWindowCompactionPriorityQueueIterator(priority_queue);
      buf = static_cast<char *>(buf) + sizeof(ObWindowCompactionPriorityQueueIterator);
      list_iter_ = new(buf) ObWindowCompactionReadyListIterator(ready_list);
      iter_step_ = ITER_QUEUE_SUMMARY;
      is_inited_ = true;
      SERVER_LOG(INFO, "Successfully init window loop info iterator");
    }
  }
  return ret;
}

void ObWindowLoopInfoIterator::reset()
{
  if (IS_INIT) {
    queue_iter_->~ObWindowCompactionPriorityQueueIterator();
    list_iter_->~ObWindowCompactionReadyListIterator();
    allocator_.reset();
    queue_iter_ = nullptr;
    list_iter_ = nullptr;
    iter_step_ = ITER_NOT_INITED;
    is_inited_ = false;
  }
}

int ObWindowLoopInfoIterator::get_next_info(ObWindowLoopInfo &info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "window loop info iterator is not inited, ", K(ret));
  } else if (ITER_QUEUE_SUMMARY == iter_step_) {
    if (OB_FAIL(queue_iter_->init())) { // lock prio queue here
      SERVER_LOG(WARN, "fail to init queue iterator, ", K(ret));
    } else if (OB_FAIL(queue_iter_->get_queue_summary(info.summary_info_))) {
      SERVER_LOG(WARN, "fail to get queue summary, ", K(ret));
    } else {
      info.is_summary_ = true;
      iter_step_ = ITER_QUEUE;
    }
  } else if (ITER_QUEUE == iter_step_) {
    if (OB_FAIL(queue_iter_->get_next(info.score_))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        queue_iter_->destroy();
        iter_step_ = ITER_LIST_SUMMARY;
        if (OB_FAIL(get_next_info(info))) {
          SERVER_LOG(WARN, "fail to get list summary info, ", K(ret));
        }
      } else {
        SERVER_LOG(WARN, "failed to get next score from queue, ", K(ret));
      }
    }
  } else if (ITER_LIST_SUMMARY == iter_step_) {
    if (OB_FAIL(list_iter_->init())) {
      SERVER_LOG(WARN, "fail to init list guard, ", K(ret));
    } else if (OB_FAIL(list_iter_->get_list_summary(info.summary_info_))) {
      SERVER_LOG(WARN, "fail to get list summary, ", K(ret));
    } else {
      info.is_summary_ = true;
      iter_step_ = ITER_LIST;
    }
  } else if (ITER_LIST == iter_step_) {
    if (OB_FAIL(list_iter_->get_next(info.score_))) {
      if (OB_ITER_END == ret) {
        list_iter_->destroy();
      } else {
        SERVER_LOG(WARN, "failed to get next score from list, ", K(ret));
      }
    }
  }
  return ret;
}


/*-------------------------------- ObAllVirtualTabletWindowLoopInfo --------------------------------*/
ObAllVirtualTabletWindowLoopInfo::ObAllVirtualTabletWindowLoopInfo()
  : ip_buf_(),
    comment_allocator_(),
    info_iter_(),
    is_inited_(false)
{
}

ObAllVirtualTabletWindowLoopInfo::~ObAllVirtualTabletWindowLoopInfo()
{
  reset();
}

int ObAllVirtualTabletWindowLoopInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    if (ret != OB_ITER_END) {
      SERVER_LOG(WARN, "fail to execute", K(ret));
    }
  }
  return ret;
}

void ObAllVirtualTabletWindowLoopInfo::reset()
{
  omt::ObMultiTenantOperator::reset();
  MEMSET(ip_buf_, 0, sizeof(ip_buf_));
  comment_allocator_.reset();
  info_iter_.reset();
  is_inited_ = false;
  ObVirtualTableScannerIterator::reset();
}

bool ObAllVirtualTabletWindowLoopInfo::is_need_process(uint64_t tenant_id)
{
  bool bret = false;
  if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
    bret = true;
  }
  return bret;
}

void ObAllVirtualTabletWindowLoopInfo::release_last_tenant()
{
  comment_allocator_.reset();
  info_iter_.reset();
  is_inited_ = false;
}

int ObAllVirtualTabletWindowLoopInfo::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  const uint64_t tenant_id = MTL_ID();

  if (IS_NOT_INIT) {
    compaction::ObWindowLoop &window_loop = MTL(compaction::ObTenantTabletScheduler *)->get_window_loop();
    if (OB_FAIL(info_iter_.init(window_loop))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to init info iterator", K(ret), K(tenant_id));
      }
    } else {
      is_inited_ = true;
    }
  }

  if (OB_SUCC(ret)) {
    ObWindowLoopInfo info;
    if (OB_FAIL(info_iter_.get_next_info(info))) {
      if (OB_ITER_END == ret) {
        SERVER_LOG(INFO, "window loop info iterator has finished, ", K(ret));
      } else {
        SERVER_LOG(WARN, "fail to get next info, ", K(ret));
      }
    } else if (OB_FAIL(fill_cells(info))) {
      SERVER_LOG(WARN, "fail to fill cells, ", K(ret));
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualTabletWindowLoopInfo::fill_cells(ObWindowLoopInfo &info)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  const uint64_t tenant_id = MTL_ID();
  ObTabletCompactionScore tmp_score;
  ObTabletCompactionScore *score = info.is_summary_ ? &tmp_score : info.score_;
  const ObTabletCompactionScoreDecisionInfo &decision_info = score->decision_info_;
  const ObTabletCompactionScoreDynamicInfo &dynamic_info = score->decision_info_.dynamic_info_;
  const ObTableModeFlag table_mode = static_cast<ObTableModeFlag>(dynamic_info.queuing_mode_);
  const char* queuing_mode = table_mode_flag_to_str(table_mode);
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
    case SVR_IP:
      if (ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
        cells[i].set_varchar(ip_buf_);
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      }
      break;
    case SVR_PORT:
      cells[i].set_int(ObServerConfig::get_instance().self_addr_.get_port());
      break;
    case TENANT_ID:
      cells[i].set_int(tenant_id);
      break;
    case LS_ID:
      cells[i].set_int(score->get_key().ls_id_.id());
      break;
    case TABLET_ID:
      cells[i].set_int(score->get_key().tablet_id_.id());
      break;
    case INC_ROW_CNT:
      cells[i].set_int(decision_info.base_inc_row_cnt_);
      break;
    case SCORE:
      cells[i].set_int(score->score_);
      break;
    case COMPACT_STATUS:
      cells[i].set_varchar(ObTabletCompactionScore::get_compact_status_str(score->compact_status_));
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case ADD_TIMESTAMP:
      cells[i].set_timestamp(score->add_timestamp_);
      break;
    case QUEUING_MODE:
      cells[i].set_varchar(queuing_mode);
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case IS_HOT:
      cells[i].set_bool(dynamic_info.is_hot_tablet_);
      break;
    case IS_INSERT_MOSTLY:
      cells[i].set_bool(dynamic_info.is_insert_mostly_);
      break;
    case IS_UPDATE_OR_DELETE_MOSTLY:
      cells[i].set_bool(dynamic_info.is_update_or_delete_mostly_);
      break;
    case HAS_ACCUMULATED_DELETE:
      cells[i].set_bool(dynamic_info.has_accumulated_delete_);
      break;
    case NEED_RECYCLE_MDS:
      cells[i].set_bool(dynamic_info.need_recycle_mds_);
      break;
    case NEED_PROGRESSIVE_MERGE:
      cells[i].set_bool(dynamic_info.need_progressive_merge_);
      break;
    case HAS_SLOW_QUERY:
      cells[i].set_bool(dynamic_info.has_slow_query_);
      break;
    case QUERY_CNT:
      cells[i].set_int(dynamic_info.query_cnt_);
      break;
    case READ_AMPLIFICATION:
      cells[i].set_double(dynamic_info.read_amplification_factor_);
      break;
    case COMMENT:
      if (info.is_summary_) {
        char *comment_str = nullptr;
        if (OB_FAIL(ob_dup_cstring(comment_allocator_, info.summary_info_.string(), comment_str))) {
          SERVER_LOG(WARN, "fail to deep copy comment", KR(ret), K(info.summary_info_.string()));
        } else {
          cells[i].set_varchar(comment_str);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
      } else {
        cells[i].set_null();
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid column id", K(ret), K(col_id));
      break;
    } // end switch
  } // end for
  return ret;
}

} // end namespace observer
} // end namespace oceanbase