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

#include "observer/virtual_table/ob_all_virtual_session_wait.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace observer
{

ObAllVirtualSessionWait::ObAllVirtualSessionWait()
    : ObVirtualTableScannerIterator(),
    session_status_(),
    addr_(NULL),
    ipstr_(),
    port_(0),
    session_iter_(0),
    collect_(NULL)
{
}

ObAllVirtualSessionWait::~ObAllVirtualSessionWait()
{
  reset();
}

void ObAllVirtualSessionWait::reset()
{
  ObVirtualTableScannerIterator::reset();
  addr_ = NULL;
  session_iter_ = 0;
  port_ = 0;
  ipstr_.reset();
  session_status_.reset();
  collect_ = NULL;
}

int ObAllVirtualSessionWait::set_ip(common::ObAddr *addr)
{
  int ret = OB_SUCCESS;
  char ipbuf[common::OB_IP_STR_BUFF];
  if (NULL == addr){
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!addr_->ip_to_string(ipbuf, sizeof(ipbuf))) {
    SERVER_LOG(ERROR, "ip to string failed");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ipstr_ = ObString::make_string(ipbuf);
    if (OB_FAIL(ob_write_string(*allocator_, ipstr_, ipstr_))) {
      SERVER_LOG(WARN, "failed to write string", K(ret));
    }
    port_ = addr_->get_port();
  }
  return ret;
}

int ObAllVirtualSessionWait::get_all_diag_info()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ObDISessionCache::get_instance().get_all_diag_info(session_status_))) {
    SERVER_LOG(WARN, "Fail to get session status, ", K(ret));
  }
  return ret;
}

int ObAllVirtualSessionWait::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is NULL", K(ret));
  } else {
    const int64_t col_count = output_column_ids_.count();
    ObWaitEventDesc *event_desc = NULL;
    if (0 == session_iter_) {
      if (OB_SUCCESS != (ret = set_ip(addr_))){
        SERVER_LOG(WARN, "can't get ip", K(ret));
      } else if (OB_SUCCESS != (ret = get_all_diag_info())) {
        SERVER_LOG(WARN, "can't get session status", K(ret));
      }
    }
    while (OB_SUCCESS == ret && session_iter_ < session_status_.count()) {
      collect_ = session_status_.at(session_iter_).second;
      if (NULL != collect_ && OB_SUCCESS == collect_->lock_.try_rdlock()) {
        const uint64_t tenant_id = collect_->base_value_.get_tenant_id();
        if (session_status_.at(session_iter_).first == collect_->session_id_
            && (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
          if (OB_FAIL(collect_->base_value_.get_event_history().get_last_wait(event_desc))) {
            if (OB_ITEM_NOT_SETTED == ret) {
              ret = OB_SUCCESS;
              session_iter_++;
              collect_->lock_.unlock();
            } else {
              collect_->lock_.unlock();
              SERVER_LOG(WARN, "Unexpected err", K(ret));
            }
          } else if (NULL == event_desc) {
            ret = OB_ERR_UNEXPECTED;
            collect_->lock_.unlock();
            SERVER_LOG(WARN, "Unexpected err", K(ret));
          } else {
            break;
          }
        } else {
          session_iter_++;
          collect_->lock_.unlock();
        }
      } else {
        session_iter_++;
      }
    }
    if (OB_SUCCESS == ret && session_iter_ >= session_status_.count()) {
      ret = OB_ITER_END;
    }
    if (OB_SUCCESS == ret && session_status_.count() != 0) {
      if (OB_ISNULL(event_desc) || OB_ISNULL(collect_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "event_desc or collect_ is NULL", K(ret), KP(event_desc), KP(collect_));
      }
      uint64_t cell_idx = 0;
      int64_t curr_time = ObTimeUtility::current_time();
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
        uint64_t col_id = output_column_ids_.at(i);
        switch(col_id) {
          case SESSION_ID: {
            cells[cell_idx].set_int(collect_->session_id_);
            break;
          }
          case TENANT_ID: {
            cells[cell_idx].set_int(collect_->base_value_.get_tenant_id());
            break;
          }
          case SVR_IP: {
            cells[cell_idx].set_varchar(ipstr_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SVR_PORT: {
            cells[cell_idx].set_int(port_);
            break;
          }
          case EVENT: {
            cells[cell_idx].set_varchar(OB_WAIT_EVENTS[event_desc->event_no_].event_name_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case P1TEXT: {
            cells[cell_idx].set_varchar(OB_WAIT_EVENTS[event_desc->event_no_].param1_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case P1: {
            cells[cell_idx].set_uint64(event_desc->p1_);
            break;
          }
          case P2TEXT: {
            cells[cell_idx].set_varchar(OB_WAIT_EVENTS[event_desc->event_no_].param2_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case P2: {
            cells[cell_idx].set_uint64(event_desc->p2_);
            break;
          }
          case P3TEXT: {
            cells[cell_idx].set_varchar(OB_WAIT_EVENTS[event_desc->event_no_].param3_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case P3: {
            cells[cell_idx].set_uint64(event_desc->p3_);
            break;
          }
          case LEVEL: {
            cells[cell_idx].set_int(event_desc->level_);
            break;
          }
          case WAIT_CLASS_ID: {
            cells[cell_idx].set_int(OB_WAIT_CLASSES[OB_WAIT_EVENTS[event_desc->event_no_].wait_class_].wait_class_id_);
            break;
          }
          case WAIT_CLASS_NO: {
            cells[cell_idx].set_int(OB_WAIT_EVENTS[event_desc->event_no_].wait_class_);
            break;
          }
          case WAIT_CLASS: {
            cells[cell_idx].set_varchar(OB_WAIT_CLASSES[OB_WAIT_EVENTS[event_desc->event_no_].wait_class_].wait_class_);
            cells[cell_idx].set_default_collation_type();
            break;
          }
          case STATE: {
            if (event_desc->wait_end_time_ == 0) {
              cells[cell_idx].set_varchar("WAITING");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            } else if (event_desc->wait_time_ < 10000) {
              cells[cell_idx].set_varchar("WAITED SHORT TIME");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            } else if (event_desc->wait_time_ >= 10000) {
              cells[cell_idx].set_varchar("WAITED KNOWN TIME");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            } else {
              cells[cell_idx].set_varchar("");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
          case WAIT_TIME_MICRO: {
            if (event_desc->wait_end_time_ == 0) {
              cells[cell_idx].set_int(curr_time - event_desc->wait_begin_time_);
            } else {
              cells[cell_idx].set_int(event_desc->wait_time_);
            }
            break;
          }
          case TIME_REMAINING_MICRO: {
            if (event_desc->timeout_ms_ > 0) {
              if (event_desc->wait_end_time_ == 0) {
                cells[cell_idx].set_int((event_desc->timeout_ms_) * 1000 - (curr_time - event_desc->wait_begin_time_));
              } else {
                if (event_desc->wait_time_ / 1000 >= static_cast<int64_t>(event_desc->timeout_ms_)) {
                  cells[cell_idx].set_int(0);
                } else {
                  cells[cell_idx].set_null();
                }
              }
            } else {
              cells[cell_idx].set_int(-1);
            }
            break;
          }
          case TIME_SINCE_LAST_WAIT_MICRO: {
            if (event_desc->wait_end_time_ != 0) {
              cells[cell_idx].set_int(curr_time - event_desc->wait_end_time_);
            } else {
              cells[cell_idx].set_int(0);
            }
            break;
          }
          default: {
            if (nullptr != collect_) {
              collect_->lock_.unlock();
            }
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx),
                       K(output_column_ids_), K(col_id));
            break;
          }
        }
        if (OB_SUCC(ret)) {
          cell_idx++;
        }
      }
    }

    if (OB_SUCC(ret)) {
      session_iter_++;
      row = &cur_row_;
      collect_->lock_.unlock();
    }
  }
  return ret;
}


int ObAllVirtualSessionWaitI1::get_all_diag_info()
{
  int ret = OB_SUCCESS;
  int64_t index_id = -1;
  uint64_t key = 0;
  std::pair<uint64_t, common::ObDISessionCollect*> pair;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_index_ids().count(); ++i) {
    index_id = get_index_ids().at(i);
    if (0 < index_id) {
      key = static_cast<uint64_t>(index_id);
      pair.first = key;
      if (OB_SUCCESS != (ret = ObDISessionCache::get_instance().get_the_diag_info(key, pair.second))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          SERVER_LOG(WARN, "Fail to get session status, ", K(ret));
        }
      } else {
        if (OB_SUCCESS != (ret = session_status_.push_back(pair))) {
          SERVER_LOG(WARN, "Fail to push diag info value to array, ", K(ret));
        }
      }
    }
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
