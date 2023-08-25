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

#include "observer/virtual_table/ob_all_virtual_session_event.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace observer
{

ObAllVirtualSessionEvent::ObAllVirtualSessionEvent()
    : ObVirtualTableScannerIterator(),
    session_status_(),
    addr_(NULL),
    ipstr_(),
    port_(0),
    session_iter_(0),
    event_iter_(0),
    collect_(NULL)
{
}

ObAllVirtualSessionEvent::~ObAllVirtualSessionEvent()
{
  reset();
}

void ObAllVirtualSessionEvent::reset()
{
  ObVirtualTableScannerIterator::reset();
  addr_ = NULL;
  session_iter_ = 0;
  event_iter_ = 0;
  port_ = 0;
  ipstr_.reset();
  session_status_.reset();
  collect_ = NULL;
}

int ObAllVirtualSessionEvent::set_ip(common::ObAddr *addr)
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

int ObAllVirtualSessionEvent::get_all_diag_info()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ObDISessionCache::get_instance().get_all_diag_info(session_status_))) {
    SERVER_LOG(WARN, "Fail to get session status, ", K(ret));
  }
  return ret;
}

int ObAllVirtualSessionEvent::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells =  cur_row_.cells_;
  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is NULL", K(ret));
  } else {
    const int64_t col_count = output_column_ids_.count();
    if (0 == session_iter_ && 0 == event_iter_) {
      if (OB_SUCCESS != (ret = set_ip(addr_))) {
        SERVER_LOG(WARN, "can't get ip", K(ret));
      } else if (OB_SUCCESS != (ret = get_all_diag_info())) {
        SERVER_LOG(WARN, "can't get session status", K(ret));
      }
    }
    if (0 == event_iter_) {
      while (OB_SUCCESS == ret && session_iter_ < session_status_.count()) {
        collect_ = session_status_.at(session_iter_).second;
        if (NULL != collect_ && OB_SUCCESS == collect_->lock_.try_rdlock()) {
          const uint64_t tenant_id = collect_->base_value_.get_tenant_id();
          if (session_status_.at(session_iter_).first == collect_->session_id_
              && (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
            break;
          } else {
            session_iter_++;
            collect_->lock_.unlock();
          }
        } else {
          session_iter_++;
        }
      }
    }
    if (OB_SUCCESS == ret && session_iter_ >= session_status_.count()) {
      ret = OB_ITER_END;
    }

    if (OB_SUCCESS == ret && NULL == collect_) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "collect_ is null", K(ret));
    }

    if (OB_SUCCESS == ret) {
      double value = 0;
      for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < col_count; ++cell_idx) {
        uint64_t col_id = output_column_ids_.at(cell_idx);
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
          case EVENT_ID: {
            cells[cell_idx].set_int(OB_WAIT_EVENTS[event_iter_].event_id_);
            break;
          }
          case EVENT: {
            cells[cell_idx].set_varchar(OB_WAIT_EVENTS[event_iter_].event_name_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case WAIT_CLASS_ID: {
            cells[cell_idx].set_int(OB_WAIT_CLASSES[OB_WAIT_EVENTS[event_iter_].wait_class_].wait_class_id_);
            break;
          }
          case WAIT_CLASS_NO: {
            cells[cell_idx].set_int(OB_WAIT_EVENTS[event_iter_].wait_class_);
            break;
          }
          case WAIT_CLASS: {
            cells[cell_idx].set_varchar(OB_WAIT_CLASSES[OB_WAIT_EVENTS[event_iter_].wait_class_].wait_class_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case TOTAL_WAITS: {
            cells[cell_idx].set_int(collect_->base_value_.get_event_stats().get(event_iter_)->total_waits_);
            break;
          }
          case TOTAL_TIMEOUTS: {
            cells[cell_idx].set_int(collect_->base_value_.get_event_stats().get(event_iter_)->total_timeouts_);
            break;
          }
          case TIME_WAITED: {
            value = static_cast<double>(collect_->base_value_.get_event_stats().get(event_iter_)->time_waited_) / 10000;
            cells[cell_idx].set_double(value);
            break;
          }
          case MAX_WAIT: {
            value = static_cast<double>(collect_->base_value_.get_event_stats().get(event_iter_)->max_wait_) / 10000;
            cells[cell_idx].set_double(value);
            break;
          }
          case AVERAGE_WAIT: {
            if (0 != collect_->base_value_.get_event_stats().get(event_iter_)->total_waits_) {
              value = (static_cast<double>(collect_->base_value_.get_event_stats().get(event_iter_)->time_waited_)
                    / static_cast<double>(collect_->base_value_.get_event_stats().get(event_iter_)->total_waits_)) / 10000;
              cells[cell_idx].set_double(value);
            } else {
              cells[cell_idx].set_double(0);
            }
            break;
          }
          case TIME_WAITED_MICRO: {
            cells[cell_idx].set_int(collect_->base_value_.get_event_stats().get(event_iter_)->time_waited_);
            break;
          }
          default: {
            collect_->lock_.unlock();
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx),
                       K(output_column_ids_), K(col_id));
            break;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      event_iter_++;
      row = &cur_row_;
      if (event_iter_ >= WAIT_EVENTS_TOTAL) {
        event_iter_ = 0;
        session_iter_++;
        collect_->lock_.unlock();
      }
    }
  }
  return ret;
}

int ObAllVirtualSessionEventI1::get_all_diag_info()
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
