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

#include "observer/virtual_table/ob_all_virtual_px_p2p_datahub.h"
#include "observer/ob_server_utils.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
namespace observer
{

static const char *msg_type_str[] = {
    "NOT_INIT",
    "BLOOM_FILTER_MSG",
    "RANGE_FILTER_MSG",
    "IN_FILTER_MSG",
    "MAX_TYPE"
};

int ObAllPxP2PDatahubTable::P2PMsgTraverseCall::operator() (
    common::hash::HashMapPair<ObP2PDhKey,
    ObP2PDatahubMsgBase *> &entry)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(entry.second)) {
    if (!is_sys_tenant(tenant_id_) &&
        entry.second->get_tenant_id() != tenant_id_) {
      /*do nothing*/
    } else {
      P2PDatahubNode node;
      node.p2p_datahub_id_ = entry.second->get_p2p_datahub_id();
      node.msg_type_ = (int64_t)entry.second->get_msg_type();
      node.start_time_ = entry.second->get_start_time();
      node.tenant_id_ = entry.second->get_tenant_id();
      node.trace_id_ = entry.second->get_trace_id();
      node.timeout_ts_ = entry.second->get_timeout_ts();
      if (OB_FAIL(node_array_.push_back(node))) {
        SERVER_LOG(WARN, "fail to push back node", K(ret));
      }
    }
  }
  return ret;
}


ObAllPxP2PDatahubTable::ObAllPxP2PDatahubTable():addr_(NULL), start_to_read_(false),
    node_array_(), index_(0)
{
}
ObAllPxP2PDatahubTable::~ObAllPxP2PDatahubTable()
{
}

void ObAllPxP2PDatahubTable::reset()
{
  addr_ = NULL;
  start_to_read_ = false;
  node_array_.reset();
  index_ = 0;
}

int ObAllPxP2PDatahubTable::p2p_datahub_map_to_array()
{
  int ret = OB_SUCCESS;
  P2PMsgTraverseCall call(node_array_, effective_tenant_id_);
  if (OB_FAIL(PX_P2P_DH.get_map().foreach_refactored(call))) {
    SERVER_LOG(WARN,  "fail to convert map to array", K(ret));
  }
  return ret;
}

int ObAllPxP2PDatahubTable::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  ObString ipstr;
  if (OB_ISNULL(allocator_) || OB_ISNULL(addr_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ or addr_ is null", K_(allocator), K_(addr), K(ret));
  } else if (OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
  } else if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
    SERVER_LOG(ERROR, "get server ip failed", K(ret));
  } else {
    if (!start_to_read_) {
      if (OB_FAIL(p2p_datahub_map_to_array())) {
         SERVER_LOG(WARN, "fail to convert map to array", K(ret));
      }
    }
    if (index_ >= node_array_.size()) {
      ret = OB_ITER_END;
    }
    for (int64_t cell_idx = 0;
        OB_SUCC(ret) && cell_idx < output_column_ids_.count();
        ++cell_idx) {
      const uint64_t column_id = output_column_ids_.at(cell_idx);
      switch(column_id) {
        case SVR_IP: {
          cells[cell_idx].set_varchar(ipstr);
          cells[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT: {
          cells[cell_idx].set_int(addr_->get_port());
          break;
        }
        case TRACE_ID: {
          int len = node_array_.at(index_).trace_id_.to_string(trace_id_, sizeof(trace_id_));
          cells[cell_idx].set_varchar(trace_id_, len);
          cells[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case DATAHUB_ID: {
         int64_t p2p_datahub_id = node_array_.at(index_).p2p_datahub_id_;
         cells[cell_idx].set_int(p2p_datahub_id);
         break;
        }
        case TENANT_ID: {
          int64_t tenant_id = node_array_.at(index_).tenant_id_;
          cells[cell_idx].set_int(tenant_id);
          break;
        }
        case MESSAGE_TYPE: {
          int64_t msg_idx = node_array_.at(index_).msg_type_;
          int64_t str_cnt = sizeof(msg_type_str) / sizeof(msg_type_str[0]);
          if (msg_idx >= str_cnt || msg_idx < 0) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "unexpected msg type", K(ret), K(msg_idx), K(str_cnt), K(node_array_.at(index_).msg_type_));
          } else {
            const char *msg_str = msg_type_str[msg_idx];
            cells[cell_idx].set_varchar(msg_str, strlen(msg_str));
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }

          break;
        }
        case HOLD_SIZE: {
          int64_t hold_size = node_array_.at(index_).hold_size_;
          cells[cell_idx].set_int(hold_size);
          break;
        }
        case TIMEOUT_TS: {
          int64_t timeout_ts = node_array_.at(index_).timeout_ts_;
          cells[cell_idx].set_timestamp(timeout_ts);
          break;
        }
        case START_TIME: {
          int64_t start_time = node_array_.at(index_).start_time_;
          cells[cell_idx].set_timestamp(start_time);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(cell_idx),
              K_(output_column_ids), K(ret));
          break;
        }
      }
    }
    ++index_;
  }
  if (OB_SUCC(ret)) {
    start_to_read_ = true;
    row = &cur_row_;
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
