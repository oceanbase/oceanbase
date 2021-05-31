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

#include "observer/virtual_table/ob_all_virtual_lock_wait_stat.h"

#include "storage/memtable/ob_lock_wait_mgr.h"
#include "observer/ob_server_utils.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::rpc;
using namespace oceanbase::common;
using namespace oceanbase::memtable;
using namespace oceanbase::storage;
namespace oceanbase {
namespace observer {

void ObAllVirtualLockWaitStat::reset()
{
  ObVirtualTableScannerIterator::reset();
  node_iter_ = NULL;
}

int ObAllVirtualLockWaitStat::inner_open()
{
  int ret = OB_SUCCESS;
  node_iter_ = NULL;
  return ret;
}

int ObAllVirtualLockWaitStat::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (!start_to_read_ && OB_FAIL(make_this_ready_to_read())) {
    SERVER_LOG(WARN, "fail to make_this_ready_to_read", K(ret), K(start_to_read_));
  } else {
    if (NULL == (node_iter_ = memtable::get_global_lock_wait_mgr().next(node_iter_, &cur_node_))) {
      ret = OB_ITER_END;
    } else {
      const int64_t col_count = output_column_ids_.count();
      ObString ipstr;
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
        uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
          // svr_ip
          case SVR_IP: {
            ipstr.reset();
            if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
              SERVER_LOG(ERROR, "get server ip failed", K(ret));
            } else {
              cur_row_.cells_[i].set_varchar(ipstr);
              cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
          // svr_port
          case SVR_PORT: {
            cur_row_.cells_[i].set_int(GCTX.self_addr_.get_port());
            break;
          }
          case TABLE_ID:
            cur_row_.cells_[i].set_int(node_iter_->table_id_);
            break;
          case ROWKEY: {
            ObString rowkey;
            if (OB_FAIL(ob_write_string(*allocator_, node_iter_->key_, rowkey))) {
              SERVER_LOG(WARN, "fail to deep copy rowkey", K(*node_iter_), K(ret));
            } else {
              cur_row_.cells_[i].set_varchar(rowkey);
              cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
          case ADDR:
            cur_row_.cells_[i].set_uint64((uint64_t)node_iter_->addr_);
            break;
          case NEED_WAIT:
            cur_row_.cells_[i].set_bool(node_iter_->need_wait_);
            break;
          case RECV_TS:
            cur_row_.cells_[i].set_int(node_iter_->recv_ts_);
            break;
          case LOCK_TS:
            cur_row_.cells_[i].set_int(node_iter_->lock_ts_);
            break;
          case ABS_TIMEOUT:
            cur_row_.cells_[i].set_int(node_iter_->abs_timeout_);
            break;
          case TRY_LOCK_TIMES:
            cur_row_.cells_[i].set_int(node_iter_->try_lock_times_);
            break;
          case TIME_AFTER_RECV: {
            int64_t cur_ts = ObTimeUtility::current_time();
            cur_row_.cells_[i].set_int(cur_ts - node_iter_->recv_ts_);
            break;
          }
          case SESSION_ID:
            cur_row_.cells_[i].set_int(node_iter_->sessid_);
            break;
          case BLOCK_SESSION_ID:
            cur_row_.cells_[i].set_int(node_iter_->block_sessid_);
            break;
          case TYPE:
            cur_row_.cells_[i].set_int(0);
            break;
          case LMODE:
            cur_row_.cells_[i].set_int(0);
            break;
          case TOTAL_UPDATE_CNT:
            cur_row_.cells_[i].set_int(node_iter_->total_update_cnt_);
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid col_id", K(ret), K(col_id));
            break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualLockWaitStat::make_this_ready_to_read()
{
  int ret = OB_SUCCESS;
  ObObj* cells = NULL;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "invalid allocator is NULL", K(allocator_), K(ret));
  } else if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else {
    start_to_read_ = true;
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
