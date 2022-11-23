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

#include "ob_information_session_status_table.h"

#include "lib/time/ob_time_utility.h"
#include "share/ob_time_utility2.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace observer
{
const char *const ObInfoSchemaSessionStatusTable::variables_name[] =
{
  "Threads_connected",
  "Uptime",
  NULL,
};

ObInfoSchemaSessionStatusTable::ObInfoSchemaSessionStatusTable() :
    ObVirtualTableScannerIterator(), cur_session_(NULL), global_ctx_(NULL)
{
}

ObInfoSchemaSessionStatusTable::~ObInfoSchemaSessionStatusTable()
{
  reset();
}

void ObInfoSchemaSessionStatusTable::reset()
{
  ObVirtualTableScannerIterator::reset();
  cur_session_ = NULL;
  global_ctx_ = NULL;
}

int ObInfoSchemaSessionStatusTable::fetch_all_session_status(
    AllStatus &all_status)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(global_ctx_) ||
      OB_ISNULL(global_ctx_->session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "global_ctx_ is NULL or session_mgr_ is NULL", K(ret));
  } else {
    ObObj obj;
    for (int64_t i = 0; OB_SUCC(ret) && NULL != variables_name[i]; ++i) {
      obj.reset();
      switch (i) {
      case THREADS_CONNECTED: {
          int64_t session_cnt = 0;
          if (OB_FAIL(global_ctx_->session_mgr_->get_session_count(session_cnt))) {
            SERVER_LOG(WARN, "get session count failed", K(ret));
          } else {
            obj.set_int(session_cnt);
          }
          break;
        }
      case UPTIME: {
          int64_t uptime = ObTimeUtility2::extract_second(ObTimeUtility::current_time())
              - ObTimeUtility2::extract_second(global_ctx_->start_time_);
          obj.set_int(uptime);
          break;
        }
      default: {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(all_status.set_refactored(variables_name[i], obj))) {
          SERVER_LOG(WARN, "insert to all_status failed", K(ret),
              K(variables_name[i]), K(obj));
        }
      }
    }
  }

  return ret;
}

int ObInfoSchemaSessionStatusTable::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_) || OB_ISNULL(global_ctx_) || OB_ISNULL(cur_session_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is NULL or gloabl_ctx_ or cur_session_ is NULL", K(ret),
               KP(allocator_), KP(global_ctx_), KP(cur_session_));
  } else {
    if (!start_to_read_) {
      ObObj *cells = NULL;
      const int64_t col_count = output_column_ids_.count();
      if (OB_ISNULL(cells = cur_row_.cells_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
      } else if (OB_UNLIKELY(col_count < 0 ||
                             col_count > SESSION_STATUS_COLUMN_COUNT)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "column count error ", K(ret), K(col_count));
      } else if (OB_UNLIKELY(col_count > reserved_column_cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "cells count error", K(ret), K(col_count),
                   K(reserved_column_cnt_));
      } else {
        AllStatus all_status;
        if (OB_FAIL(all_status.create(SESSION_STATUS_MAP_BUCKET_NUM,
                                      ObModIds::OB_HASH_BUCKET_SESSION_STATUS_MAP))) {
          SERVER_LOG(WARN, "fail to init all status map", K(ret));
        } else if (OB_FAIL(fetch_all_session_status(all_status))) {
          SERVER_LOG(WARN, "fail to fetch all status", K(ret));
        } else {
          ObObj casted_cell;
          const ObDataTypeCastParams dtc_params
                  = sql::ObBasicSessionInfo::create_dtc_params(session_);
          ObCastCtx cast_ctx(allocator_, &dtc_params, CM_NONE, ObCharset::get_system_collation());
          AllStatus::const_iterator it_begin = all_status.begin();
          AllStatus::const_iterator it_end = all_status.end();
          for (; OB_SUCC(ret) && it_begin != it_end; ++it_begin) {
            uint64_t cell_idx = 0;
            for (int64_t j = 0; OB_SUCC(ret) && j < col_count; ++j) {
              uint64_t col_id = output_column_ids_.at(j);
              switch (col_id) {
                case VARIABLE_NAME: {
                  cells[cell_idx].set_varchar(it_begin->first);
                  cells[cell_idx].set_collation_type(ObCharset::get_default_collation(

                                                         ObCharset::get_default_charset()));
                  break;
                }
                case VARIABLE_VALUE: {
                  casted_cell.reset();
                  const ObObj *res_cell = NULL;
                  if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx, it_begin->second,
                                                     casted_cell, res_cell))) {
                    SERVER_LOG(WARN, "failed to cast to ObVarcharType object",
                               K(ret), K(it_begin->second));
                  } else if (OB_ISNULL(res_cell)) {
                    ret = OB_ERR_UNEXPECTED;
                    SERVER_LOG(ERROR, "succ to cast to ObVarcharType, but res_cell is NULL",
                               K(ret), K(it_begin->second));
                  } else {
                    cells[cell_idx]= *res_cell;
                  }
                  break;
                }
                default: {
                  ret = OB_ERR_UNEXPECTED;
                  SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(j),
                             K(output_column_ids_), K(col_id));
                  break;
                }
              }
              if (OB_SUCC(ret)) {
                ++cell_idx;
              }
            }
            if (OB_SUCCESS == ret && OB_FAIL(scanner_.add_row(cur_row_))) {
              SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
            }
          }
          if (OB_SUCC(ret)) {
            scanner_it_ = scanner_.begin();
            start_to_read_ = true;
          }
        }
        // 无需考虑返回码，不管成功与否都需要释放内存
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = all_status.destroy())) {
          SERVER_LOG(WARN, "fail to destroy all status", K(tmp_ret));
          ret = tmp_ret;
        }
      }
    }
    if (OB_SUCCESS == ret && start_to_read_) {
      if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next row", K(ret));
        }
      } else {
        row = &cur_row_;
      }
    }
  }

  return ret;
}

} // namespace observer
} // namespace oceanbase
