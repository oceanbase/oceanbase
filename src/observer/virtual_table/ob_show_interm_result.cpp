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

#define USING_LOG_PREFIX SERVER
#include "observer/virtual_table/ob_show_interm_result.h"
#include "lib/string/ob_string.h"
#include "observer/ob_server.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/executor/ob_interm_result_manager.h"

namespace oceanbase {
using namespace common;
using namespace sql;
namespace observer {

ObShowIntermResult::ObShowIntermResult() : ObVirtualTableScannerIterator()
{}

ObShowIntermResult::~ObShowIntermResult()
{
  reset();
}

void ObShowIntermResult::reset()
{
  // ir_map_ = NULL;
  ObVirtualTableScannerIterator::reset();
}

int ObShowIntermResult::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObObj* cells = NULL;
  char ip_buf[common::OB_IP_STR_BUFF];
  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("allocator is NULL", K(ret));
  } else {
    if (!start_to_read_) {
      ObIntermResultManager* ir_manager = ObIntermResultManager::get_instance();
      if (OB_ISNULL(cells = cur_row_.cells_) || OB_ISNULL(ir_manager)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("some variable is NULL", K(ret), K(cur_row_.cells_), K(ir_manager));
      } else if (OB_UNLIKELY(cur_row_.count_ < output_column_ids_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN(
            "cells count is less than output column count", K(ret), K(cur_row_.count_), K(output_column_ids_.count()));
      } else {
        const common::hash::ObHashMap<ObIntermResultInfo, ObIntermResult*>& ir_map = ir_manager->get_ir_map();
        common::hash::ObHashMap<ObIntermResultInfo, ObIntermResult*>::const_iterator iter = ir_map.begin();
        for (; OB_SUCC(ret) && iter != ir_map.end(); ++iter) {
          ObServer& server = ObServer::get_instance();
          uint64_t cell_idx = 0;
          const ObIntermResultInfo& oiri = iter->first;
          ObIntermResult* oir = iter->second;
          ObIntermResultIterator oir_iter;
          if (OB_FAIL(ir_manager->get_result(oiri, oir_iter))) {
            LOG_WARN("fail to get result", K(ret));
          } else {
            oir = oir_iter.get_interm_result();
            const ObSliceID& osi = oiri.slice_id_;
            uint64_t slice_id = osi.get_slice_id();
            uint64_t task_id = osi.get_task_id();
            uint64_t job_id = osi.get_job_id();
            uint64_t execution_id = osi.get_execution_id();
            const common::ObAddr& oa = osi.get_server();
            char ip_s[OB_MAX_SERVER_ADDR_SIZE] = "";
            if (OB_UNLIKELY(NULL == oir)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("ObInterResultInfo ptr is NULl", K(ret));
            } else if (!oa.ip_to_string(ip_s, OB_MAX_SERVER_ADDR_SIZE) ||
                       !server.get_self().ip_to_string(ip_buf, common::OB_IP_STR_BUFF)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("ip convert to string wrong", K(ret), K(ip_s));
            } else {
              int32_t port = oa.get_port();
              int64_t expire_time = oir->get_expire_time();
              int64_t scanner_count = oir->get_scanner_count();
              int64_t row_count = 0;
              int64_t used_memory_size = 0;
              int64_t used_disk_size = 0;
              if (OB_FAIL(oir->get_all_row_count(row_count))) {
                LOG_WARN("fail to get all row count", K(ret));
              } else if (OB_FAIL(oir->get_data_size_detail(used_memory_size, used_disk_size))) {
                LOG_WARN("fail to get all used memory size", K(ret));
              } else { /*do nothing*/
              }

              for (int64_t j = 0; OB_SUCC(ret) && j < output_column_ids_.count(); ++j) {
                uint64_t col_id = output_column_ids_.at(j);
                switch (col_id) {
                  case share::TENANT_VIRTUAL_INTERM_RESULT_CDE::JOB_ID: {
                    cells[cell_idx].set_int(job_id);
                    break;
                  }
                  case share::TENANT_VIRTUAL_INTERM_RESULT_CDE::TASK_ID: {
                    cells[cell_idx].set_int(task_id);
                    break;
                  }
                  case share::TENANT_VIRTUAL_INTERM_RESULT_CDE::SLICE_ID: {
                    cells[cell_idx].set_int(slice_id);
                    break;
                  }
                  case share::TENANT_VIRTUAL_INTERM_RESULT_CDE::EXECUTION_ID: {
                    cells[cell_idx].set_int(execution_id);
                    break;
                  }
                  case share::TENANT_VIRTUAL_INTERM_RESULT_CDE::SVR_IP: {
                    cells[cell_idx].set_varchar(ObString::make_string(ip_s));
                    cells[cell_idx].set_collation_type(
                        ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    break;
                  }
                  case share::TENANT_VIRTUAL_INTERM_RESULT_CDE::SVR_PORT: {
                    cells[cell_idx].set_int(port);
                    break;
                  }
                  case share::TENANT_VIRTUAL_INTERM_RESULT_CDE::EXPIRE_TIME: {
                    cells[cell_idx].set_int(expire_time);
                    break;
                  }
                  case share::TENANT_VIRTUAL_INTERM_RESULT_CDE::ROW_COUNT: {
                    cells[cell_idx].set_int(row_count);
                    break;
                  }
                  case share::TENANT_VIRTUAL_INTERM_RESULT_CDE::SCANNER_COUNT: {
                    cells[cell_idx].set_int(scanner_count);
                    break;
                  }
                  case share::TENANT_VIRTUAL_INTERM_RESULT_CDE::USED_MEMORY_SIZE: {
                    cells[cell_idx].set_int(used_memory_size);
                    break;
                  }
                  case share::TENANT_VIRTUAL_INTERM_RESULT_CDE::USED_DISK_SIZE: {
                    cells[cell_idx].set_int(used_disk_size);
                    break;
                  }
                  case share::TENANT_VIRTUAL_INTERM_RESULT_CDE::PARTITION_IP: {
                    cells[cell_idx].set_varchar(ObString::make_string(ip_buf));
                    cells[cell_idx].set_collation_type(
                        ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    break;
                  }
                  case share::TENANT_VIRTUAL_INTERM_RESULT_CDE::PARTITION_PORT: {
                    cells[cell_idx].set_int(server.get_self().get_port());
                    break;
                  }
                  default: {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("invalid column id", K(ret), K(cell_idx), K(j), K(output_column_ids_), K(col_id));
                    break;
                  }
                }
                if (OB_SUCC(ret)) {
                  cell_idx++;
                }
              }
              if (OB_UNLIKELY(OB_SUCCESS == ret && OB_SUCCESS != (ret = scanner_.add_row(cur_row_)))) {
                LOG_WARN("fail to add row", K(ret), K(cur_row_));
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          scanner_it_ = scanner_.begin();
          start_to_read_ = true;
        }
      }
    }
    if (OB_LIKELY(OB_SUCCESS == ret && start_to_read_)) {
      if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row", K(ret));
        }
      } else {
        row = &cur_row_;
      }
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
