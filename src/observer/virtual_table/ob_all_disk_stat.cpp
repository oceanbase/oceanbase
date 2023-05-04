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

#include "observer/virtual_table/ob_all_disk_stat.h"

#include "storage/blocksstable/ob_block_manager.h"
#include "observer/ob_server.h"
#include "storage/slog/ob_storage_logger_manager.h"

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;

namespace oceanbase
{
namespace observer
{
using namespace storage;
ObInfoSchemaDiskStatTable::ObInfoSchemaDiskStatTable()
    : ObVirtualTableScannerIterator(),
    addr_(NULL),
    ipstr_(),
    port_(0),
    is_end_(false)
{
}

ObInfoSchemaDiskStatTable::~ObInfoSchemaDiskStatTable()
{
  reset();
}

void ObInfoSchemaDiskStatTable::reset()
{
  ObVirtualTableScannerIterator::reset();
  addr_ = NULL;
  port_ = 0;
  ipstr_.reset();
  is_end_ = false;
}

int ObInfoSchemaDiskStatTable::set_ip(common::ObAddr *addr)
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

int ObInfoSchemaDiskStatTable::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  if (OB_ISNULL(allocator_) || OB_ISNULL(cells)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is NULL", KP(allocator_), KP(cells), K(ret));
  } else {
    const int64_t col_count = output_column_ids_.count();

    if (OB_SUCCESS != (ret = set_ip(addr_))){
      SERVER_LOG(WARN, "can't get ip", K(ret));
    } else if (is_end_) {
      ret = OB_ITER_END;
    }

    int temp_ret = OB_SUCCESS;
    ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
    int64_t data_disk_abnormal_time = 0;
    if (OB_SUCCESS != (temp_ret = ObIOManager::get_instance().get_device_health_status(dhs,
        data_disk_abnormal_time))) {
      SERVER_LOG(WARN, "get device health status failed", KR(temp_ret));
    }

    if (OB_SUCC(ret)) {
      for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < col_count; ++cell_idx) {
        uint64_t col_id = output_column_ids_.at(cell_idx);
        switch(col_id) {
          case SVR_IP: {
            cells[cell_idx].set_varchar(ipstr_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SVR_PORT: {
            cells[cell_idx].set_int(port_);
            break;
          }
          case TOTAL_SIZE:{
            int64_t reserved_size = 4 * 1024 * 1024 * 1024L; // default RESERVED_DISK_SIZE -> 4G
            if (OB_FAIL(SLOGGERMGR.get_reserved_size(reserved_size))) {
              SERVER_LOG(WARN, "fail to get reserved size", K(ret));
            } else {
              cells[cell_idx].set_int(OB_SERVER_BLOCK_MGR.get_max_macro_block_count(reserved_size) * OB_SERVER_BLOCK_MGR.get_macro_block_size());
            }
            break;
          }
          case ALLOCATED_SIZE: {
            cells[cell_idx].set_int(OB_SERVER_BLOCK_MGR.get_total_macro_block_count() * OB_SERVER_BLOCK_MGR.get_macro_block_size());
            break;
          }
          case USED_SIZE: {
            cells[cell_idx].set_int(OB_SERVER_BLOCK_MGR.get_used_macro_block_count() * OB_SERVER_BLOCK_MGR.get_macro_block_size());
            break;
          }
          case FREE_SIZE: {
            cells[cell_idx].set_int(OB_SERVER_BLOCK_MGR.get_free_macro_block_count() * OB_SERVER_BLOCK_MGR.get_macro_block_size());
            break;
          }
          case IS_DISK_VALID: {
            cells[cell_idx].set_int(DEVICE_HEALTH_NORMAL != dhs ? 0 : 1);
            break;
          }
          case DISK_ERROR_BEGIN_TS: {
            cells[cell_idx].set_int(DEVICE_HEALTH_NORMAL != dhs ? data_disk_abnormal_time : 0);
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx),
                       K(output_column_ids_), K(col_id));
            break;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      row = &cur_row_;
      is_end_ = true;
    }
  }
  return ret;
}
}/* ns observer*/
}/* ns oceanbase */
