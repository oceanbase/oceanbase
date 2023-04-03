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

#include "ob_all_virtual_io_stat.h"

#include "storage/blocksstable/ob_block_manager.h"
#include "observer/ob_server.h"
#include "share/io/ob_io_manager.h"

namespace oceanbase
{
using namespace common;
namespace observer
{

ObAllVirtualIOStat::ObAllVirtualIOStat()
{
}

ObAllVirtualIOStat::~ObAllVirtualIOStat()
{
}

int ObAllVirtualIOStat::inner_get_next_row(ObNewRow *&row)
{
  row = NULL;
  return OB_ITER_END;
//  int ret = OB_SUCCESS;
//  ObObj *cells = cur_row_.cells_;
//  if (!start_to_read_) {
//    start_to_read_ = true;
//  } else {
//    ret = OB_ITER_END;
//  }
//  for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
//    const uint64_t column_id = output_column_ids_.at(i);
//    const ObIOManager &io_manager = ObIOManager::get_instance();
//    const ObIOConfig &io_conf = io_manager.get_io_conf();
//    const int64_t MACRO_BLOCK_SIZE_IN_MB = OBSERVER.get_partition_service().get_base_storage()->get_data_file().get_macro_block_size() / 1024 / 1024;
//    switch(column_id) {
//      case SVR_IP: {
//        if (!OBSERVER.get_self().ip_to_string(svr_ip_, sizeof(svr_ip_))) {
//          ret = OB_BUF_NOT_ENOUGH;
//          SERVER_LOG(WARN, "svr ip buffer is not enough", K(ret));
//        } else {
//          cells[i].set_varchar(svr_ip_);
//          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
//        }
//        break;
//      }
//      case SVR_PORT: {
//        cells[i].set_int(OBSERVER.get_self().get_port());
//        break;
//      }
//      case DISK_TYPE: {
//        if (OB_FAIL(ObIOBenchmark::get_instance().get_disk_type_str(disk_type_, OB_MAX_DISK_TYPE_LENGTH))) {
//          SERVER_LOG(WARN, "failed to get disk type str", K(ret));
//        } else {
//          cells[i].set_varchar(disk_type_);
//        }
//        break;
//      }
//      case SYS_IO_UP_LIMIT_IN_MB: {
//        cells[i].set_int(io_manager.get_sys_iops_up_limit() * MACRO_BLOCK_SIZE_IN_MB);
//        break;
//      }
//      case SYS_IO_BAND_IN_MB: {
//        const int64_t sys_iops = (io_manager.get_sys_iops_up_limit() * io_manager.get_sys_io_percent() / 100 + 1);
//        cells[i].set_int(sys_iops * MACRO_BLOCK_SIZE_IN_MB);
//        break;
//      }
//      case SYS_IO_LOW_WATERMARK_IN_MB: {
//        const int64_t sys_iops_low = io_manager.get_sys_io_low_percent(io_conf) * io_manager.get_sys_iops_up_limit() / 100 + 1;
//        cells[i].set_int(sys_iops_low * MACRO_BLOCK_SIZE_IN_MB);
//        break;
//      }
//      case SYS_IO_HIGH_WATERMARK_IN_MB: {
//        const int64_t sys_iops_high = io_conf.sys_io_high_percent_ * io_manager.get_sys_iops_up_limit() / 100 + 1;
//        cells[i].set_int(sys_iops_high * MACRO_BLOCK_SIZE_IN_MB);
//        break;
//      }
//      case IO_BENCH_RESULT: {
//        if (OB_FAIL(ObIOBenchmark::get_instance().get_io_bench_result_str(io_bench_result_, OB_MAX_IO_BENCH_RESULT_LENGTH))) {
//          SERVER_LOG(WARN, "failed to get io bench result", K(ret));
//        } else {
//          cells[i].set_varchar(io_bench_result_);
//        }
//        break;
//      }
//      default: {
//        ret = OB_ERR_UNEXPECTED;
//        SERVER_LOG(WARN, "unkown column", K(column_id));
//        break;
//      }
//    }
//  }
//  if (OB_SUCC(ret)) {
//    row = &cur_row_;
//  }
//  return ret;
}

}
}
