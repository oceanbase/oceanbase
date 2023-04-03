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

#include "ob_all_virtual_ls_archive_stat.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "logservice/archiveservice/ob_archive_service.h"
#include "logservice/archiveservice/ob_ls_task.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace observer
{
int ObAllVirtualLSArchiveStat::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (false == start_to_read_) {
    auto func_iter_ls = [&](const archive::ObLSArchiveTask &task) -> int
    {
      int ret = OB_SUCCESS;
      archive::LSArchiveStat ls_stat;
      if (FALSE_IT(task.stat(ls_stat))) {
        SERVER_LOG(WARN, "ls_archive_task stat failed", K(ret), K(task));
      } else if (OB_FAIL(insert_stat_(ls_stat))) {
        SERVER_LOG(WARN, "insert stat failed", K(ret));
      } else {
        SERVER_LOG(INFO, "iter ls archive stat succ", K(ls_stat));
        scanner_.add_row(cur_row_);
      }
      return ret;
    };
    auto func_iterate_tenant = [&func_iter_ls]() -> int
    {
      int ret = OB_SUCCESS;
      archive::ObArchiveService *archive_service = MTL(archive::ObArchiveService*);
      if (NULL == archive_service) {
        SERVER_LOG(INFO, "tenant has no ObArchiveService", K(MTL_ID()));
      } else if (OB_FAIL(archive_service->iterate_ls(func_iter_ls))) {
        SERVER_LOG(WARN, "iter ls failed", K(ret));
      } else {
        SERVER_LOG(INFO, "iter ls succ", K(ret));
      }
      return ret;
    };
    if (OB_FAIL(omt_->operate_each_tenant_for_sys_or_self(func_iterate_tenant))) {
      SERVER_LOG(WARN, "iter tenant failed", K(ret));
    } else {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  if (OB_SUCC(ret) && start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "get next row failed", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualLSArchiveStat::insert_stat_(archive::LSArchiveStat &ls_stat)
{
  int ret = OB_SUCCESS;
  const int64_t count = output_column_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case OB_APP_MIN_COLUMN_ID:
        cur_row_.cells_[i].set_int(static_cast<int64_t>(ls_stat.tenant_id_));
        break;
      case OB_APP_MIN_COLUMN_ID + 1:
        cur_row_.cells_[i].set_int(ls_stat.ls_id_);
        break;
      case OB_APP_MIN_COLUMN_ID + 2:
        if (false == GCTX.self_addr().ip_to_string(ip_, common::OB_IP_PORT_STR_BUFF)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "ip_to_string failed", K(ret));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(ip_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      case OB_APP_MIN_COLUMN_ID + 3:
        cur_row_.cells_[i].set_int(GCTX.self_addr().get_port());
        break;
      case OB_APP_MIN_COLUMN_ID + 4:
        cur_row_.cells_[i].set_int(ls_stat.dest_id_);
        break;
      case OB_APP_MIN_COLUMN_ID + 5:
        cur_row_.cells_[i].set_int(ls_stat.incarnation_);
        break;
      case OB_APP_MIN_COLUMN_ID + 6:
        cur_row_.cells_[i].set_int(ls_stat.round_);
        break;
      case OB_APP_MIN_COLUMN_ID + 7:
        cur_row_.cells_[i].set_varchar(ObString("Location"));
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case OB_APP_MIN_COLUMN_ID + 8:
        cur_row_.cells_[i].set_varchar(ObString("DefaultDestLocation"));
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case OB_APP_MIN_COLUMN_ID + 9:
        cur_row_.cells_[i].set_int(ls_stat.lease_id_);
        break;
      case OB_APP_MIN_COLUMN_ID + 10:
        cur_row_.cells_[i].set_uint64(ls_stat.round_start_scn_.get_val_for_inner_table_field());
        break;
      case OB_APP_MIN_COLUMN_ID + 11:
        cur_row_.cells_[i].set_uint64(ls_stat.max_issued_log_lsn_ < 0 ? 0 : ls_stat.max_issued_log_lsn_);
        break;
      case OB_APP_MIN_COLUMN_ID + 12:
        cur_row_.cells_[i].set_int(ls_stat.issued_task_count_);
        break;
      case OB_APP_MIN_COLUMN_ID + 13:
        cur_row_.cells_[i].set_int(ls_stat.issued_task_size_);
        break;
      case OB_APP_MIN_COLUMN_ID + 14:
        cur_row_.cells_[i].set_int(ls_stat.max_prepared_piece_id_);
        break;
      case OB_APP_MIN_COLUMN_ID + 15:
        cur_row_.cells_[i].set_uint64(ls_stat.max_prepared_lsn_ < 0 ? 0 : ls_stat.max_prepared_lsn_);
        break;
      case OB_APP_MIN_COLUMN_ID + 16:
        cur_row_.cells_[i].set_uint64(ls_stat.max_prepared_scn_.get_val_for_inner_table_field());
        break;
      case OB_APP_MIN_COLUMN_ID + 17:
        cur_row_.cells_[i].set_int(ls_stat.wait_send_task_count_);
        break;
      case OB_APP_MIN_COLUMN_ID + 18:
        cur_row_.cells_[i].set_int(ls_stat.archive_piece_id_);
        break;
      case OB_APP_MIN_COLUMN_ID + 19:
        cur_row_.cells_[i].set_uint64(ls_stat.archive_lsn_ < 0 ? 0 : ls_stat.archive_lsn_);
        break;
      case OB_APP_MIN_COLUMN_ID + 20:
        cur_row_.cells_[i].set_uint64(ls_stat.archive_scn_.get_val_for_inner_table_field());
        break;
      case OB_APP_MIN_COLUMN_ID + 21:
        cur_row_.cells_[i].set_int(ls_stat.archive_file_id_);
        break;
      case OB_APP_MIN_COLUMN_ID + 22:
        cur_row_.cells_[i].set_int(ls_stat.archive_file_offset_);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unkown column");
        break;
    }
  }
  return ret;
}
} // namespace observer
} // namespace oceanbase
