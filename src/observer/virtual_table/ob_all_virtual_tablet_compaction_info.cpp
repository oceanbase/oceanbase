//Copyright (c) 2022 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE

#include "observer/virtual_table/ob_all_virtual_tablet_compaction_info.h"
#include "storage/compaction/ob_medium_compaction_func.h"

namespace oceanbase
{
using namespace oceanbase;
using namespace common;
using namespace memtable;
using namespace storage;
namespace observer
{

ObAllVirtualTabletCompactionInfo::ObAllVirtualTabletCompactionInfo()
    : ObVirtualTableTabletIter(),
      medium_info_buf_()
{
}

ObAllVirtualTabletCompactionInfo::~ObAllVirtualTabletCompactionInfo()
{
  reset();
}

void ObAllVirtualTabletCompactionInfo::reset()
{
  ObVirtualTableTabletIter::reset();
  MEMSET(medium_info_buf_, '\0', OB_MAX_VARCHAR_LENGTH);
}

int ObAllVirtualTabletCompactionInfo::process_curr_tenant(common::ObNewRow *&row)
{
  // each get_next_row will switch to required tenant, and released guard later
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObITable *table = nullptr;
  ObArenaAllocator allocator;
  const compaction::ObMediumCompactionInfoList *medium_info_list = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObTabletTableStore *table_store = nullptr;
  if (OB_UNLIKELY(!start_to_read_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(start_to_read_), K(ret));
  } else if (OB_ISNULL(cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else {
    // get next valid tablet;
    do {
      if (OB_FAIL(get_next_tablet())) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "get_next_table failed", K(ret));
        }
      } else if (OB_ISNULL(tablet = tablet_handle_.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "tablet is null", K(ret), K(tablet_handle_));
      } else if (!tablet->get_tablet_meta().ha_status_.is_data_status_complete()) {
        ret = OB_EAGAIN;
        LOG_DEBUG("query all_virtual_tablet_compaction_info, tablet_data not complete", K(ret), K(tablet->get_tablet_id()), K(tablet->get_ls_id()));
      }
    // quit while, excepted errcode : OB_ITER_END, OB_SUCCESS
    } while(OB_EAGAIN == ret);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tablet->read_medium_info_list(allocator, medium_info_list))) {
    SERVER_LOG(WARN, "tablet read medium info list failed", K(ret), K(tablet_handle_), KPC(tablet_handle_.get_obj()));
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    SERVER_LOG(WARN,"fail to fetch table store", K(ret));
  } else if (OB_FAIL(table_store_wrapper.get_member(table_store))) {
    SERVER_LOG(WARN,"fail to get table store", K(ret), K(table_store_wrapper));
  } else {
    const int64_t col_count = output_column_ids_.count();
    int64_t max_sync_medium_scn = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case SVR_IP:
          if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cur_row_.cells_[i].set_varchar(ip_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
          }
          break;
        case SVR_PORT:
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case TENANT_ID:
          cur_row_.cells_[i].set_int(MTL_ID());
          break;
        case LS_ID:
          cur_row_.cells_[i].set_int(ls_id_);
          break;
        case TABLET_ID:
          cur_row_.cells_[i].set_int(tablet->get_tablet_meta().tablet_id_.id());
          break;
        case FINISH_SCN: {
          int64_t last_major_snapshot_version = tablet->get_last_major_snapshot_version();
          cur_row_.cells_[i].set_int(last_major_snapshot_version);
          break;
        }
        case WAIT_CHECK_SCN:
          cur_row_.cells_[i].set_int(medium_info_list->get_wait_check_medium_scn());
          break;
        case MAX_RECEIVED_SCN:
          if (OB_SUCCESS == compaction::ObMediumCompactionScheduleFunc::get_max_sync_medium_scn(
              *tablet, *medium_info_list, max_sync_medium_scn)) {
            cur_row_.cells_[i].set_int(max_sync_medium_scn);
          } else {
            cur_row_.cells_[i].set_int(-1);
          }
          break;
        case SERIALIZE_SCN_LIST:
          if (medium_info_list->size() > 0
            || compaction::ObMediumCompactionInfo::MAJOR_COMPACTION == medium_info_list->get_last_compaction_type()
            || !table_store->get_major_ckm_info().is_empty()) {
            int64_t pos = 0;
            MEMSET(medium_info_buf_, '\0', OB_MAX_VARCHAR_LENGTH);
            medium_info_list->gene_info(medium_info_buf_, OB_MAX_VARCHAR_LENGTH, pos);
            table_store->get_major_ckm_info().gene_info(medium_info_buf_, OB_MAX_VARCHAR_LENGTH, pos);
            cur_row_.cells_[i].set_varchar(medium_info_buf_);
            SERVER_LOG(DEBUG, "get medium info mgr", KPC(medium_info_list), K(medium_info_buf_));
          } else {
            cur_row_.cells_[i].set_varchar("");
          }
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case VALIDATED_SCN:
          if (table_store->get_major_ckm_info().is_empty()) {
            cur_row_.cells_[i].set_int(0);
          } else {
            cur_row_.cells_[i].set_int(table_store->get_major_ckm_info().get_compaction_scn());
          }
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
  return ret;
}

} /* namespace observer */
} /* namespace oceanbase */