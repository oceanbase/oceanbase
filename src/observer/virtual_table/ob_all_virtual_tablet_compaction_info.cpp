//Copyright (c) 2022 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include "observer/virtual_table/ob_all_virtual_tablet_compaction_info.h"
#include "observer/ob_server.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/compaction/ob_medium_compaction_mgr.h"
#include "storage/compaction/ob_medium_compaction_func.h"

using namespace oceanbase;
using namespace common;
using namespace memtable;
using namespace storage;
using namespace observer;

ObAllVirtualTabletCompactionInfo::ObAllVirtualTabletCompactionInfo()
    : ObVirtualTableScannerIterator(),
      ObMultiTenantOperator(),
      addr_(),
      tablet_iter_(nullptr),
      tablet_allocator_("VTTable"),
      tablet_handle_(),
      ls_id_(share::ObLSID::INVALID_LS_ID),
      iter_buf_(nullptr),
      medium_info_buf_()
{
}

ObAllVirtualTabletCompactionInfo::~ObAllVirtualTabletCompactionInfo()
{
  reset();
}

void ObAllVirtualTabletCompactionInfo::reset()
{
  omt::ObMultiTenantOperator::reset();
  addr_.reset();
  ls_id_ = share::ObLSID::INVALID_LS_ID;

  if (OB_NOT_NULL(tablet_iter_)) {
    tablet_iter_->~ObTenantTabletIterator();
    tablet_iter_ = nullptr;
  }
  if (OB_NOT_NULL(iter_buf_)) {
    allocator_->free(iter_buf_);
    iter_buf_ = nullptr;
  }
  tablet_handle_.reset();
  tablet_allocator_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualTabletCompactionInfo::init(
    common::ObIAllocator *allocator,
    common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (start_to_read_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(ret), KP(allocator));
  } else if (OB_ISNULL(iter_buf_ = allocator->alloc(sizeof(ObTenantTabletIterator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "fail to alloc tablet iter buf", K(ret));
  } else {
    allocator_ = allocator;
    addr_ = addr;
    start_to_read_ = true;
  }
  return ret;
}

int ObAllVirtualTabletCompactionInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "fail to execute", K(ret));
  }
  return ret;
}

void ObAllVirtualTabletCompactionInfo::release_last_tenant()
{
  if (OB_NOT_NULL(tablet_iter_)) {
    tablet_iter_->~ObTenantTabletIterator();
    tablet_iter_ = nullptr;
  }
  tablet_handle_.reset();
  tablet_allocator_.reset();
}

bool ObAllVirtualTabletCompactionInfo::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)){
    return true;
  }
  return false;
}

int ObAllVirtualTabletCompactionInfo::get_next_tablet()
{
  int ret = OB_SUCCESS;

  tablet_handle_.reset();
  tablet_allocator_.reuse();
  if (nullptr == tablet_iter_) {
    tablet_allocator_.set_tenant_id(MTL_ID());
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    if (OB_ISNULL(tablet_iter_ = new (iter_buf_) ObTenantTabletIterator(*t3m, tablet_allocator_, nullptr/*no op*/))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "fail to new tablet_iter_", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tablet_iter_->get_next_tablet(tablet_handle_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      SERVER_LOG(WARN, "fail to get tablet iter", K(ret));
    }
  } else if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected invalid tablet", K(ret), K(tablet_handle_));
  } else {
    ls_id_ = tablet_handle_.get_obj()->get_tablet_meta().ls_id_.id();
  }

  return ret;
}

int ObAllVirtualTabletCompactionInfo::process_curr_tenant(common::ObNewRow *&row)
{
  // each get_next_row will switch to required tenant, and released guard later
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObITable *table = nullptr;
  ObArenaAllocator allocator;
  const compaction::ObMediumCompactionInfoList *medium_info_list = nullptr;
  if (OB_UNLIKELY(!start_to_read_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(start_to_read_), K(ret));
  } else if (NULL == cur_row_.cells_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_FAIL(get_next_tablet())) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "get_next_table failed", K(ret));
    }
  } else if (OB_ISNULL(tablet = tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "tablet is null", K(ret), K(tablet_handle_));
  } else if (OB_FAIL(tablet->read_medium_info_list(allocator, medium_info_list))) {
    SERVER_LOG(WARN, "tablet read medium info list failed", K(ret), K(tablet_handle_));
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
          if (medium_info_list->size() > 0 || compaction::ObMediumCompactionInfo::MAJOR_COMPACTION == medium_info_list->get_last_compaction_type()) {
            int64_t pos = 0;
            medium_info_list->gene_info(medium_info_buf_, OB_MAX_VARCHAR_LENGTH, pos);
            cur_row_.cells_[i].set_varchar(medium_info_buf_);
            SERVER_LOG(DEBUG, "get medium info mgr", KPC(medium_info_list), K(medium_info_buf_));
          } else {
            cur_row_.cells_[i].set_varchar("");
          }
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
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
