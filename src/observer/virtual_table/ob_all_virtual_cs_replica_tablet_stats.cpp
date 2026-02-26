/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "observer/virtual_table/ob_all_virtual_cs_replica_tablet_stats.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace observer
{
/* --------------------------- ObAllVirtualTableLSTabletIter --------------------------- */
ObAllVirtualTableLSTabletIter::ObAllVirtualTableLSTabletIter()
  : ObVirtualTableScannerIterator(),
    ObMultiTenantOperator(),
    addr_(),
    ls_id_(share::ObLSID::INVALID_LS_ID),
    ls_iter_guard_(),
    ls_tablet_iter_(ObMDSGetTabletMode::READ_WITHOUT_CHECK)
{
}

ObAllVirtualTableLSTabletIter::~ObAllVirtualTableLSTabletIter()
{
  reset();
}

void ObAllVirtualTableLSTabletIter::reset()
{
  omt::ObMultiTenantOperator::reset();
  inner_reset();
  addr_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualTableLSTabletIter::init(common::ObIAllocator *allocator, common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(start_to_read_)) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_ISNULL(allocator) || OB_UNLIKELY(!addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(ret), KP(allocator), K(addr));
  } else {
    allocator_ = allocator;
    addr_ = addr;
    start_to_read_ = true;
  }
  return ret;
}

int ObAllVirtualTableLSTabletIter::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "execute fail", K(ret));
  }
  return ret;
}

bool ObAllVirtualTableLSTabletIter::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
    return true;
  }
  return false;
}

void ObAllVirtualTableLSTabletIter::inner_reset()
{
  ls_tablet_iter_.reset();
  ls_iter_guard_.reset();
  ls_id_ = share::ObLSID::INVALID_LS_ID;
}

int ObAllVirtualTableLSTabletIter::check_need_iterate_ls(const ObLS &ls, bool &need_iterate)
{
  int ret = OB_SUCCESS;
  UNUSED(ls);
  need_iterate = true;
  return ret;
}

int ObAllVirtualTableLSTabletIter::get_next_ls(ObLS *&ls)
{
  int ret = OB_SUCCESS;
  bool need_iterate = false;

  while (OB_SUCC(ret)) {
    if (!ls_iter_guard_.get_ptr()
        || OB_FAIL(ls_iter_guard_->get_next(ls))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to switch tenant", K(ret));
      }
      // switch to next tenant
      ret = OB_ITER_END;
    } else if (OB_ISNULL(ls)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "ls is null", K(ret));
    } else if (OB_FAIL(check_need_iterate_ls(*ls, need_iterate))) {
      SERVER_LOG(WARN, "fail to check if ls need iterate", K(ret), KPC(ls));
    } else if (need_iterate) {
      ls_id_ = ls->get_ls_id().id();
      if (OB_FAIL(inner_get_ls_infos(*ls))) {
        SERVER_LOG(WARN, "fail to get ls infos", K(ret), KPC(ls));
      }
      break;
    }
  }

  return ret;
}

int ObAllVirtualTableLSTabletIter::get_next_tablet(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  // when switch to a next new tenant, guard is reset and need rebuild ls iter.
  if (OB_ISNULL(ls_iter_guard_.get_ptr())) {
    ObLSService *ls_service = MTL(ObLSService*);
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "ls service is null", K(ret));
    } else if (OB_FAIL(ls_service->get_ls_iter(ls_iter_guard_, ObLSGetMod::OBSERVER_MOD))) {
      SERVER_LOG(WARN, "failed to get ls iter", K(ret));
    }
  }

  while (OB_SUCC(ret)) {
    if (!ls_tablet_iter_.is_valid()) {
      ObLS *ls = nullptr;
      if (OB_FAIL(get_next_ls(ls))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next ls", K(ret));
        }
      } else if (OB_FAIL(ls->build_tablet_iter(ls_tablet_iter_))) {
        SERVER_LOG(WARN, "fail to build tablet iter", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ls_tablet_iter_.get_next_tablet(tablet_handle))) {
      if (OB_ITER_END == ret) {
        ls_tablet_iter_.reset();
        ret = OB_SUCCESS;
      } else {
        SERVER_LOG(WARN, "fail to get next tablet", K(ret));
      }
    } else {
      break;
    }
  }

  return ret;
}

/* --------------------------- ObAllVirtualCSReplicaTabletStats --------------------------- */
ObAllVirtualCSReplicaTabletStats::ObAllVirtualCSReplicaTabletStats()
  : ObAllVirtualTableLSTabletIter(),
    migration_status_(ObMigrationStatus::OB_MIGRATION_STATUS_MAX)
{
}

ObAllVirtualCSReplicaTabletStats::~ObAllVirtualCSReplicaTabletStats()
{
  reset();
}

void ObAllVirtualCSReplicaTabletStats::inner_reset()
{
  ObAllVirtualTableLSTabletIter::inner_reset();
  migration_status_ = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
}

int ObAllVirtualCSReplicaTabletStats::inner_get_ls_infos(const ObLS &ls)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls.get_ls_meta().get_migration_status(migration_status_))) {
    SERVER_LOG(WARN, "failed to get migration status", K(ret), K(ls));
  }
  return ret;
}

int ObAllVirtualCSReplicaTabletStats::check_need_iterate_ls(const ObLS &ls, bool &need_iterate)
{
  int ret = OB_SUCCESS;
  need_iterate = ls.is_cs_replica();
  return ret;
}

int ObAllVirtualCSReplicaTabletStats::process_curr_tenant(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  if (OB_UNLIKELY(!start_to_read_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(start_to_read_), K(ret));
  } else if (NULL == cur_row_.cells_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_FAIL(get_next_tablet(tablet_handle))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next tablet", K(ret));
    }
  } else if (OB_UNLIKELY(!tablet_handle.is_valid()) || OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "tablet is invalid", K(tablet_handle), KPC(tablet));
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case TENANT_ID:
          cur_row_.cells_[i].set_int(MTL_ID());
          break;
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
        case LS_ID:
          cur_row_.cells_[i].set_int(ls_id_);
          break;
        case TABLET_ID:
          cur_row_.cells_[i].set_int(tablet->get_tablet_id().id());
          break;
        case MACRO_BLOCK_CNT:
          cur_row_.cells_[i].set_int(tablet->get_last_major_total_macro_block_count());
          break;
        case IS_CS:
          cur_row_.cells_[i].set_bool(tablet->is_last_major_column_store());
          break;
        case IS_CS_REPLICA:
          cur_row_.cells_[i].set_bool(tablet->is_cs_replica_compat());
          break;
        case AVAILABLE:
          if (!tablet->is_user_data_table() || !tablet->is_user_tablet()) {
            // no need to process cs replica
            cur_row_.cells_[i].set_bool(true);
          } else if (tablet->is_row_store()) {
            // tablet->is_row_store() means storage schema is row store.
            cur_row_.cells_[i].set_bool(false);
          } else if (tablet->get_last_major_snapshot_version() <= 0 // no major sstable
                  || ObMigrationStatus::OB_MIGRATION_STATUS_NONE != migration_status_) {
            // there maybe column store storage schema with row store major when rebuild, need waiting
            cur_row_.cells_[i].set_bool(false);
          } else {
            cur_row_.cells_[i].set_bool(tablet->is_last_major_column_store());
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

} // end observer
} // end oceanbase
