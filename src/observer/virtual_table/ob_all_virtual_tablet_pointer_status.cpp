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

#include "ob_all_virtual_tablet_pointer_status.h"
#include "share/ob_ls_id.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "observer/ob_server.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace observer
{
ObAllVirtualTabletPtr::ObAllVirtualTabletPtr()
  : ObVirtualTableScannerIterator(),
    addr_(),
    tablet_iter_(nullptr),
    iter_buf_(nullptr)
{
}

ObAllVirtualTabletPtr::~ObAllVirtualTabletPtr()
{
  reset();
}

void ObAllVirtualTabletPtr::reset()
{
  omt::ObMultiTenantOperator::reset();
  addr_.reset();
  if (OB_NOT_NULL(iter_buf_)) {
    allocator_->free(iter_buf_);
    iter_buf_ = nullptr;
  }
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualTabletPtr::init(ObIAllocator *allocator, common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(start_to_read_)) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_ISNULL(iter_buf_ = allocator->alloc(sizeof(ObTenantTabletPtrWithInMemObjIterator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "fail to alloc tablet iter buf", K(ret));
  } else {
    allocator_ = allocator;
    addr_ = addr;
    start_to_read_ = true;
  }
  return ret;
}

int ObAllVirtualTabletPtr::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "fail to execute", K(ret));
  }
  return ret;
}

void ObAllVirtualTabletPtr::release_last_tenant()
{
  if (OB_NOT_NULL(tablet_iter_)) {
    tablet_iter_->~ObTenantTabletPtrWithInMemObjIterator();
    tablet_iter_ = nullptr;
  }
}

bool ObAllVirtualTabletPtr::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)){
    return true;
  }
  return false;
}

int ObAllVirtualTabletPtr::get_next_tablet_pointer(
    ObTabletMapKey &key,
    ObTabletPointerHandle &ptr_hdl,
    ObTabletHandle &tablet_hdl)
{
  int ret = OB_SUCCESS;
  if (nullptr == tablet_iter_) {
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    tablet_iter_ = new (iter_buf_) ObTenantTabletPtrWithInMemObjIterator(*t3m);
    if (OB_ISNULL(tablet_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "fail to new tablet_iter_", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(tablet_iter_->get_next_tablet_pointer(key, ptr_hdl, tablet_hdl))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      SERVER_LOG(WARN, "fail to get tablet iter", K(ret));
    }
  } else if (OB_UNLIKELY(!ptr_hdl.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected invalid tablet", K(ret), K(key), K(ptr_hdl), K(tablet_hdl));
  }
  return ret;
}

int ObAllVirtualTabletPtr::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObTabletMapKey key;
  ObTabletPointerHandle ptr_hdl;
  ObTabletHandle tablet_hdl;
  ObTablet *tablet = nullptr;
  int64_t pos = 0;
  const ObTabletPointer *tablet_pointer = nullptr;
  share::ObLSID ls_id;
  ObTabletID tablet_id;

  if (OB_UNLIKELY(!start_to_read_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(start_to_read_), K(ret));
  } else if (OB_ISNULL(cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_FAIL(get_next_tablet_pointer(key, ptr_hdl, tablet_hdl))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get_next_table", K(ret));
    }
  } else {
    tablet = tablet_hdl.get_obj();
    ls_id = key.ls_id_;
    tablet_id = key.tablet_id_;
    tablet_pointer = static_cast<const ObTabletPointer*>(ptr_hdl.get_resource_ptr());
    const int64_t col_cnt = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; i++) {
      const uint64_t col_id = output_column_ids_.at(i);
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
          cur_row_.cells_[i].set_int(ls_id.id());
          break;
        case TABLET_ID:
          cur_row_.cells_[i].set_int(tablet_id.id());
          break;
        case ADDRESS:
          tablet_pointer->get_addr().to_string(address_, STR_LEN);
          cur_row_.cells_[i].set_varchar(address_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case POINTER_REF:
          cur_row_.cells_[i].set_int(ptr_hdl.get_ref_cnt());
          break;
        case IN_MEMORY:
          cur_row_.cells_[i].set_bool(tablet_pointer->is_in_memory());
          break;
        case TABLET_REF:
          cur_row_.cells_[i].set_int(nullptr == tablet ? 0 : tablet->get_ref());
          break;
        case WASH_SCORE:
          cur_row_.cells_[i].set_int(nullptr == tablet ? 0 : tablet->get_wash_score());
          break;
        case TABLET_PTR:
          MEMSET(pointer_, 0, STR_LEN);
          pos = 0;
          databuff_print_obj(pointer_, STR_LEN, pos, static_cast<void *>(tablet));
          cur_row_.cells_[i].set_varchar(pointer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case INITIAL_STATE:
          cur_row_.cells_[i].set_bool(tablet_pointer->get_initial_state());
          break;
        case OLD_CHAIN:
          MEMSET(old_chain_, 0, STR_LEN);
          if (OB_FAIL(MTL(ObTenantMetaMemMgr*)->print_old_chain(key, *tablet_pointer, STR_LEN, old_chain_))) {
            SERVER_LOG(WARN, "fail to print old chain", K(ret), K(key), KPC(tablet_pointer));
          } else {
            cur_row_.cells_[i].set_varchar(old_chain_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
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

}
}
