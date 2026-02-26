//Copyright (c) 2022 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include "observer/virtual_table/ob_virtual_table_tablet_iter.h"
#include "storage/tx_storage/ob_ls_service.h"

using namespace oceanbase;
using namespace common;
using namespace memtable;
using namespace storage;
using namespace observer;

ObVirtualTableTabletIter::ObVirtualTableTabletIter()
    : ObVirtualTableScannerIterator(),
      ObMultiTenantOperator(),
      addr_(),
      tablet_iter_(nullptr),
      tablet_allocator_("VTTable"),
      tablet_handle_(),
      ls_id_(share::ObLSID::INVALID_LS_ID),
      iter_buf_(nullptr)
{
}

ObVirtualTableTabletIter::~ObVirtualTableTabletIter()
{
  reset();
}

void ObVirtualTableTabletIter::reset()
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

int ObVirtualTableTabletIter::init(
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

int ObVirtualTableTabletIter::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "fail to execute", K(ret));
  }
  return ret;
}

void ObVirtualTableTabletIter::release_last_tenant()
{
  if (OB_NOT_NULL(tablet_iter_)) {
    tablet_iter_->~ObTenantTabletIterator();
    tablet_iter_ = nullptr;
  }
  tablet_handle_.reset();
  tablet_allocator_.reset();
}

bool ObVirtualTableTabletIter::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)){
    return true;
  }
  return false;
}

int ObVirtualTableTabletIter::get_next_tablet()
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
