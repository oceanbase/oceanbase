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

#include "ob_all_virtual_ss_ls_tablet_reorg_info.h"
#include "observer/ob_server.h"
#include "storage/multi_data_source/runtime_utility/common_define.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/scn.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
namespace oceanbase
{
namespace observer
{

ObAllVirtualSSLsTabletReorgInfo::ObAllVirtualSSLsTabletReorgInfo()
    : ObVirtualTableScannerIterator(),
      addr_(),
      ls_id_(share::ObLSID::INVALID_LS_ID),
      ls_iter_guard_(),
      read_op_(nullptr)
{
  buf_[0] = '\0';
}

ObAllVirtualSSLsTabletReorgInfo::~ObAllVirtualSSLsTabletReorgInfo()
{
  reset();
}

void ObAllVirtualSSLsTabletReorgInfo::reset()
{
  omt::ObMultiTenantOperator::reset();
  addr_.reset();
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  ls_iter_guard_.reset();
  free_read_op_(read_op_);
  ObVirtualTableScannerIterator::reset();
}

void ObAllVirtualSSLsTabletReorgInfo::release_last_tenant()
{
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  ls_iter_guard_.reset();
  free_read_op_(read_op_);
}

int ObAllVirtualSSLsTabletReorgInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "execute fail", K(ret));
  }
  return ret;
}

bool ObAllVirtualSSLsTabletReorgInfo::is_need_process(uint64_t tenant_id)
{
  bool b_ret = false;
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
    b_ret = true;
  }
  return b_ret;
}

int ObAllVirtualSSLsTabletReorgInfo::init_read_op_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObTabletReorgInfoTableReadOperator *read_op = nullptr;
  void *buf = nullptr;
  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "init read op get invalid argument", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_NOT_NULL(read_op_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "read op should be nullptr", K(ret), KP(read_op_));
  } else {
    if (FALSE_IT(buf = ob_malloc(sizeof(ObTabletReorgInfoTableReadOperator), "MemberTabReader"))) {
    } else if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WARN, "failed to alloc memory", K(ret), KP(buf));
    } else if (FALSE_IT(read_op = new (buf) ObTabletReorgInfoTableReadOperator())) {
    } else if (OB_FAIL(read_op->init(tenant_id, ls_id))) {
      SERVER_LOG(WARN, "failed to init member table read operator", K(ret), K(tenant_id), K(ls_id));
    } else {
      read_op_ = read_op;
      read_op = nullptr;
    }
  }

  if (OB_NOT_NULL(read_op)) {
    free_read_op_(read_op);
  }
  return ret;
}

void ObAllVirtualSSLsTabletReorgInfo::free_read_op_(ObTabletReorgInfoTableReadOperator *&read_op)
{
  if (OB_NOT_NULL(read_op)) {
    read_op->~ObTabletReorgInfoTableReadOperator();
    ob_free(read_op);
    read_op = nullptr;
  }
}

int ObAllVirtualSSLsTabletReorgInfo::get_next_ls_(ObLS *&ls)
{
  int ret = OB_SUCCESS;

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
    } else {
      ls_id_ = ls->get_ls_id().id();
      break;
    }
  }

  return ret;
}

int ObAllVirtualSSLsTabletReorgInfo::get_next_reorg_info_data_(
    ObTabletReorgInfoData &data, share::SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();

  while (OB_SUCC(ret)) {
    if (OB_ISNULL(read_op_)) {
      ObLS *ls = nullptr;
      if (OB_FAIL(get_next_ls_(ls))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next ls", K(ret));
        }
      } else if (OB_FAIL(init_read_op_(tenant_id, ls->get_ls_id()))) {
        SERVER_LOG(WARN, "failed to init read op", K(ret), K(tenant_id), KPC(ls));
        if (OB_REPLICA_NOT_READABLE == ret) {
          //overwrite ret
          ret = OB_SUCCESS;
          continue;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(read_op_->get_next(data, commit_scn))) {
      if (OB_ITER_END == ret) {
        free_read_op_(read_op_);
        ret = OB_SUCCESS;
      } else {
        SERVER_LOG(WARN, "fail to get member table data", K(ret));
      }
    } else {
      break;
    }
  }
  return ret;
}

int ObAllVirtualSSLsTabletReorgInfo::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObTabletReorgInfoData data;
  share::SCN commit_scn;

  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (FALSE_IT(start_to_read_ = true)) {
  } else if (ls_iter_guard_.get_ptr() == nullptr
      && OB_FAIL(MTL(ObLSService*)->get_ls_iter(ls_iter_guard_, ObLSGetMod::OBSERVER_MOD))) {
    SERVER_LOG(WARN, "get_ls_iter fail", K(ret));
  } else if (OB_FAIL(get_next_reorg_info_data_(data, commit_scn))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "get_next_reorg_info_data_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch(col_id) {
      case SVR_IP:
        //svr_ip
        if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
          cur_row_.cells_[i].set_varchar(ip_buf_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        } else {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
        }
        break;
      case SVR_PORT:
        //svr_port
        cur_row_.cells_[i].set_int(addr_.get_port());
        break;
      case TENANT_ID:
        //tenant_id
        cur_row_.cells_[i].set_int(MTL_ID());
        break;
      case LS_ID:
        //ls_id
        cur_row_.cells_[i].set_int(ls_id_);
        break;
      case TABLET_ID:
        //tablet_id
        cur_row_.cells_[i].set_int(data.key_.tablet_id_.id());
        break;
      case REORGANIZATION_SCN:
        //reorganization_scn
        cur_row_.cells_[i].set_int(data.key_.reorganization_scn_.get_val_for_tx());
        break;
      case DATA_TYPE:
        //data_type
        cur_row_.cells_[i].set_varchar(ObTabletReorgInfoDataType::get_str(data.key_.type_));
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case COMMIT_SCN:
        //commit_scn
        cur_row_.cells_[i].set_int(commit_scn.get_val_for_tx());
        break;
      case VALUE: {
        //value
        const int64_t pos = data.to_value_string(buf_, OB_MAX_VARCHAR_LENGTH);
        if (pos < 0) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "member table data value to string get unexpected pos", K(ret), K(data), K(pos));
        } else {
          buf_[pos] = '\0';
          cur_row_.cells_[i].set_varchar(buf_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      }
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

}/* ns observer*/
}/* ns oceanbase */
