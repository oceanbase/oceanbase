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

#include "ob_all_virtual_tablet_buffer_info.h"

namespace oceanbase
{
using namespace storage;
using namespace blocksstable;
using namespace common;
namespace observer
{
ObAllVirtualTabletBufferInfo::ObAllVirtualTabletBufferInfo()
  : addr_(), index_(0), pool_type_(ObTabletPoolType::TP_MAX), buffer_infos_()
{
}

ObAllVirtualTabletBufferInfo::~ObAllVirtualTabletBufferInfo()
{
  reset();
}

void ObAllVirtualTabletBufferInfo::reset()
{
  omt::ObMultiTenantOperator::reset();
  addr_.reset();
  index_ = 0;
  pool_type_ = ObTabletPoolType::TP_MAX;
  buffer_infos_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualTabletBufferInfo::init(common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid arg", K(ret), K(addr));
  } else if (OB_FAIL(OB_UNLIKELY(!addr.ip_to_string(ip_buf_, sizeof(ip_buf_))))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "fail to transfer ip to string", K(ret));
  } else {
    addr_ = addr;
  }
  return ret;
}

int ObAllVirtualTabletBufferInfo::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "fail to execute", K(ret));
  }
  return ret;
}

int ObAllVirtualTabletBufferInfo::get_tablet_pool_infos()
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(MTL_ID(), "TabletBuffer");
  buffer_infos_.set_attr(attr);
  if (OB_FAIL(MTL(ObTenantMetaMemMgr*)->get_tablet_buffer_infos(buffer_infos_))) {
    SERVER_LOG(WARN, "fail to get tablet buffer infos", K(ret));
  }
  return ret;
}

bool ObAllVirtualTabletBufferInfo::is_need_process(uint64_t tenant_id)
{
  bool need_process = false;
  if (!is_virtual_tenant_id(tenant_id) &&
    (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)){
    need_process = true;
  }
  return need_process;
}

int ObAllVirtualTabletBufferInfo::process_curr_tenant(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is nullptr", K(ret));
  } else if (0 == buffer_infos_.size() && OB_FAIL(get_tablet_pool_infos())) {
    SERVER_LOG(WARN, "fail to get tablet pool infos", K(ret));
  } else if (buffer_infos_.size() <= index_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(gen_row(buffer_infos_[index_], row))) {
    SERVER_LOG(WARN, "fail to gen_row", K(ret));
  } else {
    index_++;
  }
  return ret;
}

void ObAllVirtualTabletBufferInfo::release_last_tenant()
{
  buffer_infos_.reset();
  index_ = 0;
}

int ObAllVirtualTabletBufferInfo::gen_row(
    const ObTabletBufferInfo &buffer_info,
    common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); i++) {
    uint64_t col_id = output_column_ids_.at(i);
    switch(col_id) {
    case SVR_IP:
      //svr_ip
      cur_row_.cells_[i].set_varchar(ip_buf_);
      cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case SVR_PORT:
      //svr_port
      cur_row_.cells_[i].set_int(addr_.get_port());
      break;
    case TENANT_ID:
      //tenant_id
      cur_row_.cells_[i].set_int(MTL_ID());
      break;
    case TABLET_BUFFER_PTR:
      //tablet_buffer_ptr
      MEMSET(tablet_buffer_pointer_, 0, STR_LEN);
      pos = 0;
      databuff_print_obj(tablet_buffer_pointer_, STR_LEN, pos, static_cast<void *>(buffer_info.tablet_buffer_ptr_));
      cur_row_.cells_[i].set_varchar(tablet_buffer_pointer_);
      cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case TABLET_OBJ_PTR:
      //tablet_obj_ptr
      MEMSET(tablet_pointer_, 0, STR_LEN);
      pos = 0;
      databuff_print_obj(tablet_pointer_, STR_LEN, pos, static_cast<void *>(buffer_info.tablet_));
      cur_row_.cells_[i].set_varchar(tablet_pointer_);
      cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case POOL_TYPE:
      //pool_type
      cur_row_.cells_[i].set_varchar(buffer_info.pool_type_ == ObTabletPoolType::TP_LARGE
        ? "TP_LARGE" : "TP_NORMAL");
      cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case LS_ID:
      //ls_id
      cur_row_.cells_[i].set_int(buffer_info.ls_id_.id());
      break;
    case TABLET_ID:
      //tablet_id
      cur_row_.cells_[i].set_int(buffer_info.tablet_id_.id());
      break;
    case IN_MAP:
      //in_map
      cur_row_.cells_[i].set_bool(buffer_info.in_map_);
      break;
    case LAST_ACCESS_TIME:
      //last_access_time
      cur_row_.cells_[i].set_timestamp(buffer_info.last_access_time_ / 1000L);
      break;
    default:{
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "invalid column_id", K(ret), K(col_id));
    }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

} // observer
} // oceanbase