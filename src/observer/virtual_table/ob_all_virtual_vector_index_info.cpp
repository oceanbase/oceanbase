/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_all_virtual_vector_index_info.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace observer
{
/*
 * ObVectorIndexInfoIterator implement
 * */
int ObVectorIndexInfoIterator::open()
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService*);
  if (is_opened_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "The ObVectorIndexInfoIterator has been opened", K(ret));
  } else if (OB_FAIL(service->get_snapshot_ids(complete_tablet_ids_, partial_tablet_ids_))) {
    SERVER_LOG(WARN, "failed to get snapshot_ids", K(ret));
  } else if (OB_FAIL(ptr_set_.create(MAX_PTR_SET_VALUES, ObMemAttr(MTL_ID(), "AdaptorSet")))) {
    SERVER_LOG(WARN, "failed to create set", K(ret));
  } else {
    cur_idx_ = 0;
    is_opened_ = true;
  }
  return ret;
}

int ObVectorIndexInfoIterator::get_next_info(ObVectorIndexInfo &info)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else if (cur_idx_ >= complete_tablet_ids_.count() + partial_tablet_ids_.count()) {
    ret = OB_ITER_END;
  } else {
    ObLSID ls_id;
    ObTabletID tablet_id;
    if (cur_idx_ < complete_tablet_ids_.count()) {
      ls_id = complete_tablet_ids_.at(cur_idx_).ls_id_;
      tablet_id = complete_tablet_ids_.at(cur_idx_).tablet_id_;
    } else if (cur_idx_ < complete_tablet_ids_.count() + partial_tablet_ids_.count()) {
      ls_id = partial_tablet_ids_.at(cur_idx_ - complete_tablet_ids_.count()).ls_id_;
      tablet_id = partial_tablet_ids_.at(cur_idx_ - complete_tablet_ids_.count()).tablet_id_;
    }
    ObPluginVectorIndexAdapterGuard adapter_guard;
    if (OB_FAIL(MTL(ObPluginVectorIndexService*)->get_adapter_inst_guard(ls_id, tablet_id, adapter_guard))) {
      if (OB_HASH_NOT_EXIST != ret) {
        SERVER_LOG(WARN, "failed to get adapter inst guard", K(ls_id), K(tablet_id), KR(ret));
      }
    } else if (OB_HASH_EXIST == (ret = ptr_set_.exist_refactored(reinterpret_cast<int64_t>(adapter_guard.get_adatper())))) {
      ret = OB_HASH_NOT_EXIST; // set OB_HASH_NOT_EXIST to ignore this adapter
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(ptr_set_.set_refactored(reinterpret_cast<int64_t>(adapter_guard.get_adatper())))) {
        SERVER_LOG(WARN, "failed to set adapter check set", K(ret));
      } else if (OB_FAIL(adapter_guard.get_adatper()->fill_vector_index_info(info))) {
        SERVER_LOG(WARN, "failed to fill vector index info", K(ret), K(ls_id), K(tablet_id));
      } else {
        info.ls_id_ = ls_id.id();
      }
    } else {
      SERVER_LOG(WARN, "failed to check adapter ptr", K(ret));
    }
    cur_idx_++;
  }
  return ret;
}

void ObVectorIndexInfoIterator::reset()
{
  cur_idx_ = 0;
  complete_tablet_ids_.reset();
  partial_tablet_ids_.reset();
  allocator_.reset();
  ptr_set_.destroy();
  is_opened_ = false;
}

/*
 * ObAllVirtualVectorIndexInfo implement
 * */
ObAllVirtualVectorIndexInfo::ObAllVirtualVectorIndexInfo()
    : ObVirtualTableScannerIterator(),
      ip_buf_(),
      info_(),
      iter_()
{
}

ObAllVirtualVectorIndexInfo::~ObAllVirtualVectorIndexInfo()
{
  reset();
}

int ObAllVirtualVectorIndexInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    if (ret != OB_ITER_END) {
      SERVER_LOG(WARN, "execute fail", K(ret));
    }
  }
  return ret;
}
void ObAllVirtualVectorIndexInfo::release_last_tenant()
{
  iter_.reset();
}

bool ObAllVirtualVectorIndexInfo::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
    return true;
  }
  return false;
}

int ObAllVirtualVectorIndexInfo::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  info_.reset();
  if (!iter_.is_opened() && OB_FAIL(iter_.open())) {
    SERVER_LOG(WARN, "failed to open iter", K(ret));
  } else {
    do {
      if (OB_FAIL(iter_.get_next_info(info_))) {
        if (OB_ITER_END != ret && OB_HASH_NOT_EXIST != ret) {
          SERVER_LOG(WARN, "get next vector info failed", K(ret));
        }
      }
    } while (OB_HASH_NOT_EXIST == ret);
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
    case SVR_IP:
      if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
        cells[i].set_varchar(ip_buf_);
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      } else {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
      }
      break;
    case SVR_PORT:
      cells[i].set_int(addr_.get_port());
      break;
    case TENANT_ID:
      cells[i].set_int(MTL_ID());
      break;
    case LS_ID:
      // index_id
      cells[i].set_int(info_.ls_id_);
      break;
    case ROWKEY_VID_TABLE_ID:
      cells[i].set_int(info_.rowkey_vid_table_id_);
      break;
    case VID_ROWKEY_TABLE_ID:
      cells[i].set_int(info_.vid_rowkey_table_id_);
      break;
    case INC_INDEX_TABLE_ID:
      cells[i].set_int(info_.inc_index_table_id_);
      break;
    case VBITMAP_TABLE_ID:
      cells[i].set_int(info_.vbitmap_table_id_);
      break;
    case SNAPSHOT_INDEX_TABLE_ID:
      cells[i].set_int(info_.snapshot_index_table_id_);
      break;
    case DATA_TABLE_ID:
      cells[i].set_int(info_.data_table_id_);
      break;
    case ROWKEY_VID_TABLET_ID:
      cells[i].set_int(info_.rowkey_vid_tablet_id_);
      break;
    case VID_ROWKEY_TABLET_ID:
      cells[i].set_int(info_.vid_rowkey_tablet_id_);
      break;
    case INC_INDEX_TABLET_ID:
      cells[i].set_int(info_.inc_index_tablet_id_);
      break;
    case VBITMAP_TABLET_ID:
      cells[i].set_int(info_.vbitmap_tablet_id_);
      break;
    case SNAPSHOT_INDEX_TABLET_ID:
      cells[i].set_int(info_.snapshot_index_tablet_id_);
      break;
    case DATA_TABLET_ID: {
      cells[i].set_int(info_.data_tablet_id_);
      break;
    }
    case STATISTICS:
      cells[i].set_varchar(info_.statistics_);
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case SYNC_INFO:
      cells[i].set_varchar(info_.sync_info_);
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid column id", K(ret), K(col_id));
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

void ObAllVirtualVectorIndexInfo::reset()
{
  omt::ObMultiTenantOperator::reset();
  iter_.reset();
  memset(ip_buf_, 0, sizeof(ip_buf_));
  ObVirtualTableScannerIterator::reset();
}


} /* namespace observer */
} /* namespace oceanbase */
