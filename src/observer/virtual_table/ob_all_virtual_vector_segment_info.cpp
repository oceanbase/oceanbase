// owner: yutong.lzs
// owner group: vector

/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_all_virtual_vector_segment_info.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "observer/virtual_table/ob_all_virtual_vector_index_info.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace observer
{

/*
 * ObVectorSegmentInfoIterator implement
 */
int ObVectorSegmentInfoIterator::open()
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService*);
  if (is_opened_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "The ObVectorSegmentInfoIterator has been opened", K(ret));
  } else if (OB_FAIL(load_all_index_segment())) {
    SERVER_LOG(WARN, "failed to get snapshot_ids", K(ret));
  } else {
    index_idx_ = 0;
    segment_idx_ = 0;
    is_opened_ = true;
  }
  return ret;
}

int ObVectorSegmentInfoIterator::get_next_segment_info(ObVectorSegmentInfo &info)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else {
    // check if end
    if (segment_idx_ < segment_infos_.count()) {
      info = segment_infos_.at(segment_idx_);
      segment_idx_++;
    } else {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObVectorSegmentInfoIterator::load_all_index_segment()
{
  int ret = OB_SUCCESS;
  ObVectorIndexInfoIterator index_info_iter;
  if (OB_FAIL(index_info_iter.open())) {
    SERVER_LOG(WARN, "failed to open index info iter", K(ret));
  } else {
    do {
      ObLSID ls_id;
      ObTabletID tablet_id;
      common::ObSEArray<ObVectorSegmentInfo, 4> current_index_segment_infos;
      if (OB_FAIL(index_info_iter.get_next_index_all_segments(ls_id, tablet_id, current_index_segment_infos))
      && OB_ITER_END != ret && OB_HASH_NOT_EXIST != ret) {
          SERVER_LOG(WARN, "get next vector info failed", K(ret));
          break;
      } else if (OB_ITER_END == ret) {
        break;
      } else if (current_index_segment_infos.count() > 0) {
        // Append all elements from current_index_segment_infos to segment_infos_
        for (int64_t i = 0; OB_SUCC(ret) && i < current_index_segment_infos.count(); ++i) {
          if (OB_FAIL(segment_infos_.push_back(current_index_segment_infos.at(i)))) {
            SERVER_LOG(WARN, "failed to push back segment info", K(ret), K(i),
                       K(segment_infos_.count()), K(ls_id), K(tablet_id));
          }
        }
      }
    } while (OB_SUCC(ret) || OB_HASH_NOT_EXIST == ret);
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

void ObVectorSegmentInfoIterator::reset()
{
  index_idx_ = 0;
  segment_idx_ = 0;
  segment_infos_.reset();
  allocator_.reset();
  is_opened_ = false;
}

/*
 * ObAllVirtualVectorSegmentInfo implement
 */
ObAllVirtualVectorSegmentInfo::ObAllVirtualVectorSegmentInfo()
    : ObVirtualTableScannerIterator(),
      ip_buf_(),
      info_(),
      iter_()
{
}

ObAllVirtualVectorSegmentInfo::~ObAllVirtualVectorSegmentInfo()
{
  reset();
}

int ObAllVirtualVectorSegmentInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    if (ret != OB_ITER_END) {
      SERVER_LOG(WARN, "execute fail", K(ret));
    }
  }
  return ret;
}

void ObAllVirtualVectorSegmentInfo::release_last_tenant()
{
  iter_.reset();
}

bool ObAllVirtualVectorSegmentInfo::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
    return true;
  }
  return false;
}

int ObAllVirtualVectorSegmentInfo::process_curr_tenant(ObNewRow *&row)
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
      if (OB_FAIL(iter_.get_next_segment_info(info_))) {
        if (OB_ITER_END != ret && OB_HASH_NOT_EXIST != ret) {
          SERVER_LOG(WARN, "get next segment info failed", K(ret));
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
    case DATA_TABLE_ID:
      cells[i].set_int(info_.data_table_id_);
      break;
    case DATA_TABLET_ID:
      cells[i].set_int(info_.data_tablet_id_);
      break;
    case VBITMAP_TABLET_ID:
      cells[i].set_int(info_.vbitmap_tablet_id_);
      break;
    case INC_INDEX_TABLET_ID:
      cells[i].set_int(info_.inc_index_tablet_id_);
      break;
    case SNAPSHOT_INDEX_TABLET_ID:
      cells[i].set_int(info_.snapshot_index_tablet_id_);
      break;
    case BLOCK_COUNT:
      cells[i].set_int(info_.block_count_);
      break;
    case SEGMENT_TYPE:
      cells[i].set_int(static_cast<int64_t>(info_.segment_type_));
      break;
    case INDEX_TYPE:
      cells[i].set_int(info_.index_type_);
      break;
    case MEM_USED:
      cells[i].set_int(info_.mem_used_);
      break;
    case MIN_VID:
      cells[i].set_int(info_.min_vid_);
      break;
    case MAX_VID:
      cells[i].set_int(info_.max_vid_);
      break;
    case VECTOR_CNT:
      cells[i].set_int(info_.vector_cnt_);
      break;
    case REF_CNT:
      cells[i].set_int(info_.ref_cnt_);
      break;
    case SEGMENT_STATE:
      cells[i].set_int(static_cast<int64_t>(info_.segment_state_));
      break;
    case SCN:
      cells[i].set_int(info_.scn_);
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

void ObAllVirtualVectorSegmentInfo::reset()
{
  omt::ObMultiTenantOperator::reset();
  iter_.reset();
  memset(ip_buf_, 0, sizeof(ip_buf_));
  ObVirtualTableScannerIterator::reset();
}

} /* namespace observer */
} /* namespace oceanbase */
