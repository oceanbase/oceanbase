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

#include "observer/virtual_table/ob_all_virtual_tmp_file.h"
#include "observer/ob_server.h"

using namespace oceanbase::common;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace observer
{

ObAllVirtualTmpFileInfo::ObAllVirtualTmpFileInfo()
    : ObVirtualTableScannerIterator(),
      fd_arr_(),
      is_ready_(false),
      fd_idx_(-1)
{
}

ObAllVirtualTmpFileInfo::~ObAllVirtualTmpFileInfo()
{
  reset();
}

void ObAllVirtualTmpFileInfo::reset()
{
  // release tenant resources first
  omt::ObMultiTenantOperator::reset();
  ip_buffer_[0] = '\0';
  trace_id_buffer_[0] = '\0';
  file_ptr_buffer_[0] = '\0';
  file_label_buffer_[0] = '\0';
  fd_arr_.reset();
  is_ready_ = false;
  fd_idx_ = -1;
  ObVirtualTableScannerIterator::reset();
}

void ObAllVirtualTmpFileInfo::release_last_tenant()
{
  // resources related with tenant must be released by this function
  fd_arr_.reset();
  is_ready_ = false;
  fd_idx_ = -1;
}

bool ObAllVirtualTmpFileInfo::is_need_process(uint64_t tenant_id)
{
  bool bool_ret = false;
  if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
    bool_ret = true;
  }

  return bool_ret;
}

int ObAllVirtualTmpFileInfo::get_next_tmp_file_info_(tmp_file::ObTmpFileInfo *tmp_file_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > fd_idx_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected fd_idx_", KR(ret), K(fd_idx_));
  } else if (OB_ISNULL(tmp_file_info)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", KR(ret), KP(tmp_file_info));
  } else {
    bool has_get = false;
    while (OB_SUCC(ret)
           && !has_get) {
      if (fd_idx_ >= fd_arr_.count()) {
        ret = OB_ITER_END;
        SERVER_LOG(INFO, "iterate current tenant reach end", K(fd_idx_), K(fd_arr_.count()));
      } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_info(MTL_ID(), fd_arr_.at(fd_idx_), tmp_file_info))) {
        if (OB_ENTRY_NOT_EXIST == ret || OB_TIMEOUT == ret) {
          SERVER_LOG(INFO, "tmp file does not exist or is locked by others", KR(ret), K(fd_arr_.at(fd_idx_)));
          ret = OB_SUCCESS;
        } else {
          SERVER_LOG(WARN, "fail to get tmp file info", KR(ret), K(fd_idx_), K(fd_arr_), K(fd_arr_.at(fd_idx_)));
        }
      } else {
        has_get = true;
      }
      if (OB_SUCC(ret)) {
        fd_idx_++;
      }
    }
  }
  return ret;
}

int ObAllVirtualTmpFileInfo::fill_columns_(tmp_file::ObTmpFileInfo *tmp_file_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tmp_file_info)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", KR(ret), KP(tmp_file_info));
  } else {
    const int64_t col_count = output_column_ids_.count();
    ObAddr self_addr = GCONF.self_addr_;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case TENANT_ID:
          cur_row_.cells_[i].set_int(tmp_file_info->tenant_id_);
          break;
        case SVR_IP:
          MEMSET(ip_buffer_, '\0', OB_IP_STR_BUFF);
          (void)self_addr.ip_to_string(ip_buffer_, common::OB_IP_STR_BUFF);
          cur_row_.cells_[i].set_varchar(ip_buffer_);
          cur_row_.cells_[i].set_default_collation_type();
          break;
        case SVR_PORT:
          cur_row_.cells_[i].set_int(self_addr.get_port());
          break;
        case FILE_ID:
          cur_row_.cells_[i].set_int(tmp_file_info->fd_);
          break;
        case TRACE_ID:
          MEMSET(trace_id_buffer_, '\0', OB_MAX_TRACE_ID_BUFFER_SIZE);
          if (!tmp_file_info->trace_id_.is_invalid()) {
            tmp_file_info->trace_id_.to_string(trace_id_buffer_, OB_MAX_TRACE_ID_BUFFER_SIZE);
          }
          cur_row_.cells_[i].set_varchar(trace_id_buffer_);
          cur_row_.cells_[i].set_default_collation_type();
          break;
        case DIR_ID:
          cur_row_.cells_[i].set_int(tmp_file_info->dir_id_);
          break;
        case DATA_BYTES:
          cur_row_.cells_[i].set_int(tmp_file_info->file_size_);
          break;
        case START_OFFSET:
          cur_row_.cells_[i].set_int(tmp_file_info->truncated_offset_);
          break;
        case IS_DELETING:
          cur_row_.cells_[i].set_bool(tmp_file_info->is_deleting_);
          break;
        case CACHED_DATA_PAGE_NUM:
          cur_row_.cells_[i].set_int(tmp_file_info->cached_data_page_num_);
          break;
        case WRITE_BACK_DATA_PAGE_NUM:
          cur_row_.cells_[i].set_int(tmp_file_info->write_back_data_page_num_);
          break;
        case FLUSHED_DATA_PAGE_NUM:
          cur_row_.cells_[i].set_int(tmp_file_info->flushed_data_page_num_);
          break;
        case REF_CNT:
          cur_row_.cells_[i].set_int(tmp_file_info->ref_cnt_);
          break;
        case TOTAL_WRITES:
          cur_row_.cells_[i].set_int(tmp_file_info->write_req_cnt_);
          break;
        case UNALIGNED_WRITES:
          cur_row_.cells_[i].set_int(tmp_file_info->unaligned_write_req_cnt_);
          break;
        case TOTAL_READS:
          cur_row_.cells_[i].set_int(tmp_file_info->read_req_cnt_);
          break;
        case UNALIGNED_READS:
          cur_row_.cells_[i].set_int(tmp_file_info->unaligned_read_req_cnt_);
          break;
        case TOTAL_READ_BYTES:
          cur_row_.cells_[i].set_int(tmp_file_info->total_read_size_);
          break;
        case LAST_ACCESS_TIME:
          cur_row_.cells_[i].set_timestamp(tmp_file_info->last_access_ts_);
          break;
        case LAST_MODIFY_TIME:
          cur_row_.cells_[i].set_timestamp(tmp_file_info->last_modify_ts_);
          break;
        case BIRTH_TIME:
          cur_row_.cells_[i].set_timestamp(tmp_file_info->birth_ts_);
          break;
        case TMP_FILE_PTR:
          if (NULL != tmp_file_info->tmp_file_ptr_) {
            MEMSET(file_ptr_buffer_, '\0', 20);
            snprintf(file_ptr_buffer_, 18, "0x%lx", (uint64_t)tmp_file_info->tmp_file_ptr_);
            cur_row_.cells_[i].set_varchar(file_ptr_buffer_);
          } else {
            cur_row_.cells_[i].set_varchar(ObString::make_string("nullptr"));
          }
          cur_row_.cells_[i].set_default_collation_type();
          break;
        case LABEL:
          MEMSET(file_label_buffer_, '\0', OB_MAX_FILE_LABEL_SIZE);
          if (!tmp_file_info->label_.is_empty()) {
            tmp_file_info->label_.to_string(file_label_buffer_, OB_MAX_FILE_LABEL_SIZE);
          }
          cur_row_.cells_[i].set_varchar(file_label_buffer_);
          cur_row_.cells_[i].set_default_collation_type();
          break;
        case TYPE:
          break;
        case COMPRESSIBLE_FD:
          break;
        case PERSISTED_TAIL_PAGE_WRITES:
          cur_row_.cells_[i].set_int(tmp_file_info->write_persisted_tail_page_cnt_);
          break;
        case LACK_PAGE_CNT:
          cur_row_.cells_[i].set_int(tmp_file_info->lack_page_cnt_);
          break;
        case TOTAL_TRUNCATED_PAGE_READ_CNT:
          cur_row_.cells_[i].set_int(tmp_file_info->total_truncated_page_read_cnt_);
          break;
        case TRUNCATED_PAGE_HITS:
          cur_row_.cells_[i].set_int(tmp_file_info->truncated_page_read_hits_);
          break;
        case TOTAL_KV_CACHE_PAGE_READ_CNT:
          cur_row_.cells_[i].set_int(tmp_file_info->total_kv_cache_page_read_cnt_);
          break;
        case KV_CACHE_PAGE_HITS:
          cur_row_.cells_[i].set_int(tmp_file_info->kv_cache_page_read_hits_);
          break;
        case TOTAL_UNCACHED_PAGE_READ_CNT:
          cur_row_.cells_[i].set_int(tmp_file_info->total_uncached_page_read_cnt_);
          break;
        case UNCACHED_PAGE_HITS:
          cur_row_.cells_[i].set_int(tmp_file_info->uncached_page_read_hits_);
          break;
        case TOTAL_WBP_PAGE_READ_CNT:
          cur_row_.cells_[i].set_int(tmp_file_info->total_wbp_page_read_cnt_);
          break;
        case WBP_PAGE_HITS:
          cur_row_.cells_[i].set_int(tmp_file_info->wbp_page_read_hits_);
          break;
      #ifdef OB_BUILD_SHARED_STORAGE
        /* columns in ss modes begin */
        case AGGREGATE_READ_IO_CNT:
          if (GCTX.is_shared_storage_mode()) {
            tmp_file::ObSSTmpFileInfo *ss_tmp_file_info = static_cast<tmp_file::ObSSTmpFileInfo *>(tmp_file_info);
            if (OB_FAIL(fill_ss_column_(i, ss_tmp_file_info))) {
              SERVER_LOG(WARN, "fail to fill ss column", KR(ret), K(i), KPC(ss_tmp_file_info));
            }
          }
          break;
        /* columns in ss modes end */
      #endif
        /* columns in sn modes begin */
        case META_TREE_EPOCH:
        case META_TREE_LEVELS:
        case META_BYTES:
        case CACHED_META_PAGE_NUM:
        case WRITE_BACK_META_PAGE_NUM:
        case PAGE_FLUSH_CNT:
          if (!GCTX.is_shared_storage_mode()) {
            tmp_file::ObSNTmpFileInfo *sn_tmp_file_info = static_cast<tmp_file::ObSNTmpFileInfo *>(tmp_file_info);
            if (OB_FAIL(fill_sn_column_(i, sn_tmp_file_info))) {
              SERVER_LOG(WARN, "fail to fill sn column", KR(ret), K(i), KPC(sn_tmp_file_info));
            }
          }
          break;
        /* columns in sn modes end */
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column_id", KR(ret), K(col_id));
          break;
      }
    }
  }
  return ret;
}

int ObAllVirtualTmpFileInfo::fill_sn_column_(const uint64_t col_index, tmp_file::ObSNTmpFileInfo *tmp_file_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tmp_file_info)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", KR(ret), KP(tmp_file_info));
  } else {
    uint64_t col_id = output_column_ids_.at(col_index);
    switch (col_id) {
      case META_TREE_EPOCH:
        cur_row_.cells_[col_index].set_int(tmp_file_info->meta_tree_epoch_);
        break;
      case META_TREE_LEVELS:
        cur_row_.cells_[col_index].set_int(tmp_file_info->meta_tree_level_cnt_);
        break;
      case META_BYTES:
        cur_row_.cells_[col_index].set_int(tmp_file_info->meta_size_);
        break;
      case CACHED_META_PAGE_NUM:
        cur_row_.cells_[col_index].set_int(tmp_file_info->cached_meta_page_num_);
        break;
      case WRITE_BACK_META_PAGE_NUM:
        cur_row_.cells_[col_index].set_int(tmp_file_info->write_back_meta_page_num_);
        break;
      case PAGE_FLUSH_CNT:
        cur_row_.cells_[col_index].set_int(tmp_file_info->all_type_page_flush_cnt_);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid column_id", KR(ret), K(col_id));
        break;
    }
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObAllVirtualTmpFileInfo::fill_ss_column_(const uint64_t col_index, tmp_file::ObSSTmpFileInfo *tmp_file_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tmp_file_info)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", KR(ret), KP(tmp_file_info));
  } else {
    uint64_t col_id = output_column_ids_.at(col_index);
    switch (col_id) {
      case AGGREGATE_READ_IO_CNT:
        cur_row_.cells_[col_index].set_int(tmp_file_info->aggregate_read_io_cnt_);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid column_id", KR(ret), K(col_id));
        break;
    }
  }
  return ret;
}
#endif

int ObAllVirtualTmpFileInfo::process_curr_tenant(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (nullptr == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be nullptr", K(allocator_), KR(ret));
  } else if (FALSE_IT(start_to_read_ = true)) {
  } else if (!is_ready_) {
    if (OB_UNLIKELY(!fd_arr_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected fd_arr_", KR(ret), K(fd_arr_));
    } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_fds(MTL_ID(), fd_arr_))) {
      SERVER_LOG(WARN, "fail to get tmp file fd arr", KR(ret));
      if (OB_NOT_INIT == ret) {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      is_ready_ = true;
      fd_idx_ = 0;
    }
  }

  if (OB_SUCC(ret)) {
    tmp_file::ObTmpFileInfo *tmp_file_info = nullptr;
    ObMemAttr attr(MTL_ID(), "TmpFileInfo");
    if (!GCTX.is_shared_storage_mode()) {
      tmp_file_info = OB_NEW(tmp_file::ObSNTmpFileInfo, attr);
    #ifdef OB_BUILD_SHARED_STORAGE
    } else {
      tmp_file_info = OB_NEW(tmp_file::ObSSTmpFileInfo, attr);
    #endif
    }
    if (OB_FAIL(get_next_tmp_file_info_(tmp_file_info))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next tmp file info", KR(ret));
      }
    } else if (OB_FAIL(fill_columns_(tmp_file_info))) {
      SERVER_LOG(WARN, "fail to fill columns", KR(ret));
    }

    if (OB_NOT_NULL(tmp_file_info)) {
      tmp_file_info->~ObTmpFileInfo();
      ob_free(tmp_file_info);
      tmp_file_info = NULL;
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

}
}
