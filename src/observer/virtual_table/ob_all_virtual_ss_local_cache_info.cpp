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

#include "ob_all_virtual_ss_local_cache_info.h"
#include "share/ob_server_struct.h"
#include "share/ash/ob_di_util.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_disk_space_manager.h"
#endif

using namespace oceanbase::storage;

namespace oceanbase
{
namespace observer
{

ObAllVirtualSSLocalCacheInfo::ObAllVirtualSSLocalCacheInfo()
    : str_buf_(),
      tenant_id_(OB_INVALID_TENANT_ID),
      tenant_di_info_(),
      cur_idx_(0),
      inst_list_()
{
  ip_buf_[0] = '\0';
}

ObAllVirtualSSLocalCacheInfo::~ObAllVirtualSSLocalCacheInfo()
{
  reset();
}

void ObAllVirtualSSLocalCacheInfo::reset()
{
  ip_buf_[0] = '\0';
  str_buf_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  tenant_di_info_.reset();
  cur_idx_ = 0;
  inst_list_.reset();
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualSSLocalCacheInfo::inner_open()
{
  int ret = OB_SUCCESS;
  return ret;
}


int ObAllVirtualSSLocalCacheInfo::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "execute fail", KR(ret));
  }
  return ret;
}

int ObAllVirtualSSLocalCacheInfo::get_the_diag_info(
    const uint64_t tenant_id,
    common::ObDiagnoseTenantInfo &diag_info)
{
  int ret = OB_SUCCESS;
  diag_info.reset();
  if (OB_FAIL(oceanbase::share::ObDiagnosticInfoUtil::get_the_diag_info(tenant_id, diag_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      SERVER_LOG(WARN, "Fail to get tenant stat event", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObAllVirtualSSLocalCacheInfo::add_micro_cache_inst_()
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    ObSSLocalCacheInfoInst inst;
    inst.cache_name_ = OB_SS_MICRO_CACHE_NAME;
    inst.priority_ = OB_SS_MICRO_CACHE_PRIORITY;

    if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_MICRO_CACHE_HIT,
                                         inst.total_hit_cnt_))) {
      SERVER_LOG(WARN, "fail to get micro cache hit cnt",
          KR(ret), K(ObStatEventIds::SS_MICRO_CACHE_HIT));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_MICRO_CACHE_HIT_BYTES,
                                                inst.total_hit_bytes_))) {
      SERVER_LOG(WARN, "fail to get micro cache hit bytes",
          KR(ret), K(ObStatEventIds::SS_MICRO_CACHE_HIT_BYTES));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_MICRO_CACHE_MISS,
                                                inst.total_miss_cnt_))) {
      SERVER_LOG(WARN, "fail to get micro cache miss cnt",
          KR(ret), K(ObStatEventIds::SS_MICRO_CACHE_MISS));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_MICRO_CACHE_MISS_BYTES,
                                                inst.total_miss_bytes_))) {
      SERVER_LOG(WARN, "fail to get micro cache miss bytes",
          KR(ret), K(ObStatEventIds::SS_MICRO_CACHE_MISS_BYTES));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_MICRO_CACHE_HOLD_SIZE,
                                                inst.hold_size_))) {
      SERVER_LOG(WARN, "fail to get micro cache hold size",
          KR(ret), K(ObStatEventIds::SS_MICRO_CACHE_HOLD_SIZE));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_MICRO_CACHE_USED_DISK_SIZE,
                                                inst.used_disk_size_))) {
      SERVER_LOG(WARN, "fail to get micro cache used disk size",
          KR(ret), K(ObStatEventIds::SS_MICRO_CACHE_USED_DISK_SIZE));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_MICRO_CACHE_ALLOC_DISK_SIZE,
                                                inst.alloc_disk_size_))) {
      SERVER_LOG(WARN, "fail to get micro cache alloc disk size",
          KR(ret), K(ObStatEventIds::SS_MICRO_CACHE_ALLOC_DISK_SIZE));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_MICRO_CACHE_USED_MEM_SIZE,
                                                inst.used_mem_size_))) {
      SERVER_LOG(WARN, "fail to get micro cache used mem size",
          KR(ret), K(ObStatEventIds::SS_MICRO_CACHE_USED_MEM_SIZE));
    } else {
      const int64_t total_hit_num = inst.total_hit_cnt_ + inst.total_miss_cnt_;
      inst.hit_ratio_ = ((total_hit_num <= 0) ? 0 : (double)inst.total_hit_cnt_ / (double)total_hit_num);
    }

    if (FAILEDx(inst_list_.push_back(inst))) {
      SERVER_LOG(WARN, "fail to push back micro cache inst",
          KR(ret), K(inst), K(inst_list_.count()));
    }
  }
#endif
  return ret;
}

int ObAllVirtualSSLocalCacheInfo::add_tmpfile_cache_inst_()
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    ObSSLocalCacheInfoInst inst;
    inst.cache_name_ = OB_SS_LOCAL_CACHE_TMPFILE_NAME;
    inst.priority_ = OB_LOCAL_CACHE_TMPFILE_PRIORITY;

    // tmp file cache_hit_bytes/cache_miss_bytes has no stat event, thus get from disk_space_mgr
    ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
    if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "disk sapce manager is null", KR(ret));
    } else {
      inst.total_hit_bytes_ = disk_space_mgr->get_tmp_file_cache_stat().cache_hit_bytes_;
      inst.total_miss_bytes_ = disk_space_mgr->get_tmp_file_cache_stat().cache_miss_bytes_;
    }
    inst.hold_size_ = 0;
    inst.used_mem_size_ = 0;

    int64_t tmp_file_used_disk_size_r = 0;
    int64_t tmp_file_used_disk_size_w = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_TMPFILE_CACHE_HIT,
                                                inst.total_hit_cnt_))) {
      SERVER_LOG(WARN, "fail to get tmp file cache hit cnt",
          KR(ret), K(ObStatEventIds::SS_TMPFILE_CACHE_HIT));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_TMPFILE_CACHE_MISS,
                                                inst.total_miss_cnt_))) {
      SERVER_LOG(WARN, "fail to get tmp file cache miss cnt",
          KR(ret), K(ObStatEventIds::SS_TMPFILE_CACHE_MISS));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_LOCAL_CACHE_TMPFILE_ALLOC_SIZE,
                                                inst.alloc_disk_size_))) {
      SERVER_LOG(WARN, "fail to get tmp file cache alloc disk size",
          KR(ret), K(ObStatEventIds::SS_LOCAL_CACHE_TMPFILE_ALLOC_SIZE));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_LOCAL_CACHE_TMPFILE_USED_DISK_SIZE_R,
                                                tmp_file_used_disk_size_r))) {
      SERVER_LOG(WARN, "fail to get tmp file cache used disk size for read",
          KR(ret), K(ObStatEventIds::SS_LOCAL_CACHE_TMPFILE_USED_DISK_SIZE_R));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_LOCAL_CACHE_TMPFILE_USED_DISK_SIZE_W,
                                                tmp_file_used_disk_size_w))) {
      SERVER_LOG(WARN, "fail to get tmp file cache used disk size for write",
          KR(ret), K(ObStatEventIds::SS_LOCAL_CACHE_TMPFILE_USED_DISK_SIZE_W));
    } else {
      inst.used_disk_size_ = tmp_file_used_disk_size_r + tmp_file_used_disk_size_w;
      const int64_t total_hit_num = inst.total_hit_cnt_ + inst.total_miss_cnt_;
      inst.hit_ratio_ = ((total_hit_num <= 0) ? 0 : (double)inst.total_hit_cnt_ / (double)total_hit_num);
    }

    if (FAILEDx(inst_list_.push_back(inst))) {
      SERVER_LOG(WARN, "fail to push back tmp file cache inst",
          KR(ret), K(inst), K(inst_list_.count()));
    }
  }
#endif
  return ret;
}

int ObAllVirtualSSLocalCacheInfo::add_major_macro_cache_inst_()
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    ObSSLocalCacheInfoInst inst;
    inst.cache_name_ = OB_SS_LOCAL_CACHE_MAJOR_MACRO_NAME;
    inst.priority_ = OB_LOCAL_CACHE_MAJOR_MACRO_PRIORITY;

    // major macro cache_hit_bytes/cache_miss_bytes has no stat event, thus get from disk_space_mgr
    ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
    if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "disk sapce manager is null", KR(ret));
    } else {
      inst.total_hit_bytes_ = disk_space_mgr->get_major_macro_cache_stat().cache_hit_bytes_;
      inst.total_miss_bytes_ = disk_space_mgr->get_major_macro_cache_stat().cache_miss_bytes_;
    }
    inst.hold_size_ = 0;
    inst.used_mem_size_ = 0;

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_MAJOR_MACRO_CACHE_HIT,
                                                inst.total_hit_cnt_))) {
      SERVER_LOG(WARN, "fail to get major macro cache hit cnt",
          KR(ret), K(ObStatEventIds::SS_MAJOR_MACRO_CACHE_HIT));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_MAJOR_MACRO_CACHE_MISS,
                                                inst.total_miss_cnt_))) {
      SERVER_LOG(WARN, "fail to get major macro cache miss cnt",
          KR(ret), K(ObStatEventIds::SS_MAJOR_MACRO_CACHE_MISS));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_LOCAL_CACHE_TMPFILE_ALLOC_SIZE,
                                                inst.alloc_disk_size_))) {
      SERVER_LOG(WARN, "fail to get major macro cache alloc disk size",
          KR(ret), K(ObStatEventIds::SS_LOCAL_CACHE_TMPFILE_ALLOC_SIZE));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_LOCAL_CACHE_MAJOR_MACRO_USED_DISK_SIZE,
                                                inst.used_disk_size_))) {
      SERVER_LOG(WARN, "fail to get major macro cache used disk size",
          KR(ret), K(ObStatEventIds::SS_LOCAL_CACHE_MAJOR_MACRO_USED_DISK_SIZE));
    } else {
      const int64_t total_hit_num = inst.total_hit_cnt_ + inst.total_miss_cnt_;
      inst.hit_ratio_ = ((total_hit_num <= 0) ? 0 : (double)inst.total_hit_cnt_ / (double)total_hit_num);
    }

    if (FAILEDx(inst_list_.push_back(inst))) {
      SERVER_LOG(WARN, "fail to push back major macro cache inst",
          KR(ret), K(inst), K(inst_list_.count()));
    }
  }
#endif
  return ret;
}

int ObAllVirtualSSLocalCacheInfo::add_private_macro_cache_inst_()
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    ObSSLocalCacheInfoInst inst;
    inst.cache_name_ = OB_SS_LOCAL_CACHE_PRIVATE_MACRO_NAME;
    inst.priority_ = OB_LOCAL_CACHE_PRIVATE_MACRO_PRIORITY;

    ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
    if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "disk sapce manager is null", KR(ret));
    } else {
      const ObLocalCacheHitStat cache_stat = disk_space_mgr->get_private_macro_cache_stat();
      inst.hold_size_ = 0;
      inst.used_mem_size_ = 0;
      inst.total_hit_bytes_ = cache_stat.cache_hit_bytes_;
      inst.total_miss_bytes_ = cache_stat.cache_miss_bytes_;
      inst.total_hit_cnt_ = cache_stat.cache_hit_cnt_;
      inst.total_miss_cnt_ = cache_stat.cache_miss_cnt_;
      inst.alloc_disk_size_ = disk_space_mgr->get_private_macro_disk_space_size();
      inst.used_disk_size_ = disk_space_mgr->get_private_macro_alloc_size();

      const int64_t total_req_cnt = inst.total_hit_cnt_ + inst.total_miss_cnt_;
      inst.hit_ratio_ = (total_req_cnt <= 0) ? 0 : (static_cast<double>(inst.total_hit_cnt_) / total_req_cnt);
    }

    if (FAILEDx(inst_list_.push_back(inst))) {
      SERVER_LOG(WARN, "fail to push back private macro cache inst", KR(ret), K(inst), K(inst_list_.count()));
    }
  }
#endif
  return ret;
}

int ObAllVirtualSSLocalCacheInfo::set_local_cache_insts_()
{
  int ret = OB_SUCCESS;
  inst_list_.reset();
#ifdef OB_BUILD_SHARED_STORAGE
  if (!GCTX.is_shared_storage_mode() || !oceanbase::lib::is_diagnose_info_enabled()) {
    // skip
  } else if (OB_FAIL(set_ss_stats(tenant_id_, tenant_di_info_))) {
    SERVER_LOG(WARN, "fail to set ss stats", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(add_micro_cache_inst_())) {
    SERVER_LOG(WARN, "fail to add micro cache inst", KR(ret));
  } else if (OB_FAIL(add_tmpfile_cache_inst_())) {
    SERVER_LOG(WARN, "fail to add tmp file cache inst", KR(ret));
  } else if (OB_FAIL(add_major_macro_cache_inst_())) {
    SERVER_LOG(WARN, "fail to add major macro cache inst", KR(ret));
  } else if (OB_FAIL(add_private_macro_cache_inst_())) {
    SERVER_LOG(WARN, "fail to add private macro cache inst", KR(ret));
  }
#endif
  return ret;
}

int ObAllVirtualSSLocalCacheInfo::process_curr_tenant(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  ObObj *cells = cur_row_.cells_;
  ObAddr addr = GCTX.self_addr();
  const int64_t col_count = output_column_ids_.count();
  if (OB_ISNULL(cells)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", KR(ret), K(cells));
  } else if (oceanbase::lib::is_diagnose_info_enabled()) {
    if (MTL_ID() != tenant_id_) {
      tenant_id_ = MTL_ID();
      if (OB_FAIL(get_the_diag_info(tenant_id_, tenant_di_info_))) {
        SERVER_LOG(WARN, "get diag info fail", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(set_local_cache_insts_())) {
        SERVER_LOG(WARN, "fail to set local cache status", KR(ret), K(tenant_id_));
      } else {
        cur_idx_ = 0;
      }
    }

    if (OB_SUCC(ret) && cur_idx_ >= inst_list_.count()) {
      ret = OB_ITER_END;
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
      const uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
      case SVR_IP: {
        if (addr.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
          cells[i].set_varchar(ip_buf_);
          cells[i].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
        } else {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "fail to execute ip_to_string", KR(ret));
        }
        break;
      }
      case SVR_PORT: {
        cells[i].set_int(addr.get_port());
        break;
      }
      case TENANT_ID: {
        cells[i].set_int(tenant_id_);
        break;
      }
      case CACHE_NAME: {
        cells[i].set_varchar(inst_list_[cur_idx_].cache_name_);
        cells[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case PRIORITY: {
        cells[i].set_int(inst_list_[cur_idx_].priority_);
        break;
      }
      case HIT_RATIO: {
        str_buf_.reset();
        number::ObNumber num;
        double value = inst_list_[cur_idx_].hit_ratio_ * 100;
        static const int64_t MAX_DOUBLE_PRINT_SIZE = 64;
        char buf[MAX_DOUBLE_PRINT_SIZE] = {0};
        memset(buf, 0, MAX_DOUBLE_PRINT_SIZE);
        if (OB_FAIL(databuff_printf(buf, sizeof(buf), "%lf", value))) {
          SERVER_LOG(WARN, "fail to print hit ratio", KR(ret), K(cur_idx_), K(inst_list_[cur_idx_]));
        } else if (OB_FAIL(num.from(buf, str_buf_))) {
          SERVER_LOG(WARN, "fail to cast to number", KR(ret), K(cur_idx_), K(inst_list_[cur_idx_]));
        } else {
          cells[i].set_number(num);
        }
        break;
      }
      case TOTAL_HIT_CNT: {
        cells[i].set_int(inst_list_[cur_idx_].total_hit_cnt_);
        break;
      }
      case TOTAL_HIT_BYTES: {
        cells[i].set_int(inst_list_[cur_idx_].total_hit_bytes_);
        break;
      }
      case TOTAL_MISS_CNT: {
        cells[i].set_int(inst_list_[cur_idx_].total_miss_cnt_);
        break;
      }
      case TOTAL_MISS_BYTES: {
        cells[i].set_int(inst_list_[cur_idx_].total_miss_bytes_);
        break;
      }
      case HOLD_SIZE: {
        cells[i].set_int(inst_list_[cur_idx_].hold_size_);
        break;
      }
      case ALLOC_DISK_SIZE: {
        cells[i].set_int(inst_list_[cur_idx_].alloc_disk_size_);
        break;
      }
      case USED_DISK_SIZE: {
        cells[i].set_int(inst_list_[cur_idx_].used_disk_size_);
        break;
      }
      case USED_MEM_SIZE: {
        cells[i].set_int(inst_list_[cur_idx_].used_mem_size_);
        break;
      }
      } // end switch
    } // end for
    if (OB_SUCC(ret)) {
      row = &cur_row_;
      cur_idx_++;
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

void ObAllVirtualSSLocalCacheInfo::release_last_tenant()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
}

bool ObAllVirtualSSLocalCacheInfo::is_need_process(uint64_t tenant_id)
{
  return is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_;
}

} // namespace observer
} // namespace oceanbase