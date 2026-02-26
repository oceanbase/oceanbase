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
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
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
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      SERVER_LOG(WARN, "execute fail", KR(ret));
    }
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

  int ObAllVirtualSSLocalCacheInfo::get_macro_cache_used_disk_size(ObSSLocalCacheInfoInst &inst)
  {
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    int64_t tmp_used_size = 0;
    int64_t macro_used_size = 0;
    int64_t hot_macro_used_size = 0;
    int64_t meta_used_size = 0;
    if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_MACRO_CACHE_TMP_USED_SIZE,
      tmp_used_size))) {
      SERVER_LOG(WARN, "fail to get macro cache tmp used disk size",
        KR(ret), K(ObStatEventIds::SS_MACRO_CACHE_TMP_USED_SIZE));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_MACRO_CACHE_MACRO_USED_SIZE,
      macro_used_size))) {
      SERVER_LOG(WARN, "fail to get macro cache macro used disk size",
        KR(ret), K(ObStatEventIds::SS_MACRO_CACHE_MACRO_USED_SIZE));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_MACRO_CACHE_HOT_MACRO_USED_SIZE,
      hot_macro_used_size))) {
      SERVER_LOG(WARN, "fail to get macro cache hot macro used disk size",
        KR(ret), K(ObStatEventIds::SS_MACRO_CACHE_HOT_MACRO_USED_SIZE));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_MACRO_CACHE_META_USED_SIZE,
      meta_used_size))) {
      SERVER_LOG(WARN, "fail to get macro cache meta used disk size",
        KR(ret), K(ObStatEventIds::SS_MACRO_CACHE_META_USED_SIZE));
    } else {
      inst.used_disk_size_ = tmp_used_size + macro_used_size + hot_macro_used_size + meta_used_size;
    }
  }
#endif
  return ret;
}

int ObAllVirtualSSLocalCacheInfo::add_macro_cache_inst_()
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    ObSSLocalCacheInfoInst inst;
    inst.cache_name_ = OB_SS_MACRO_CACHE_NAME;
    inst.priority_ = OB_SS_MACRO_CACHE_PRIORITY;

    ObSSLocalCacheService *local_cache_service = nullptr;
    if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService *))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "local_cache_service is null", KR(ret));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_MACRO_CACHE_ALLOC_DISK_SIZE,
      inst.alloc_disk_size_))) {
      SERVER_LOG(WARN, "fail to get macro cache alloc disk size",
        KR(ret), K(ObStatEventIds::SS_MACRO_CACHE_ALLOC_DISK_SIZE));
    } else if (OB_FAIL(get_macro_cache_used_disk_size(inst))) {
      SERVER_LOG(WARN, "fail to get macro cache used disk size", KR(ret));
    } else {
      ObSSLocalCacheStat &local_cache_stat = local_cache_service->get_local_cache_stat();
      ObSSMacroCacheAllStat &macro_cache_stat = local_cache_stat.macro_cache_stat_;
      inst.total_hit_cnt_ = macro_cache_stat.get_total_hit_cnt();
      inst.total_hit_bytes_ = macro_cache_stat.get_total_hit_bytes();
      inst.total_miss_cnt_ = macro_cache_stat.get_total_miss_cnt();
      inst.total_miss_bytes_ = macro_cache_stat.get_total_miss_bytes();
      const int64_t total_access_cnt = inst.total_hit_cnt_ + inst.total_miss_cnt_;
      inst.hit_ratio_ = ((total_access_cnt <= 0) ? 0 : ((double)inst.total_hit_cnt_ / (double)total_access_cnt));
    }
    if (FAILEDx(inst_list_.push_back(inst))) {
      SERVER_LOG(WARN, "fail to push back macro cache inst", KR(ret), K(inst), K(inst_list_.count()));
    }
  }
#endif
  return ret;
}
#ifdef OB_BUILD_SHARED_STORAGE
void ObAllVirtualSSLocalCacheInfo::get_hit_stat_(const ObStorageCacheHitStat &hit_stat, ObSSLocalCacheInfoInst &inst)
{
  inst.total_hit_cnt_ = hit_stat.get_hit_cnt();
  inst.total_hit_bytes_ = hit_stat.get_hit_bytes();
  inst.total_miss_cnt_ = hit_stat.get_miss_cnt();
  inst.total_miss_bytes_ = hit_stat.get_miss_bytes();
  const int64_t total_access_cnt = inst.total_hit_cnt_ + inst.total_miss_cnt_;
  inst.hit_ratio_ = ((total_access_cnt <= 0) ? 0 : ((double)inst.total_hit_cnt_ / (double)total_access_cnt));
}
#endif
int ObAllVirtualSSLocalCacheInfo::add_tmpfile_cache_inst_()
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    ObSSLocalCacheInfoInst inst;
    inst.cache_name_ = OB_SS_LOCAL_CACHE_TMPFILE_NAME;
    inst.priority_ = OB_LOCAL_CACHE_TMPFILE_PRIORITY;

    ObSSLocalCacheService *local_cache_service = nullptr;
    if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService *))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "local_cache_service is null", KR(ret));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_MACRO_CACHE_TMP_USED_SIZE,
      inst.used_disk_size_))) {
      SERVER_LOG(WARN, "fail to get macro cache tmp used disk size",
        KR(ret), K(ObStatEventIds::SS_MACRO_CACHE_TMP_USED_SIZE));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_MACRO_CACHE_ALLOC_DISK_SIZE,
      inst.alloc_disk_size_))) {
      SERVER_LOG(WARN, "fail to get macro cache alloc disk size",
        KR(ret), K(ObStatEventIds::SS_MACRO_CACHE_ALLOC_DISK_SIZE));
    } else {
      ObSSLocalCacheStat &local_cache_stat = local_cache_service->get_local_cache_stat();
      ObStorageCacheHitStat &hit_stat = local_cache_stat.macro_cache_stat_.tmpfile_stat_.hit_stat_;
      get_hit_stat_(hit_stat, inst);
    }

    if (FAILEDx(inst_list_.push_back(inst))) {
      SERVER_LOG(WARN, "fail to push back tmpfile cache inst", KR(ret), K(inst), K(inst_list_.count()));
    }
  }
#endif
  return ret;
}

int ObAllVirtualSSLocalCacheInfo::add_macro_block_inst_(const ObSSMacroBlockType macro_block_type)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    ObSSLocalCacheInfoInst inst;
    switch (macro_block_type) {
      case ObSSMacroBlockType::SHARED_MACRO: {
        inst.cache_name_ = OB_SS_LOCAL_CACHE_SHARED_MACRO_NAME;
        inst.priority_ = OB_LOCAL_CACHE_SHARED_MACRO_PRIORITY;
        break;
      }
      case ObSSMacroBlockType::PRIVATE_MACRO: {
        inst.cache_name_ = OB_SS_LOCAL_CACHE_PRIVATE_MACRO_NAME;
        inst.priority_ = OB_LOCAL_CACHE_SHARED_MACRO_PRIORITY;
        break;
      }
      case ObSSMacroBlockType::EXTERNAL_TABLE: {
        inst.cache_name_ = OB_SS_LOCAL_CACHE_EXTERNAL_TABLE_NAME;
        inst.priority_ = OB_LOCAL_CACHE_EXTERNAL_TABLE_PRIORITY;
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        SERVER_LOG(WARN, "invalid macro block type", KR(ret), K(macro_block_type));
        break;
      }
    }
    ObSSLocalCacheService *local_cache_service = nullptr;
    ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService *))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "local_cache_service is null", KR(ret));
    } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "macro_cache_mgr is null", KR(ret));
    } else if (OB_FAIL(tenant_di_info_.get_stat(ObStatEventIds::SS_MACRO_CACHE_ALLOC_DISK_SIZE,
      inst.alloc_disk_size_))) {
      SERVER_LOG(WARN, "fail to get macro cache alloc disk size",
        KR(ret), K(ObStatEventIds::SS_MACRO_CACHE_ALLOC_DISK_SIZE));
    } else {
      // micro cache is ignored
      ObSSLocalCacheStat &local_cache_stat = local_cache_service->get_local_cache_stat();
      ObStorageCacheHitStat &hit_stat = local_cache_stat.macro_cache_stat_.macro_block_stat_[static_cast<uint8_t>(macro_block_type)].hit_stat_;
      get_hit_stat_(hit_stat, inst);

      ObSSCacheStatInfo cache_stat_info;
      if (OB_FAIL(macro_cache_mgr->get_macro_blocks_stat(macro_block_type, cache_stat_info))) {
        SERVER_LOG(WARN, "fail to get macro block stat", KR(ret), K(macro_block_type));
      } else if (FALSE_IT(inst.used_disk_size_ = cache_stat_info.get_used_disk_size())) {
      } else if (OB_FAIL(inst_list_.push_back(inst))) {
        SERVER_LOG(WARN, "fail to push back shared macro cache inst", KR(ret), K(inst), K(inst_list_.count()));
      }
    }
  }
#endif
  return ret;
}

int ObAllVirtualSSLocalCacheInfo::add_local_cache_inst_()
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    ObSSLocalCacheInfoInst inst;
    inst.cache_name_ = OB_SS_LOCAL_CACHE_NAME;
    inst.priority_ = OB_SS_LOCAL_CACHE_PRIORITY;

    ObSSLocalCacheService *local_cache_service = nullptr;
    if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService *))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "local_cache_service is null", KR(ret));
    } else {
      ObSSLocalCacheStat &local_cache_stat = local_cache_service->get_local_cache_stat();
      ObStorageCacheHitStat &hit_stat = local_cache_stat.local_cache_stat_.hit_stat_;
      get_hit_stat_(hit_stat, inst);
      // local cache space usage is the sum of micro cache and macro cache
      inst.used_disk_size_ = inst_list_[0].used_disk_size_ + inst_list_[1].used_disk_size_;
      inst.used_mem_size_ = inst_list_[0].used_mem_size_ + inst_list_[1].used_mem_size_;
      inst.alloc_disk_size_ = inst_list_[0].alloc_disk_size_ + inst_list_[1].alloc_disk_size_;
      inst.hold_size_ = inst_list_[0].hold_size_ + inst_list_[1].hold_size_;
    }

    if (FAILEDx(inst_list_.push_back(inst))) {
      SERVER_LOG(WARN, "fail to push back local cache inst", KR(ret), K(inst), K(inst_list_.count()));
    }
  }
#endif
  return ret;
}

int ObAllVirtualSSLocalCacheInfo::set_all_cache_insts_()
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
  } else if (OB_FAIL(add_macro_cache_inst_())) {
    SERVER_LOG(WARN, "fail to add macro cache inst", KR(ret));
  } else if (OB_FAIL(add_tmpfile_cache_inst_())) {
    SERVER_LOG(WARN, "fail to add tmp file cache inst", KR(ret));
  } else if (OB_FAIL(add_macro_block_inst_(ObSSMacroBlockType::SHARED_MACRO))) {
    SERVER_LOG(WARN, "fail to add shared macro cache inst", KR(ret));
  } else if (OB_FAIL(add_macro_block_inst_(ObSSMacroBlockType::PRIVATE_MACRO))) {
    SERVER_LOG(WARN, "fail to add private macro cache inst", KR(ret));
  } else if (OB_FAIL(add_local_cache_inst_())) {
    SERVER_LOG(WARN, "fail to add local cache inst", KR(ret));
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
      } else if (OB_FAIL(set_all_cache_insts_())) {
        SERVER_LOG(WARN, "fail to set all cache status", KR(ret), K(tenant_id_));
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