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
#include "ob_all_virtual_ss_macro_cache_info.h"
#include "share/ob_server_struct.h"
#include "share/ash/ob_di_util.h"
#include "storage/blocksstable/ob_macro_block_id.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_dir_manager.h"
#endif

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace observer
{

ObAllVirtualSSMacroCacheInfo::ObAllVirtualSSMacroCacheInfo()
  : tenant_id_(OB_INVALID_TENANT_ID), bucket_count_(0), bucket_idx_(0), arr_idx_(0)
{
  ip_buf_[0] = '\0';
  macro_id_buf_[0] = '\0';
  macro_id_str_buf_[0] = '\0';
  local_path_buf_[0] = '\0';
  remote_path_buf_[0] = '\0';
#ifdef OB_BUILD_SHARED_STORAGE
  macro_cache_meta_arr_.reset();
#endif
}

ObAllVirtualSSMacroCacheInfo::~ObAllVirtualSSMacroCacheInfo()
{
  reset();
}

void ObAllVirtualSSMacroCacheInfo::reset()
{
  // Note: ObMultiTenantOperator::reset() will call release_last_tenant() within ObTenantSwitchGuard.
  // ObAllVirtualSSMacroCacheInfo::release_last_tenant() calls macro_cache_meta_arr_.reset().
  // this ensures macro_cache_meta_arr_ is reset within ObTenantSwitchGuard.
  omt::ObMultiTenantOperator::reset();
#ifdef OB_BUILD_SHARED_STORAGE
  macro_cache_meta_arr_.reset();
#endif
  arr_idx_ = 0;
  bucket_idx_ = 0;
  bucket_count_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  remote_path_buf_[0] = '\0';
  local_path_buf_[0] = '\0';
  macro_id_str_buf_[0] = '\0';
  macro_id_buf_[0] = '\0';
  ip_buf_[0] = '\0';
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualSSMacroCacheInfo::inner_open()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObAllVirtualSSMacroCacheInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      SERVER_LOG(WARN, "execute fail", KR(ret));
    }
  }
  return ret;
}

int ObAllVirtualSSMacroCacheInfo::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  ObObj *cells = cur_row_.cells_;
  ObAddr addr = GCTX.self_addr();
  const int64_t col_count = output_column_ids_.count();
  if (OB_ISNULL(cells)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", KR(ret), K(cells));
  } else if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ITER_END;
  }

#ifdef OB_BUILD_SHARED_STORAGE
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ObSSMacroCacheMgr is NULL", KR(ret));
  } else {
    // switch tenant
    if (MTL_ID() != tenant_id_) {
      tenant_id_ = MTL_ID();
      bucket_idx_ = 0;
      arr_idx_ = 0;
      bucket_count_ = macro_cache_mgr->get_meta_map_bucket_count();
    }
    MacroBlockId macro_id;
    ObTabletID tablet_id;
    int64_t size = 0;
    int64_t last_access_time_us = 0;
    ObSSMacroCacheType cache_type = ObSSMacroCacheType::MAX_TYPE;
    bool is_write_cache = false;
    bool is_in_fifo_list = false;
    int32_t ref_cnt = 0;
    int64_t access_cnt = 0;
    if (OB_FAIL(get_meta_if_need())) { // to save memory, iterate in bucket level
      SERVER_LOG(WARN, "fail to get meta if need", KR(ret), K_(bucket_count), K_(bucket_idx), K_(arr_idx));
    } else if (OB_UNLIKELY((bucket_idx_ < 0) || (arr_idx_ < 0))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid idx", KR(ret), K_(bucket_idx), K_(arr_idx));
    } else if ((bucket_idx_ >= bucket_count_) && (arr_idx_ >= macro_cache_meta_arr_.count())) {
      ret = OB_ITER_END;
      SERVER_LOG(INFO, "already iterate all macro cache meta", K_(bucket_count), K_(bucket_idx));
    } else {
      const ObSSMacroCacheMetaHandle &meta_handle = macro_cache_meta_arr_.at(arr_idx_);
      if (OB_UNLIKELY(!meta_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid meta handle", KR(ret), K(meta_handle), K_(bucket_count),
                   K_(bucket_idx), K_(arr_idx), "arr_count", macro_cache_meta_arr_.count());
      } else {
        macro_id = meta_handle()->get_macro_id();
        tablet_id = meta_handle()->get_effective_tablet_id();
        size = meta_handle()->get_size();
        last_access_time_us = meta_handle()->get_last_access_time_us();
        cache_type = meta_handle()->get_cache_type();
        is_write_cache = meta_handle()->get_is_write_cache();
        is_in_fifo_list = meta_handle()->get_is_in_fifo_list();
        ref_cnt = meta_handle()->get_ref_cnt();
        access_cnt = meta_handle()->get_access_cnt();
      }
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
            SERVER_LOG(WARN, "fail to execute ip_to_string", KR(ret), K(addr));
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
        case MACRO_ID: {
          if (OB_FAIL(get_macro_id_buf(macro_id, macro_id_buf_, sizeof(macro_id_buf_)))) {
            SERVER_LOG(WARN, "fail to get macro id buf", KR(ret), K(macro_id));
          } else {
            cells[i].set_varchar(macro_id_buf_);
          }
          break;
        }
        case TABLET_ID: {
          cells[i].set_int(tablet_id.id());
          break;
        }
        case OBJECT_TYPE: {
          cells[i].set_varchar(get_storage_objet_type_str(macro_id.storage_object_type()));
          break;
        }
        case SIZE: {
          cells[i].set_int(size);
          break;
        }
        case LAST_ACCESS_TIME: {
          cells[i].set_timestamp(last_access_time_us);
          break;
        }
        case CACHE_TYPE: {
          cells[i].set_varchar(get_ss_macro_cache_type_str(cache_type));
          break;
        }
        case IS_WRITE_CACHE: {
          cells[i].set_bool(is_write_cache);
          break;
        }
        case IS_IN_FIFO_LIST: {
          cells[i].set_bool(is_in_fifo_list);
          break;
        }
        case REF_CNT: {
          cells[i].set_int(ref_cnt);
          break;
        }
        case ACCESS_CNT: {
          cells[i].set_int(access_cnt);
          break;
        }
        case MACRO_ID_STR: {
          if (OB_FAIL(get_macro_id_str_buf(macro_id, macro_id_str_buf_, sizeof(macro_id_str_buf_)))) {
            SERVER_LOG(WARN, "fail to get macro id str buf", KR(ret), K(macro_id));
          } else {
            cells[i].set_varchar(macro_id_str_buf_);
          }
          break;
        }
        case LOCAL_PATH: {
          if (OB_FAIL(get_path_buf(macro_id, true/*is_local_cache*/))) {
            SERVER_LOG(WARN, "fail to get path buf", KR(ret), K(macro_id));
          } else {
            cells[i].set_varchar(local_path_buf_);
          }
          break;
        }
        case REMOTE_PATH: {
          if (OB_FAIL(get_path_buf(macro_id, false/*is_local_cache*/))) {
            SERVER_LOG(WARN, "fail to get path buf", KR(ret), K(macro_id));
          } else {
            cells[i].set_varchar(remote_path_buf_);
          }
          break;
        }
        case INFO: {
          // fill info if need later
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", KR(ret), K(col_id));
          break;
        }
      } // end switch
    } // end for
    if (OB_SUCC(ret)) {
      row = &cur_row_;
      arr_idx_++;
    }
  }
#endif
  return ret;
}

void ObAllVirtualSSMacroCacheInfo::release_last_tenant()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
#ifdef OB_BUILD_SHARED_STORAGE
  macro_cache_meta_arr_.reset();
#endif
}

bool ObAllVirtualSSMacroCacheInfo::is_need_process(uint64_t tenant_id)
{
  return (is_sys_tenant(effective_tenant_id_) || (tenant_id == effective_tenant_id_));
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObAllVirtualSSMacroCacheInfo::get_meta_if_need()
{
  int ret = OB_SUCCESS;
  bool is_complete = false;
  while (OB_SUCC(ret) && !is_complete) {
    if (arr_idx_ > macro_cache_meta_arr_.count()) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "arr idx is greater than macro cache meta arr count", KR(ret), K_(arr_idx),
                 "arr_cnt", macro_cache_meta_arr_.count());
    } else if (arr_idx_ < macro_cache_meta_arr_.count()) {
      // no need to get meta, exists un-consumed meta in macro_cache_meta_arr_
      is_complete = true;
    } else { // arr_idx_ == macro_cache_meta_arr_.count()
      if (bucket_idx_ > bucket_count_) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "bucket idx is greater than bucket count", KR(ret), K_(bucket_idx), K_(bucket_count));
      } else if (bucket_idx_ == bucket_count_) {
        // no need to get meta, already ITER_END
        is_complete = true;
      } else { // bucket_idx_ < bucket_count_
        macro_cache_meta_arr_.reuse();
        ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
        if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "ObSSMacroCacheMgr is NULL", KR(ret));
        } else if (OB_FAIL(macro_cache_mgr->get_meta_by_bucket(bucket_idx_, macro_cache_meta_arr_))) {
          SERVER_LOG(WARN, "fail to get meta by bucket", KR(ret), K_(bucket_idx));
        } else {
          bucket_idx_++;
          arr_idx_ = 0;
          if (macro_cache_meta_arr_.count() > 0) {
            is_complete = true;
          } else {
            // Note: this bucket has no meta entry, need to get meta from next bucket
            is_complete = false;
          }
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualSSMacroCacheInfo::get_macro_id_buf(
    const MacroBlockId &macro_id,
    char *buf,
    const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY((!macro_id.is_valid()) || (buf_len <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", KR(ret), K(macro_id), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "%016lX_%016lX_%016lX_%016lX", macro_id.first_id(),
                     macro_id.second_id(), macro_id.third_id(), macro_id.fourth_id()))) {
    SERVER_LOG(WARN, "fail to print macro id", KR(ret), K(macro_id), K(buf_len));
  } else {
    buf[buf_len - 1] = '\0'; // defence
  }
  return ret;
}

int ObAllVirtualSSMacroCacheInfo::get_macro_id_str_buf(
    const MacroBlockId &macro_id,
    char *buf,
    const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY((!macro_id.is_valid()) || (buf_len <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", KR(ret), K(macro_id), KP(buf), K(buf_len));
  } else {
    macro_id.to_string(buf, buf_len);
    buf[buf_len - 1] = '\0'; // defence
  }
  return ret;
}

int ObAllVirtualSSMacroCacheInfo::get_path_buf(
  const MacroBlockId &macro_id,
  const bool is_local_cache)
{
  int ret = OB_SUCCESS;
  ObPathContext ctx;
  const char *root_dir = nullptr;
  if (is_local_cache) { // 1. local path
    root_dir = OB_DIR_MGR.get_local_cache_root_dir();
  } else { // 2. remote path
    char *object_storage_root_dir = nullptr;
    if (OB_FAIL(OB_DIR_MGR.get_object_storage_root_dir(object_storage_root_dir))) {
      SERVER_LOG(WARN, "fail to get object storage root dir", KR(ret));
    } else {
      root_dir = object_storage_root_dir;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(root_dir)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "root dir is null", KR(ret));
  } else if (OB_FAIL(ctx.set_file_ctx(macro_id, 0/*ls_epoch_id*/, is_local_cache))) {
    if (OB_NOT_SUPPORTED == ret) {
      // ignore ret, and display empty path
      ret = OB_SUCCESS;
      SERVER_LOG(TRACE, "ignore not supported path convert", K(macro_id), K(is_local_cache));
      char *path_buf = (is_local_cache ? local_path_buf_ : remote_path_buf_);
      path_buf[0] = '\0';
    } else {
      SERVER_LOG(WARN, "fail to set file ctx", KR(ret), K(macro_id));
    }
  } else {
    char *path_buf = (is_local_cache ? local_path_buf_ : remote_path_buf_);
    // Note: '+ 1' to skip the '/', which connects the root dir and the path
    MEMCPY(path_buf, ctx.get_path() + STRLEN(root_dir) + 1, MAX_PATH_SIZE - STRLEN(root_dir) - 1);
    path_buf[MAX_PATH_SIZE - 1] = '\0'; // defence
  }
  return ret;
}
#endif

} // namespace observer
} // namespace oceanbase