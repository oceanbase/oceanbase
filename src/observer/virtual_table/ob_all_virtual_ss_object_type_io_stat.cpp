/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_all_virtual_ss_object_type_io_stat.h"
#include "share/ob_server_struct.h"
#include "share/ash/ob_di_util.h"
#include "storage/blocksstable/ob_storage_object_type.h"
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;

namespace oceanbase
{
namespace observer
{

ObAllVirtualSSObjectTypeIoStat::ObAllVirtualSSObjectTypeIoStat()
    : tenant_id_(OB_INVALID_TENANT_ID),
      cur_idx_(0)
{
  ip_buf_[0] = '\0';
}

ObAllVirtualSSObjectTypeIoStat::~ObAllVirtualSSObjectTypeIoStat()
{
  reset();
}

void ObAllVirtualSSObjectTypeIoStat::reset()
{
  ip_buf_[0] = '\0';
  tenant_id_ = OB_INVALID_TENANT_ID;
  cur_idx_ = 0;
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualSSObjectTypeIoStat::inner_open()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObAllVirtualSSObjectTypeIoStat::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      SERVER_LOG(WARN, "execute fail", KR(ret));
    }
  }
  return ret;
}
#ifdef OB_BUILD_SHARED_STORAGE
int ObAllVirtualSSObjectTypeIoStat::get_object_type_stat(const int64_t cur_idx_, ObStorageObjectType &object_type,
  ObSSObjectTypeStat &ss_object_type_stat, ObSSObjectTypeCachedStat &ss_object_type_cached_stat, bool &is_remote)
{
  int ret = OB_SUCCESS;
  is_remote = false;
  int64_t idx = cur_idx_;
  if (idx >= SS_OBJECT_MAX_TYPE_VAL) {
    is_remote = true;
    idx -= SS_OBJECT_MAX_TYPE_VAL;
  }
  object_type = static_cast<ObStorageObjectType>(idx);
  ObSSLocalCacheService *local_cache_service = nullptr;
  if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService *))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ObSSLocalCacheService is NULL", KR(ret));
  } else if (OB_FAIL(local_cache_service->get_object_type_stat(object_type, is_remote, ss_object_type_stat))) {
    SERVER_LOG(WARN, "fail to get ss object type stat", KR(ret), "object_type", STI(object_type));
  } else if (OB_FAIL(local_cache_service->get_object_type_cached_stat(object_type, is_remote, ss_object_type_cached_stat))) {
    SERVER_LOG(WARN, "fail to get ss object type cached stat", KR(ret), "object_type", STI(object_type));
  }
  return ret;
}
#endif
int ObAllVirtualSSObjectTypeIoStat::process_curr_tenant(common::ObNewRow *&row)
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
  if (MTL_ID() != tenant_id_) {
    tenant_id_ = MTL_ID();
    cur_idx_ = 0;
  }
#ifdef OB_BUILD_SHARED_STORAGE
  ObSSLocalCacheService *local_cache_service = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService *))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ObSSLocalCacheService is NULL", KR(ret));
  } else if (OB_UNLIKELY(cur_idx_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cur_idx_ is invalid", KR(ret), K(cur_idx_));
  } else if (cur_idx_ >= SS_OBJECT_MAX_TYPE_VAL * 2) {
    ret = OB_ITER_END;
  } else {
    ObStorageObjectType object_type = ObStorageObjectType::MAX;
    ObSSObjectTypeStat ss_object_type_stat;
    ObSSObjectTypeCachedStat ss_object_type_cached_stat;
    bool is_remote = false;
    if (OB_FAIL(get_object_type_stat(cur_idx_, object_type, ss_object_type_stat, ss_object_type_cached_stat, is_remote))) {
      SERVER_LOG(WARN, "fail to get ss object type stat", KR(ret), "object_type", STI(object_type));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
      const uint64_t col_id = output_column_ids_.at(i);
      ObSSBaseStat stat;
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
      case OBJECT_TYPE: {
        cells[i].set_varchar(get_storage_objet_type_str(object_type));
        break;
      }
      case MODE: {
        cells[i].set_varchar(is_remote ? "remote" : "local");
        break;
      }
      case READ_CNT: {
        ss_object_type_stat.get_stat(ObSSObjectTypeStatType::READ, stat);
        cells[i].set_int(stat.get_cnt());
        break;
      }
      case READ_SIZE: {
        ss_object_type_stat.get_stat(ObSSObjectTypeStatType::READ, stat);
        cells[i].set_int(stat.get_size());
        break;
      }
      case READ_FAIL_CNT: {
        ss_object_type_stat.get_stat(ObSSObjectTypeStatType::READ, stat);
        cells[i].set_int(stat.get_fail_cnt());
        break;
      }
      case READ_IOPS: {
        cells[i].set_int(ss_object_type_cached_stat.get_iops(ObSSObjectTypeStatType::READ));
        break;
      }
      case WRITE_CNT: {
        ss_object_type_stat.get_stat(ObSSObjectTypeStatType::WRITE, stat);
        cells[i].set_int(stat.get_cnt());
        break;
      }
      case WRITE_SIZE: {
        ss_object_type_stat.get_stat(ObSSObjectTypeStatType::WRITE, stat);
        cells[i].set_int(stat.get_size());
        break;
      }
      case WRITE_FAIL_CNT: {
        ss_object_type_stat.get_stat(ObSSObjectTypeStatType::WRITE, stat);
        cells[i].set_int(stat.get_fail_cnt());
        break;
      }
      case WRITE_IOPS: {
        cells[i].set_int(ss_object_type_cached_stat.get_iops(ObSSObjectTypeStatType::WRITE));
        break;
      }
      case DELETE_CNT: {
        ss_object_type_stat.get_stat(ObSSObjectTypeStatType::DELETE, stat);
        cells[i].set_int(stat.get_cnt());
        break;
      }
      case DELETE_FAIL_CNT: {
        ss_object_type_stat.get_stat(ObSSObjectTypeStatType::DELETE, stat);
        cells[i].set_int(stat.get_fail_cnt());
        break;
      }
      case DELETE_IOPS: {
        cells[i].set_int(ss_object_type_cached_stat.get_iops(ObSSObjectTypeStatType::DELETE));
        break;
      }
      } // end switch
    } // end for
    if (OB_SUCC(ret)) {
      row = &cur_row_;
      cur_idx_++;
    }
  }
#endif
  return ret;
}

void ObAllVirtualSSObjectTypeIoStat::release_last_tenant()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
}

bool ObAllVirtualSSObjectTypeIoStat::is_need_process(uint64_t tenant_id)
{
  return is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_;
}

} // namespace observer
} // namespace oceanbase