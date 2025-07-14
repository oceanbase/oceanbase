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

#include "ob_all_virtual_tenant_vector_mem_info.h"
#include "lib/alloc/memory_dump.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace observer
{

ObAllVirtualTenantVectorMemInfo::ObAllVirtualTenantVectorMemInfo()
    : ObVirtualTableScannerIterator(),
      addr_(),
      current_pos_(0)
{
}

ObAllVirtualTenantVectorMemInfo::~ObAllVirtualTenantVectorMemInfo()
{
  reset();
}

void ObAllVirtualTenantVectorMemInfo::reset()
{
  current_pos_ = 0;
  addr_.reset();
  ObVirtualTableScannerIterator::reset();
}

int64_t ObAllVirtualTenantVectorMemInfo::fill_glibc_used_info(uint64_t tenant_id)
{
  int64_t used_size = 0;
  for (it_ = malloc_sample_map_.begin(); it_ != malloc_sample_map_.end(); ++it_) {
    if (tenant_id == it_->first.tenant_id_) {
      if (0 == STRNCMP("VIndex", it_->first.label_, strlen("VIndex")) &&
          0 == STRNCMP("GLIBC", get_global_ctx_info().get_ctx_name(it_->first.ctx_id_), strlen("GLIBC"))) {
        used_size += it_->second.alloc_bytes_;
      }
    }
  }
  return used_size;
}

int ObAllVirtualTenantVectorMemInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (!start_to_read_) {
    ObObj *cells = NULL;
    // allocator_ is allocator of PageArena type, no need to free
    if (NULL == (cells = cur_row_.cells_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
    } else {
      uint64_t tenant_id = OB_INVALID_ID;
      char ip_buf[common::OB_IP_STR_BUFF];
      omt::ObMultiTenant *omt = GCTX.omt_;
      omt::TenantIdList current_ids(nullptr, ObModIds::OMT);
      if (OB_ISNULL(omt)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "omt is null", K(ret));
      } else if (OB_FAIL(malloc_sample_map_.create(1000, "MallocInfoMap", "MallocInfoMap"))) {
        SERVER_LOG(WARN, "create memory info map failed", K(ret));
      } else if (OB_FAIL(ObMemoryDump::get_instance().load_malloc_sample_map(malloc_sample_map_))) {
        SERVER_LOG(WARN, "load memory info map failed", K(ret));
      } else {
        omt->get_tenant_ids(current_ids);
      }
      // does not check ret code, we need iter all the tenant.
      for (int64_t i = 0; i < current_ids.size(); ++i) {
        tenant_id = current_ids.at(i);
        int64_t manage_used = 0;
        int64_t vector_hold = 0;
        int64_t vector_limit = 0;
        int64_t freeze_cnt = 0;
        if (is_virtual_tenant_id(tenant_id)
            || (!is_sys_tenant(effective_tenant_id_) && tenant_id != effective_tenant_id_)) {
          continue;
        }
        MTL_SWITCH(tenant_id) {
          ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService*);
          ObSharedMemAllocMgr *shared_mem_mgr = MTL(ObSharedMemAllocMgr*);
          manage_used = service->get_allocator().used();
          vector_hold = shared_mem_mgr->vector_allocator().hold();
          int64_t rb_used = shared_mem_mgr->vector_allocator().get_rb_mem_used();
          int64_t vector_used = shared_mem_mgr->vector_allocator().used();
          int64_t pos = 0;
          int64_t glibc_used = fill_glibc_used_info(tenant_id);
          MEMSET(vector_used_str_, 0, sizeof(vector_used_str_));
          complete_tablet_ids_.reset();
          partial_tablet_ids_.reset();
          cache_tablet_ids_.reset();
          if (OB_FAIL(service->get_snapshot_ids(complete_tablet_ids_, partial_tablet_ids_))) {
            SERVER_LOG(WARN, "failed to get snapshot_ids", K(ret));
          } else if (OB_FAIL(service->get_cache_ids(cache_tablet_ids_))) {
            SERVER_LOG(WARN, "failed to get cache_ids", K(ret));
          } else if (OB_FAIL(databuff_printf(vector_used_str_, OB_MAX_MYSQL_VARCHAR_LENGTH, pos, "{\"rb_used\":%lu", rb_used))) {
            SERVER_LOG(WARN, "failed to print total vector mem usage", K(ret), K(vector_hold));
          } else if (OB_FAIL(ObPluginVectorIndexUtils::get_mem_context_detail_info(service, complete_tablet_ids_,
             partial_tablet_ids_, cache_tablet_ids_, vector_used_str_, OB_MAX_MYSQL_VARCHAR_LENGTH, pos))) {
            SERVER_LOG(WARN, "failed to print vector mem usage detail", K(ret), K(vector_hold));
          } else if (OB_FAIL(databuff_printf(vector_used_str_, OB_MAX_MYSQL_VARCHAR_LENGTH, pos, "}"))) {
            SERVER_LOG(WARN, "failed to print total vector mem usage", K(ret));
          }
          vector_limit = shared_mem_mgr->share_resource_throttle_tool().get_resource_limit<ObTenantVectorAllocator>();
          int64_t tx_share_limit = shared_mem_mgr->share_resource_throttle_tool().get_resource_limit<FakeAllocatorForTxShare>();
          for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
            uint64_t col_id = output_column_ids_.at(i);
            switch (col_id) {
              case SVR_IP:
                if (!addr_.ip_to_string(ip_buf, sizeof(ip_buf))) {
                  STORAGE_LOG(ERROR, "ip to string failed");
                  ret = OB_ERR_UNEXPECTED;
                } else {
                  cells[i].set_varchar(ip_buf);
                  cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                }
                break;
              case SVR_PORT:
                cells[i].set_int(addr_.get_port());
                break;
              case TENANT_ID:
                cells[i].set_int(tenant_id);
                break;
              case RAW_MALLOC_SIZE:
                cells[i].set_int(glibc_used);
                break;
              case INDEX_METADATA_SIZE:
                cells[i].set_int(manage_used);
                break;
              case VECTOR_MEM_HOLD:
                cells[i].set_int(vector_hold);
                break;
              case VECTOR_MEM_USED:
                cells[i].set_int(vector_used);
                break;
              case VECTOR_MEM_LIMIT:
                cells[i].set_int(vector_limit);
                break;
              case TX_SHARE_LIMIT:
                cells[i].set_int(tx_share_limit);
                break;
              case VECTOR_MEM_DETAIL_INFO:
                cells[i].set_varchar(vector_used_str_);
                cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                break;
              default:
                // abnormal column id
                ret = OB_ERR_UNEXPECTED;
                SERVER_LOG(WARN, "unexpected column id", K(ret));
                break;
            }
          }
          if (OB_SUCCESS == ret
              && OB_SUCCESS != (ret = scanner_.add_row(cur_row_))) {
            SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
          }
        }
      }
      // always start to read, event it failed.
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  // always get next row, if we have start to read.
  if (start_to_read_) {
    if (OB_SUCCESS != (ret = scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
