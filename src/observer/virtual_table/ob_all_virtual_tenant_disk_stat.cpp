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

#include "ob_all_virtual_tenant_disk_stat.h"
#include "observer/ob_server.h"
#include "observer/ob_server_utils.h"
#include "share/ob_tenant_mgr.h"

namespace oceanbase {
using namespace common;

namespace observer {
ObAllVirtualTenantDiskStat::ObAllVirtualTenantDiskStat()
    : ObVirtualTableIterator(),
      tenant_ids_(),
      ipstr_(),
      tenant_ids_index_(0),
      col_count_(0),
      attr_(0),
      has_start_(false)
{}

ObAllVirtualTenantDiskStat::~ObAllVirtualTenantDiskStat()
{
  reset();
}

int ObAllVirtualTenantDiskStat::inner_open()
{
  int ret = OB_SUCCESS;
  ret = tenant_ids_.reserve(OB_MAX_RESERVED_TENANT_ID - OB_INVALID_TENANT_ID);
  return ret;
}

void ObAllVirtualTenantDiskStat::reset()
{
  tenant_ids_.reset();
  ipstr_.reset();
  tenant_ids_index_ = 0;
  col_count_ = 0;
  attr_ = 0;
  has_start_ = false;
}

int ObAllVirtualTenantDiskStat::fill_tenant_ids()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == GCTX.omt_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "GCTX.omt_ shouldn't be NULL", K_(GCTX.omt), K(GCTX), K(ret));
  } else {
    omt::TenantIdList ids(NULL, ObModIds::OMT_VIRTUAL_TABLE);
    GCTX.omt_->get_tenant_ids(ids);
    for (int64_t i = 0; OB_SUCC(ret) && i < ids.size(); i++) {
      ret = tenant_ids_.push_back(ids[i]);
    }
  }

  return ret;
}

// count all types of disk used for system tenant, only sstable for other tenants
int ObAllVirtualTenantDiskStat::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K_(allocator), K(ret));
  } else if (!has_start_) {
    if (OB_FAIL(fill_tenant_ids())) {
      SERVER_LOG(WARN, "fail to fill tenant ids", K(ret));
    } else if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr_))) {
      SERVER_LOG(ERROR, "get server ip failed", K(ret));
    } else {
      has_start_ = true;
      col_count_ = output_column_ids_.count();
      attr_ = blocksstable::ObMacroBlockCommonHeader::SSTableData;
      tenant_ids_index_ = 0;
    }
  } else if (tenant_ids_index_ >= tenant_ids_.count()) {
    ret = OB_ITER_END;
  }

  if (OB_SUCC(ret)) {
    ObObj* cells = cur_row_.cells_;
    const uint64_t tenant_id = tenant_ids_.at(tenant_ids_index_);

    if (OB_UNLIKELY(NULL == cells)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
    } else {
      int64_t disk_used = 0;
      ObString attr_name = ObString("");
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
        const uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
          case TENANT_ID: {
            cells[i].set_int(static_cast<int64_t>(tenant_id));
            break;
          }
          case SVR_IP: {
            cells[i].set_varchar(ipstr_);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SVR_PORT: {
            cells[i].set_int(GCONF.self_addr_.get_port());
            break;
          }
          case ZONE: {
            cells[i].set_varchar(GCONF.zone);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case BLOCK_TYPE: {
            tmp_ret = blocksstable::ObMacroBlockCommonHeader::get_attr_name(attr_, attr_name);
            if (OB_SUCCESS != tmp_ret) {
              SERVER_LOG(WARN, "fail to get name of the macro block type", K(attr_), K(attr_name), K(ret));
              cells[i].set_varchar("unexpected type");
            } else {
              cells[i].set_varchar(attr_name);
            }
            break;
          }
          case BLOCK_SIZE: {
            tmp_ret = ObTenantManager::get_instance().get_tenant_disk_used(tenant_id, disk_used, attr_);
            if (OB_SUCCESS != tmp_ret) {
              SERVER_LOG(WARN, "fail to get size of disk used", K(tenant_id), K(attr_), K(disk_used), K(ret));
              cells[i].set_int(0);
            } else {
              cells[i].set_int(disk_used);
            }
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "unexpected column id", K(col_id), K(i), K(ret));
            break;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      row = &cur_row_;
      if (OB_SYS_TENANT_ID == tenant_id) {
        if (blocksstable::ObMacroBlockCommonHeader::MaxMacroType - 1 == attr_) {
          tenant_ids_index_++;
          attr_ = blocksstable::ObMacroBlockCommonHeader::SSTableData;
        } else if (blocksstable::ObMacroBlockCommonHeader::PartitionMeta == attr_) {
          attr_ = blocksstable::ObMacroBlockCommonHeader::MacroMeta;
        } else {
          attr_++;
        }
      } else {
        tenant_ids_index_++;
      }
    }
  } else {
    // do nothing;
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
