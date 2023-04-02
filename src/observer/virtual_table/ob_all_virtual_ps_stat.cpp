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

#include "observer/virtual_table/ob_all_virtual_ps_stat.h"

#include "observer/ob_server_utils.h"
#include "observer/ob_server_struct.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "sql/plan_cache/ob_ps_cache.h"

using namespace oceanbase;
using namespace sql;
using namespace observer;
using namespace common;

int ObAllVirtualPsStat::inner_open()
{
  int ret = OB_SUCCESS;
  // sys tenant show all tenant infos
  if (is_sys_tenant(effective_tenant_id_)) {
    if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_id_array_))) {
      SERVER_LOG(WARN, "failed to add tenant id", K(ret));
    }
  } else {
    // user tenant show self tenant infos
    if (OB_FAIL(tenant_id_array_.push_back(effective_tenant_id_))) {
      SERVER_LOG(WARN, "failed to push back tenant id", KR(ret), K(effective_tenant_id_),
          K(tenant_id_array_));
    }
  }
  return ret;
}

int ObAllVirtualPsStat::fill_cells(ObPsCache &ps_cache, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  int64_t col_count = output_column_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case share::ALL_VIRTUAL_PS_STAT_CDE::TENANT_ID: {
        cells[i].set_int(tenant_id);
        break;
      }
      case share::ALL_VIRTUAL_PS_STAT_CDE::SVR_IP: {
        ObString ipstr;
        if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
          SERVER_LOG(WARN, "get server ip failed", K(ret));
        } else {
          cells[i].set_varchar(ipstr);
          cells[i].set_collation_type(ObCharset::get_default_collation(
                                      ObCharset::get_default_charset()));
        }
        break;
      }
      case share::ALL_VIRTUAL_PS_STAT_CDE::SVR_PORT: {
        cells[i].set_int(GCTX.self_addr().get_port());
        break;
      }
      case share::ALL_VIRTUAL_PS_STAT_CDE::STMT_COUNT: {
        // @shaoge 以stmt_id_map的大小作为ps_cache的大小
        cells[i].set_int(ps_cache.get_stmt_id_map_size());
        break;
      }
      case share::ALL_VIRTUAL_PS_STAT_CDE::HIT_COUNT: {
        uint64_t hit_count = ps_cache.get_hit_count();
        cells[i].set_int(hit_count);
        break;
      }
      case share::ALL_VIRTUAL_PS_STAT_CDE::ACCESS_COUNT: {
        uint64_t access_count = ps_cache.get_access_count();
        cells[i].set_int(access_count);
        break;
      }
      case share::ALL_VIRTUAL_PS_STAT_CDE::MEM_HOLD: {
        int64_t mem_total = 0;
        if (OB_FAIL(ps_cache.mem_total(mem_total))) {
          SERVER_LOG(WARN, "ps_cache.mem_total failed", K(ret));
        } else {
          cells[i].set_int(mem_total);
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid column id", K(ret), K(i), K(output_column_ids_), K(col_id));
        break;
      }
    }
  }
  return ret;
}

int ObAllVirtualPsStat::inner_get_next_row()
{
  int ret = OB_SUCCESS;

  if (tenant_id_array_idx_ >= tenant_id_array_.count()) {
    ret = OB_ITER_END;
  } else {
    uint64_t tenant_id = tenant_id_array_.at(tenant_id_array_idx_);
    ++tenant_id_array_idx_;
    MTL_SWITCH(tenant_id) {
      ObPsCache *ps_cache = MTL(ObPsCache*);
      if (OB_ISNULL(ps_cache)) {
        SERVER_LOG(DEBUG, "ps_cache is NULL, ignore this", K(ret), K(tenant_id),
            K(effective_tenant_id_));
      } else if (false == ps_cache->is_inited()) {
        SERVER_LOG(DEBUG, "ps_cache is not init, ignore this", K(ret));
      } else if (OB_FAIL(fill_cells(*ps_cache, tenant_id))) {
        SERVER_LOG(WARN, "fill_cells failed", K(ret), K(tenant_id));
      } else {
        SERVER_LOG(DEBUG, "fill_cells succeed", K(tenant_id), K(effective_tenant_id_),
            K(tenant_id_array_), K(tenant_id_array_idx_));
      }
    }
    // ignore error
    if (ret != OB_SUCCESS) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}
