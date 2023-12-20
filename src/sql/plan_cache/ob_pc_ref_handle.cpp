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

#define USING_LOG_PREFIX SQL_PC

#include "ob_pc_ref_handle.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
const char* ObCacheRefHandleMgr::handle_name(const CacheRefHandleID handle_id)
{
  OB_ASSERT(handle_id < MAX_HANDLE && handle_id >= 0);
  static const char* handle_names[] = {
    "pc_ref_plan_local_handle",
    "pc_ref_plan_remote_handle",
    "pc_ref_plan_dist_handle",
    "pc_ref_plan_arr_handle",
    "pc_ref_plan_stat_handle",
    "pc_ref_pl_handle",
    "pc_ref_pl_stat_handle",
    "plan_gen_handle",
    "cli_query_handle",
    "outline_exec_handle",
    "plan_explain_handle",
    "check_evolution_plan_handle",
    "load_baseline_handle",
    "ps_exec_handle",
    "gv_sql_handle",
    "pl_anon_handle",
    "pl_routine_handle",
    "package_var_handle",
    "package_type_handle",
    "package_spec_handle",
    "package_body_handle",
    "package_resv_handle",
    "get_pkg_handle",
    "index_builder_handle",
    "pcv_set_handle",
    "pcv_wr_handle",
    "pcv_rd_handle",
    "pcv_get_plan_key_handle",
    "pcv_get_pl_key_handle",
    "pcv_expire_by_used_handle",
    "pcv_expire_by_mem_handle",
    "lc_ref_cache_node_handle",
    "lc_node_handle",
    "lc_node_rd_handle",
    "lc_node_wr_handle",
    "lc_ref_cache_obj_stat_handle",
    "plan_baseline_handle",
    "tableapi_node_handle",
    "sql_plan_handle",
    "callstmt_handle",
    "pc_diag_handle"
  };
  static_assert(sizeof(handle_names)/sizeof(const char*) == MAX_HANDLE, "invalid handle name array");
  if (handle_id < MAX_HANDLE) {
    return handle_names[handle_id];
  } else {
    return "invalid handle";
  }
}

int ObCacheRefHandleMgr::dump_handle_info(ObIAllocator &allocator,
                                          ObString &dump_info)
{
  int ret = OB_SUCCESS;
  const int handle_len = 128;
  const int dump_len = MAX_HANDLE * handle_len;
  char *dump_buf = NULL;
  int64_t pos = 0;
  if (OB_ISNULL(dump_buf = (char *)allocator.alloc(dump_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_PC_LOG(WARN, "failed to allocate memory", K(ret));
  } else if (OB_FAIL(databuff_printf(dump_buf, dump_len, pos, "Cache Ref Handle Infos: {"))) {
    SQL_PC_LOG(WARN, "failed to databuff_printf", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < MAX_HANDLE; i++) {
      if (MAX_HANDLE - 1 != i) {
        if (OB_FAIL(databuff_printf(dump_buf, dump_len, pos, "%s: %ld, ",
                                     handle_name(static_cast<CacheRefHandleID>(i)),
                                     CACHE_REF_HANDLES[i]))) {
          SQL_PC_LOG(WARN, "failed to databuff_printf", K(ret));
        }
      } else if (OB_FAIL(databuff_printf(dump_buf, dump_len, pos, "%s: %ld}",
                                         handle_name(static_cast<CacheRefHandleID>(i)),
                                         CACHE_REF_HANDLES[i]))) {
        SQL_PC_LOG(WARN, "failed to databuff_print", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      dump_info.assign_ptr(dump_buf, pos);
    }
  }
  return ret;
}
} // end namespace sql

} // end namespace oceanbase


