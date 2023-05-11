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

#ifndef DEV_SRC_SQL_DAS_OB_DAS_UTILS_H_
#define DEV_SRC_SQL_DAS_OB_DAS_UTILS_H_
#include "share/ob_define.h"
#include "share/ob_ls_id.h"
#include "share/location_cache/ob_location_struct.h"
#include "common/ob_tablet_id.h"
#include "sql/dtl/ob_dtl_task.h"
#include "sql/ob_phy_table_location.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "sql/das/ob_das_define.h"
#include "sql/das/ob_das_dml_ctx_define.h"
namespace oceanbase
{
namespace sql
{
class ObDASUtils
{
public:
  static void log_user_error_and_warn(const obrpc::ObRpcResultCode &rcode);
  static int store_warning_msg(const common::ObWarningBuffer &wb, obrpc::ObRpcResultCode &rcode);
  static int get_tablet_loc_by_id(const ObTabletID &tablet_id,
                                  ObDASTableLoc &table_loc,
                                  ObDASTabletLoc *&tablet_loc);
  static int check_nested_sql_mutating(common::ObTableID ref_table_id,
                                       ObExecContext &exec_ctx,
                                       bool is_reading = false);
  static ObDASTabletLoc *get_related_tablet_loc(const ObDASTabletLoc &tablet_loc,
                                                common::ObTableID related_table_id);
  static int build_table_loc_meta(common::ObIAllocator &allocator,
                                  const ObDASTableLocMeta &src,
                                  ObDASTableLocMeta *&dst);
  static int serialize_das_ctdefs(char *buf, int64_t buf_len, int64_t &pos,
                                  const DASDMLCtDefArray &ctdefs);
  static int64_t das_ctdefs_serialize_size(const DASDMLCtDefArray &ctdefs);
  static int deserialize_das_ctdefs(const char *buf, const int64_t data_len, int64_t &pos,
                                    common::ObIAllocator &allocator,
                                    ObDASOpType op_type,
                                    DASDMLCtDefArray &ctdefs);
  static int project_storage_row(const ObDASDMLBaseCtDef &dml_ctdef,
                                 const ObDASWriteBuffer::DmlRow &dml_row,
                                 const IntFixedArray &row_projector,
                                 common::ObIAllocator &allocator,
                                 common::ObNewRow &storage_row);
  static int reshape_storage_value(const common::ObObjMeta &col_type,
                                   const common::ObAccuracy &col_accuracy,
                                   common::ObIAllocator &allocator,
                                   common::ObObj &value);
  static int generate_spatial_index_rows(ObIAllocator &allocator,
                                         const ObDASDMLBaseCtDef &das_ctdef,
                                         const ObString &wkb_str,
                                         const IntFixedArray &row_projector,
                                         const ObDASWriteBuffer::DmlRow &dml_row,
                                         ObSpatIndexRow &spat_rows);
  static int wait_das_retry(int64_t retry_cnt);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_DAS_OB_DAS_UTILS_H_ */
