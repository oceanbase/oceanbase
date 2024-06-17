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
#include "sql/das/ob_das_def_reg.h"
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
  static int reshape_datum_value(const ObObjMeta &col_type,
                                 const ObAccuracy &col_accuracy,
                                 const bool enable_oracle_empty_char_reshape_to_null,
                                 ObIAllocator &allocator,
                                 blocksstable::ObStorageDatum &datum_value);
  static int padding_fixed_string_value(int64_t max_len, ObIAllocator &alloc, ObObj &value);
  static int wait_das_retry(int64_t retry_cnt);
  static int find_child_das_def(const ObDASBaseCtDef *root_ctdef,
                                ObDASBaseRtDef *root_rtdef,
                                ObDASOpType op_type,
                                const ObDASBaseCtDef *&target_ctdef,
                                ObDASBaseRtDef *&target_rtdef);
  template <typename CtDefType, typename RtDefType>
  static int find_target_das_def(const ObDASBaseCtDef *root_ctdef,
                                 ObDASBaseRtDef *root_rtdef,
                                 ObDASOpType op_type,
                                 const CtDefType *&target_ctdef,
                                 RtDefType *&target_rtdef)
  {
    int ret = common::OB_SUCCESS;
    const ObDASBaseCtDef *base_ctdef = nullptr;
    ObDASBaseRtDef *base_rtdef = nullptr;
    if (OB_FAIL(find_child_das_def(root_ctdef, root_rtdef, op_type, base_ctdef, base_rtdef))) {
      SQL_DAS_LOG(WARN, "find chld das def failed", K(ret));
    } else if (OB_ISNULL(base_ctdef) || OB_ISNULL(base_rtdef)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_DAS_LOG(WARN, "can not find the target op def", K(ret), K(op_type), KP(base_ctdef), KP(base_rtdef));
    } else {
      target_ctdef = static_cast<const CtDefType*>(base_ctdef);
      target_rtdef = static_cast<RtDefType*>(base_rtdef);
    }
    return ret;
  }
  static int generate_mlog_row(const common::ObTabletID &tablet_id,
                               const storage::ObDMLBaseParam &dml_param,
                               common::ObNewRow &row,
                               ObDASOpType op_type,
                               bool is_old_row);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_DAS_OB_DAS_UTILS_H_ */
