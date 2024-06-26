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

#ifndef DEV_SRC_SQL_CODE_GENERATOR_OB_TSC_CG_SERVICE_H_
#define DEV_SRC_SQL_CODE_GENERATOR_OB_TSC_CG_SERVICE_H_
#include "sql/optimizer/ob_log_table_scan.h"
namespace oceanbase
{
namespace sql
{
class ObStaticEngineCG;
class ObTableScanSpec;
class ObPushdownExprSpec;
struct ObTableScanCtDef;
struct ObDASScanCtDef;
struct AgentVtAccessMeta;
//help to cg the tsc ctdef
class ObTscCgService
{
public:
  ObTscCgService(ObStaticEngineCG &cg)
    : cg_(cg)
  { }

  int generate_tsc_ctdef(ObLogTableScan &op, ObTableScanCtDef &tsc_ctdef);
  int generate_agent_vt_access_meta(const ObLogTableScan &op, ObTableScanSpec &spec);
  int generate_tsc_filter(const ObLogTableScan &op, ObTableScanSpec &spec);
  int generate_pd_storage_flag(const ObLogPlan *log_plan,
                               const uint64_t ref_table_id,
                               const ObIArray<ObRawExpr *> &access_exprs,
                               const log_op_def::ObLogOpType op_type,
                               const bool is_global_index_lookup,
                               ObPushdownExprSpec &pd_spec);
  int generate_table_loc_meta(uint64_t table_loc_id,
                              const ObDMLStmt &stmt,
                              const share::schema::ObTableSchema &table_schema,
                              const ObSQLSessionInfo &session,
                              ObDASTableLocMeta &loc_meta);
  int generate_das_result_output(const common::ObIArray<uint64_t> &output_cids,
                                 ObDASScanCtDef &scan_ctdef,
                                 const ObRawExpr *trans_info_expr,
                                 const bool include_agg = false);
private:
  int generate_access_ctdef(const ObLogTableScan &op, ObDASScanCtDef &scan_ctdef, bool &has_rowscn);
  int generate_pushdown_aggr_ctdef(const ObLogTableScan &op, ObDASScanCtDef &scan_ctdef);
  int generate_das_scan_ctdef(const ObLogTableScan &op, ObDASScanCtDef &scan_ctdef, bool &has_rowscn);
  int generate_table_param(const ObLogTableScan &op, ObDASScanCtDef &scan_ctdef, common::ObIArray<uint64_t> &tsc_out_cols);
  int extract_das_output_column_ids(const ObLogTableScan &op,
                                    ObDASScanCtDef &scan_ctdef,
                                    const ObTableSchema &index_schema,
                                    common::ObIArray<uint64_t> &output_cids);

  int extract_das_access_exprs(const ObLogTableScan &op,
                               ObDASScanCtDef &scan_ctdef,
                               common::ObIArray<ObRawExpr*> &access_exprs);
  //extract these column exprs need by TSC operator, these column will output by DAS scan
  int extract_tsc_access_columns(const ObLogTableScan &op, common::ObIArray<ObRawExpr*> &access_exprs);
  int extract_das_column_ids(const common::ObIArray<ObRawExpr*> &column_exprs, common::ObIArray<uint64_t> &column_ids);
  int generate_geo_access_ctdef(const ObLogTableScan &op, const ObTableSchema &index_schema, ObArray<ObRawExpr*> &access_exprs);
  int generate_text_ir_ctdef(const ObLogTableScan &op, ObTableScanCtDef &tsc_ctdef, ObDASBaseCtDef *&root_ctdef);
  int extract_text_ir_access_columns(const ObLogTableScan &op,
                                     const ObDASScanCtDef &scan_ctdef,
                                     ObIArray<ObRawExpr*> &access_exprs);
  int extract_text_ir_das_output_column_ids(const ObLogTableScan &op,
                                            const ObDASScanCtDef &scan_ctdef,
                                            ObIArray<uint64_t> &output_cids);
  int generate_text_ir_pushdown_expr_ctdef(const ObLogTableScan &op, ObDASScanCtDef &scan_ctdef);
  int generate_text_ir_spec_exprs(const ObLogTableScan &op,
                                  ObDASIRScanCtDef &text_ir_scan_ctdef);
  int generate_doc_id_lookup_ctdef(const ObLogTableScan &op,
                                   ObTableScanCtDef &tsc_ctdef,
                                   ObDASBaseCtDef *ir_scan_ctdef,
                                   ObExpr *doc_id_expr,
                                   ObDASIRAuxLookupCtDef *&aux_lookup_ctdef);
  int generate_table_lookup_ctdef(const ObLogTableScan &op,
                                  ObTableScanCtDef &tsc_ctdef,
                                  ObDASBaseCtDef *scan_ctdef,
                                  ObDASTableLookupCtDef *&lookup_ctdef);
  int extract_doc_id_index_back_access_columns(const ObLogTableScan &op,
                                               ObIArray<ObRawExpr *> &access_exprs);
  int extract_doc_id_index_back_output_column_ids(const ObLogTableScan &op,
                                                  ObIArray<uint64_t> &output_cids);
  int filter_out_match_exprs(ObIArray<ObRawExpr*> &exprs);
  int append_fts_relavence_project_col(
      ObDASIRAuxLookupCtDef *aux_lookup_ctdef,
      ObDASIRScanCtDef *ir_scan_ctdef);
  int generate_das_sort_ctdef(const ObIArray<OrderItem> &sort_keys,
                              const bool fetch_with_ties,
                              ObRawExpr *topk_limit_expr,
                              ObRawExpr *topk_offset_expr,
                              ObDASBaseCtDef *child_ctdef,
                              ObDASSortCtDef *&sort_ctdef);
  int mapping_oracle_real_agent_virtual_exprs(const ObLogTableScan &op,
                                              common::ObIArray<ObRawExpr*> &access_exprs);

private:
  ObStaticEngineCG &cg_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_CODE_GENERATOR_OB_TSC_CG_SERVICE_H_ */
