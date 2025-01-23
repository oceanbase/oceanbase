/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_FTS_CG_SERVICE_H_
#define OCEANBASE_OBSERVER_OB_TABLE_FTS_CG_SERVICE_H_
#include "observer/table/ob_table_context.h"

namespace oceanbase
{
namespace table
{
class ObTableFtsExprCgService
{
public:
  static int fill_doc_id_expr_param(ObTableCtx &ctx, ObRawExpr *&doc_id_expr);
  static bool need_calc_doc_id(ObTableCtx &ctx);
  static int generate_text_retrieval_dep_exprs(ObTableCtx &ctx, common::ObIAllocator &allocator);
  static int add_all_text_retrieval_scan_dep_exprs(ObTableCtx &ctx);
private:
  static int generate_match_against_exprs(ObTableCtx &ctx,
                                          ObMatchFunRawExpr *&match_expr,
                                          ObRawExpr *&pushdown_match_filter);
  static int generate_topn_related_params(ObTableCtx &ctx, ObTextRetrievalInfo &tr_info);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableFtsExprCgService);
};

class ObTableFtsDmlCgService
{
public:
  static int generate_scan_with_doc_id_ctdef_if_need(ObTableCtx &ctx,
                                                     ObIAllocator &allocator,
                                                     ObDASScanCtDef &scan_ctdef,
                                                     ObDASAttachSpec &attach_spec);
private:
  static int generate_rowkey_doc_ctdef(ObTableCtx &ctx,
                                       ObIAllocator &allocator,
                                       ObDASAttachSpec &attach_spec,
                                       ObDASScanCtDef *&rowkey_doc_scan_ctdef);
  static int generate_rowkey_doc_das_ctdef(ObTableCtx &ctx, ObDASScanCtDef &rowkey_doc_scan_ctdef);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableFtsDmlCgService);
};

class ObTableFtsTscCgService
{
public:
  ObTableFtsTscCgService() {}
  virtual ~ObTableFtsTscCgService() {}
  static int extract_rowkey_doc_exprs(const ObTableCtx &ctx,
                                         common::ObIArray<ObRawExpr*> &rowkey_doc_exprs);
  static int extract_doc_rowkey_exprs(const ObTableCtx &ctx,
                                      common::ObIArray<ObRawExpr*> &doc_rowkey_exprs);
  static int extract_text_ir_das_output_column_ids(const ObTableCtx &ctx,
                                                   ObDASScanCtDef &scan_ctdef,
                                                   ObIArray<uint64_t> &tsc_out_cols);
  static int generate_das_scan_ctdef_with_doc_id(ObIAllocator &alloc,
                                                 const ObTableCtx &ctx,
                                                 ObTableApiScanCtDef &tsc_ctdef,
                                                 ObDASScanCtDef *scan_ctdef,
                                                 ObDASDocIdMergeCtDef *&doc_id_merge_ctdef);
  static int get_fts_schema(const ObTableCtx &ctx, uint64_t table_id, const ObTableSchema *&index_schema);
  static int extract_text_ir_access_columns(const ObTableCtx &ctx,
                                            ObDASScanCtDef &scan_ctdef,
                                            ObIArray<ObRawExpr *> &access_expr);
  static int generate_text_ir_pushdown_expr_ctdef(const ObTableCtx &ctx, ObDASScanCtDef &scan_ctdef);
  static int generate_text_ir_ctdef(const ObTableCtx &ctx,
                                    ObIAllocator &allocator,
                                    ObTableApiScanCtDef &tsc_ctdef,
                                    ObDASBaseCtDef *&root_ctdef);
private:
  static int generate_text_ir_spec_exprs(const ObTableCtx &ctx, ObDASIRScanCtDef &text_ir_scan_ctdef);
  static int generate_das_sort_ctdef(const ObTableCtx &ctx,
                                     ObIAllocator &allocator,
                                     ObTextRetrievalInfo &tr_info,
                                     ObDASBaseCtDef *child_ctdef,
                                     ObDASSortCtDef *&sort_ctdef);
  static int generate_doc_id_lookup_ctdef(const ObTableCtx &ctx,
                                          ObIAllocator &allocator,
                                          ObTableApiScanCtDef &tsc_ctdef,
                                          ObDASBaseCtDef *ir_scan_ctdef,
                                          ObExpr *doc_id_expr,
                                          ObDASIRAuxLookupCtDef *&aux_lookup_ctdef);
  static int append_fts_relavence_project_col(ObDASIRAuxLookupCtDef *aux_lookup_ctdef,
                                              ObDASIRScanCtDef *ir_scan_ctdef);
  static int generate_rowkey_doc_ctdef(ObIAllocator &alloc,
                                       const ObTableCtx &ctx,
                                       ObTableApiScanCtDef &tsc_ctdef,
                                       ObDASScanCtDef *&rowkey_doc_scan_ctdef);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableFtsTscCgService);
};

} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_FTS_CG_SERVICE_H_ */