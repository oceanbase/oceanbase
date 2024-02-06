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

#ifndef DEV_SRC_SQL_CODE_GENERATOR_OB_DML_CG_SERVICE_H_
#define DEV_SRC_SQL_CODE_GENERATOR_OB_DML_CG_SERVICE_H_
#include "sql/optimizer/ob_log_del_upd.h"
#include "sql/das/ob_das_dml_ctx_define.h"
#include "sql/engine/dml/ob_dml_ctx_define.h"
#include "sql/engine/dml/ob_conflict_checker.h"

namespace oceanbase
{
namespace sql
{
class ObStaticEngineCG;
class ObLogDupKeyChecker;

class ObDmlCgService
{
public:
  ObDmlCgService(ObStaticEngineCG &cg)
    : cg_(cg)
  { }

  int generate_insert_ctdef(ObLogDelUpd &op,
                            const IndexDMLInfo &index_dml_info,
                            ObInsCtDef *&ins_ctdef);
  int generate_delete_ctdef(ObLogDelUpd &op,
                            const IndexDMLInfo &index_dml_info,
                            ObDelCtDef *&del_ctdef);
  int generate_update_ctdef(ObLogDelUpd &op,
                            const IndexDMLInfo &index_dml_info,
                            ObUpdCtDef *&upd_ctdef);

  int generate_insert_ctdef(ObLogDelUpd &op,
                            const IndexDMLInfo &index_dml_info,
                            ObInsCtDef &ins_ctdef);
  int generate_delete_ctdef(ObLogDelUpd &op,
                            const IndexDMLInfo &index_dml_info,
                            ObDelCtDef &del_ctdef);
  int generate_update_ctdef(ObLogDelUpd &op,
                            const IndexDMLInfo &index_dml_info,
                            ObUpdCtDef &upd_ctdef);

  int generate_lock_ctdef(ObLogForUpdate &op,
                          const IndexDMLInfo &index_dml_info,
                          ObLockCtDef *&lock_ctdef);

  int generate_merge_ctdef(ObLogMerge &op,
                           ObMergeCtDef *&merge_ctdef,
                           uint64_t idx);

  int generate_replace_ctdef(ObLogInsert &op,
                             const IndexDMLInfo &ins_index_dml_info,
                             const IndexDMLInfo &del_index_dml_info,
                             ObReplaceCtDef *&replace_ctdef);

  int generate_insert_up_ctdef(ObLogInsert &op,
                               const IndexDMLInfo &ins_index_dml_info,
                               const IndexDMLInfo &upd_index_dml_info,
                               ObInsertUpCtDef *&insert_up_ctdef);

  int generate_conflict_checker_ctdef(ObLogInsert &op,
                                      const IndexDMLInfo &index_dml_info,
                                      ObConflictCheckerCtdef &conflict_checker_ctdef);

  int generate_constraint_infos(ObLogInsert &op,
                                const IndexDMLInfo &index_dml_info,
                                ObRowkeyCstCtdefArray &cst_ctdefs);

  int generate_scan_ctdef(ObLogInsert &op,
                          const IndexDMLInfo &index_dml_info,
                          ObDASScanCtDef &scan_ctdef);

  int generate_err_log_ctdef(const ObErrLogDefine &err_log_define,
                             ObErrLogCtDef &err_log_ins_ctdef);

  int convert_data_table_rowkey_info(ObLogDelUpd &op,
                                     const IndexDMLInfo *primary_dml_info,
                                     ObInsCtDef &ins_ctdef);

  int get_table_unique_key_exprs(ObLogDelUpd &op,
                                 const IndexDMLInfo &index_dml_info,
                                 ObIArray<ObRawExpr*> &part_key_exprs);

  int table_unique_key_for_conflict_checker(ObLogDelUpd &op,
                                            const IndexDMLInfo &index_dml_info,
                                            ObIArray<ObRawExpr*> &rowkey_exprs);

  int get_heap_table_part_exprs(const ObLogicalOperator &op,
                                const IndexDMLInfo &index_dml_info,
                                ObIArray<ObRawExpr*> &part_key_exprs);

  int adjust_unique_key_exprs(ObIArray<ObRawExpr*> &unique_key_exprs);
  int get_table_rowkey_exprs(const IndexDMLInfo &index_dml_info,
                             ObIArray<ObRawExpr*> &rowkey_exprs);

  int check_is_heap_table(ObLogicalOperator &op,
                          uint64_t ref_table_id,
                          bool &is_heap_table);

private:
  int generate_dml_column_ids(const ObLogicalOperator &op,
                              const common::ObIArray<ObColumnRefRawExpr*> &columns_exprs,
                              common::ObIArray<uint64_t> &column_ids);
  int generate_updated_column_ids(const ObLogDelUpd &log_op,
                                  const ObAssignments &assigns,
                                  const common::ObIArray<uint64_t> &column_ids,
                                  common::ObIArray<uint64_t> &updated_column_ids);
  int convert_dml_column_info(common::ObTableID index_tid,
                              bool only_rowkey,
                              ObDASDMLBaseCtDef &das_dml_info);
  template<typename OldExprType, typename NewExprType>
  int generate_das_projector(const common::ObIArray<uint64_t> &dml_column_ids,
                             const common::ObIArray<uint64_t> &storage_column_ids,
                             const common::ObIArray<OldExprType*> &old_row,
                             const common::ObIArray<NewExprType*> &new_row,
                             const common::ObIArray<ObRawExpr*> &full_row,
                             ObDASDMLBaseCtDef &das_ctdef);
  template<typename ExprType>
  int add_geo_col_projector(const ObIArray<ExprType*> &cur_row,
                            const ObIArray<ObRawExpr*> &full_row,
                            const ObIArray<uint64_t> &dml_column_ids,
                            uint32_t proj_idx,
                            ObDASDMLBaseCtDef &das_ctdef,
                            IntFixedArray &row_projector);
  int get_column_ref_base_cid(const ObLogicalOperator &op, const ObColumnRefRawExpr *col, uint64_t &base_cid);
  int get_table_schema_version(const ObLogicalOperator &op, uint64_t table_id, int64_t &schema_version);
  int generate_das_dml_ctdef(ObLogDelUpd &op,
                             common::ObTableID index_tid,
                             const IndexDMLInfo &index_dml_info,
                             ObDASDMLBaseCtDef &das_dml_ctdef);
  int generate_das_ins_ctdef(ObLogDelUpd &op,
                             common::ObTableID index_tid,
                             const IndexDMLInfo &index_dml_info,
                             ObDASInsCtDef &das_ins_ctdef,
                             const common::ObIArray<ObRawExpr*> &new_row);
  int generate_das_del_ctdef(ObLogDelUpd &op,
                             common::ObTableID index_tid,
                             const IndexDMLInfo &index_dml_info,
                             ObDASDelCtDef &das_del_ctdef,
                             const common::ObIArray<ObRawExpr*> &old_row);
  int generate_related_del_ctdef(ObLogDelUpd &op,
                                 const common::ObIArray<common::ObTableID> &related_tids,
                                 const IndexDMLInfo &index_dml_info,
                                 const common::ObIArray<ObRawExpr*> &old_row,
                                 DASDelCtDefArray &del_ctdefs);
  int generate_das_upd_ctdef(ObLogDelUpd &op,
                             ObTableID index_tid,
                             const IndexDMLInfo &index_dml_info,
                             ObDASUpdCtDef &das_upd_ctdef,
                             const common::ObIArray<ObRawExpr*> &old_row,
                             const common::ObIArray<ObRawExpr*> &new_row,
                             const common::ObIArray<ObRawExpr*> &full_row);
  int generate_related_upd_ctdef(ObLogDelUpd &op,
                                 const common::ObIArray<common::ObTableID> &related_tids,
                                 const IndexDMLInfo &index_dml_info,
                                 const common::ObIArray<ObRawExpr*> &old_row,
                                 const common::ObIArray<ObRawExpr*> &new_row,
                                 const common::ObIArray<ObRawExpr*> &full_row,
                                 DASUpdCtDefArray &upd_ctdefs);
  int generate_das_lock_ctdef(ObLogicalOperator &op,
                              const IndexDMLInfo &index_dml_info,
                              ObDASLockCtDef &das_lock_ctdef,
                              const common::ObIArray<ObRawExpr*> &old_row);
  int convert_table_dml_param(ObLogicalOperator &op, ObDASDMLBaseCtDef &das_dml_ctdef);
  int fill_table_dml_param(share::schema::ObSchemaGetterGuard *guard,
                           uint64_t table_id,
                           ObDASDMLBaseCtDef &das_dml_ctdef);

  int generate_dml_base_ctdef(ObLogDelUpd &op,
                              const IndexDMLInfo &index_dml_info,
                              ObDMLBaseCtDef &dml_base_ctdef,
                              uint64_t dml_event,
                              common::ObIArray<ObRawExpr*> &old_row,
                              common::ObIArray<ObRawExpr*> &new_row);
  int generate_dml_base_ctdef(ObLogicalOperator &op,
                              const IndexDMLInfo &index_dml_info,
                              ObDMLBaseCtDef &dml_base_ctdef,
                              common::ObIArray<ObRawExpr*> &old_row,
                              common::ObIArray<ObRawExpr*> &new_row);
  int convert_normal_triggers(ObLogDelUpd &log_op,
                              const IndexDMLInfo &dml_info,
                              ObDMLBaseCtDef &dml_ctdef,
                              bool is_instead_of,
                              uint64_t dml_event);
  int convert_trigger_rowid(ObLogDelUpd &log_op,
                            const IndexDMLInfo &dml_info,
                            ObDMLBaseCtDef &dml_ctdef);
  int add_trigger_arg(const ObTriggerInfo &trigger_info, ObDMLBaseCtDef &dml_ctdef);
  int convert_triggers(ObLogDelUpd &log_op,
                       const IndexDMLInfo &dml_info,
                       ObDMLBaseCtDef &dml_ctdef,
                       uint64_t dml_event);
  int add_all_column_infos(ObLogDelUpd &op,
                           const common::ObIArray<ObColumnRefRawExpr*> &columns,
                           bool is_heap_table,
                           ColContentFixedArray &column_infos);
  int convert_upd_assign_infos(bool is_heap_table,
                               const IndexDMLInfo &index_dml_info,
                               ColContentFixedArray &assign_infos);
  int convert_check_constraint(ObLogDelUpd &log_op,
                               uint64_t ref_table_id,
                               ObDMLBaseCtDef &dml_base_ctdef,
                               const IndexDMLInfo &index_dml_info);
  int generate_multi_lock_ctdef(const IndexDMLInfo &index_dml_info,
                                ObMultiLockCtDef &multi_lock_ctdef);
  int generate_multi_ins_ctdef(const IndexDMLInfo &index_dml_info,
                               ObMultiInsCtDef &multi_ins_ctdef);
  int generate_multi_del_ctdef(const IndexDMLInfo &index_dml_info,
                               ObMultiDelCtDef &multi_del_ctdef);
  int generate_multi_upd_ctdef(const ObLogDelUpd &op,
                               const IndexDMLInfo &index_dml_info,
                               ObMultiUpdCtDef &multi_upd_ctdef);
  int convert_insert_new_row_exprs(const IndexDMLInfo &index_dml_info,
                                   common::ObIArray<ObRawExpr*> &new_row);
  int convert_old_row_exprs(const common::ObIArray<ObColumnRefRawExpr*> &columns,
                               common::ObIArray<ObRawExpr*> &access_exprs,
                               int64_t col_cnt = -1);
  int need_foreign_key_handle(const ObForeignKeyArg &fk_arg,
                              const common::ObIArray<uint64_t> &updated_column_ids,
                              const common::ObIArray<uint64_t> &value_column_ids,
                              const ObDASOpType &op_type,
                              bool &need_handle);
  int generate_fk_arg(ObForeignKeyArg &fk_arg,
                      bool check_parent_table,
                      const IndexDMLInfo &index_dml_info,
                      const ObForeignKeyInfo &fk_info,
                      const ObLogDelUpd &op,
                      ObRawExpr* fk_part_id_expr,
                      share::schema::ObSchemaGetterGuard &schema_guard,
                      ObDMLBaseCtDef &dml_ctdef);

  int get_fk_check_scan_table_id(const uint64_t parent_table_id,
                                 const common::ObIArray<uint64_t> &name_column_ids,
                                 share::schema::ObSchemaGetterGuard &schema_guard,
                                 uint64_t &index_table_id);

  int generate_fk_check_ctdef(const ObLogDelUpd &op,
                              uint64_t name_table_id,
                              ObRawExpr* fk_part_id_expr,
                              const common::ObIArray<uint64_t> &name_column_ids,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              ObForeignKeyCheckerCtdef &fk_chk_ctdef);

  int generate_fk_scan_ctdef(share::schema::ObSchemaGetterGuard &schema_guard,
                             const uint64_t index_tid,
                             ObDASScanCtDef &scan_ctdef);

  int generate_fk_scan_part_id_expr(ObLogDelUpd &op,
                                    uint64_t parent_table_id,
                                    uint64_t index_tid,
                                    ObForeignKeyCheckerCtdef &fk_ctdef);

  int generate_fk_table_loc_info(uint64_t index_table_id,
                                 ObDASTableLocMeta &loc_meta,
                                 ObTabletID &tablet_id,
                                 bool &is_part_table_);

  int generate_rowkey_idx_for_foreign_key(const common::ObIArray<uint64_t> &name_column_ids,
                             const ObTableSchema *parent_table,
                             ObIArray<int64_t> &rowkey_ids_);

  int convert_foreign_keys(ObLogDelUpd &op,
                           const IndexDMLInfo &index_dml_info,
                           ObDMLBaseCtDef &dml_ctdef);
  int generate_related_ins_ctdef(ObLogDelUpd &op,
                                 const common::ObIArray<common::ObTableID> &related_tids,
                                 const IndexDMLInfo &index_dml_info,
                                 const common::ObIArray<ObRawExpr*> &new_row,
                                 DASInsCtDefArray &ins_ctdefs);
  int generate_access_exprs(const common::ObIArray<ObColumnRefRawExpr*> &columns,
                               common::ObIArray<ObRawExpr*> &access_exprs);
private:
  int need_fire_update_event(const ObTableSchema &table_schema,
                            const ObString &update_events,
                            const ObLogUpdate &log_op,
                            const ObSQLSessionInfo &session,
                            ObIAllocator &allocator,
                            bool &need_fire);
#ifdef OB_BUILD_TDE_SECURITY
  int init_encrypt_metas_(const share::schema::ObTableSchema *table_schema,
                          share::schema::ObSchemaGetterGuard *guard,
                          ObIArray<transaction::ObEncryptMetaCache>&meta_array);
  int init_encrypt_table_meta_(const share::schema::ObTableSchema *table_schema,
                               share::schema::ObSchemaGetterGuard *guard,
                               ObIArray<transaction::ObEncryptMetaCache>&meta_array);
#endif
  int generate_table_loc_meta(const IndexDMLInfo &index_dml_info, ObDASTableLocMeta &loc_meta);
private:
  ObStaticEngineCG &cg_;
};
}  // namespace sql
}  // namespace name
#endif /* DEV_SRC_SQL_CODE_GENERATOR_OB_DML_CG_SERVICE_H_ */
