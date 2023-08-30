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

#ifndef _OB_LOG_DEL_UPD_H
#define _OB_LOG_DEL_UPD_H 1
#include "sql/resolver/dml/ob_del_upd_stmt.h"
#include "ob_logical_operator.h"

namespace oceanbase
{
namespace sql
{

struct IndexDMLInfo
{
public:
  typedef common::ObSEArray<common::ObTableID, 4, common::ModulePageAllocator, true> TableIDArray;
  IndexDMLInfo() :
    table_id_(common::OB_INVALID_ID),
    loc_table_id_(common::OB_INVALID_ID),
    ref_table_id_(common::OB_INVALID_ID),
    rowkey_cnt_(0),
    spk_cnt_(0),
    need_filter_null_(false),
    is_primary_index_(false),
    ck_cst_exprs_(),
    part_ids_(),
    is_update_unique_key_(false),
    is_update_part_key_(false),
    distinct_algo_(T_DISTINCT_NONE),
    lookup_part_id_expr_(NULL),
    old_part_id_expr_(NULL),
    new_part_id_expr_(NULL),
    old_rowid_expr_(NULL),
    new_rowid_expr_(NULL),
    trans_info_expr_(NULL),
    related_index_ids_(),
    fk_lookup_part_id_expr_()
  {
  }
  inline void reset()
  {
    table_id_ = common::OB_INVALID_ID;
    loc_table_id_ = common::OB_INVALID_ID;
    ref_table_id_ = common::OB_INVALID_ID;
    index_name_.reset();
    spk_cnt_ = 0;
    rowkey_cnt_ = 0;
    column_exprs_.reset();
    column_convert_exprs_.reset();
    column_old_values_exprs_.reset();
    assignments_.reset();
    need_filter_null_ = false;
    is_primary_index_ = false;
    ck_cst_exprs_.reset();
    part_ids_.reset();
    is_update_unique_key_ = false;
    is_update_part_key_ = false;
    distinct_algo_ = T_DISTINCT_NONE;
    lookup_part_id_expr_ = NULL;
    old_part_id_expr_ = NULL;
    new_part_id_expr_ = NULL,
    old_rowid_expr_ = NULL,
    new_rowid_expr_ = NULL,
    trans_info_expr_ = NULL,
    related_index_ids_.reset();
    fk_lookup_part_id_expr_.reset();
  }
  int64_t to_explain_string(char *buf, int64_t buf_len, ExplainType type) const;
  int init_assignment_info(const ObAssignments &assignments,
                           ObRawExprFactory &expr_factory);

  int assign_basic(const IndexDMLInfo &other);
  int assign(const ObDmlTableInfo& info);

  int deep_copy(ObIRawExprCopier &expr_copier,
                const IndexDMLInfo &other);

  uint64_t hash(uint64_t seed) const
  {
    seed = do_hash(table_id_, seed);
    seed = do_hash(loc_table_id_, seed);
    seed = do_hash(ref_table_id_, seed);
    seed = do_hash(rowkey_cnt_, seed);
    seed = do_hash(spk_cnt_, seed);
    for (int64_t i = 0; i < column_exprs_.count(); ++i) {
      if (NULL != column_exprs_.at(i)) {
        seed = do_hash(*column_exprs_.at(i), seed);
      }
    }
    for (int64_t i = 0; i < column_convert_exprs_.count(); ++i) {
      if (NULL != column_convert_exprs_.at(i)) {
        seed = do_hash(*column_convert_exprs_.at(i), seed);
      }
    }
    for (int64_t i = 0; i < assignments_.count(); ++i) {
      seed = do_hash(assignments_.at(i), seed);
    }
    seed = do_hash(need_filter_null_, seed);
    seed = do_hash(is_primary_index_, seed);
    return seed;
  }
  int get_rowkey_exprs(common::ObIArray<ObColumnRefRawExpr *> &rowkey, bool need_spk = false) const;
  int get_rowkey_exprs(common::ObIArray<ObRawExpr *> &rowkey, bool need_spk = false) const;
  //real_uk_cnt: strip the shadow primary key in unique index
  int64_t get_real_uk_cnt() const { return rowkey_cnt_ - spk_cnt_; }
  int init_column_convert_expr(const ObAssignments &assignments);
  int convert_old_row_exprs(const ObIArray<ObColumnRefRawExpr*> &columns,
                                          ObIArray<ObRawExpr*> &access_exprs,
                                          int64_t col_cnt = -1);
  int generate_column_old_values_exprs();
  int is_new_row_expr(const ObRawExpr *expr, bool &bret) const;
public:
  // e.g.:
  //   create view V as select * from T1 as T;
  //   update V set ...;
  //
  //   table_id_: table_id_ of V table item
  //   loc_table_id_: table_id_ of T table item
  //   ref_table_id: ref_id_ of T table item
  //
  //
  uint64_t table_id_; // table id for the view table item
  uint64_t loc_table_id_; // table id for the updated table
  uint64_t ref_table_id_; // refer table id for the updated table
  common::ObString index_name_;
  int64_t rowkey_cnt_;
  //spk_cnt_:shadow primary key count, used for unique index, unseen in SQL layer,
  //but PDML-sstable-insert will resolve shadow pk in SQL layer
  int64_t spk_cnt_;
  // 索引表要被更新的列
  // 例如：create table t0 (c1 int primary key, c2 int, c3 int, c4 int, c5 int, c6 int);
  //       update /*+ parallel(3) enable_parallel_dml */ t0 set c3 = 9;
  //       如果 binlog_row_image = MINIMAL 那么这个 update 计划中：
  //        - column_exprs_ 为  (c1, c3)，它会显示在 explain 结果 table_columns 中
  //       如果 binlog_row_image = FULL 那么这个 update 计划中：
  //        - column_exprs_ 为  (c1,c2,c3,c4,c5,c6)，它会显示在 explain 结果 table_columns 中
  common::ObSEArray<ObColumnRefRawExpr*, 8, common::ModulePageAllocator, true> column_exprs_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> column_convert_exprs_;
  // 更新表达式，因为可以有多个列被更新，所以有多个 assignment
  // 至于这个索引表的分区键有没有被更新，由 ObTablesAssignment 中的 is_updated_part_key_ 记录
  ObAssignments assignments_;
  bool need_filter_null_;
  bool is_primary_index_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> ck_cst_exprs_;
  //partition used for base table
  common::ObSEArray<ObObjectID, 1, common::ModulePageAllocator, true> part_ids_;
  bool is_update_unique_key_;
  bool is_update_part_key_;
  DistinctType distinct_algo_;
  ObRawExpr *lookup_part_id_expr_; // for replace and insert_up conflict scene
  ObRawExpr *old_part_id_expr_;
  ObRawExpr *new_part_id_expr_;
  ObRawExpr *old_rowid_expr_;
  ObRawExpr *new_rowid_expr_;
  // When the defensive check level is set to 2,
  // the transaction information of the current row is recorded for 4377 diagnosis
  ObRawExpr *trans_info_expr_;
  // for generated column, the diff between column_exprs_ and column_old_values_exprs_
  // is virtual generated column is replaced.
  common::ObSEArray<ObRawExpr*, 64, common::ModulePageAllocator, true> column_old_values_exprs_;
  // local index id related to current dml
  TableIDArray related_index_ids_;

  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> fk_lookup_part_id_expr_;

  TO_STRING_KV(K_(table_id),
               K_(ref_table_id),
               K_(loc_table_id),
               K_(index_name),
               K_(rowkey_cnt),
               K_(spk_cnt),
               K_(column_exprs),
               K_(column_convert_exprs),
               K_(column_old_values_exprs),
               K_(assignments),
               K_(need_filter_null),
               K_(is_primary_index),
               K_(ck_cst_exprs),
               K_(is_update_unique_key),
               K_(is_update_part_key),
               K_(distinct_algo),
               K_(related_index_ids));
};

class ObDelUpdLogPlan;
class ObLogDelUpd: public ObLogicalOperator
{
public:
  ObLogDelUpd(ObDelUpdLogPlan &plan);
  virtual ~ObLogDelUpd() = default;
  int assign_dml_infos(const ObIArray<IndexDMLInfo *> &index_dml_infos);

  int add_table_columns_to_ctx(ObAllocExprContext &ctx,
                               const ObIArray<IndexDMLInfo> &index_dml_infos);

  IndexDMLInfo *get_primary_dml_info();
  const IndexDMLInfo *get_primary_dml_info() const;

  IndexDMLInfo *get_primary_dml_info(uint64_t loc_table_id);
  const IndexDMLInfo *get_primary_dml_info(uint64_t loc_table_id) const;

  common::ObIArray<IndexDMLInfo *> &get_index_dml_infos()
  { return index_dml_infos_; }
  const common::ObIArray<IndexDMLInfo *> &get_index_dml_infos() const
  { return index_dml_infos_; }

  int get_index_dml_infos(uint64_t loc_table_id,
                          ObIArray<IndexDMLInfo *> &index_infos);
  int get_index_dml_infos(uint64_t loc_table_id,
                          ObIArray<const IndexDMLInfo *> &index_infos) const;

  const ObIArray<uint64_t>& get_table_list() const;

  int get_table_index_name(const IndexDMLInfo &index_info,
                           ObString &table_name,
                           ObString &index_name);


  void set_lock_row_flag_expr(ObRawExpr *expr) { lock_row_flag_expr_ = expr; }
  ObRawExpr *get_lock_row_flag_expr() const { return lock_row_flag_expr_; }
  inline const common::ObIArray<ObRawExpr*> &get_view_check_exprs() const
  {
    return view_check_exprs_;
  }
  inline common::ObIArray<ObRawExpr*> &get_view_check_exprs()
  {
    return view_check_exprs_;
  }

  inline const common::ObIArray<ObRawExpr*> &get_produced_trans_exprs() const
  {
    return produced_trans_exprs_;
  }
  inline common::ObIArray<ObRawExpr*> &get_produced_trans_exprs()
  {
    return produced_trans_exprs_;
  }


  virtual bool is_single_value() const { return false; }
  virtual uint64_t get_hash(uint64_t seed) const { return seed; }
  virtual uint64_t hash(uint64_t seed) const override;
  void set_ignore(bool is_ignore) { ignore_ = is_ignore; }
  bool is_ignore() const { return ignore_; }
  void set_is_returning(bool is) { is_returning_ = is; }
  bool is_returning() const { return is_returning_; }
  bool is_multi_part_dml() const { return is_multi_part_dml_; }
  void set_is_multi_part_dml(bool is_multi_part_dml) { is_multi_part_dml_ = is_multi_part_dml; }
  bool has_instead_of_trigger() const { return has_instead_of_trigger_; }
  void set_has_instead_of_trigger(bool v) { has_instead_of_trigger_ = v;}
  bool is_pdml() const { return is_pdml_; }
  void set_is_pdml(bool is_pdml) { is_pdml_ = is_pdml; }
  bool need_barrier() const { return need_barrier_; }
  void set_need_barrier(bool need_barrier) { need_barrier_ = need_barrier; }
  void set_first_dml_op(bool is_first_dml_op)
  { is_first_dml_op_ = is_first_dml_op; }
  bool is_first_dml_op() const { return is_first_dml_op_; }
  void set_index_maintenance(bool is_index_maintenance)
  { is_index_maintenance_ = is_index_maintenance; }
  bool is_index_maintenance() const { return is_index_maintenance_; }
  // update 拆成 del+ins 时，ins 的 table location 是 uncertain 的，需要全表更新
  //
  void set_table_location_uncertain(bool uncertain) { table_location_uncertain_ = uncertain; }
  bool is_table_location_uncertain() const { return table_location_uncertain_; }
  void set_pdml_update_split(bool is_pdml_update_split) { is_pdml_update_split_ = is_pdml_update_split; }
  bool is_pdml_update_split() const { return is_pdml_update_split_; }
  // 返回的是基表对应的id
  uint64_t get_loc_table_id() const;
  // 返回的是索引id，数据表或者索引表真是的id
  uint64_t get_index_tid() const;
  // 返回的是主表对应的逻辑id
  uint64_t get_table_id() const;
  void set_gi_above(bool is_gi_above) { gi_charged_ = is_gi_above; }
  bool is_gi_above() const override { return gi_charged_; }
  const ObRawExpr *get_stmt_id_expr() const { return stmt_id_expr_; }
  const common::ObIArray<ObColumnRefRawExpr*> *get_table_columns() const;
  inline void set_table_partition_info(ObTablePartitionInfo *table_partition_info)
  {
    table_partition_info_ = table_partition_info;
  }
  inline const ObTablePartitionInfo *get_table_partition_info() const
  {
    return table_partition_info_;
  }
  inline ObTablePartitionInfo *get_table_partition_info()
  {
    return table_partition_info_;
  }
  int get_table_location_type(ObTableLocationType &type);
  inline void set_need_allocate_partition_id_expr(bool need_alloc_part_id_expr) {
    need_alloc_part_id_expr_ = need_alloc_part_id_expr;
  }
  virtual int allocate_granule_pre(AllocGIContext &ctx) override;
  virtual int allocate_granule_post(AllocGIContext &ctx) override;
  void set_pdml_is_returning(bool v) { pdml_is_returning_ = v; }
  bool pdml_is_returning() { return pdml_is_returning_; }
  bool has_part_id_expr() const { return nullptr != pdml_partition_id_expr_; }
  ObRawExpr* get_partition_id_expr() { return pdml_partition_id_expr_; }
  // add for error logging
  ObErrLogDefine &get_err_log_define() { return err_log_define_; }
  const ObErrLogDefine &get_err_log_define() const { return err_log_define_; }

  virtual int est_cost() override;
  virtual int compute_op_ordering() override;
  virtual int compute_plan_type() override;
  virtual int compute_sharding_info() override;
  int get_rowid_version(int64_t &rowid_version);
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override = 0;
  int inner_get_op_exprs(ObIArray<ObRawExpr*> &all_exprs, bool need_column_expr);
  int find_trans_info_producer();
  int find_trans_info_producer(ObLogicalOperator &op,
                               const uint64_t tid,
                               ObLogicalOperator *&producer);
  int get_table_columns_exprs(const ObIArray<IndexDMLInfo *> &index_dml_infos,
                              ObIArray<ObRawExpr*> &all_exprs,
                              bool need_column_expr);
  virtual int allocate_expr_post(ObAllocExprContext &ctx) override;
  int extract_err_log_info();
  static int generate_errlog_info(const ObDelUpdStmt &stmt, ObErrLogDefine &errlog_define);
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;
  int replace_dml_info_exprs(
        ObRawExprReplacer &replacer,
        const ObIArray<IndexDMLInfo *> &index_dml_infos);
  virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;
protected:
  virtual int generate_rowid_expr_for_trigger() = 0;
  virtual int generate_part_id_expr_for_foreign_key(ObIArray<ObRawExpr*> &all_exprs) = 0;
  virtual int generate_multi_part_partition_id_expr() = 0;
  int generate_old_rowid_expr(IndexDMLInfo &table_dml_info);
  int generate_update_new_rowid_expr(IndexDMLInfo &table_dml_info);
  int generate_insert_new_rowid_expr(IndexDMLInfo &table_dml_info);
  int generate_old_calc_partid_expr(IndexDMLInfo &index_info);
  int generate_lookup_part_id_expr(IndexDMLInfo &index_info);
  int generate_fk_lookup_part_id_expr(IndexDMLInfo &index_info);
  int convert_insert_new_fk_lookup_part_id_expr(ObIArray<ObRawExpr*> &all_exprs,IndexDMLInfo &index_dml_info);
  int convert_update_new_fk_lookup_part_id_expr(ObIArray<ObRawExpr*> &all_exprs, IndexDMLInfo &index_dml_info);
  int replace_expr_for_fk_part_expr(const ObIArray<ObRawExpr *> &dml_columns,
                                    const ObIArray<ObRawExpr *> &dml_new_values,
                                    ObRawExpr *fk_part_id_expr);
  int generate_insert_new_calc_partid_expr(IndexDMLInfo &index_dml_info);
  int generate_update_new_calc_partid_expr(IndexDMLInfo &index_dml_info);

  int convert_expr_by_dml_operation(const ObIArray<ObRawExpr *> &dml_columns,
                                    const ObIArray<ObRawExpr *> &dml_new_values,
                                    ObRawExpr *cur_value,
                                    ObRawExpr *&new_value);

  static int get_update_exprs(const IndexDMLInfo &dml_info,
                              ObIArray<ObRawExpr *> &dml_columns,
                              ObIArray<ObRawExpr *> &dml_values);

  static int get_insert_exprs(const IndexDMLInfo &dml_info,
                              ObIArray<ObRawExpr *> &dml_columns,
                              ObIArray<ObRawExpr *> &dml_values);

   // 当前的DML算子作为partition id expr的consumer添加到ctx中
  // partition id 列是pdml操作中特有的一个column.
  int generate_pdml_partition_id_expr();

  int print_table_infos(const ObString &prefix,
                        char *buf,
                        int64_t &buf_len,
                        int64_t &pos,
                        ExplainType type);
  int print_assigns(const ObAssignments &assigns,
                    char *buf,
                    int64_t &buf_len,
                    int64_t &pos,
                    ExplainType type);
  int check_has_trigger(uint64_t tid, bool &has_trg);
  int build_rowid_expr(uint64_t table_id,
                       uint64_t table_ref_id,
                       const ObIArray<ObRawExpr *> &all_cols,
                       ObRawExpr *&rowid_expr);

  // The pseudo partition_id for PDML may be produced by repart exchange or TSC.
  // set %producer to NULL if not found
  static int find_pdml_part_id_producer(ObLogicalOperator &op,
                                        const uint64_t tid,
                                        ObLogicalOperator *&producer);

  virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;

  virtual int print_outline_data(PlanText &plan_text) override;

  virtual int print_used_hint(PlanText &plan_text) override;
protected:

  ObDelUpdLogPlan &my_dml_plan_;

  common::ObSEArray<IndexDMLInfo *, 1, common::ModulePageAllocator, true> index_dml_infos_;
  common::ObSEArray<uint64_t, 1, common::ModulePageAllocator, true> loc_table_list_;

  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> view_check_exprs_;
  // 用于保存当前 DML 算子的 partition 信息
  ObTablePartitionInfo *table_partition_info_;
  const ObRawExpr *stmt_id_expr_;
  ObRawExpr *lock_row_flag_expr_;
  bool ignore_;
  bool is_returning_; // 表示当前的dml计划，是否需要return结果
  bool is_multi_part_dml_;
  bool is_pdml_; // 标记当前逻辑算子是 PDML 算子，CG 阶段会据此决定为其生成 PDML 物理算子
  bool gi_charged_;
  bool is_index_maintenance_; // 启用了 PDML，并且当前算子负责索引表维护
  bool need_barrier_; // row movement 场景下为了避免 insert、delete 同时操作同一行，需要加入 barrier
  bool is_first_dml_op_; // 第一个 dml op 可以和 tsc 形成 partition wise 结构，可少分配一个 exchange
  // update 拆成 del+ins 时，ins 的 table location 是 uncertain 的，需要全表更新
  //
  bool table_location_uncertain_;
  bool is_pdml_update_split_; // 标记delete, insert op是否由update拆分而来
private:
  // 如果是PDML，那么对应的DML算子（insert，update，delete）需要一个partition id expr
  ObRawExpr *pdml_partition_id_expr_;
  bool pdml_is_returning_; // 如果计划是pdml计划，表示当前逻辑算子转化为的物理算子是否需要吐/返回行
  // add for error logging
  ObErrLogDefine err_log_define_;
protected:
  // 对于非分区表而言，pdml中的dml是不需要分配partition id expr
  // 但是对于非分区表，pdml中的dml是需要分配partition id expr
  bool need_alloc_part_id_expr_; // pdml计划中，用于判断当前dml 算子是否需要分配partition id expr
  bool has_instead_of_trigger_;
  // Only when trans_info_expr can be pushed down to the corresponding table_scan operator,
  // the expression will be added to produced_trans_exprs_
  // When trans_info_expr does not find a producer operator,
  // the upper layer dml operator cannot consume the expression
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> produced_trans_exprs_;
};
}
}
#endif
