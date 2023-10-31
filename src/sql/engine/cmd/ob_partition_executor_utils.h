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

#ifndef OCEANBASE_SQL_OB_PARTITION_EXECUTOR_UTILS_
#define OCEANBASE_SQL_OB_PARTITION_EXECUTOR_UTILS_
#include "common/object/ob_object.h"
#include "lib/container/ob_se_array.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "sql/resolver/ob_stmt_type.h"
#include "sql/engine/expr/ob_expr_res_type.h"
#include "sql/resolver/ddl/ob_table_stmt.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
struct ObPartition;
struct ObSubPartition;
}
}
namespace sql
{
class ObExecContext;
class ObRawExpr;
class ObCreateTableStmt;
class ObCreateTablegroupStmt;
class ObCreateIndexStmt;
class ObTableStmt;
class ObTablegroupStmt;

class ObPartitionExecutorUtils
{
public:
  const static int OB_DEFAULT_ARRAY_SIZE = 16;
  static int calc_values_exprs(ObExecContext &ctx, ObCreateTableStmt &stmt);
  static int calc_values_exprs(ObExecContext &ctx, ObCreateIndexStmt &stmt);
  static int calc_values_exprs_for_alter_table(ObExecContext &ctx, 
                                               share::schema::ObTableSchema &table_schema,
                                               ObPartitionedStmt &stmt);

  static int cast_list_expr_to_obj(ObExecContext &ctx,
                                   const stmt::StmtType stmt_type,
                                   const bool is_subpart,
                                   const int64_t real_part_num,
                                   share::schema::ObPartition **partition_array,
                                   share::schema::ObSubPartition **subpartition_array,
                                   common::ObIArray<ObRawExpr *> &list_fun_expr,
                                   common::ObIArray<ObRawExpr *> &list_values_exprs);

  static int cast_expr_to_obj(ObExecContext &ctx,
                              const stmt::StmtType stmt_type,
                              bool is_list_part,
                              common::ObIArray<ObRawExpr *> &partition_fun_expr,
                              common::ObIArray<ObRawExpr *> &partition_value_exprs,
                              common::ObIArray<common::ObObj> &partition_value_objs);

  static int set_range_part_high_bound(ObExecContext &ctx,
                                       const stmt::StmtType stmt_type,
                                       share::schema::ObTableSchema &table_schema,
                                       ObPartitionedStmt &stmt,
                                       bool is_subpart);
                                       
  static int check_transition_interval_valid(const stmt::StmtType stmt_type,
                                             ObExecContext &ctx,
                                             ObRawExpr *transition_expr,
                                             ObRawExpr *interval_expr);

  static int set_interval_value(ObExecContext &ctx,
                                const stmt::StmtType stmt_type,
                                share::schema::ObTableSchema &table_schema,
                                ObRawExpr *interval_expr);

  /*--------------tablegroup related start------------------*/
  static int calc_values_exprs(ObExecContext &ctx, ObCreateTablegroupStmt &stmt);

  static int calc_values_exprs(ObExecContext &ctx, ObCreateTablegroupStmt &stmt, bool is_subpart);


  static int cast_range_expr_to_obj(ObExecContext &ctx,
                                    ObCreateTablegroupStmt &stmt,
                                    bool is_subpart,
                                    common::ObIArray<common::ObObj> &range_partition_obj);

  static int cast_range_expr_to_obj(ObExecContext &ctx,
                                    common::ObIArray<ObRawExpr *> &range_values_exprs,
                                    const int64_t fun_expr_num,
                                    const stmt::StmtType stmt_type,
                                    const bool is_subpart,
                                    const int64_t real_part_num,
                                    share::schema::ObPartition **partition_array,
                                    share::schema::ObSubPartition **subpartition_array,
                                    common::ObIArray<common::ObObj> &range_partition_obj);

  static int cast_list_expr_to_obj(ObExecContext &ctx,
                                   ObCreateTablegroupStmt &stmt,
                                   bool is_subpar);

  static int cast_list_expr_to_obj(ObExecContext &ctx,
                                   ObTablegroupStmt &stmt,
                                   const bool is_subpart,
                                   share::schema::ObPartition **partition_array,
                                   share::schema::ObSubPartition **subpartition_array);

  static int cast_expr_to_obj(ObExecContext &ctx,
                              int64_t fun_expr_num,
                              common::ObIArray<ObRawExpr *> &range_values_exprs,
                              common::ObIArray<common::ObObj> &range_partition_obj);
  /*--------------tablegroup related end------------------*/

  static int calc_range_values_exprs(ObExecContext &ctx, ObCreateIndexStmt &stmt);

  template<typename T>
  static int check_increasing_range_value(T **array,
                                          int64_t part_num,
                                          const stmt::StmtType stmt_type);
  
  static int expr_cal_and_cast(const sql::stmt::StmtType &stmt_type,
                               bool is_list_part,
                               ObExecContext &ctx,
                               const sql::ObExprResType &dst_res_type,
                               const common::ObCollationType fun_collation_type,
                               ObRawExpr *expr,
                               common::ObObj &value_obj);
  
  static int expr_cal_and_cast_with_check_varchar_len(const stmt::StmtType &stmt_type,
                                                      bool is_list_part,
                                                      ObExecContext &ctx,
                                                      const sql::ObExprResType &dst_res_type,
                                                      ObRawExpr *expr,
                                                      common::ObObj &value_obj);

  static int set_list_part_rows(ObExecContext &ctx,
                                ObPartitionedStmt &stmt,
                                const stmt::StmtType stmt_type,
                                share::schema::ObTableSchema &table_schema,
                                ObIArray<ObRawExpr *> &list_fun_exprs,
                                ObIArray<ObRawExpr *> &list_values_exprs,
                                bool is_subpart);

  static int set_individual_range_part_high_bound(ObExecContext &ctx,
                                                  const stmt::StmtType stmt_type,
                                                  share::schema::ObTableSchema &table_schema,
                                                  ObPartitionedStmt &stmt);

  static int set_individual_list_part_rows(ObExecContext &ctx,
                                           ObPartitionedStmt &stmt,
                                           const stmt::StmtType stmt_type,
                                           share::schema::ObTableSchema &table_schema,
                                           ObIArray<ObRawExpr *> &list_fun_exprs,
                                           ObDDLStmt::array_array_t &list_values_exprs_array);

  static int row_expr_to_array(ObRawExpr *row_expr,
                               ObIArray<ObRawExpr *> &list_values_expr_array);

private:
  static int calc_values_exprs(ObExecContext &ctx,
                               const stmt::StmtType stmt_type,
                               share::schema::ObTableSchema &table_schema,
                               ObPartitionedStmt &stmt,
                               bool is_subpart);

  static int sort_list_paritition_if_need(share::schema::ObTableSchema &table_schema);
};
} //end namespace sql
} //end namespace oceanbase


#endif //OCEANBASE_SQL_OB_PARTITION_EXECUTOR_UTILS_
