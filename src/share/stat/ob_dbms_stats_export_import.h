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

#ifndef OB_DBMS_STATS_EXPORT_IMPORT_H
#define OB_DBMS_STATS_EXPORT_IMPORT_H

#include "share/stat/ob_stat_define.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_stat_item.h"

namespace oceanbase {
using namespace sql;
namespace common {

enum StatType {
  INVALID_STAT_TYPE = -1,
  TABLE_STAT,
  INDEX_STAT,
  COLUMN_STAT
};

enum StatTableColumnName {
  STATID = 0,//NULL
  TYPE,      //stat type('T' is stand for table stat and 'C' )
  VERSION,   //NULL
  FLAGS,     //mark hist type('C')
  C1,        //table name
  C2,        //partition name
  C3,        //subpartition name
  C4,        //column name('C')
  C5,        //Owner
  C6,        //NULL
  N1 = 10,   //Num rows('T') or Num distinct('C')
  N2,        //Blocks('T') or Density('C')
  N3,        //Average row length('T')
  N4,        //Sample size
  N5,        //Num nulls('C')
  N6,        //Lower value('C')
  N7,        //high value('C')
  N8,        //Average column length('C')
  N9,        //always 1('C')
  N10,       //Endpoint number('C')
  N11 = 20,  //Endpoint value('C')
  N12,       //Endpoint repeat count('C')
  N13,       //NULL
  D1,        //Last analyzed
  T1,        //NULL
  R1,        //Lower raw value('C')
  R2,        //High raw value('C')
  R3,        //Endpoint actual value raw('C')
  CH1,       //NULL
  CL1,       //NULL
  BL1 = 30,  //NULL
  /*!!!!!!!!!above col is compatible oracle 12c and following col is unique to ob!!!!!!!!!!!*/
  OB_SPEC1,  //ob macro block cnt('T') or ob llc bitmap size('C')==> ndv synopsis size
  OB_SPEC2,  //ob micro block cnt('T')
  OB_SPEC3,  //ob llc bitmap('C') ==> ndv synopsis
  MAX_COL    //please add new column name before MAX_COL!!!
};

class ObDbmsStatsExportImport
{
public:
  static int create_stat_table(ObExecContext &ctx, const ObTableStatParam &param);

  static int drop_stat_table(ObExecContext &ctx, const ObTableStatParam &param);

  static int export_table_stats(ObExecContext &ctx,
                                const ObTableStatParam &param,
                                const ObString &data_name);

  static int export_column_stats(ObExecContext &ctx, const ObTableStatParam &param);

  static int import_table_stats(ObExecContext &ctx, const ObTableStatParam &param);

  static int import_column_stats(ObExecContext &ctx, const ObTableStatParam &param);

private:
  static int do_execute_sql(ObExecContext &ctx, uint64_t tenant_id, const ObSqlString &raw_sql);

  static int do_import_stats(ObExecContext &ctx,
                             const ObTableStatParam &param,
                             const ObSqlString &raw_sql);

  static int get_opt_stat(ObExecContext &ctx,
                          const ObTableStatParam &param,
                          const ObIArray<ObObj> &result_objs,
                          bool &is_index_stat,
                          ObIArray<ObOptTableStat *> &all_tstats,
                          ObIArray<ObOptColumnStat *> &all_cstats);

  static int init_opt_stat(ObExecContext &ctx,
                           const ObTableStatParam &param,
                           const StatType stat_type,
                           const ObString &part_str,
                           const ObString &subpart_str,
                           const ObString &col_str,
                           ObOptTableStat *&tbl_stat,
                           ObOptColumnStat *&col_stat,
                           ObIArray<ObOptTableStat *> &all_tstats,
                           ObIArray<ObOptColumnStat *> &all_cstats);

  static int get_part_info(const ObTableStatParam &param,
                           const ObString &part_str,
                           const ObString &subpart_str,
                           int64_t &part_id,
                           StatLevel &type,
                           uint64_t &stattype);

  static int get_opt_col_stat(const ObTableStatParam &param,
                              const ObString &col_str,
                              const int64_t part_id,
                              const StatLevel type,
                              uint64_t &column_id,
                              common::ObCollationType &cs_type,
                              ObOptColumnStat *&col_stat,
                              ObIArray<ObOptColumnStat *> &all_cstats);

  static int gen_part_and_subpart_sel_str(const ObTableStatParam &param,
                                          ObSqlString &part_str,
                                          ObSqlString &subpart_str);

  static int gen_col_sel_str(const ObTableStatParam &param,
                             ObSqlString &col_str);

  static int convert_bin_hex_raw_to_obj(ObIAllocator &allocator,
                                        const ObObj &src_obj,
                                        ObObj &dst_obj);

  static int convert_bin_hex_text_to_obj(ObIAllocator &allocator,
                                         const ObObj &src_obj,
                                         ObObj &dst_obj);

  static int gen_import_partition_list(const ObTableStatParam &param,
                                       ObSqlString &partition_list);

  static int do_execute_sql(ObExecContext &ctx,
                            uint64_t tenant_id,
                            const ObSqlString &delete_sql,
                            const ObSqlString &insert_sql);

  static int create_oracle_stat_table(ObExecContext &ctx, const ObTableStatParam &param);

  static int create_mysql_stat_table(ObExecContext &ctx, const ObTableStatParam &param);

  static int gen_import_column_list(const ObIArray<ObColumnStatParam> &column_param,
                                    ObSqlString &column_list);

  static int check_col_stat_validity(ObIArray<ObOptColumnStat *> &all_cstats);

};

} // end of sql
} // end of namespace

#endif //OB_DBMS_STATS_EXPORT_IMPORT_H