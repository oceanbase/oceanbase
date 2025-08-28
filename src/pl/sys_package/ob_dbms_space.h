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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_OB_DBMS_SPACE_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_OB_DBMS_SPACE_H_

#include "sql/engine/ob_exec_context.h"
#include "pl/ob_pl_type.h"
#include "lib/ob_define.h"
#include "share/stat/ob_opt_table_stat.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_opt_column_stat_cache.h"
#include "sql/resolver/ddl/ob_create_index_stmt.h"

namespace oceanbase
{
using namespace sql;
namespace pl
{
class ObDbmsSpace
{
public:
  struct IndexCostInfo final { 
  public:
    IndexCostInfo(): tenant_id_(OB_SYS_TENANT_ID),
                 table_id_(common::OB_INVALID_ID),
                 part_ids_(),
                 column_ids_(),
                 compression_ratio_(1.0),
                 default_index_len_(0),
                 svr_addr_(),
                 tablet_ids_(),
                 table_tenant_id_(OB_INVALID_TENANT_ID) {}
    ~IndexCostInfo() = default; 
    TO_STRING_KV(K_(tenant_id), K_(table_id), K_(part_ids), K_(column_ids),
                 K_(compression_ratio), K_(default_index_len), K_(svr_addr),
                 K_(tablet_ids), K_(table_tenant_id));
  public:
    uint64_t tenant_id_;
    uint64_t table_id_;
    ObSEArray<int64_t, 1> part_ids_;
    ObSEArray<uint64_t, 4> column_ids_;
    double compression_ratio_;
    uint64_t default_index_len_;
    ObSEArray<ObAddr, 1> svr_addr_;
    ObSEArray<ObTabletID, 4> tablet_ids_;
    uint64_t table_tenant_id_;
  };

  struct OptStats final {
  public:
    OptStats(): table_stats_(), column_stats_() {};
    ~OptStats() = default;
    TO_STRING_KV(K_(table_stats), K_(column_stats));
  public:
    ObSEArray<ObOptTableStat, 1> table_stats_;
    ObSEArray<ObOptColumnStatHandle, 4> column_stats_;
  };

  struct TabletInfo final {
  public: 
    TabletInfo(): tablet_id_(OB_INVALID_ID), partition_id_(OB_INVALID_PARTITION_ID), row_len_(0), row_count_(0), compression_ratio_(0) {}
    TabletInfo(ObTabletID tablet_id, ObObjectID partition_id): tablet_id_(tablet_id), partition_id_(partition_id), row_len_(0), row_count_(0), compression_ratio_(0) {}
    ~TabletInfo() = default;
    int assign(const TabletInfo &other) {
      int ret = OB_SUCCESS;
      tablet_id_ = other.tablet_id_;
      partition_id_ = other.partition_id_;
      row_len_ = other.row_len_;
      row_count_ = other.row_count_;
      compression_ratio_ = other.compression_ratio_;
      return ret;
    }
    TO_STRING_KV(K_(tablet_id), K_(partition_id), K_(row_len), K_(row_count), K_(compression_ratio));
  public:
    ObTabletID tablet_id_;
    ObObjectID partition_id_;
    double row_len_;
    uint64_t row_count_;
    double compression_ratio_;
  };
  typedef ObSEArray<TabletInfo, 1> TabletInfoList;

public:
  static int create_index_cost(sql::ObExecContext &ctx,
                               sql::ParamStore &params,
                               common::ObObj &result);

  // interface for auto_split
  /* in param:
   *  info tenant_id_; // default set to OB_SYS_TENANT_ID
   *  info table_id_; // data table's table id;
   *  info.part_ids_; // the part id(data table) we need to calc.
   *  info.column_ids_; // index columns' column id (in data table schema).
   * out param:
   *  the table_size in info.part_ids_'s order
   */
  static int estimate_index_table_size(ObMySQLProxy *sql_proxy,
                                       const ObTableSchema *table_schema,
                                       IndexCostInfo &info,
                                       ObIArray<uint64_t> &table_size);

  // interface to get each tablet_size in the table
  /*
   *  in param:
   *    table_schema -- the target table we need to calc size.
   *  out param:
   *    each tablet's size. 
   */
  static int get_each_tablet_size(ObMySQLProxy *sql_proxy,
                                  const ObTableSchema *table_schema,
                                  ObIArray<std::pair<ObTabletID, uint64_t>> &tablet_size);
  static int check_stats_valid(const OptStats &opt_stats, bool &is_valid);
private:

  static int parse_ddl_sql(ObExecContext &ctx,
                           const ObString &ddl_sql,
                           ObCreateIndexStmt *&stmt);

  static int extract_info_from_stmt(ObExecContext &ctx,
                                    ObCreateIndexStmt *stmt,
                                    IndexCostInfo &info);

  static int get_index_column_ids(const share::schema::ObTableSchema *table_schema,
                                   const obrpc::ObCreateIndexArg &arg,
                                   IndexCostInfo &info);
  
  static int get_optimizer_stats(const IndexCostInfo &info,
                                 OptStats &opt_stats);

  static int calc_index_size(OptStats &opt_stats,
                             IndexCostInfo &info,
                             ObObjParam &actual_size,
                             ObObjParam &alloc_size);

  static int get_compressed_ratio(ObExecContext &ctx,
                                  IndexCostInfo &info);

  static int inner_get_compressed_ratio(ObMySQLProxy *sql_proxy,
                                        IndexCostInfo &info);

  static int extract_total_compression_ratio(const sqlclient::ObMySQLResult *result,
                                             double &compression_ratio);

  static int inner_calc_index_size(const ObOptTableStat &table_stat,
                                   const ObIArray<ObOptColumnStatHandle> &column_stats,
                                   const IndexCostInfo &info,
                                   uint64_t &actual_size,
                                   uint64_t &alloc_size);

  static int estimate_index_table_size_by_opt_stats(ObMySQLProxy *sql_proxy,
                                                    const ObTableSchema *table_schema,
                                                    const OptStats &opt_stats,
                                                    IndexCostInfo &info,
                                                    ObIArray<uint64_t> &table_size);

  static int estimate_index_table_size_default(ObMySQLProxy *sql_proxy,
                                               const ObTableSchema *table_schema,
                                               IndexCostInfo &info,
                                               ObIArray<uint64_t> &table_size);

  static int inner_calc_index_size_by_default(IndexCostInfo &info,
                                              TabletInfoList &tablet_infos,
                                              ObIArray<uint64_t> &table_size);
  
  static int fill_tablet_infos(const ObTableSchema *table_schema,
                               TabletInfoList &tablet_infos);

  static int extract_tablet_size(const sqlclient::ObMySQLResult *result,
                                 TabletInfoList &tablet_infos);

  static int extract_tablet_size(const sqlclient::ObMySQLResult *result,
                                 ObIArray<std::pair<ObTabletID, uint64_t>> &tablet_size);

  static int get_each_tablet_size(ObMySQLProxy *sql_proxy,
                                  TabletInfoList &tablet_infos,
                                  IndexCostInfo &info);

  static const TabletInfo* get_tablet_info_by_tablet_id(const TabletInfoList &tablet_infos,
                                                  const ObTabletID tablet_id);

  static const TabletInfo* get_tablet_info_by_part_id(const TabletInfoList &tablet_infos,
                                                const ObObjectID partition_id);
  
  static int set_tablet_info_by_tablet_id(const ObTabletID tablet_id,
                                          const double row_len,
                                          const uint64_t row_count,
                                          const double compression_ratio,
                                          TabletInfoList &tablet_infos);

  static int get_default_index_column_len(const ObTableSchema *table_schema,
                                          const TabletInfoList &table_infos,
                                          IndexCostInfo &info);
  static int get_svr_info_from_schema(const ObTableSchema *table_schema,
                                      ObIArray<ObAddr> &addr_list,
                                      ObIArray<ObTabletID> &tablet_list,
                                      uint64_t &tenant_id);
  
  static int generate_part_key_str(ObSqlString &target_str,
                                   const ObIArray<ObAddr> &addr_list);

  static int generate_tablet_predicate_str(ObSqlString &target_str,
                                           const ObIArray<ObTabletID> &tablet_list);
                            
};

}
}

#endif