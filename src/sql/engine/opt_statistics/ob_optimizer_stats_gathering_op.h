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

#ifndef SRC_SQL_ENGINE_BASIC_OB_OPTIMIZER_STATS_GATHERING_OP_H_
#define SRC_SQL_ENGINE_BASIC_OB_OPTIMIZER_STATS_GATHERING_OP_H_
#include "sql/engine/ob_operator.h"
#include "share/datum/ob_datum.h"
#include "sql/engine/expr/ob_expr_sys_op_opnsize.h"
#include "lib/hash/ob_hashmap.h"
#include "sql/engine/px/datahub/components/ob_dh_opt_stats_gather.h"
#include "share/stat/ob_opt_table_stat.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_opt_osg_column_stat.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "share/stat/ob_basic_stats_estimator.h"
#include "share/stat/ob_dbms_stats_executor.h"
#include "share/stat/ob_stat_define.h"

namespace oceanbase
{
namespace sql
{

class ObOptimizerStatsGatheringSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:

  ObOptimizerStatsGatheringSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
  int register_to_datahub(ObExecContext &ctx) const;
  inline void set_osg_type(OSG_TYPE type) {type_ = type; };
  inline void set_target_osg_id(uint64_t target_id) { target_osg_id_ = target_id; };
  inline bool is_part_table() const {
    return part_level_ == share::schema::PARTITION_LEVEL_ONE ||
           part_level_ == share::schema::PARTITION_LEVEL_TWO;
  };
  inline bool is_two_level_part() const {
    return part_level_ == share::schema::PARTITION_LEVEL_TWO;
  }

  ObPartitionLevel part_level_;
  ObExpr *calc_part_id_expr_;
  uint64_t table_id_;
  OSG_TYPE type_;
  uint64_t target_osg_id_;
  ExprFixedArray generated_column_exprs_;
  ExprFixedArray col_conv_exprs_;
  ObFixedArray<uint64_t, common::ObIAllocator> column_ids_;
  double online_sample_rate_;
};

class ObOptimizerStatsGatheringOp : public ObOperator
{
public:
  // store global/part/subpart part_id.
  struct PartIds {
    PartIds() : global_part_id_(common::OB_INVALID_ID),
                part_id_(common::OB_INVALID_ID),
                first_part_id_(common::OB_INVALID_ID) {};
    int64_t global_part_id_;
    int64_t part_id_;
    int64_t first_part_id_; // for two_level partition.

    TO_STRING_KV(K(global_part_id_), K(part_id_), K(first_part_id_));
  };

public:
  ObOptimizerStatsGatheringOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;

  virtual void reset();
  // will be called from datahub, when qc/sqc finish gathering stats, the function will be called in dh.
  int on_piece_msg(const ObOptStatsGatherPieceMsg &piece_msg);
  // after dh receive all piece_msg, msg_end will be called.
  int msg_end();
  inline const TabStatIndMap& get_tab_stat_map() const { return table_stats_map_;};
  int get_col_stat_map(ColStatIndMap &col_stat_map);
private:
  static const int64_t DEFAULT_HASH_MAP_BUCKETS_COUNT = 100;

  // set piece_msg's basic info, copy tab/column stats from stat_map to piece_msg's array.
  int build_piece_msg(ObOptStatsGatherPieceMsg &piece,
                      ObPxSQCProxy &proxy);

  // send piece_msg, no need to wait the whole msg.
  int send_stats();
  int calc_stats();
  // calc stats for each column
  int calc_column_stats(ObExpr *expr, uint64_t column_id, int64_t &row_len);
  int calc_columns_stats(int64_t &row_len);
  int calc_table_stats(int64_t &row_len, bool is_sample_row);
  // generate stat_param that is used to write inner_table.
  int generate_stat_param(ObTableStatParam &param);

  // get tab stat by key(tenant_id, table_id, partition_id), if NOT_EXISTS, alloc a new one.
  int get_tab_stat_by_key(ObOptTableStat::Key &key, ObOptTableStat *&tab_stat);
  // get tab stat by key(tenant_id, table_id, partition_id, column_id), if NOT_EXISTS, alloc a new one.
  int get_col_stat_by_key(ObOptColumnStat::Key &key, ObOptOSGColumnStat *&osg_col_stat);

  int merge_tab_stat(ObOptTableStat *src_tab_stat);
  int merge_col_stat(ObOptColumnStat *src_col_stat);

  // get stats array from stats map
  int get_tab_stats(common::ObIArray<ObOptTableStat*>& tab_stats);
  int get_col_stats(common::ObIArray<ObOptColumnStat*>& col_stats);

  uint64_t tenant_id_;

  TabStatIndMap table_stats_map_;
  OSGColStatIndMap osg_col_stats_map_;

  OSGPartMap part_map_;
  // ------- data struct for piece msg;
  ObOptStatsGatherPieceMsg piece_msg_;
  ObArenaAllocator arena_;
  ObOptOSGSampleHelper sample_helper_;
};
}
}
#endif /* SRC_SQL_ENGINE_BASIC_OB_MONITORING_DUMP_OP_H_ */
