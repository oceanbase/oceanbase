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

#ifndef OCEANBASE_SQL_OB_TABLE_PARTITION_INFO_
#define OCEANBASE_SQL_OB_TABLE_PARTITION_INFO_

#include "sql/optimizer/ob_phy_table_location_info.h"
#include "sql/optimizer/ob_table_location.h"
#include "share/partition_table/ob_partition_location.h"

namespace oceanbase
{
namespace sql
{

enum class PartitionInfoType
{
  INTERNAL_TABLE,
  LAKE_TABLE
};

class ObTablePartitionInfo
{
public:
  ObTablePartitionInfo(PartitionInfoType type = PartitionInfoType::INTERNAL_TABLE) :
  inner_allocator_(common::ObModIds::OB_SQL_TABLE_LOCATION),
  allocator_(inner_allocator_),
  type_(type)
  {}
  ObTablePartitionInfo(common::ObIAllocator &allocator, PartitionInfoType type = PartitionInfoType::INTERNAL_TABLE)
  : allocator_(allocator),
  table_location_(allocator),
  type_(type)
  {}
  virtual ~ObTablePartitionInfo() {}

  virtual int assign(const ObTablePartitionInfo &other);
  virtual uint64_t get_table_id() const { return table_location_.get_table_id(); }
  virtual uint64_t get_ref_table_id() const { return table_location_.get_ref_table_id(); }
  virtual share::schema::ObPartitionLevel get_part_level() const { return table_location_.get_part_level(); }
  virtual bool is_lake_table_partition_info() const { return type_ != PartitionInfoType::INTERNAL_TABLE; }

  int init_table_location(ObSqlSchemaGuard &schema_guard,
                          const ObDMLStmt &stmt,
                          ObExecContext *exec_ctx,
                          const common::ObIArray<ObRawExpr*> &filter_exprs,
                          const uint64_t table_id,
                          const uint64_t ref_table_id,
                          //const uint64_t index_table_id,
                          const common::ObIArray<ObObjectID> *part_ids,
                          const common::ObDataTypeCastParams &dtc_params,
                          const bool is_dml_table,
                          common::ObIArray<ObRawExpr *> *sort_exprs = NULL);

  int get_not_insert_dml_part_sort_expr(const ObDMLStmt &stmt,
                                        common::ObIArray<ObRawExpr *> *sort_exprs) const;

  int calculate_phy_table_location_info(ObExecContext &exec_ctx,
                                        const ParamStore &params,
                                        const common::ObDataTypeCastParams &dtc_params);

  int calc_phy_table_loc_and_select_leader(ObExecContext &exec_ctx,
                                           const ParamStore &params,
                                           const common::ObDataTypeCastParams &dtc_params);
  virtual int replace_final_location_key(ObExecContext &exec_ctx, uint64_t ref_table_id, bool is_local_index);

  int get_location_type(const common::ObAddr &server, ObTableLocationType &type) const;
  static int get_location_type(const common::ObAddr &server,
                               const ObCandiTabletLocIArray &phy_part_loc_info_list,
                               ObTableLocationType &type);
  int get_all_servers(common::ObIArray<common::ObAddr> &servers) const;

  ObTableLocation &get_table_location()
  {
    return table_location_;
  }

  const ObTableLocation &get_table_location() const
  {
    return table_location_;
  }

  virtual const ObDASTableLocMeta &get_loc_meta() const
  {
    return table_location_.get_loc_meta();
  }

  virtual ObDASTableLocMeta &get_loc_meta()
  {
    return table_location_.get_loc_meta();
  }

  const ObCandiTableLoc &get_phy_tbl_location_info() const
  {
    return candi_table_loc_;
  }
  ObCandiTableLoc &get_phy_tbl_location_info_for_update()
  {
    return candi_table_loc_;
  }

  int set_table_location(const ObTableLocation &tbl)
  {
    return table_location_.assign(tbl);
  }

  TO_STRING_KV(K_(table_location),
               K_(candi_table_loc));

private:
  DISALLOW_COPY_AND_ASSIGN(ObTablePartitionInfo);

protected:
  /**
   * ObTableLocation is the structure we stored to calculate the physical table location,
   * which is represented by ObCandiTableLoc, for a given table(logical) in a given statement.
   *
   * The info we keep in ObTableLocation is constant for a parameterized query among multiple
   * times of execution, which is stored to short-cut to plan matching and location calculation
   * since we can skip a bunch of steps in query processing, such as parsing, resolving,
   * optimization and code generation.
   */
  common::ObArenaAllocator inner_allocator_;
  ObIAllocator &allocator_;
  ObTableLocation table_location_;
  ObCandiTableLoc candi_table_loc_;
  PartitionInfoType type_;
};

}
}
#endif // OCEANBASE_SQL_OB_TABLE_PARTITION_INFO_
