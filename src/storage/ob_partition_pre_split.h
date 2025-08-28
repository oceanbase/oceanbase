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

#ifndef OB_PARTITION_PRE_SPLIT_H
#define OB_PARTITION_PRE_SPLIT_H

#include "lib/string/ob_string.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_ddl_common.h"
#include "share/schema/ob_table_schema.h"
#include "common/ob_range.h"
#include "lib/container/ob_array.h" 
#include "rootserver/ob_ddl_service.h"


namespace oceanbase
{
namespace rootserver
{
class ObDDLService;
}
namespace storage
{
class ObPartitionPreSplit final
{
public:
  typedef std::pair<ObTabletID, uint64_t> TabletIDSize;
  const static int MAX_SPLIT_RANGE_NUM = 128;
  
public:
  explicit ObPartitionPreSplit(rootserver::ObDDLService &ddl_service);
  ObPartitionPreSplit();
  ~ObPartitionPreSplit() = default;
  void reset();

  int get_global_index_pre_split_schema_if_need(
      const int64_t tenant_id, 
      const int64_t session_id,
      const ObString &database_name,
      const ObString &table_name,
      common::ObIArray<obrpc::ObIndexArg *> &index_arg_list);

  int do_table_pre_split_if_need(
      const ObString &db_name,
      const ObDDLType ddl_type, 
      const bool is_building_global_index,
      const ObTableSchema &data_table_schema,
      const ObTableSchema &ori_table_schema,
      ObTableSchema &new_table_schema);
  
private:
  void get_split_num(
       const int64_t tablet_size,
       const int64_t split_size,
       int64_t &split_num);

  int do_pre_split_main_table(
      const ObString &db_name,
      const ObTableSchema &ori_table_schema,
      ObTableSchema &new_table_schema);
  
  int do_pre_split_global_index(
      const ObString &db_name,
      const ObTableSchema &data_table_schema,
      const ObTableSchema &ori_index_schema,
      const int64_t auto_part_size,
      ObTableSchema &new_index_schema);

  int generate_tablet_and_part_id(ObTableSchema &new_table_schema);
  int check_table_can_do_pre_split(const ObTableSchema &data_table_schema, const ObTableSchema &index_table_schema);

  int build_tablet_pre_split_ranges(
      const int64_t tenant_id,
      const int64_t tablet_phycical_size,
      const int64_t split_num,
      const ObTabletID &tablet_id,
      const ObString &db_name,
      const ObTableSchema &old_table_schema,
      const ObTableSchema &new_table_schema);

  int build_global_index_pre_split_ranges(
      const int64_t tenant_id,
      const int64_t data_table_phycical_size,
      const int64_t split_num,
      const ObString &db_name,
      const ObTableSchema &index_schema,
      const ObTableSchema &table_schema);
  
  int build_split_tablet_partition_schema(
      const int64_t tenant_id,
      const ObTabletID &source_tablet_id,
      const bool need_generate_part_name,
      ObPartitionSchema &new_partition_schema);

  int build_table_pre_split_schema(
      const int64_t tenant_id,
      const int64_t split_size,
      const ObString &db_name,
      const ObTableSchema &ori_table_schema,
      ObTableSchema &table_schema);

  int generate_all_partition_schema(
      const ObIArray<ObTabletID> &split_tablet_ids,
      const ObTableSchema &ori_table_schema,
      const ObPartitionSchema &inc_partition_schema,
      ObPartitionSchema &all_partition_schema);

  int rebuild_partition_table_schema(
      const ObIArray<ObTabletID> &split_tablet_ids,
      const ObTableSchema &ori_table_schema,
      const ObTableSchema &inc_partition_schema,
      ObTableSchema &all_partition_schema,
      ObTableSchema &table_schema);

  int build_new_partition_table_schema(
      const ObTableSchema &ori_table_schema, 
      ObTableSchema &inc_partition_schema, 
      ObTableSchema &new_table_schema);

  int build_new_table_schema(
      const ObTableSchema &ori_table_schema,
      ObPartitionSchema &partition_schema,
      ObTableSchema &new_table_schema);

  int get_and_set_part_column_info(
      ObTableSchema &table_schema, 
      char *rowkey_column_buf, 
      int64_t &buf_pos,
      int64_t &column_cnt);

  int extract_table_columns_id(
      const ObTableSchema &table_schema, 
      ObIArray<uint64_t> &column_ids);

  int get_data_table_part_ids(
      const ObTableSchema &data_table_schema, 
      ObIArray<int64_t> &part_ids);

  int get_estimated_table_size(
      const ObTableSchema &data_table_schema, 
      const ObTableSchema &index_table_schema, 
      int64_t &table_size);

  int get_exist_table_size(
      const ObTableSchema &table_schema, 
      ObIArray<TabletIDSize> &tablet_size);
  int get_exist_table_size(
      const ObTableSchema &table_schema, 
      int64_t &table_size);
  int get_table_partition_bounder(
      const ObTableSchema &table_schema,
      const int64_t part_key_length,
      ObRowkey &src_l_bound_val,
      ObRowkey &src_h_bound_val,
      ObObj *obj_l_buf,
      ObObj *obj_h_buf);
  int check_and_get_split_range(
      const ObRowkey &src_low_bound_val, 
      const ObRowkey &src_high_bound_val,
      const int64_t part_key_length,
      ObIArray<ObNewRange> &tmp_split_ranges);

  int get_partition_table_tablet_bounder(
      const ObTableSchema &table_schema, 
      const ObTabletID &source_tablet_id, 
      ObRowkey &low_bound_val, 
      ObRowkey &high_bound_val);

  int get_partition_columns_name(
      const ObTableSchema &table_schema, 
      ObIArray<ObString> &rowkey_columns);
    
  int get_partition_ranges(
      const ObTableSchema &table_schema,
      ObIArray<ObNewRange> &part_range);

  int get_partition_columns_range(
      const ObTableSchema &table_schema,
      const int64_t part_columns_cnt,
      ObIArray<ObNewRange> &part_range);

  int check_is_modify_partition_rule(
      const ObTableSchema &new_table_schema,
      const ObTableSchema &old_table_schema,
      bool &has_modify_partition_rule);

  int modify_partition_func_type_if_need(ObTableSchema &new_table_schema);

private:
  typedef common::ObArray<std::pair<ObRowkey, ObPartition*> > PartLowBound;
  class PartRangeCmpFunc
  {
  public:
    PartRangeCmpFunc() {}
    ~PartRangeCmpFunc() {}

    bool operator()(const std::pair<ObRowkey, ObPartition*> &left,
                    const std::pair<ObRowkey, ObPartition*> &right) const
    {
      return left.first < right.first;
    }
  };

private:
  rootserver::ObDDLService *ddl_service_;
  common::ObArenaAllocator allocator_; // use as ranges allocator
  ObArray<ObNewRange> split_ranges_;
};

}
}

#endif
