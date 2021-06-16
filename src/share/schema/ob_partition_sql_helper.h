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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_PARTITION_SQL_HELPER_H_
#define OCEANBASE_SHARE_SCHEMA_OB_PARTITION_SQL_HELPER_H_
#include "share/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

namespace oceanbase {
namespace obrpc {
class ObSplitPartitionArg;
}
namespace share {
class ObDMLSqlSplicer;
class ObSplitInfo;
namespace schema {

class ObPartDMLGenerator {
public:
  ObPartDMLGenerator()
  {
    MEMSET(high_bound_val_, 0, common::OB_MAX_B_HIGH_BOUND_VAL_LENGTH);
    MEMSET(b_high_bound_val_, 0, common::OB_MAX_B_HIGH_BOUND_VAL_LENGTH);
    MEMSET(list_val_, 0, common::OB_MAX_PARTITION_EXPR_LENGTH);
    MEMSET(b_list_val_, 0, common::OB_MAX_B_PARTITION_EXPR_LENGTH);
  }
  int gen_dml(ObDMLSqlSplicer& dml);
  static int add_part_name_column(const ObPartitionSchema* table, const ObPartition& part, share::ObDMLSqlSplicer& dml);

protected:
  struct PartInfo {
    uint64_t tenant_id_;
    uint64_t table_id_;
    int64_t part_id_;
    int64_t schema_version_;
    uint64_t sub_part_num_;
    int32_t sub_part_space_;
    uint64_t new_sub_part_num_;
    int32_t new_sub_part_space_;
    ObPartitionStatus status_;
    int64_t spare1_;
    int64_t spare2_;
    common::ObString spare3_;
    common::ObString comment_;
    common::ObString part_name_;
    common::ObString high_bound_val_;
    common::ObString b_high_bound_val_;
    common::ObString list_val_;
    common::ObString b_list_val_;
    char source_part_ids_str_[common::OB_MAX_CHAR_LENGTH];
    int64_t part_idx_;
    int64_t mapping_pg_part_id_;
    int64_t max_used_sub_part_id_;
    int64_t drop_schema_version_;

    int64_t sub_part_id_;  // Secondary partition field
    int64_t sub_part_idx_;
    int64_t mapping_pg_sub_part_id_;
  };
  virtual int extract_part_info(PartInfo& part_info) = 0;
  virtual int convert_to_dml(const PartInfo& part_info, ObDMLSqlSplicer& dml) = 0;
  int gen_high_bound_val_str(const common::ObRowkey& high_bound_val, common::ObString& high_bound_val_str,
      common::ObString& b_high_bound_val_str, uint64_t tenant_id);
  int gen_list_val_str(const common::ObIArray<common::ObNewRow>& list_value, common::ObString& list_val_str,
      common::ObString& b_list_val_str, uint64_t tenant_id);

private:
  char high_bound_val_[common::OB_MAX_B_HIGH_BOUND_VAL_LENGTH];
  char b_high_bound_val_[common::OB_MAX_B_HIGH_BOUND_VAL_LENGTH];
  char list_val_[common::OB_MAX_PARTITION_EXPR_LENGTH];
  char b_list_val_[common::OB_MAX_B_PARTITION_EXPR_LENGTH];
};

class ObAddIncPartDMLGenerator : public ObPartDMLGenerator {
public:
  ObAddIncPartDMLGenerator(const ObPartitionSchema* ori_table, const ObPartition& part, const int64_t inc_part_num,
      const int64_t part_idx, const int64_t schema_version)
      : ObPartDMLGenerator(),
        ori_table_(ori_table),
        part_(part),
        inc_part_num_(inc_part_num),
        part_idx_(part_idx),
        schema_version_(schema_version)
  {}
  virtual ~ObAddIncPartDMLGenerator()
  {}
  int gen_dml_for_delay_delete(ObDMLSqlSplicer& dml);

protected:
  int extract_part_info(PartInfo& part_info);
  int extract_part_info_for_delay_delete(PartInfo& part_info);
  int convert_to_dml(const PartInfo& part_info, ObDMLSqlSplicer& dml);

private:
  const ObPartitionSchema* ori_table_;
  const ObPartition& part_;
  int64_t inc_part_num_;
  int64_t part_idx_;
  int64_t schema_version_;
};

class ObAddIncSubPartDMLGenerator : public ObPartDMLGenerator {
public:
  ObAddIncSubPartDMLGenerator(const ObPartitionSchema* ori_table, const ObPartition& part,
      const ObSubPartition& sub_part, const int64_t inc_part_num, const int64_t part_idx, const int64_t subpart_idx,
      const int64_t schema_version)
      : ObPartDMLGenerator(),
        ori_table_(ori_table),
        part_(part),
        sub_part_(sub_part),
        inc_part_num_(inc_part_num),
        part_idx_(part_idx),
        subpart_idx_(subpart_idx),
        schema_version_(schema_version)
  {}
  virtual ~ObAddIncSubPartDMLGenerator()
  {}
  int gen_dml_for_delay_delete(ObDMLSqlSplicer& dml);

protected:
  int extract_part_info(PartInfo& part_info);
  int extract_part_info_for_delay_delete(PartInfo& part_info);
  int convert_to_dml(const PartInfo& part_info, ObDMLSqlSplicer& dml);

private:
  const ObPartitionSchema* ori_table_;
  const ObPartition& part_;
  const ObSubPartition& sub_part_;
  int64_t inc_part_num_;
  int64_t part_idx_;
  int64_t subpart_idx_;
  int64_t schema_version_;
};

class ObDropIncPartDMLGenerator : public ObPartDMLGenerator {
public:
  ObDropIncPartDMLGenerator(const ObPartitionSchema* ori_table, const ObPartition& part, const int64_t schema_version)
      : ObPartDMLGenerator(), ori_table_(ori_table), part_(part), schema_version_(schema_version)
  {}
  virtual ~ObDropIncPartDMLGenerator()
  {}

protected:
  int extract_part_info(PartInfo& part_info);
  int convert_to_dml(const PartInfo& part_info, ObDMLSqlSplicer& dml);

private:
  const ObPartitionSchema* ori_table_;
  const ObPartition& part_;
  int64_t schema_version_;
};

class ObDropIncSubPartDMLGenerator : public ObPartDMLGenerator {
public:
  ObDropIncSubPartDMLGenerator(
      const ObPartitionSchema* ori_table, const ObSubPartition& sub_part, const int64_t schema_version)
      : ObPartDMLGenerator(), ori_table_(ori_table), sub_part_(sub_part), schema_version_(schema_version)
  {}
  virtual ~ObDropIncSubPartDMLGenerator()
  {}

protected:
  int extract_part_info(PartInfo& part_info);
  int convert_to_dml(const PartInfo& part_info, ObDMLSqlSplicer& dml);

private:
  const ObPartitionSchema* ori_table_;
  const ObSubPartition& sub_part_;
  int64_t schema_version_;
};

class ObPartSqlHelper {
public:
  ObPartSqlHelper(const ObPartitionSchema* table, common::ObISQLClient& sql_client, bool is_tablegroup_def)
      : table_(table), sql_client_(sql_client), is_tablegroup_def_(is_tablegroup_def)
  {}
  virtual ~ObPartSqlHelper()
  {}
  virtual bool is_tablegroup_def();

protected:
  virtual bool is_deleted() const = 0;
  virtual int add_part_info_dml_column(
      const uint64_t exec_tenant_id, const ObPartitionSchema* table, ObDMLSqlSplicer& dml) = 0;
  virtual int add_part_dml_column(
      const uint64_t exec_tenant_id, const ObPartitionSchema* table, const ObPartition& part, ObDMLSqlSplicer& dml) = 0;
  virtual int add_subpart_dml_column(const uint64_t exec_tenant_id, const ObPartitionSchema* table,
      const int64_t part_idx, const int64_t subpart_idx, const ObSubPartition* subpart, ObDMLSqlSplicer& dml) = 0;
  virtual int add_def_subpart_dml_column(const uint64_t exec_tenant_id, const ObPartitionSchema* table,
      const int64_t def_subpart_idx, const int64_t mapping_pg_sub_part_id, const ObSubPartition* subpart,
      ObDMLSqlSplicer& dml) = 0;

  int iterate_part_info(const bool only_history);
  int iterate_all_part(const bool only_history, const bool deal_with_delay_delete_parts = false);
  int iterate_all_sub_part(const bool only_history, const bool deal_with_delay_delete_parts = false,
      const bool deal_with_delay_delete_subparts = false);
  int iterate_all_def_sub_part(const bool only_history);

private:
  int iterate_all_sub_part_for_template(const bool only_history);
  int iterate_all_sub_part_for_nontemplate(const bool only_history, const bool deal_with_delay_delete_parts = false,
      const bool deal_with_delay_delete_subparts = false);

protected:
  static const int64_t MAX_DML_NUM = 128;
  const ObPartitionSchema* table_;
  common::ObISQLClient& sql_client_;
  bool is_tablegroup_def_;
};

class ObAddPartInfoHelper : public ObPartSqlHelper {
public:
  ObAddPartInfoHelper(const ObPartitionSchema* table, common::ObISQLClient& sql_client, bool is_tablegroup_def)
      : ObPartSqlHelper(table, sql_client, is_tablegroup_def), high_bound_val_(NULL), list_val_(NULL), allocator_()
  {}
  virtual ~ObAddPartInfoHelper()
  {}
  int add_partition_info();
  int add_partition_info_for_gc();

protected:
  bool is_deleted() const
  {
    return false;
  }
  int add_part_info_dml_column(const uint64_t exec_tenant_id, const ObPartitionSchema* table, ObDMLSqlSplicer& dml);
  int add_part_dml_column(
      const uint64_t exec_tenant_id, const ObPartitionSchema* table, const ObPartition& part, ObDMLSqlSplicer& dml);
  int add_subpart_dml_column(const uint64_t exec_tenant_id, const ObPartitionSchema* table, const int64_t part_idx,
      const int64_t subpart_idx, const ObSubPartition* subpart, ObDMLSqlSplicer& dml);
  int add_def_subpart_dml_column(const uint64_t exec_tenant_id, const ObPartitionSchema* table,
      const int64_t def_subpart_idx, const int64_t mapping_pg_sub_part_id, const ObSubPartition* subpart,
      ObDMLSqlSplicer& dml);

private:
  int add_subpart_name_column(const ObPartitionSchema* table, const int64_t part_idx, const int64_t subpart_idx,
      const bool is_def_subpart, share::ObDMLSqlSplicer& dml);
  // add part high bound val column to dml
  int add_part_high_bound_val_column(
      const ObPartitionSchema* table, const ObBasePartition& part, share::ObDMLSqlSplicer& dml);
  // add subpart high bound val column to dml
  int add_subpart_high_bound_val_column(
      const ObPartitionSchema* table, const ObBasePartition& part, share::ObDMLSqlSplicer& dml);
  template <class P>  // ObPartition or ObSubPartition
  int add_high_bound_val_column(const P& partition, ObDMLSqlSplicer& dml);

  // add part high bound val column to dml
  int add_part_list_val_column(
      const ObPartitionSchema* table, const ObBasePartition& part, share::ObDMLSqlSplicer& dml);
  // add subpart high bound val column to dml
  int add_subpart_list_val_column(
      const ObPartitionSchema* table, const ObBasePartition& part, share::ObDMLSqlSplicer& dml);

  template <class P>  // ObPartition or ObSubPartition
  int add_list_val_column(const P& partition, ObDMLSqlSplicer& dml);

private:
  char* high_bound_val_;
  char* list_val_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObAddPartInfoHelper);
};

class ObDropPartInfoHelper : public ObPartSqlHelper {
public:
  ObDropPartInfoHelper(const ObPartitionSchema* table, common::ObISQLClient& sql_client, bool is_tablegroup_def)
      : ObPartSqlHelper(table, sql_client, is_tablegroup_def)
  {}
  virtual ~ObDropPartInfoHelper()
  {}
  int delete_partition_info();
  int delete_dropped_partition_info();

protected:
  bool is_deleted() const
  {
    return true;
  }
  int add_part_info_dml_column(const uint64_t exec_tenant_id, const ObPartitionSchema* table, ObDMLSqlSplicer& dml);
  int add_part_dml_column(
      const uint64_t exec_tenant_id, const ObPartitionSchema* table, const ObPartition& part, ObDMLSqlSplicer& dml);
  int add_subpart_dml_column(const uint64_t exec_tenant_id, const ObPartitionSchema* table, const int64_t part_idx,
      const int64_t subpart_idx, const ObSubPartition* subpart, ObDMLSqlSplicer& dml);
  int add_def_subpart_dml_column(const uint64_t exec_tenant_id, const ObPartitionSchema* table,
      const int64_t def_subpart_idx, const int64_t mapping_pg_sub_part_id, const ObSubPartition* subpart,
      ObDMLSqlSplicer& dml);

private:
  DISALLOW_COPY_AND_ASSIGN(ObDropPartInfoHelper);
};

class ObAddIncPartHelper {
public:
  ObAddIncPartHelper(const ObPartitionSchema* ori_table, const ObPartitionSchema* inc_table,
      const int64_t schema_version, common::ObISQLClient& sql_client, bool is_tablegroup_def)
      : ori_table_(ori_table),
        inc_table_(inc_table),
        schema_version_(schema_version),
        sql_client_(sql_client),
        is_tablegroup_def_(is_tablegroup_def)
  {}
  virtual ~ObAddIncPartHelper()
  {}
  int add_partition_info(const bool is_delay_delete = false);
  int add_partition_info_for_template(const bool is_delay_delete = false);
  int add_partition_info_for_nontemplate(const bool is_delay_delete = false);
  int add_partition_info_for_gc();

private:
  const ObPartitionSchema* ori_table_;
  const ObPartitionSchema* inc_table_;
  int64_t schema_version_;
  common::ObISQLClient& sql_client_;
  bool is_tablegroup_def_;
  DISALLOW_COPY_AND_ASSIGN(ObAddIncPartHelper);
};

class ObAddIncSubPartHelper {
public:
  ObAddIncSubPartHelper(const ObPartitionSchema* ori_table, const ObPartitionSchema* inc_table,
      const int64_t schema_version, common::ObISQLClient& sql_client, bool is_tablegroup_def)
      : ori_table_(ori_table),
        inc_table_(inc_table),
        schema_version_(schema_version),
        sql_client_(sql_client),
        is_tablegroup_def_(is_tablegroup_def)
  {}
  virtual ~ObAddIncSubPartHelper()
  {}
  int add_subpartition_info(const bool is_delay_delete = false);
  int add_subpartition_info_for_template(const bool is_delay_delete = false);
  int add_subpartition_info_for_nontemplate(const bool is_delay_delete = false);
  int add_subpartition_info_for_gc();

private:
  const ObPartitionSchema* ori_table_;
  const ObPartitionSchema* inc_table_;
  int64_t schema_version_;
  common::ObISQLClient& sql_client_;
  bool is_tablegroup_def_;
  DISALLOW_COPY_AND_ASSIGN(ObAddIncSubPartHelper);
};

class ObDropIncPartHelper {
public:
  ObDropIncPartHelper(const ObPartitionSchema* ori_table, const ObPartitionSchema* inc_table,
      const int64_t schema_version, common::ObISQLClient& sql_client, bool is_tablegroup_def)
      : ori_table_(ori_table),
        inc_table_(inc_table),
        schema_version_(schema_version),
        sql_client_(sql_client),
        is_tablegroup_def_(is_tablegroup_def)
  {}
  virtual ~ObDropIncPartHelper()
  {}
  int drop_partition_info();
  int drop_partition_info_for_template();
  int drop_partition_info_for_nontemplate();

private:
  const ObPartitionSchema* ori_table_;
  const ObPartitionSchema* inc_table_;
  int64_t schema_version_;
  common::ObISQLClient& sql_client_;
  bool is_tablegroup_def_;
  DISALLOW_COPY_AND_ASSIGN(ObDropIncPartHelper);
};

class ObDropIncSubPartHelper {
public:
  ObDropIncSubPartHelper(const ObPartitionSchema* ori_table, const ObPartitionSchema* inc_table,
      const int64_t schema_version, common::ObISQLClient& sql_client, bool is_tablegroup_def)
      : ori_table_(ori_table),
        inc_table_(inc_table),
        schema_version_(schema_version),
        sql_client_(sql_client),
        is_tablegroup_def_(is_tablegroup_def)
  {}
  virtual ~ObDropIncSubPartHelper()
  {}
  int drop_subpartition_info();
  int drop_dropped_subpartition_array();
  int drop_subpartition_info_for_template();
  int drop_subpartition_info_for_nontemplate(const bool deal_with_delay_delete_parts = false);

private:
  const ObPartitionSchema* ori_table_;
  const ObPartitionSchema* inc_table_;
  int64_t schema_version_;
  common::ObISQLClient& sql_client_;
  bool is_tablegroup_def_;
  DISALLOW_COPY_AND_ASSIGN(ObDropIncSubPartHelper);
};

class ObSplitPartHelperV2 {
public:
  ObSplitPartHelperV2(const ObPartitionSchema& new_schema, const int64_t schema_version,
      const share::ObSplitInfo& split_info, common::ObISQLClient& sql_client)
      : new_schema_(new_schema), schema_version_(schema_version), sql_client_(sql_client), split_info_(split_info)
  {}
  virtual ~ObSplitPartHelperV2()
  {}
  int split_partition();

private:
  const ObPartitionSchema& new_schema_;
  int64_t schema_version_;
  common::ObISQLClient& sql_client_;
  const share::ObSplitInfo& split_info_;
  DISALLOW_COPY_AND_ASSIGN(ObSplitPartHelperV2);
};

class ObSplitPartDMLGeneratorV2 : public ObPartDMLGenerator {
public:
  ObSplitPartDMLGeneratorV2(const ObPartitionSchema& new_schema, const ObPartition& part, const int64_t schema_version)
      : ObPartDMLGenerator(), new_schema_(new_schema), part_(part), schema_version_(schema_version)
  {}
  virtual ~ObSplitPartDMLGeneratorV2()
  {}

protected:
  int extract_part_info(PartInfo& part_info);
  int convert_to_dml(const PartInfo& part_info, ObDMLSqlSplicer& dml);

private:
  const ObPartitionSchema& new_schema_;
  const ObPartition& part_;
  int64_t schema_version_;
};

class ObUpdatePartHisDMLGenerator : public ObPartDMLGenerator {
public:
  ObUpdatePartHisDMLGenerator(const ObPartition& part, int64_t schema_version, const ObPartitionSchema& table_schema);
  virtual ~ObUpdatePartHisDMLGenerator()
  {}

protected:
  int extract_part_info(PartInfo& part_info);
  int convert_to_dml(const PartInfo& part_info, ObDMLSqlSplicer& dml);

private:
  const ObPartition& part_;
  int64_t schema_version_;
  const ObPartitionSchema& table_schema_;
};

class ObUpdatePartHisInfoHelper {
public:
  ObUpdatePartHisInfoHelper(const obrpc::ObSplitPartitionArg& arg, const ObSimpleTableSchemaV2& table_schema,
      common::ObISQLClient& sql_client, const int64_t schema_version)
      : arg_(arg), table_schema_(table_schema), sql_client_(sql_client), schema_version_(schema_version)
  {}
  virtual ~ObUpdatePartHisInfoHelper()
  {}
  int update_partition_info();

private:
  const obrpc::ObSplitPartitionArg& arg_;
  const ObSimpleTableSchemaV2& table_schema_;
  common::ObISQLClient& sql_client_;
  int64_t schema_version_;
};
class ObUpdatePartHisInfoHelperV2 {
public:
  ObUpdatePartHisInfoHelperV2(const ObTablegroupSchema& tablegroup_schema, common::ObISQLClient& sql_client)
      : schema_(tablegroup_schema), sql_client_(sql_client)
  {}
  virtual ~ObUpdatePartHisInfoHelperV2()
  {}
  int update_partition_info();

private:
  const ObTablegroupSchema& schema_;
  common::ObISQLClient& sql_client_;
};

}  // namespace schema
}  // namespace share
}  // namespace oceanbase

#endif /* OCEANBASE_SHARE_SCHEMA_OB_PARTITION_SQL_HELPER_H_ */
