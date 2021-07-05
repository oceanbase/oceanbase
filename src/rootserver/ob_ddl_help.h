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

#ifndef OCEANBASE_ROOTSERVER_OB_DDL_HELP_H
#define OCEANBASE_ROOTSERVER_OB_DDL_HELP_H
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_partition_modify.h"
namespace oceanbase {
namespace common {
class ObMySQLProxy;
}
namespace share {
class ObSplitInfo;
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share
namespace rootserver {
class ObDDLService;
class ObTableGroupHelp {
public:
  ObTableGroupHelp(ObDDLService& ddl_service, share::schema::ObMultiVersionSchemaService& schema_service,
      common::ObMySQLProxy& sql_proxy)
      : ddl_service_(&ddl_service), schema_service_(&schema_service), sql_proxy_(&sql_proxy)
  {}
  ~ObTableGroupHelp()
  {}

  int process_tablegroup_option_for_create_table(
      share::schema::ObSchemaGetterGuard& schema_guard, share::schema::ObTableSchema& table_schema);

  int process_tablegroup_option_for_alter_table(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema& orig_table_schema, share::schema::ObTableSchema& new_table_schema);

  int check_partition_option_for_create_table(
      share::schema::ObSchemaGetterGuard& schema_guard, share::schema::ObTableSchema& table_schema);

  int add_tables_to_tablegroup(common::ObMySQLTransaction& trans, share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTablegroupSchema& tablegroup_schema, const obrpc::ObAlterTablegroupArg& arg);

  int modify_partition_option(common::ObMySQLTransaction& trans, share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTablegroupSchema& tablegroup_schema, const obrpc::ObAlterTablegroupArg& arg);

  int check_table_alter_tablegroup(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema& orig_table_schema, share::schema::ObTableSchema& new_table_schema,
      bool alter_primary_zone = false);
  int update_max_part_id_if_needed(ObDDLService& ddl_service, share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTablegroupSchema& tablegroup_schema, const obrpc::ObAlterTablegroupArg& arg);
  int batch_force_drop_table_parts(const uint64_t tenant_id, share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTablegroupSchema* orig_tablegroup_schema,
      const share::schema::ObTablegroupSchema& alter_tablegroup_schema, ObMySQLTransaction& trans);

private:
  int check_partition_option(const share::schema::ObTablegroupSchema& tablegroup, share::schema::ObTableSchema& table);

  int check_partition_option(const share::schema::ObTablegroupSchema& tablegroup, share::schema::ObTableSchema& table,
      bool is_subpart, bool& is_matched);

  int set_mapping_pg_part_id(const share::schema::ObTablegroupSchema& tablegroup, share::schema::ObTableSchema& table);

  int set_mapping_pg_sub_part_id(
      const share::schema::ObTablegroupSchema& tablegroup, share::schema::ObTableSchema& table);

  int check_alter_partition(const share::schema::ObPartitionSchema*& orig_part_schema,
      share::schema::ObPartitionSchema*& alter_part_schema,
      const obrpc::ObAlterTablegroupArg::ModifiableOptions alter_part_type, int64_t expr_num, bool is_tablegroup);
  int check_split_partition(const share::schema::ObPartitionSchema*& orig_part_schema,
      share::schema::ObPartitionSchema*& alter_part_schema,
      const obrpc::ObAlterTablegroupArg::ModifiableOptions alter_part_type, int64_t expr_num, bool is_tablegroup);
  int check_add_partition(const share::schema::ObPartitionSchema*& orig_part_schema,
      share::schema::ObPartitionSchema*& alter_part_schema, int64_t expr_num, bool is_tablegroup);
  int check_drop_partition(const share::schema::ObPartitionSchema*& orig_part_schema,
      const share::schema::ObPartitionSchema* alter_part_schema, bool is_tablegroup);
  int check_partarray_expr_name_valid(const share::schema::ObPartitionSchema*& orig_part_schema,
      const share::schema::ObPartitionSchema* alter_part_schema, int64_t expr_num,
      const ObString* split_part_name = NULL);

  int batch_modify_table_partitions(common::ObMySQLTransaction& trans, share::schema::ObSchemaGetterGuard& schema_guard,
      share::schema::ObTablegroupSchema& inc_tablegroup_schema,
      const share::schema::ObTablegroupSchema& orig_tablegroup_schema, const int64_t new_schema_version,
      const int64_t expr_num, obrpc::ObAlterTablegroupArg::ModifiableOptions alter_part_type,
      obrpc::ObCreateTableMode create_mode);

  int modify_drop_partition(common::ObMySQLTransaction& trans, share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTablegroupSchema& orig_tablegroup_schema,
      share::schema::ObTablegroupSchema& inc_tablegroup_schema, const int64_t new_schema_version,
      const int64_t expr_num, const obrpc::ObAlterTablegroupArg& arg);
  int modify_add_partition(common::ObMySQLTransaction& trans, share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTablegroupSchema& orig_tablegroup_schema,
      share::schema::ObTablegroupSchema& inc_tablegroup_schema, const int64_t new_schema_version,
      const int64_t expr_num, const obrpc::ObAlterTablegroupArg& arg);
  int modify_split_partition(common::ObMySQLTransaction& trans, share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTablegroupSchema& orig_tablegroup_schema,
      share::schema::ObTablegroupSchema& inc_tablegroup_schema, const int64_t new_schema_version,
      const int64_t expr_num, obrpc::ObAlterTablegroupArg::ModifiableOptions alter_part_type,
      const obrpc::ObAlterTablegroupArg& arg);

  int check_part_expr_num_and_value_type(const share::schema::ObPartition* alter_part,
      const share::schema::ObPartition* orig_part, const share::schema::ObPartitionFuncType part_func_type,
      int64_t expr_num);

  int check_locality_in_modification(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema& orig_table_schema, const uint64_t tablegroup_id);

  template <typename SCHEMA>
  int check_locality_and_primary_zone_complete_match(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema& orig_table_schema, const SCHEMA& schema);

  int set_table_options_for_reset_tablegroup(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema& orig_table_schema, share::schema::ObTableSchema& new_table_schema,
      bool alter_primary_zone);
  int check_table_partition_key_exist(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObPartitionFuncType part_func_type, const int64_t tenant_id, const int64_t tg_id);
  int split_table_partition_in_tablegroup(const share::schema::ObTableSchema& orig_table_schema,
      const share::schema::ObTablegroupSchema& inc_tablegroup_schema, const int64_t schema_version,
      const obrpc::ObAlterTableArg::AlterPartitionType op_type, share::schema::AlterTableSchema& alter_table_schema,
      obrpc::ObCreateTableMode create_mode, common::ObMySQLTransaction& client);
  int add_table_partition_in_tablegroup(const share::schema::ObTableSchema& orig_table_schema,
      const share::schema::ObTablegroupSchema& inc_tablegroup_schema, const int64_t schema_version,
      share::schema::AlterTableSchema& alter_table_schema, obrpc::ObCreateTableMode create_mode,
      common::ObMySQLTransaction& client);
  int set_add_partition_part_id(
      const share::schema::ObPartitionSchema& orig_schema, share::schema::ObPartitionSchema& alter_schema);

  DISALLOW_COPY_AND_ASSIGN(ObTableGroupHelp);

private:
  ObDDLService* ddl_service_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  common::ObMySQLProxy* sql_proxy_;
};

class ObPartitionSplitHelper {
public:
  ObPartitionSplitHelper()
  {}
  virtual ~ObPartitionSplitHelper()
  {}

  static int build_split_info(
      const share::schema::ObPartitionSchema& table_schema, share::ObSplitPartition& split_info);
  static int build_split_pair(const share::schema::ObPartitionSchema* table_schema, share::ObSplitPartitionPair& pair);
  static int check_split_result(const obrpc::ObSplitPartitionArg& arg, const obrpc::ObSplitPartitionResult& result,
      share::ObSplitProgress& split_status);
  template <typename X, typename Y>
  static int add_dest_part_for_split_table(const X& origin_schema, const Y& alter_schema, X& new_schema,
      const common::ObIArray<int64_t>& part_id, const int64_t source_partition_id);
  template <typename X, typename Y>
  static int fill_split_info(const X& origin_schema, const Y& alter_schema, X& new_schema,
      const obrpc::ObAlterTableArg::AlterPartitionType& type, const int64_t schema_version,
      share::ObSplitInfo& split_info, const bool modify_table_according_to_tablegroup);
};

// Generate new part_id when split from non-partitioned table to partitioned table
template <typename X, typename Y>
int ObPartitionSplitHelper::add_dest_part_for_split_table(const X& origin_schema, const Y& alter_schema, X& new_schema,
    const ObIArray<int64_t>& part_ids, const int64_t source_partition_id)
{
  int ret = common::OB_SUCCESS;
  if (share::schema::PARTITION_LEVEL_ONE != new_schema.get_part_level() || OB_INVALID_ID == source_partition_id) {
    ret = OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", K(ret), K(source_partition_id), K(origin_schema), K(new_schema));
  }
  int64_t partition_num = alter_schema.get_partition_num();
  // There is no partition_array for HASH partition now; add non-HASH partition to schema
  for (int64_t i = 0; i < partition_num && OB_SUCC(ret); i++) {
    if (OB_ISNULL(alter_schema.get_part_array()) || OB_ISNULL(alter_schema.get_part_array()[i])) {
      ret = OB_ERR_UNEXPECTED;
      RS_LOG(WARN, "invalid part array", K(ret), K(alter_schema));
    } else {
      share::schema::ObPartition& part = *alter_schema.get_part_array()[i];
      if (OB_FAIL(new_schema.check_part_name(part))) {
        RS_LOG(WARN, "fail to check part name", K(ret), K(part));
      } else if (OB_FAIL(new_schema.check_part_id(part))) {
        RS_LOG(WARN, "fail to check part id", K(ret), K(part));
      } else if (OB_FAIL(new_schema.add_partition(part))) {
        RS_LOG(WARN, "fail to add partition", K(ret), K(part));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    share::schema::ObPartition partition;
    for (int64_t i = 0; i < part_ids.count() && OB_SUCC(ret); i++) {
      int64_t part_id = part_ids.at(i);
      if (OB_ISNULL(new_schema.get_part_array()) || i >= new_schema.get_partition_num()) {
        // For HASH partition
        partition.reset();
        partition.set_part_id(part_id);
        partition.set_part_idx(i);
        partition.set_tenant_id(origin_schema.get_tenant_id());
        partition.set_table_id(origin_schema.get_table_id());
        common::ObString part_name;
        char name_buf[OB_MAX_PARTITION_NAME_LENGTH];
        MEMSET(name_buf, 0, OB_MAX_PARTITION_NAME_LENGTH);
        if (OB_FAIL(partition.get_source_part_ids().push_back(source_partition_id))) {
          RS_LOG(WARN, "fail to push back", K(ret));
        } else if (OB_FAIL(new_schema.gen_hash_part_name(i,
                       share::schema::ObHashNameType::FIRST_PART,
                       false, /*need_upper_case*/
                       name_buf,
                       OB_MAX_PARTITION_NAME_LENGTH,
                       NULL))) {
          RS_LOG(WARN, "fail to get part name", K(ret), K(part_id));
        } else {
          part_name.assign_ptr(name_buf, OB_MAX_PARTITION_NAME_LENGTH);
          if (OB_FAIL(partition.set_part_name(part_name))) {
            RS_LOG(WARN, "fail to set part name", K(ret), K(part_name));
          } else if (OB_FAIL(new_schema.check_part_name(partition))) {
            RS_LOG(WARN, "fail to check part name", K(ret), K(partition));
          } else if (OB_FAIL(new_schema.check_part_id(partition))) {
            RS_LOG(WARN, "fail to check part id", K(ret));
          } else if (OB_FAIL(new_schema.add_partition(partition))) {
            RS_LOG(WARN, "fail to add partition", K(ret), K(partition));
          }
        }
      } else {
        share::schema::ObPartition* part = new_schema.get_part_array()[i];
        if (OB_ISNULL(part)) {
          ret = OB_ERR_UNEXPECTED;
          RS_LOG(WARN, "partition is null", K(ret), K(i), K(new_schema));
        } else {
          part->get_source_part_ids().reset();
          if (OB_FAIL(part->get_source_part_ids().push_back(source_partition_id))) {
            RS_LOG(WARN, "fail to push back", K(ret), K(part));
          } else {
            part->set_part_id(part_id);
          }
        }
      }
    }
  }
  RS_LOG(DEBUG, "set new part id", K(ret), K(new_schema), K(origin_schema), K(part_ids));
  return ret;
}

// 1. Check split table or split partition
// 2. Special processing of split hash partition
// 3. Fill schema and split_info after splitting
template <typename X, typename Y>
int ObPartitionSplitHelper::fill_split_info(const X& origin_schema, const Y& alter_schema, X& new_schema,
    const obrpc::ObAlterTableArg::AlterPartitionType& type, const int64_t schema_version,
    share::ObSplitInfo& split_info, const bool modify_table_according_to_tablegroup)

{
  int ret = common::OB_SUCCESS;
  uint64_t table_id = origin_schema.get_table_id();
  common::ObArray<int64_t> new_partition_ids;
  if (!origin_schema.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", K(origin_schema));
  } else if (obrpc::ObAlterTableArg::PARTITIONED_TABLE != type &&
             obrpc::ObAlterTableArg::PARTITIONED_PARTITION != type &&
             obrpc::ObAlterTableArg::REORGANIZE_PARTITION != type && obrpc::ObAlterTableArg::SPLIT_PARTITION != type) {
    split_info.reset();
  }

  if (OB_SUCC(ret)) {
    share::schema::ObPartitionOption& option = new_schema.get_part_option();
    split_info.table_id_ = origin_schema.get_table_id();
    new_schema.set_table_id(table_id);
    new_schema.set_tenant_id(origin_schema.get_tenant_id());
    new_schema.set_database_id(origin_schema.get_database_id());
    new_schema.set_tablegroup_id(origin_schema.get_tablegroup_id());
    new_schema.set_schema_version(schema_version);
    new_schema.set_part_level(alter_schema.get_part_level());
    new_schema.set_table_name(origin_schema.get_table_name());
    new_schema.set_partition_schema_version(schema_version);
    new_schema.set_part_func_expr_num(alter_schema.get_part_func_expr_num());
    new_schema.set_sub_part_func_expr_num(alter_schema.get_sub_part_func_expr_num());
    new_schema.get_part_option().set_max_used_part_id(origin_schema.get_part_option().get_max_used_part_id());
    new_schema.get_part_option().set_auto_part(origin_schema.get_part_option().is_auto_range_part());
    new_schema.get_part_option().set_auto_part_size(origin_schema.get_auto_part_size());
    int64_t source_part_id = 0;
    if (1 == alter_schema.get_all_part_num()) {
      // When the split destination is only 1, no real split is performed
      new_schema.set_partition_status(share::schema::PARTITION_STATUS_ACTIVE);
      if (OB_FAIL(new_partition_ids.push_back(0))) {
        RS_LOG(WARN, "fail to push back", K(ret));
      }
    } else {
      new_schema.set_partition_status(share::schema::PARTITION_STATUS_LOGICAL_SPLITTING);
      share::schema::ObPartIdsGeneratorForAdd<X> gen(origin_schema, alter_schema);
      if (OB_FAIL(gen.gen(new_partition_ids))) {
        RS_LOG(WARN, "fail to gen partition ids", K(ret));
      } else {
        RS_LOG(DEBUG, "generate new partition", K(ret), K(new_partition_ids), K(origin_schema), K(alter_schema));
      }
    }
    if (OB_FAIL(ret)) {
      // nothing todo
    } else if (obrpc::ObAlterTableArg::PARTITIONED_TABLE == type) {
      option.set_part_num(alter_schema.get_part_option().get_part_num());
    } else if (obrpc::ObAlterTableArg::REORGANIZE_PARTITION == type ||
               obrpc::ObAlterTableArg::SPLIT_PARTITION == type) {
      if (OB_FAIL(origin_schema.get_part_id_by_name(alter_schema.get_split_partition_name(), source_part_id))) {
        RS_LOG(WARN, "failed to get part_id", K(ret));
      } else {
        option.set_part_num(alter_schema.get_partition_num() + origin_schema.get_partition_num() - 1);
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      split_info.set_split_info(share::TABLE_SPLIT, origin_schema.get_part_option().get_part_func_type());
      option.set_part_func_type(alter_schema.get_part_option().get_part_func_type());
      new_schema.set_part_level(alter_schema.get_part_level());
      option.set_partition_cnt_within_partition_table(
          origin_schema.get_part_option().get_partition_cnt_within_partition_table());
      if (modify_table_according_to_tablegroup) {
        option.set_part_expr(origin_schema.get_part_option().get_part_func_expr_str());
      } else {
        option.set_part_expr(alter_schema.get_part_option().get_part_func_expr_str());
      }
      if (OB_FAIL(split_info.source_part_ids_.push_back(source_part_id))) {
        RS_LOG(WARN, "fail to add source partition", K(ret), K(source_part_id));
      } else if (OB_FAIL(add_dest_part_for_split_table(
                     origin_schema, alter_schema, new_schema, new_partition_ids, source_part_id))) {
        RS_LOG(WARN, "fail to set new part id", K(ret), K(source_part_id), K(new_schema));
      }
      RS_LOG(TRACE,
          "fill split info for partition table",
          K(ret),
          K(new_schema),
          K(split_info),
          K(origin_schema),
          K(alter_schema));
    }
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
#endif
