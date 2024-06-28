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

#define USING_LOG_PREFIX RS
#include "ob_partition_exchange.h"
#include "share/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "ob_ddl_service.h"
#include "ob_root_service.h"
#include "observer/ob_sql_client_decorator.h" // ObSQLClientRetryWeak
#include "share/ob_ddl_common.h"
#include "share/ob_debug_sync.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/schema/ob_schema_struct.h"
#include "share/tablet/ob_tablet_to_table_history_operator.h" // ObTabletToTableHistoryOperator
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
namespace rootserver
{
ObPartitionExchange::ObPartitionExchange(ObDDLService &ddl_service, const uint64_t data_version)
  : ddl_service_(ddl_service), data_version_(data_version)
{
}

ObPartitionExchange::~ObPartitionExchange()
{
}

int ObPartitionExchange::check_and_exchange_partition(const obrpc::ObExchangePartitionArg &arg, obrpc::ObAlterTableRes &res, ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.tenant_id_;
  const ObTableSchema *base_table_schema = NULL;
  const ObTableSchema *inc_table_schema = NULL;
  bool is_oracle_mode = false;
  if (OB_UNLIKELY(!ddl_service_.is_inited())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, arg.base_table_id_, base_table_schema))) {
    LOG_WARN("failed to get base table schema", K(ret), K(tenant_id), K(arg.base_table_id_));
  } else if (OB_ISNULL(base_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not found", K(ret), K(arg));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, arg.inc_table_id_, inc_table_schema))) {
    LOG_WARN("failed to get inc table schema", K(ret), K(tenant_id), K(arg.inc_table_id_));
  } else if (OB_ISNULL(inc_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not found", K(ret), K(arg));
  } else if (OB_FAIL(base_table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("check_if_oracle_compat_mode failed", K(ret), K(is_oracle_mode));
  } else if (OB_FAIL(check_partition_exchange_conditions_(arg, *base_table_schema, *inc_table_schema, is_oracle_mode, schema_guard))) {
    LOG_WARN("fail to check partition exchange conditions", K(ret), K(arg), KPC(base_table_schema), KPC(inc_table_schema), K(is_oracle_mode));
  } else if (OB_FAIL(do_exchange_partition_(arg, res, *base_table_schema, *inc_table_schema, is_oracle_mode, schema_guard))) {
    LOG_WARN("fail to do exchange partition", K(ret), K(arg), K(res), KPC(base_table_schema), KPC(inc_table_schema), K(is_oracle_mode));
  }
  return ret;
}

int ObPartitionExchange::check_partition_exchange_conditions_(const obrpc::ObExchangePartitionArg &arg, const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const bool is_oracle_mode, ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!base_table_schema.is_partitioned_table() || inc_table_schema.is_partitioned_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table type of exchanging partition tables don't match the conditions", K(ret), K(base_table_schema.is_partitioned_table()), K(inc_table_schema.is_partitioned_table()));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "exchange partition table type");
  } else if (OB_UNLIKELY(!in_supported_table_type_white_list_(base_table_schema) || !in_supported_table_type_white_list_(inc_table_schema))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("exchange partition table is not user table", K(ret), K(base_table_schema.is_user_table()), K(base_table_schema.is_ctas_tmp_table()), K(inc_table_schema.is_user_table()), K(inc_table_schema.is_ctas_tmp_table()));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Partition exchange operations for non-user tables are");
  } else if (OB_FAIL(used_pt_nt_id_map_.create(MAX_INDEXES, lib::ObLabel("ExchangePart")))) {
    LOG_WARN("failed to create used pt nt id map", K(ret));
  } else if (OB_FAIL(used_table_to_tablet_id_map_.create(MAX_INDEXES, lib::ObLabel("ExchangePart")))) {
    LOG_WARN("failed to create used pt nt tablet id map", K(ret));
  } else if (OB_FAIL(check_data_table_partition_exchange_conditions_(base_table_schema, inc_table_schema, arg.base_table_part_name_, arg.exchange_partition_level_, is_oracle_mode))) {
    LOG_WARN("failed to check data table partition exchange conditions", K(ret), K(base_table_schema), K(inc_table_schema), K(arg), K(is_oracle_mode));
  } else if (OB_FAIL(generate_auxiliary_table_mapping_(base_table_schema,
                                                       inc_table_schema,
                                                       arg.base_table_part_name_,
                                                       arg.exchange_partition_level_,
                                                       is_oracle_mode,
                                                       schema_guard))) {
    LOG_WARN("fail to generate auxiliary table mapping", K(ret), K(base_table_schema), K(inc_table_schema), K(is_oracle_mode));
  }
  return ret;
}

int ObPartitionExchange::do_exchange_partition_(const obrpc::ObExchangePartitionArg &arg, obrpc::ObAlterTableRes &res, const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const bool is_oracle_mode, ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.tenant_id_;
  int64_t schema_version = OB_INVALID_VERSION;
  ObDDLSQLTransaction trans(&ddl_service_.get_schema_service());
  ObDDLOperator ddl_operator(ddl_service_.get_schema_service(), ddl_service_.get_sql_proxy());
  if (OB_FAIL(schema_guard.get_schema_version(tenant_id, schema_version))) {
    LOG_WARN("failed to get tenant schema version", K(ret), K(tenant_id), K(schema_version));
  } else if (OB_FAIL(trans.start(&ddl_service_.get_sql_proxy(), tenant_id, schema_version))) {
    LOG_WARN("start transaction failed", K(ret), K(tenant_id), K(schema_version));
  } else {
    if (PARTITION_LEVEL_ONE == arg.exchange_partition_level_) {
      const ObPartition *data_part = nullptr;
      int64_t data_partition_index = OB_INVALID_INDEX;
      if (OB_FAIL(get_data_partition_and_index_(base_table_schema, arg.base_table_part_name_, data_part, data_partition_index))) {
        LOG_WARN("fail to get data partition and index", K(ret), K(base_table_schema), K(arg.base_table_part_name_));
      } else if (OB_ISNULL(data_part)) {
        ret = OB_PARTITION_NOT_EXIST;
        LOG_WARN("partition not found", K(ret), K(base_table_schema), K(arg.base_table_part_name_));
      } else if (OB_UNLIKELY(OB_INVALID_INDEX == data_partition_index)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid partition index", K(ret), K(base_table_schema), K(arg.base_table_part_name_));
      } else if (OB_FAIL(lock_exchange_data_table_and_partition_(tenant_id, base_table_schema, inc_table_schema, data_part->get_tablet_id(), trans))) {
        LOG_WARN("fail to exchange data table partition", K(ret), K(tenant_id), K(base_table_schema), K(inc_table_schema), K(data_part->get_tablet_id()));
      } else if (OB_FAIL(exchange_data_table_partition_(tenant_id, base_table_schema, inc_table_schema, *data_part, is_oracle_mode, ddl_operator, trans, schema_guard))) {
        LOG_WARN("fail to exchange data table partition", K(ret), K(tenant_id), K(base_table_schema), K(inc_table_schema), KPC(data_part), K(is_oracle_mode));
      } else if (OB_FAIL(exchange_auxiliary_table_partition_(tenant_id, data_partition_index, *data_part, is_oracle_mode, ddl_operator, trans, schema_guard))) {
        LOG_WARN("fail to exchange auxiliary table partition", K(ret), K(tenant_id), K(data_partition_index), KPC(data_part), K(is_oracle_mode));
      } else if (OB_FAIL(set_global_storage_index_unusable_(tenant_id, base_table_schema, inc_table_schema, ddl_operator, trans, schema_guard))) {
        LOG_WARN("fail to set global storage index unable", K(ret), K(tenant_id), K(base_table_schema), K(inc_table_schema));
      }
    } else if (PARTITION_LEVEL_TWO == arg.exchange_partition_level_) {
      const ObPartition *data_part = nullptr;
      const ObSubPartition *data_subpart = nullptr;
      int64_t data_partition_index = OB_INVALID_INDEX;
      int64_t data_subpartition_index = OB_INVALID_INDEX;
      if (OB_FAIL(get_data_subpartition_and_index_(base_table_schema, arg.base_table_part_name_, data_part, data_subpart, data_partition_index, data_subpartition_index))) {
        LOG_WARN("fail to get data subpartition and index", K(ret), K(base_table_schema), K(arg.base_table_part_name_));
      } else if (OB_ISNULL(data_part) || OB_ISNULL(data_subpart)) {
        ret = OB_PARTITION_NOT_EXIST;
        LOG_WARN("subpartition not found", K(ret), K(base_table_schema), K(arg.base_table_part_name_), KPC(data_part), KPC(data_subpart));
      } else if (OB_UNLIKELY(OB_INVALID_INDEX == data_partition_index || OB_INVALID_INDEX == data_subpartition_index)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid subpartition index", K(ret), K(base_table_schema), K(arg.base_table_part_name_), K(data_partition_index), K(data_subpartition_index));
      } else if (OB_FAIL(lock_exchange_data_table_and_partition_(tenant_id, base_table_schema, inc_table_schema, data_subpart->get_tablet_id(), trans))) {
        LOG_WARN("fail to exchange data table partition", K(ret), K(tenant_id), K(base_table_schema), K(inc_table_schema), K(data_subpart->get_tablet_id()));
      } else if (OB_FAIL(exchange_data_table_subpartition_(tenant_id, base_table_schema, inc_table_schema, *data_part, *data_subpart, is_oracle_mode, ddl_operator, trans, schema_guard))) {
        LOG_WARN("fail to exchange data table subpartition", K(ret), K(tenant_id), K(base_table_schema), K(inc_table_schema), KPC(data_part), KPC(data_subpart), K(is_oracle_mode));
      } else if (OB_FAIL(exchange_auxiliary_table_subpartition_(tenant_id, data_partition_index, data_subpartition_index, *data_part, *data_subpart, is_oracle_mode, ddl_operator, trans, schema_guard))) {
        LOG_WARN("fail to exchange auxiliary table subpartition", K(ret), K(tenant_id), K(data_partition_index), K(data_subpartition_index), KPC(data_part), KPC(data_subpart), K(is_oracle_mode));
      } else if (OB_FAIL(set_global_storage_index_unusable_(tenant_id, base_table_schema, inc_table_schema, ddl_operator, trans, schema_guard))) {
        LOG_WARN("fail to set global storage index unable", K(ret), K(tenant_id), K(base_table_schema), K(inc_table_schema));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition level is invalid", K(ret), K(tenant_id), K(arg.exchange_partition_level_), K(is_oracle_mode), K(base_table_schema), K(inc_table_schema));
    }
    if (OB_SUCC(ret)) {
      int64_t new_nt_schema_version = OB_INVALID_VERSION;
      int64_t new_pt_schema_version = OB_INVALID_VERSION;
      if (OB_FAIL(push_data_table_schema_version_(tenant_id, inc_table_schema, nullptr/*ddl_stmt_str*/, base_table_schema.get_table_id(), new_nt_schema_version, trans))) {
        LOG_WARN("failed to push data table schema version", K(ret), K(tenant_id), K(inc_table_schema), K(base_table_schema.get_table_id()));
      } else if (OB_FAIL(push_data_table_schema_version_(tenant_id, base_table_schema, &arg.ddl_stmt_str_, inc_table_schema.get_table_id(), new_pt_schema_version, trans))) {
        LOG_WARN("failed to push data table schema version", K(ret), K(tenant_id), K(base_table_schema), K(arg.ddl_stmt_str_), K(inc_table_schema.get_table_id()));
      } else if (OB_FAIL(adapting_cdc_changes_in_exchange_partition_(tenant_id, base_table_schema.get_table_id(), inc_table_schema.get_table_id(), trans))) {
        LOG_WARN("failed to adapting cdc changes in exchange_partition", K(ret), K(tenant_id), K(base_table_schema.get_table_id()), K(inc_table_schema.get_table_id()));
      } else {
        res.schema_version_ = new_pt_schema_version;
      }
    }
  }
  if (trans.is_started()) {
    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN_RET(temp_ret, "trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
      ret = (OB_SUCC(ret)) ? temp_ret : ret;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ddl_service_.publish_schema(tenant_id))) {
    LOG_WARN("publish_schema failed", K(ret), K(tenant_id));
  }
  return ret;
}

int ObPartitionExchange::lock_exchange_data_table_and_partition_(const uint64_t tenant_id, const ObTableSchema &partitioned_table_schema, const ObTableSchema &non_partitioned_table_schema, const common::ObTabletID &tablet_id, ObDDLSQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObTabletIDArray tablet_ids;
  if (OB_UNLIKELY(!partitioned_table_schema.is_valid() || !non_partitioned_table_schema.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partitioned_table_schema), K(non_partitioned_table_schema), K(tablet_id));
  } else if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
    LOG_WARN("fail to push tablet id", K(ret), K(tenant_id), K(tablet_id));
  } else if (OB_FAIL(ddl_service_.lock_table(trans, non_partitioned_table_schema))) {
    LOG_WARN("failed to lock non_partitioned table", K(ret), K(tenant_id), K(non_partitioned_table_schema));
  } else if (OB_FAIL(ddl_service_.lock_tablets(trans, tenant_id, partitioned_table_schema.get_table_id(), tablet_ids))) {
    LOG_WARN("failed to lock tablets", K(ret), K(tenant_id), K(partitioned_table_schema.get_table_id()), K(tablet_ids));
  }
  DEBUG_SYNC(BEFORE_ALTER_TABLE_EXCHANGE_PARTITION);
  return ret;
}

int ObPartitionExchange::check_data_table_partition_exchange_conditions_(const ObTableSchema &base_table_schema,
                                                                         const ObTableSchema &inc_table_schema,
                                                                         const ObString &exchange_partition_name,
                                                                         const ObPartitionLevel exchange_partition_level,
                                                                         const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(exchange_partition_name.empty() || PARTITION_LEVEL_ZERO == exchange_partition_level)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(exchange_partition_name), K(exchange_partition_level));
  } else if (OB_FAIL(check_table_conditions_in_common_(base_table_schema, inc_table_schema, exchange_partition_name, exchange_partition_level, is_oracle_mode))) {
    LOG_WARN("fail to check table conditions in common", K(ret), K(base_table_schema), K(inc_table_schema), K(exchange_partition_name), K(exchange_partition_level), K(is_oracle_mode));
  } else if (is_oracle_mode) {
    if (OB_FAIL(check_table_conditions_in_oracle_mode_(base_table_schema, inc_table_schema))) {
      LOG_WARN("fail to check table conditions in oracle mode", K(ret), K(base_table_schema), K(inc_table_schema));
    }
  } else if (OB_FAIL(check_table_conditions_in_mysql_mode_(base_table_schema, inc_table_schema))) {
    LOG_WARN("fail to check table conditions in mysql mode", K(ret), K(base_table_schema), K(inc_table_schema));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_table_all_column_conditions_(base_table_schema, inc_table_schema, is_oracle_mode))) {
      LOG_WARN("fail to check table all column conditions", K(ret), K(base_table_schema), K(inc_table_schema));
    } else if (OB_FAIL(check_table_constraints_(base_table_schema, inc_table_schema, is_oracle_mode))) {
      LOG_WARN("fail to check table constraints", K(ret), K(base_table_schema), K(inc_table_schema), K(is_oracle_mode));
    }
  }
  return ret;
}

int ObPartitionExchange::check_table_conditions_in_common_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const ObString &exchange_partition_name, const ObPartitionLevel exchange_partition_level, const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  bool is_base_table_column_store = false;
  bool is_inc_table_column_store = false;
  bool is_equal = false;
  if (OB_UNLIKELY(exchange_partition_name.empty() || PARTITION_LEVEL_ZERO == exchange_partition_level)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(exchange_partition_name), K(exchange_partition_level));
  } else if (OB_UNLIKELY(!base_table_schema.check_can_do_ddl() || !inc_table_schema.check_can_do_ddl())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("offline ddl is being executed, other ddl operations are not allowed", K(ret), K(base_table_schema.check_can_do_ddl()), K(inc_table_schema.check_can_do_ddl()));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "execute ddl while other ddl operations are executing long running ddl");
  } else if (OB_UNLIKELY(base_table_schema.get_tenant_id() != inc_table_schema.get_tenant_id())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant id of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_tenant_id()), K(inc_table_schema.get_tenant_id()));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "exchange partition tables belong to different tenants is");
  } else if (base_table_schema.is_in_splitting() || inc_table_schema.is_in_splitting()) {
    //TODO ddl must not execute on splitting table due to split not unstable
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("table is physical or logical split can not split", K(ret), K(base_table_schema), K(inc_table_schema));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "table is in physial or logical split, ddl operation");
  } else if (OB_UNLIKELY(share::ObDuplicateScope::DUPLICATE_SCOPE_NONE != base_table_schema.get_duplicate_scope() || share::ObDuplicateScope::DUPLICATE_SCOPE_NONE != inc_table_schema.get_duplicate_scope())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can't support exchanging parition between duplicate tables", K(ret), K(base_table_schema.get_duplicate_scope()), K(inc_table_schema.get_duplicate_scope()));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "exchange partition in duplicate tables");
  } else if (OB_UNLIKELY(base_table_schema.is_aux_table() != inc_table_schema.is_aux_table())) {
    LOG_WARN("aux table attribute of exchanging partition tables are not equal", K(ret), K(base_table_schema.is_aux_table()), K(inc_table_schema.is_aux_table()));
  } else if (OB_FAIL(check_partition_and_table_tablespace_(base_table_schema, inc_table_schema, exchange_partition_name, exchange_partition_level, is_oracle_mode))) {
    LOG_WARN("fail to check partition and table tablespace", K(ret), K(base_table_schema), K(inc_table_schema), K(exchange_partition_name), K(exchange_partition_level), K(is_oracle_mode));
  } else if (OB_FAIL(check_table_rowkey_infos_(base_table_schema, inc_table_schema, is_oracle_mode))) {
    LOG_WARN("fail to check table rowkey infos", K(ret), K(base_table_schema), K(inc_table_schema), K(is_oracle_mode));
  } else if (OB_FAIL(check_table_index_infos_(base_table_schema, inc_table_schema, is_oracle_mode))) {
    LOG_WARN("fail to check table index infos", K(ret), K(base_table_schema), K(inc_table_schema), K(is_oracle_mode));
  } else if (OB_FAIL(check_table_lob_infos_(base_table_schema, inc_table_schema, is_oracle_mode))) {
    LOG_WARN("fail to check table lob infos", K(ret), K(base_table_schema), K(inc_table_schema), K(is_oracle_mode));
  } else {
    is_equal = false;
    if (OB_UNLIKELY(base_table_schema.get_tablegroup_id() != inc_table_schema.get_tablegroup_id())) {
      LOG_WARN("the tablegroup id of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_tablegroup_id()), K(inc_table_schema.get_tablegroup_id()));
    } else if (OB_UNLIKELY(base_table_schema.get_load_type() != inc_table_schema.get_load_type())) {
      LOG_WARN("the load type of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_load_type()), K(inc_table_schema.get_load_type()));
    } else if (OB_UNLIKELY(share::schema::TABLE_DEF_TYPE_USER != base_table_schema.get_def_type() || share::schema::TABLE_DEF_TYPE_USER != inc_table_schema.get_def_type())) {
      LOG_WARN("not support to exchange partition in internal table", K(ret), K(base_table_schema.get_def_type()), K(inc_table_schema.get_def_type()));
    } else if (OB_UNLIKELY(base_table_schema.is_read_only() != inc_table_schema.is_read_only())) {
      if (is_oracle_mode) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("update operation not allowed on table", K(ret), K(base_table_schema.is_read_only()), K(base_table_schema.is_read_only()));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "update operation");
      }
      LOG_WARN("read only attribute of exchanging partition tables are not equal", K(ret), K(base_table_schema.is_read_only()), K(inc_table_schema.is_read_only()));
    } else if (OB_UNLIKELY(0 != strcmp(base_table_schema.get_compress_func_name(), inc_table_schema.get_compress_func_name()))) {
      ret = OB_ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION;
      LOG_WARN("compress func name of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_compress_func_name()), K(inc_table_schema.get_compress_func_name()));
      LOG_USER_ERROR(OB_ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION, "ROW_FORMAT");
    } else if (OB_UNLIKELY(base_table_schema.get_store_format() != inc_table_schema.get_store_format())) {
      ret = OB_ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION;
      LOG_WARN("store format of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_store_format()), K(inc_table_schema.get_store_format()));
      LOG_USER_ERROR(OB_ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION, "ROW_FORMAT");
    } else if (OB_FAIL(base_table_schema.get_is_column_store(is_base_table_column_store))) {
      LOG_WARN("fail to get table is column store", K(ret), K(is_base_table_column_store));
    } else if (OB_FAIL(inc_table_schema.get_is_column_store(is_inc_table_column_store))) {
      LOG_WARN("fail to get table is column store", K(ret), K(is_inc_table_column_store));
    } else if (OB_UNLIKELY(is_base_table_column_store != is_inc_table_column_store)) {
      ret = OB_ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION;
      LOG_WARN("the column store of exchanging partition tables are not equal", K(ret), K(is_base_table_column_store), K(is_inc_table_column_store));
      LOG_USER_ERROR(OB_ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION, "COLUMN_STORAGE_FORMAT");
    } else if (OB_UNLIKELY(base_table_schema.is_use_bloomfilter() != inc_table_schema.is_use_bloomfilter())) {
      LOG_WARN("use bloomfilter flag of exchanging partition tables are not equal", K(ret), K(base_table_schema.is_use_bloomfilter()), K(inc_table_schema.is_use_bloomfilter()));
    } else if (OB_UNLIKELY(base_table_schema.get_block_size() != inc_table_schema.get_block_size())) {
      LOG_WARN("block size of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_block_size()), K(inc_table_schema.get_block_size()));
    } else if (OB_UNLIKELY(base_table_schema.get_collation_type() != inc_table_schema.get_collation_type())) {
      LOG_WARN("collation type of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_collation_type()), K(inc_table_schema.get_collation_type()));
    } else if (OB_UNLIKELY(base_table_schema.get_tablet_size() != inc_table_schema.get_tablet_size())) {
      LOG_WARN("tablet size of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_tablet_size()), K(inc_table_schema.get_tablet_size()));
    } else if (OB_UNLIKELY(base_table_schema.get_pctfree() != inc_table_schema.get_pctfree())) {
      LOG_WARN("pctfree value of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_pctfree()), K(inc_table_schema.get_pctfree()));
    } else if (OB_UNLIKELY(ObPartitionStatus::PARTITION_STATUS_ACTIVE != base_table_schema.get_partition_status() || ObPartitionStatus::PARTITION_STATUS_ACTIVE != inc_table_schema.get_partition_status())) {
      LOG_WARN("partition status of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_partition_status()), K(inc_table_schema.get_partition_status()));
    } else if (OB_UNLIKELY(base_table_schema.get_partition_schema_version() != inc_table_schema.get_partition_schema_version())) {
      LOG_WARN("partition schema version of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_partition_schema_version()), K(inc_table_schema.get_partition_schema_version()));
    } else if (OB_UNLIKELY(base_table_schema.get_storage_format_version() != inc_table_schema.get_storage_format_version())) {
      LOG_WARN("storage format version of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_storage_format_version()), K(inc_table_schema.get_storage_format_version()));
    } else if (OB_UNLIKELY(base_table_schema.get_table_mode() != inc_table_schema.get_table_mode())) {
      LOG_WARN("table mode of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_table_mode()), K(inc_table_schema.get_table_mode()));
    } else if (OB_UNLIKELY(0 != base_table_schema.get_encryption_str().compare(inc_table_schema.get_encryption_str()))) {
      LOG_WARN("encryption str of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_encryption_str()), K(inc_table_schema.get_encryption_str()));
    } else if (OB_UNLIKELY(base_table_schema.get_table_flags() != inc_table_schema.get_table_flags())) {
      LOG_WARN("table flags of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_table_flags()), K(inc_table_schema.get_table_flags()));
    } else if (OB_UNLIKELY(0 != base_table_schema.get_ttl_definition().compare(inc_table_schema.get_ttl_definition()))) {
      LOG_WARN("ttl definition of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_ttl_definition()), K(inc_table_schema.get_ttl_definition()));
    } else if (OB_UNLIKELY(0 != base_table_schema.get_kv_attributes().compare(inc_table_schema.get_kv_attributes()))) {
      LOG_WARN("kv attributes of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_kv_attributes()), K(inc_table_schema.get_kv_attributes()));
    } else if (OB_UNLIKELY(base_table_schema.get_index_using_type() != inc_table_schema.get_index_using_type())) {
      LOG_WARN("index using type of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_index_using_type()), K(inc_table_schema.get_index_using_type()));
    } else if (OB_UNLIKELY(base_table_schema.get_row_store_type() != inc_table_schema.get_row_store_type())) {
      ret = OB_ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION;
      LOG_WARN("row store type of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_row_store_type()), K(inc_table_schema.get_row_store_type()));
      LOG_USER_ERROR(OB_ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION, "ROW_FORMAT");
    } else if (OB_UNLIKELY(base_table_schema.get_charset_type() != inc_table_schema.get_charset_type())) {
      LOG_WARN("charset type of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_charset_type()), K(inc_table_schema.get_charset_type()));
    } else if (OB_UNLIKELY(base_table_schema.get_compressor_type() != inc_table_schema.get_compressor_type())) {
      LOG_WARN("compressor type of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_compressor_type()), K(inc_table_schema.get_compressor_type()));
    } else if (OB_UNLIKELY(0 != base_table_schema.get_expire_info().compare(inc_table_schema.get_expire_info()))) {
      LOG_WARN("expire info of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_expire_info()), K(inc_table_schema.get_expire_info()));
    } else if (OB_UNLIKELY(base_table_schema.get_foreign_key_infos().count() != 0 || inc_table_schema.get_foreign_key_infos().count() != 0)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "exchanging partition tables have foreign key are");
      LOG_WARN("including foreign key of exchanging partition tables", K(ret), K(base_table_schema.get_foreign_key_infos().count()), K(inc_table_schema.get_foreign_key_infos().count()));
    } else {
      is_equal = true;
      LOG_INFO("pass table level conditions check in common", K(ret), K(base_table_schema.get_table_id()), K(inc_table_schema.get_table_id()));
    }
  }
  if (OB_SUCC(ret) && !is_equal) {
    ret = OB_TABLES_DIFFERENT_DEFINITIONS;
    LOG_WARN("table conditions in common of exchange tables are not equal", K(ret), K(base_table_schema), K(inc_table_schema), K(exchange_partition_name), K(exchange_partition_level));
  }
  return ret;
}

int ObPartitionExchange::check_table_conditions_in_mysql_mode_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema)
{
  int ret = OB_SUCCESS;
  bool is_equal = false;
  if (OB_UNLIKELY((0 != base_table_schema.get_autoinc_column_id() || 0 != inc_table_schema.get_autoinc_column_id()))) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "exchanging partition tables have autoincrement column");
    LOG_WARN("exchanging partition tables have autoincrement column is not supported", K(ret), K(base_table_schema.get_autoinc_column_id()), K(base_table_schema.get_autoinc_column_id()));
  } else {
    is_equal = true;
  }
  if (OB_SUCC(ret) && !is_equal) {
    ret = OB_TABLES_DIFFERENT_DEFINITIONS;
    LOG_WARN("table conditions in mysql mode are not equal", K(ret), K(base_table_schema), K(inc_table_schema));
  }
  return ret;
}

int ObPartitionExchange::check_table_all_column_conditions_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  ObTableSchema::const_column_iterator base_iter_begin = base_table_schema.column_begin();
  ObTableSchema::const_column_iterator base_iter_end = base_table_schema.column_end();
  ObTableSchema::const_column_iterator inc_iter_begin = inc_table_schema.column_begin();
  ObTableSchema::const_column_iterator inc_iter_end = inc_table_schema.column_end();
  ObColumnSchemaV2 *base_table_col_schema = NULL;
  ObColumnSchemaV2 *inc_table_col_schema = NULL;
  while (OB_SUCC(ret) && OB_SUCC(get_next_pair_column_schema_(base_iter_begin, base_iter_end, inc_iter_begin, inc_iter_end, is_oracle_mode, base_table_col_schema, inc_table_col_schema))) {
    if (OB_ISNULL(base_table_col_schema) || OB_ISNULL(inc_table_col_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to column schema", K(ret), KPC(base_table_col_schema), KPC(inc_table_col_schema));
    } else if (OB_FAIL(check_column_level_conditions_(base_table_col_schema, inc_table_col_schema, base_table_schema.is_aux_table(), is_oracle_mode))) {
      LOG_WARN("fail to check column level conditions", K(ret), K(base_table_schema), K(inc_table_schema), KPC(base_table_col_schema), KPC(inc_table_col_schema), K(base_table_schema.is_aux_table()), K(is_oracle_mode));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    if (OB_FAIL(check_table_column_groups_(base_table_schema, inc_table_schema, is_oracle_mode))) {
      LOG_WARN("fail to check table column groups", K(ret), K(base_table_schema), K(inc_table_schema), K(is_oracle_mode));
    }
  } else {
    LOG_WARN("fail to check table all column conditions", K(ret), K(base_table_schema), K(inc_table_schema), K(is_oracle_mode));
  }
  return ret;
}

int ObPartitionExchange::check_table_conditions_in_oracle_mode_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(base_table_schema.get_database_id() != inc_table_schema.get_database_id())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "exchange partition in different databases");
    LOG_WARN("database id of exchanging partition tables are not equal in oracle mode", K(ret), K(base_table_schema.get_database_id()), K(inc_table_schema.get_database_id()));
  }
  return ret;
}

int ObPartitionExchange::check_table_constraints_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  bool is_equal = true;
  ObArray<bool> used_flag;
  if (OB_UNLIKELY(base_table_schema.get_constraint_count() != inc_table_schema.get_constraint_count())) {
    is_equal = false;
    if (is_oracle_mode) {
      ret = OB_ERR_CHECK_CONSTRAINT_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION;
    }
    LOG_WARN("constraints num of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_constraint_count()), K(inc_table_schema.get_constraint_count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < base_table_schema.get_constraint_count(); i++) {
      if (OB_FAIL(used_flag.push_back(false))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObTableSchema::const_constraint_iterator base_iter = base_table_schema.constraint_begin();
      ObTableSchema::const_constraint_iterator inc_iter = inc_table_schema.constraint_begin();
      for (ObTableSchema::const_constraint_iterator base_iter = base_table_schema.constraint_begin(); OB_SUCC(ret) && is_equal && base_iter != base_table_schema.constraint_end(); ++base_iter) {
        if (OB_ISNULL(base_iter) || OB_ISNULL(*base_iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("constraint is null", K(ret));
        } else {
          int64_t loc = 0;
          bool found_same_constraint = false;
          for (ObTableSchema::const_constraint_iterator inc_iter = inc_table_schema.constraint_begin(); OB_SUCC(ret) && !found_same_constraint && inc_iter != inc_table_schema.constraint_end(); ++inc_iter, loc++) {
            if (OB_ISNULL(inc_iter) || OB_ISNULL(*inc_iter)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("constraint is null", K(ret));
            } else if (used_flag[loc]) {
            } else if ((*base_iter)->is_match_partition_exchange_constraint_conditions(*(*inc_iter))) {
              found_same_constraint = true;
              used_flag[loc] = true;
            }
          }
          if (OB_SUCC(ret) && !found_same_constraint) {
            if (is_oracle_mode) {
              if (CONSTRAINT_TYPE_PRIMARY_KEY == (*base_iter)->get_constraint_type() || CONSTRAINT_TYPE_NOT_NULL == (*base_iter)->get_constraint_type()) {
                ret = OB_ERR_COLUMN_TYPE_OR_SIZE_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION;
              } else {
                ret = OB_ERR_CHECK_CONSTRAINT_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION;
              }
            }
            is_equal = false;
            LOG_WARN("check constraints of exchanging partition tables are not equal", K(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && !is_equal) {
    ret = OB_TABLES_DIFFERENT_DEFINITIONS;
    LOG_WARN("check constraints of exchanging partition tables are not equal", K(ret));
  }
  return ret;
}

int ObPartitionExchange::check_column_level_conditions_(const ObColumnSchemaV2 *base_table_col_schema, const ObColumnSchemaV2 *inc_table_col_schema, const bool is_aux_table_column, const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(base_table_col_schema) || OB_ISNULL(inc_table_col_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column schema is null", K(ret), KPC(base_table_col_schema), KPC(inc_table_col_schema));
  } else if (!is_oracle_mode) {
    if (OB_FAIL(check_column_conditions_in_mysql_mode_(base_table_col_schema, inc_table_col_schema, is_aux_table_column))) {
      LOG_WARN("fail to check column conditions in mysql mode", K(ret), KPC(base_table_col_schema), KPC(inc_table_col_schema), K(is_aux_table_column));
    }
  } else if (OB_FAIL(check_column_conditions_in_oracle_mode_(base_table_col_schema, inc_table_col_schema, is_aux_table_column))) {
    LOG_WARN("fail to check column conditions in oracle mode", K(ret), KPC(base_table_col_schema), KPC(inc_table_col_schema), K(is_aux_table_column));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_column_conditions_in_common_(base_table_col_schema, inc_table_col_schema, is_oracle_mode))) {
      LOG_WARN("fail to check column conditions in common", K(ret), KPC(base_table_col_schema), KPC(inc_table_col_schema), K(is_oracle_mode));
    }
  }
  return ret;
}
// TODO: If the partition exchange tables contain instant columns, an error is reported now. In subsequent versions, if there are instant columns, both base_table_col_schema and inc_table_col_schema need to be instant columns, and the instant columns need not require the same column name.
int ObPartitionExchange::check_column_conditions_in_common_(const ObColumnSchemaV2 *base_table_col_schema, const ObColumnSchemaV2 *inc_table_col_schema, const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  bool is_equal = false;
  if (OB_ISNULL(base_table_col_schema) || OB_ISNULL(inc_table_col_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column schema is null", K(ret), KPC(base_table_col_schema), KPC(inc_table_col_schema));
  } else if (OB_UNLIKELY(base_table_col_schema->get_tenant_id() != inc_table_col_schema->get_tenant_id())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("column tenant id of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->get_tenant_id()), K(inc_table_col_schema->get_tenant_id()));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "exchange partition tables belong to different tenants is");
  } else if (OB_UNLIKELY(base_table_col_schema->is_unused() || inc_table_col_schema->is_unused())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "exchanging partition tables have instant column");
    LOG_WARN("exchanging partition tables have instant column is not supported", K(ret), K(base_table_col_schema->is_unused()), K(inc_table_col_schema->is_unused()));
  } else if (OB_UNLIKELY(base_table_col_schema->get_rowkey_position() != inc_table_col_schema->get_rowkey_position())) {
    LOG_WARN("column rowkey position of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->get_rowkey_position()), K(inc_table_col_schema->get_rowkey_position()));
  } else if (OB_UNLIKELY(base_table_col_schema->get_index_position() != inc_table_col_schema->get_index_position())) {
    LOG_WARN("column index position of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->get_index_position()), K(inc_table_col_schema->get_index_position()));
  } else if (OB_UNLIKELY(base_table_col_schema->get_order_in_rowkey() != inc_table_col_schema->get_order_in_rowkey())) {
    LOG_WARN("column order in rowkey of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->get_order_in_rowkey()), K(inc_table_col_schema->get_order_in_rowkey()));
  } else if (OB_UNLIKELY(base_table_col_schema->get_data_length() != inc_table_col_schema->get_data_length())) {
    LOG_WARN("column data length of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->get_data_length()), K(inc_table_col_schema->get_data_length()));
  } else if (OB_UNLIKELY(base_table_col_schema->get_data_precision() != inc_table_col_schema->get_data_precision())) {
    LOG_WARN("column data precision of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->get_data_precision()), K(inc_table_col_schema->get_data_precision()));
  } else if (OB_UNLIKELY(base_table_col_schema->get_data_scale() != inc_table_col_schema->get_data_scale())) {
    LOG_WARN("column data scale of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->get_data_scale()), K(inc_table_col_schema->get_data_scale()));
  } else if (OB_UNLIKELY(base_table_col_schema->is_zero_fill() != inc_table_col_schema->is_zero_fill())) {
    LOG_WARN("column is zero fill option of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_zero_fill()), K(inc_table_col_schema->is_zero_fill()));
  } else if (OB_UNLIKELY(base_table_col_schema->is_autoincrement() != inc_table_col_schema->is_autoincrement())) {
    LOG_WARN("column is autoincrement option of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_autoincrement()), K(inc_table_col_schema->is_autoincrement()));
  } else if (OB_UNLIKELY(base_table_col_schema->is_hidden() != inc_table_col_schema->is_hidden())) {
    LOG_WARN("column is hidden option of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_hidden()), K(inc_table_col_schema->is_hidden()));
  } else if (OB_UNLIKELY(base_table_col_schema->get_collation_type() != inc_table_col_schema->get_collation_type())) {
    LOG_WARN("column collation type of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->get_collation_type()), K(inc_table_col_schema->get_collation_type()));
  } else if (OB_UNLIKELY(base_table_col_schema->get_srs_id() != inc_table_col_schema->get_srs_id())) {
    LOG_WARN("column srs id of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->get_srs_id()), K(inc_table_col_schema->get_srs_id()));
  } else if (OB_UNLIKELY(base_table_col_schema->get_sub_data_type() != inc_table_col_schema->get_sub_data_type())) {
    LOG_WARN("column sub data type of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->get_sub_data_type()), K(inc_table_col_schema->get_sub_data_type()));
  } else if (OB_UNLIKELY(base_table_col_schema->is_rowkey_column() != inc_table_col_schema->is_rowkey_column())) {
    LOG_WARN("is rowkey column attribute of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_rowkey_column()), K(inc_table_col_schema->is_rowkey_column()));
  } else if (OB_UNLIKELY(base_table_col_schema->get_data_type() != inc_table_col_schema->get_data_type())) {
    LOG_WARN("column data type of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->get_data_type()), K(inc_table_col_schema->get_data_type()));
  } else if (OB_UNLIKELY(base_table_col_schema->get_lob_chunk_size() != inc_table_col_schema->get_lob_chunk_size())) {
    LOG_WARN("lob chunk size of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->get_lob_chunk_size()), K(inc_table_col_schema->get_lob_chunk_size()));
  } else if (OB_UNLIKELY(!(base_table_col_schema->get_local_session_var() == inc_table_col_schema->get_local_session_var()))) {
    LOG_WARN("local session var of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->get_local_session_var()), K(inc_table_col_schema->get_local_session_var()));
  } else if (OB_UNLIKELY(base_table_col_schema->get_skip_index_attr().get_packed_value() != inc_table_col_schema->get_skip_index_attr().get_packed_value())) {
    LOG_WARN("column skip index attr of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->get_skip_index_attr()), K(inc_table_col_schema->get_skip_index_attr()));
  } else if (OB_UNLIKELY(base_table_col_schema->get_udt_set_id() != inc_table_col_schema->get_udt_set_id())) {
    LOG_WARN("column udt set id of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->get_udt_set_id()), K(inc_table_col_schema->get_udt_set_id()));
  } else if (OB_UNLIKELY(base_table_col_schema->is_not_null_rely_column() != inc_table_col_schema->is_not_null_rely_column())) {
    LOG_WARN("is not null rely column attribute of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_not_null_rely_column()), K(inc_table_col_schema->is_not_null_rely_column()));
  } else if (OB_UNLIKELY(base_table_col_schema->is_not_null_enable_column() != inc_table_col_schema->is_not_null_enable_column())) {
    LOG_WARN("is not null enable column attribute of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_not_null_enable_column()), K(inc_table_col_schema->is_not_null_enable_column()));
  } else if (OB_UNLIKELY(base_table_col_schema->is_not_null_validate_column() != inc_table_col_schema->is_not_null_validate_column())) {
    LOG_WARN("is not null validate column attribute of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_not_null_validate_column()), K(inc_table_col_schema->is_not_null_validate_column()));
  } else if (OB_UNLIKELY(base_table_col_schema->is_invisible_column() != inc_table_col_schema->is_invisible_column())) {
    LOG_WARN("is visible column attribute of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_invisible_column()), K(inc_table_col_schema->is_invisible_column()));
  } else if (OB_UNLIKELY(base_table_col_schema->is_xmltype() != inc_table_col_schema->is_xmltype())) {
    LOG_WARN("is xmltype column attribute of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_xmltype()), K(inc_table_col_schema->is_xmltype()));
  } else if (OB_FAIL(check_column_flags_(base_table_col_schema, inc_table_col_schema, is_equal))) {
    LOG_WARN("fail to check column flags", K(ret), KPC(base_table_col_schema), KPC(inc_table_col_schema), K(is_equal));
  } else if (!is_equal) {
    LOG_WARN("column flags of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->get_column_flags()), K(inc_table_col_schema->get_column_flags()), K(is_equal));
  } else if (OB_FAIL(compare_column_extended_type_info_(base_table_col_schema->get_extended_type_info(), inc_table_col_schema->get_extended_type_info(), is_equal))) {
    LOG_WARN("fail to compare column extended type info", K(ret), K(base_table_col_schema->get_extended_type_info()), K(inc_table_col_schema->get_extended_type_info()), K(is_equal));
  } else if (!is_equal) {
    LOG_WARN("column extended type info count of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->get_extended_type_info()), K(inc_table_col_schema->get_extended_type_info()), K(is_equal));
  } else if (OB_FAIL(check_generate_column_conditions_(base_table_col_schema, inc_table_col_schema, is_equal))) {
    LOG_WARN("fail to check generate column conditions", K(ret), KPC(base_table_col_schema), KPC(inc_table_col_schema), K(is_equal));
  } else if (!is_equal) {
    LOG_WARN("generate column conditions of exchanging partition tables are not equal", K(ret), KPC(base_table_col_schema), KPC(inc_table_col_schema), K(is_equal));
  } else {
    is_equal = true;
  }
  if (OB_SUCC(ret) && !is_equal) {
    if (is_oracle_mode) {
      ret = OB_ERR_COLUMN_TYPE_OR_SIZE_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION;
    } else {
      ret = OB_TABLES_DIFFERENT_DEFINITIONS;
    }
    LOG_WARN("all column conditions of exchange tables are not equal in common", K(ret), KPC(base_table_col_schema), KPC(inc_table_col_schema), K(is_oracle_mode));
  }
  return ret;
}

int ObPartitionExchange::check_column_conditions_in_mysql_mode_(const ObColumnSchemaV2 *base_table_col_schema, const ObColumnSchemaV2 *inc_table_col_schema, const bool is_aux_table_column)
{
  int ret = OB_SUCCESS;
  bool is_equal = false;
  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
  if (OB_ISNULL(base_table_col_schema) || OB_ISNULL(inc_table_col_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column schema is null", K(ret), KPC(base_table_col_schema), KPC(inc_table_col_schema));
  } else {
    ObColumnNameHashWrapper base_column_key(base_table_col_schema->get_column_name_str());
    ObColumnNameHashWrapper inc_column_key(inc_table_col_schema->get_column_name_str());
    //In the auxiliary table, the column names of redundant generated columns produced by function indexes are not required to be the same, as other columns of the indexed table from the data table, which has already been compared.
    if (OB_UNLIKELY(!is_aux_table_column && !(base_column_key == inc_column_key))) {
      LOG_WARN("column name of exchanging partition tables are not equal", K(ret), K(is_aux_table_column), K(base_table_col_schema->get_column_name_str()), K(inc_table_col_schema->get_column_name_str()));
    } else if (OB_UNLIKELY(base_table_col_schema->is_on_update_current_timestamp() != inc_table_col_schema->is_on_update_current_timestamp())) {
      LOG_WARN("column is on update_current_timestamp attribute of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_on_update_current_timestamp()), K(inc_table_col_schema->is_on_update_current_timestamp()));
    } else if (OB_UNLIKELY(base_table_col_schema->is_nullable() != inc_table_col_schema->is_nullable())) {
      LOG_WARN("column is nullable attribute of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_nullable()), K(inc_table_col_schema->is_nullable()));
    } else if (OB_FAIL(check_column_default_value_(base_table_col_schema, inc_table_col_schema, false/*is_oracle_mode*/, is_equal))) {
      LOG_WARN("fail to check column default value", K(ret), KPC(base_table_col_schema), KPC(inc_table_col_schema), K(is_equal));
    } else if (!is_equal) {
      LOG_WARN("default value of exchanging partition tables are not equal", K(ret));
    } else {
      is_equal = true;
    }
  }
  if (OB_SUCC(ret) && !is_equal) {
    ret = OB_TABLES_DIFFERENT_DEFINITIONS;
    LOG_WARN("all column conditions of exchange tables are not equal in mysql mode", K(ret), KPC(base_table_col_schema), KPC(inc_table_col_schema));
  }
  return ret;
}

int ObPartitionExchange::check_column_conditions_in_oracle_mode_(const ObColumnSchemaV2 *base_table_col_schema, const ObColumnSchemaV2 *inc_table_col_schema, const bool is_aux_table_column)
{
  int ret = OB_SUCCESS;
  bool is_equal = false;
  lib::CompatModeGuard guard(lib::Worker::CompatMode::ORACLE);
  if (OB_ISNULL(base_table_col_schema) || OB_ISNULL(inc_table_col_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column schema is null", K(ret), KPC(base_table_col_schema), KPC(inc_table_col_schema));
  } else {
    ObColumnNameHashWrapper base_column_key(base_table_col_schema->get_column_name_str());
    ObColumnNameHashWrapper inc_column_key(inc_table_col_schema->get_column_name_str());
    //In the auxiliary table, the column names of redundant generated columns produced by function indexes are not required to be the same, as other columns of the indexed table from the data table, which has already been compared.
    if (OB_UNLIKELY(!is_aux_table_column && !(base_column_key == inc_column_key))) {
      LOG_WARN("column name of exchanging partition tables are not equal", K(ret), K(is_aux_table_column), K(base_table_col_schema->get_column_name_str()), K(inc_table_col_schema->get_column_name_str()));
    } else if (OB_UNLIKELY(base_table_col_schema->is_identity_column() || inc_table_col_schema->is_identity_column())) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "exchanging partition tables have identity column");
      LOG_WARN("exchanging partition tables have identity column is not supported", K(ret), K(base_table_col_schema->is_identity_column()), K(inc_table_col_schema->is_identity_column()));
    } else if (OB_UNLIKELY(base_table_col_schema->is_identity_column() != inc_table_col_schema->is_identity_column())) {
      LOG_WARN("is identity column attribute of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_identity_column()), K(inc_table_col_schema->is_identity_column()));
    } else if (OB_UNLIKELY(base_table_col_schema->is_default_on_null_identity_column() != inc_table_col_schema->is_default_on_null_identity_column())) {
      LOG_WARN("is default on null identity column attribute of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_default_on_null_identity_column()), K(inc_table_col_schema->is_default_on_null_identity_column()));
    } else if (OB_UNLIKELY(base_table_col_schema->is_always_identity_column() != inc_table_col_schema->is_always_identity_column())) {
      LOG_WARN("is always identity column attribute of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_always_identity_column()), K(inc_table_col_schema->is_always_identity_column()));
    } else if (OB_UNLIKELY(base_table_col_schema->is_default_identity_column() != inc_table_col_schema->is_default_identity_column())) {
      LOG_WARN("is default identity column attribute of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_default_identity_column()), K(inc_table_col_schema->is_default_identity_column()));
    } else if (OB_UNLIKELY((base_table_col_schema->is_rowkey_column() && inc_table_col_schema->is_rowkey_column()) && (base_table_col_schema->is_nullable() != inc_table_col_schema->is_nullable()))) {
      LOG_WARN("column is nullable attribute of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_nullable()), K(inc_table_col_schema->is_nullable()));
    } else if (OB_FAIL(check_column_default_value_(base_table_col_schema, inc_table_col_schema, true/*is_oracle_mode*/, is_equal))) {
      LOG_WARN("fail to check column default value", K(ret), KPC(base_table_col_schema), KPC(inc_table_col_schema), K(is_equal));
    } else if (!is_equal) {
      LOG_WARN("default value of exchanging partition tables are not equal", K(ret));
    } else {
      is_equal = true;
    }
  }
  if (OB_SUCC(ret) && !is_equal) {
    ret = OB_ERR_COLUMN_TYPE_OR_SIZE_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION;
    LOG_WARN("all column conditions of exchange tables are not equal in oracle mode", K(ret), KPC(base_table_col_schema), KPC(inc_table_col_schema));
  }
  return ret;
}

int ObPartitionExchange::check_generate_column_conditions_(const ObColumnSchemaV2 *base_table_col_schema, const ObColumnSchemaV2 *inc_table_col_schema, bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  ObString base_col_expr_str;
  ObString inc_col_expr_str;
  if (OB_ISNULL(base_table_col_schema) || OB_ISNULL(inc_table_col_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column schema is null", K(ret), KPC(base_table_col_schema), KPC(inc_table_col_schema));
  } else if (OB_UNLIKELY(base_table_col_schema->is_virtual_generated_column() != inc_table_col_schema->is_virtual_generated_column())) {
    ret = OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN;
    LOG_WARN("virtual generated column of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_virtual_generated_column()), K(inc_table_col_schema->is_virtual_generated_column()));
    LOG_USER_ERROR(OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN, "Exchanging partitions for non-generated columns");
  } else if (OB_UNLIKELY(base_table_col_schema->is_stored_generated_column() != inc_table_col_schema->is_stored_generated_column())) {
    ret = OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN;
    LOG_WARN("stored generated column of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_stored_generated_column()), K(inc_table_col_schema->is_stored_generated_column()));
    LOG_USER_ERROR(OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN, "Exchanging partitions for non-generated columns");
  } else if (base_table_col_schema->is_stored_generated_column() && inc_table_col_schema->is_stored_generated_column()) {
    if (OB_FAIL(base_table_col_schema->get_cur_default_value().get_string(base_col_expr_str))) {
      LOG_WARN("fail to get base column cur default value str", K(ret), KPC(base_table_col_schema), K(base_col_expr_str));
    } else if (OB_FAIL(inc_table_col_schema->get_cur_default_value().get_string(inc_col_expr_str))) {
      LOG_WARN("fail to get inc column cur default value str", K(ret), KPC(inc_table_col_schema), K(inc_col_expr_str));
    } else if (OB_UNLIKELY(0 != base_col_expr_str.compare(inc_col_expr_str))) {
      is_equal = false;
      LOG_WARN("stored generated column expr strs are not equal", K(ret), K(base_col_expr_str), K(inc_col_expr_str));
    }
  }
  return ret;
}
int ObPartitionExchange::check_column_flags_(const ObColumnSchemaV2 *base_table_col_schema, const ObColumnSchemaV2 *inc_table_col_schema, bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (OB_ISNULL(base_table_col_schema) || OB_ISNULL(inc_table_col_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column schema is null", K(ret), KPC(base_table_col_schema), KPC(inc_table_col_schema));
  } else if (OB_UNLIKELY(base_table_col_schema->has_not_null_constraint() != inc_table_col_schema->has_not_null_constraint())) {
    LOG_WARN("has not null column attribute of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->has_not_null_constraint()), K(inc_table_col_schema->has_not_null_constraint()));
  } else if (OB_UNLIKELY(base_table_col_schema->is_fulltext_column() != inc_table_col_schema->is_fulltext_column())) {
    LOG_WARN("is full text column attribute of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_fulltext_column()), K(inc_table_col_schema->is_fulltext_column()));
  } else if (OB_UNLIKELY(base_table_col_schema->is_primary_vp_column() != inc_table_col_schema->is_primary_vp_column())) {
    LOG_WARN("is primary vp column attribute of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_primary_vp_column()), K(inc_table_col_schema->is_primary_vp_column()));
  } else if (OB_UNLIKELY(base_table_col_schema->is_aux_vp_column() != inc_table_col_schema->is_aux_vp_column())) {
    LOG_WARN("is aux vp column attribute of exchanging partition tables are not equal", K(ret), K(base_table_col_schema->is_aux_vp_column()), K(inc_table_col_schema->is_aux_vp_column()));
  } else {
    is_equal = true;
  }
  return ret;
}

int ObPartitionExchange::check_column_default_value_(const ObColumnSchemaV2 *base_table_col_schema, const ObColumnSchemaV2 *inc_table_col_schema, const bool is_oracle_mode, bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  if (OB_ISNULL(base_table_col_schema) || OB_ISNULL(inc_table_col_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column schema is null", K(ret), KPC(base_table_col_schema), KPC(inc_table_col_schema));
  } else if (base_table_col_schema->is_virtual_generated_column() && inc_table_col_schema->is_virtual_generated_column()) {
  } else {
    ObString base_col_expr_str;
    ObString inc_col_expr_str;
    ObObj base_orig_default_value = base_table_col_schema->get_orig_default_value();
    ObObj inc_orig_default_value = inc_table_col_schema->get_orig_default_value();
    ObObj base_cur_default_value = base_table_col_schema->get_cur_default_value();
    ObObj inc_cur_default_value = inc_table_col_schema->get_cur_default_value();
    if (OB_FAIL(compare_default_value_(base_orig_default_value, inc_orig_default_value, is_oracle_mode, is_equal))) {
      LOG_WARN("fail to compare orig default value", K(ret), K(base_orig_default_value), K(inc_orig_default_value), K(is_oracle_mode));
    } else if (!is_equal) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "exchanging partition tables define different default values in alter table add column ddl operation");
      LOG_WARN("orig default value are not equal", K(ret), K(base_orig_default_value), K(inc_orig_default_value), K(is_oracle_mode), K(is_equal));
    } else if (OB_FAIL(compare_default_value_(base_cur_default_value, inc_cur_default_value, is_oracle_mode, is_equal))) {
      LOG_WARN("fail to compare cur default value", K(ret), K(base_cur_default_value), K(inc_cur_default_value), K(is_oracle_mode));
    } else if (!is_equal) {
      LOG_WARN("cur default value are not equal", K(ret), K(base_cur_default_value), K(inc_cur_default_value), K(is_oracle_mode), K(is_equal));
    }
  }
  return ret;
}

int ObPartitionExchange::compare_default_value_(ObObj &l_value, ObObj &r_value, const bool is_oracle_mode, bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  int cmp = 0;
  if (OB_UNLIKELY(l_value.is_null() != r_value.is_null())) {
    LOG_WARN("default value is_null are not equal", K(ret), K(l_value), K(r_value));
  } else if (l_value.is_null() && r_value.is_null()) {
    is_equal = true;
  } else if (OB_UNLIKELY(l_value.get_type() != r_value.get_type())) {
    LOG_WARN("default value type are not equal", K(ret), K(l_value), K(r_value));
  } else if (OB_UNLIKELY(l_value.get_collation_type() != r_value.get_collation_type())) {
    LOG_WARN("default value collation type are not equal", K(ret), K(l_value), K(r_value));
  } else if (is_oracle_mode) {
    ObString l_value_expr_str;
    ObString r_value_expr_str;
    if (OB_FAIL(l_value.get_string(l_value_expr_str))) {
      LOG_WARN("fail to get column default value str", K(ret), K(l_value), K(l_value_expr_str));
    } else if (OB_FAIL(r_value.get_string(r_value_expr_str))) {
      LOG_WARN("fail to get column default value str", K(ret), K(r_value), K(r_value_expr_str));
    } else if (OB_UNLIKELY(0 != l_value_expr_str.compare(r_value_expr_str))) {
      LOG_WARN("default column expr strs are not equal", K(ret), K(l_value_expr_str), K(r_value_expr_str));
    } else {
      is_equal = true;
    }
  } else if (CS_TYPE_INVALID == l_value.get_collation_type()) {
    is_equal = true;
  } else if (OB_FAIL(l_value.compare(r_value, cmp))) {
    LOG_WARN("default value are not equal", K(ret), K(l_value), K(r_value), K(cmp));
  } else if (OB_UNLIKELY(0 != cmp)) {
    LOG_WARN("default value are not equal", K(ret), K(l_value), K(r_value), K(cmp));
  } else {
    is_equal = true;
  }
  return ret;
}

int ObPartitionExchange::check_partition_and_table_tablespace_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const ObString &exchange_partition_name, const ObPartitionLevel exchange_partition_level, const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  const ObPartition *part = nullptr;
  const ObSubPartition *subpart = nullptr;
  bool is_equal = false;
  if (OB_UNLIKELY(exchange_partition_name.empty() || (PARTITION_LEVEL_ZERO == exchange_partition_level))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(exchange_partition_name), K(exchange_partition_level));
  } else if (OB_UNLIKELY(base_table_schema.get_tablespace_id() != inc_table_schema.get_tablespace_id())) {
    LOG_WARN("tablespace id of exchanging tables are not equal", K(ret), K(base_table_schema.get_tablespace_id()), K(inc_table_schema.get_tablespace_id()));
  } else if (!base_table_schema.is_aux_table() && !inc_table_schema.is_aux_table()) {
    if (PARTITION_LEVEL_ONE == exchange_partition_level) {
      if (OB_FAIL(base_table_schema.get_partition_by_name(exchange_partition_name, part))) {
        LOG_WARN("get part by name failed", K(ret), K(exchange_partition_name), K(base_table_schema));
      } else if (OB_ISNULL(part)) {
        ret = OB_PARTITION_NOT_EXIST;
        LOG_WARN("partition not found", K(ret), K(exchange_partition_name), K(base_table_schema));
      } else if (OB_UNLIKELY(OB_INVALID_INDEX != part->get_tablespace_id())) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "When the partition contains a tablespace, partition exchange is not");
        LOG_WARN("when the partition contains a tablespace, partition exchange is not supported", K(ret), K(part->get_tablespace_id()), K(inc_table_schema.get_tablespace_id()));
      } else {
        is_equal = true;
      }
    } else if (PARTITION_LEVEL_TWO == exchange_partition_level) {
      if (OB_FAIL(base_table_schema.get_subpartition_by_name(exchange_partition_name, part, subpart))) {
        LOG_WARN("get sub part by name failed", K(ret), K(exchange_partition_name), K(base_table_schema));
      } else if (OB_ISNULL(part) || OB_ISNULL(subpart)) {
        ret = OB_PARTITION_NOT_EXIST;
        LOG_WARN("partition not found", K(ret), K(exchange_partition_name), K(base_table_schema), K(part), K(subpart));
      } else if (OB_UNLIKELY(OB_INVALID_INDEX != subpart->get_tablespace_id())) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "When the partition contains a tablespace, partition exchange is not");
        LOG_WARN("when the subpartition contains a tablespace, partition exchange is not supported", K(ret), K(subpart->get_tablespace_id()), K(inc_table_schema.get_tablespace_id()));
      } else {
        is_equal = true;
      }
    }
  } else if (base_table_schema.is_aux_table() && inc_table_schema.is_aux_table()) {
    is_equal = true;
  }
  if (OB_SUCC(ret) && !is_equal) {
    if (is_oracle_mode) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "exchange partition in different tablespaces");
    } else {
      ret = OB_ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION;
      LOG_USER_ERROR(OB_ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION, "TABLESPACE");
    }
    LOG_WARN("tablespace id are not equal between partition and table", K(ret), K(base_table_schema), K(inc_table_schema), K(exchange_partition_name), K(exchange_partition_level));
  }
  return ret;
}

int ObPartitionExchange::check_table_index_infos_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  bool is_equal = false;
  if (OB_UNLIKELY(base_table_schema.get_index_column_num() != inc_table_schema.get_index_column_num())) {
    LOG_WARN("index column num of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_index_column_num()), K(inc_table_schema.get_index_column_num()));
  } else if (OB_FAIL(compare_two_rowkey_info_(base_table_schema.get_index_info(), inc_table_schema.get_index_info(), is_equal))) {
    LOG_WARN("fail to compare two rowkey info", K(ret), K(base_table_schema.get_index_info()), K(inc_table_schema.get_index_info()));
  } else if (!is_equal) {
    LOG_WARN("index info of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_index_info()), K(inc_table_schema.get_index_info()));
  }
  if (OB_SUCC(ret) && !is_equal) {
    if (is_oracle_mode) {
      ret = OB_ERR_INDEX_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION;
    } else {
      ret = OB_TABLES_DIFFERENT_DEFINITIONS;
    }
    LOG_WARN("table index infos of exchanging partition tables are not equal", K(ret), K(base_table_schema), K(inc_table_schema));
  }
  return ret;
}

int ObPartitionExchange::check_table_lob_infos_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  bool is_equal = false;
  if (OB_UNLIKELY(base_table_schema.has_lob_column() != inc_table_schema.has_lob_column())) {
    LOG_WARN("has lob column type of exchanging partition tables are not equal", K(ret), K(base_table_schema.has_lob_column()), K(inc_table_schema.has_lob_column()));
  } else if (OB_UNLIKELY(base_table_schema.has_lob_aux_table() != inc_table_schema.has_lob_aux_table())) {
    LOG_WARN("has lob aux table type of exchanging partition tables are not equal", K(ret), K(base_table_schema.has_lob_aux_table()), K(inc_table_schema.has_lob_aux_table()));
  } else if (OB_UNLIKELY(base_table_schema.get_lob_inrow_threshold() != inc_table_schema.get_lob_inrow_threshold())) {
    LOG_WARN("lob inrow threshold of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_lob_inrow_threshold()), K(inc_table_schema.get_lob_inrow_threshold()));
  } else {
    is_equal = true;
  }
  if (OB_SUCC(ret) && !is_equal) {
    if (is_oracle_mode) {
      ret = OB_ERR_COLUMN_TYPE_OR_SIZE_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION;
    } else {
      ret = OB_TABLES_DIFFERENT_DEFINITIONS;
    }
    LOG_WARN("table lob infos of exchanging partition tables are not equal", K(ret), K(base_table_schema), K(inc_table_schema));
  }
  return ret;
}

int ObPartitionExchange::check_table_rowkey_infos_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  bool is_equal = false;
  if (OB_UNLIKELY(base_table_schema.get_rowkey_column_num() != inc_table_schema.get_rowkey_column_num())) {
    LOG_WARN("rowkey column num of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_rowkey_column_num()), K(inc_table_schema.get_rowkey_column_num()));
  } else if (OB_UNLIKELY(base_table_schema.get_shadow_rowkey_column_num() != inc_table_schema.get_shadow_rowkey_column_num())) {
    LOG_WARN("shadow rowkey column num of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_shadow_rowkey_column_num()), K(inc_table_schema.get_shadow_rowkey_column_num()));
  } else if (OB_UNLIKELY(base_table_schema.get_rowkey_split_pos() != inc_table_schema.get_rowkey_split_pos())) {
    LOG_WARN("rowkey split pos of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_rowkey_split_pos()), K(inc_table_schema.get_rowkey_split_pos()));
  } else if (OB_FAIL(compare_two_rowkey_info_(base_table_schema.get_rowkey_info(), inc_table_schema.get_rowkey_info(), is_equal))) {
    LOG_WARN("fail to compare two rowkey info", K(ret), K(base_table_schema.get_rowkey_info()), K(inc_table_schema.get_rowkey_info()));
  } else if (!is_equal) {
    LOG_WARN("rowkey info of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_rowkey_info()), K(inc_table_schema.get_rowkey_info()));
  } else if (OB_FAIL(compare_two_rowkey_info_(base_table_schema.get_shadow_rowkey_info(), inc_table_schema.get_shadow_rowkey_info(), is_equal))) {
    LOG_WARN("fail to compare two rowkey info", K(ret), K(base_table_schema.get_shadow_rowkey_info()), K(inc_table_schema.get_shadow_rowkey_info()));
  } else if (!is_equal) {
    LOG_WARN("shadow rowkey info of exchanging partition tables are not equal", K(ret), K(base_table_schema.get_shadow_rowkey_info()), K(inc_table_schema.get_shadow_rowkey_info()));
  }
  if (OB_SUCC(ret) && !is_equal) {
    if (is_oracle_mode) {
      ret = OB_ERR_INDEX_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION;
    } else {
      ret = OB_TABLES_DIFFERENT_DEFINITIONS;
    }
    LOG_WARN("table rowkey infos of exchanging partition tables are not equal", K(ret), K(base_table_schema), K(inc_table_schema));
  }
  return ret;
}

int ObPartitionExchange::compare_two_rowkey_info_(const common::ObRowkeyInfo &l_rowkey_info, const common::ObRowkeyInfo &r_rowkey_info, bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  if (OB_UNLIKELY(l_rowkey_info.get_size() != r_rowkey_info.get_size())) {
    is_equal = false;
    LOG_WARN("rowkey info size are not equal", K(ret), K(l_rowkey_info.get_size()), K(r_rowkey_info.get_size()));
  } else if (0 == l_rowkey_info.get_size()) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < l_rowkey_info.get_size(); i++) {
      const ObRowkeyColumn *l_rowkey_column = l_rowkey_info.get_column(i);
      const ObRowkeyColumn *r_rowkey_column = r_rowkey_info.get_column(i);
      is_equal = false;
      // Since the rowkey info array is already sorted, there is no need to validate the column id again.
      if (OB_ISNULL(l_rowkey_column) || OB_ISNULL(r_rowkey_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowkey column is null", K(ret), KPC(l_rowkey_column), KPC(r_rowkey_column));
      } else if (OB_UNLIKELY(!l_rowkey_column->is_equal_except_column_id(*r_rowkey_column))) {
        LOG_WARN("rowkey column infos are not equal", K(ret), KPC(l_rowkey_column), KPC(r_rowkey_column));
      } else {
        is_equal = true;
      }
    }
  }
  return ret;
}

int ObPartitionExchange::check_table_column_groups_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  bool is_equal = false;
  int64_t base_store_column_group_cnt = 0;
  int64_t inc_store_column_group_cnt = 0;
  ObSEArray<const ObColumnGroupSchema *, 8> base_column_group_metas;
  ObSEArray<const ObColumnGroupSchema *, 8> inc_column_group_metas;
  if (OB_UNLIKELY(base_table_schema.get_column_group_count() != inc_table_schema.get_column_group_count())) {
    LOG_WARN("column group count of exchanging tables are not equal", K(ret), K(base_table_schema.get_column_group_count()), K(inc_table_schema.get_column_group_count()));
  } else if (OB_FAIL(base_table_schema.get_store_column_group_count(base_store_column_group_cnt))) {
    LOG_WARN("Failed to get column group count", K(ret), K(base_table_schema));
  } else if (OB_FAIL(inc_table_schema.get_store_column_group_count(inc_store_column_group_cnt))) {
    LOG_WARN("Failed to get column group count", K(ret), K(inc_table_schema));
  } else if (OB_UNLIKELY(base_store_column_group_cnt != inc_store_column_group_cnt)) {
    LOG_WARN("store column group count of exchanging tables are not equal", K(ret), K(base_store_column_group_cnt), K(inc_store_column_group_cnt));
  } else if (base_store_column_group_cnt < 1) {
    is_equal = true;
  } else if (OB_FAIL(base_table_schema.get_store_column_groups(base_column_group_metas))) {
    LOG_WARN("Failed to get column group metas", K(ret), K(base_table_schema));
  } else if (OB_FAIL(inc_table_schema.get_store_column_groups(inc_column_group_metas))) {
    LOG_WARN("Failed to get column group metas", K(ret), K(inc_table_schema));
  } else if (OB_UNLIKELY(base_column_group_metas.count() != inc_column_group_metas.count())) {
    LOG_WARN("column group metas count of exchanging tables are not equal", K(ret), K(base_column_group_metas.count()), K(inc_column_group_metas.count()));
  } else {
    is_equal = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < base_column_group_metas.count(); i++) {
      const ObColumnGroupSchema *base_cg_schema = nullptr;
      const ObColumnGroupSchema *inc_cg_schema = nullptr;
      if (OB_ISNULL(base_cg_schema = base_column_group_metas.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null base_cg_schema", K(ret));
      } else if (OB_ISNULL(inc_cg_schema = inc_column_group_metas.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null inc_cg_schema", K(ret));
      } else if (OB_FAIL(compare_two_column_group_schema_(base_table_schema, inc_table_schema, *base_cg_schema, *inc_cg_schema, is_oracle_mode, is_equal))) {
        LOG_WARN("fail to compare two column group schema", K(ret), K(base_table_schema), K(inc_table_schema), KPC(base_cg_schema), KPC(inc_cg_schema), K(is_oracle_mode));
      }
    }
  }
  if (OB_SUCC(ret) && !is_equal) {
    ret = OB_ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION;
    LOG_WARN("column groups of exchanging partition tables are not equal", K(ret));
    LOG_USER_ERROR(OB_ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION, "COLUMN_STORAGE_FORMAT");
  }
  return ret;
}

int ObPartitionExchange::compare_two_column_group_schema_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const ObColumnGroupSchema &base_cg_schema, const ObColumnGroupSchema &inc_cg_schema, const bool is_oracle_mode, bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (OB_UNLIKELY(base_table_schema.is_aux_table() != inc_table_schema.is_aux_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is aux table attribute of exchange partition tables are different", K(ret), K(base_table_schema.is_aux_table()), K(inc_table_schema.is_aux_table()));
  } else if (OB_UNLIKELY(!base_cg_schema.has_same_column_group_attributes_for_part_exchange(inc_cg_schema))) {
    LOG_WARN("column group attributes are different", K(ret), K(base_cg_schema), K(inc_cg_schema));
  } else {
    is_equal = true;
    uint64_t *base_column_id_arr = base_cg_schema.get_column_ids();
    uint64_t *inc_column_id_arr = inc_cg_schema.get_column_ids();
    int64_t column_id_count = base_cg_schema.get_column_id_count();
    for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < column_id_count; i++) {
      const ObColumnSchemaV2 *base_cg_col_schema = NULL;
      const ObColumnSchemaV2 *inc_cg_col_schema = NULL;
      if (OB_ISNULL(base_cg_col_schema = base_table_schema.get_column_schema(base_column_id_arr[i]))) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("fail to get column schema", K(ret), K(base_table_schema), K(base_column_id_arr[i]));
      } else if (OB_ISNULL(inc_cg_col_schema = inc_table_schema.get_column_schema(inc_column_id_arr[i]))) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("fail to get column schema", K(ret), K(inc_table_schema), K(inc_column_id_arr[i]));
      } else if (OB_FAIL(check_column_level_conditions_(base_cg_col_schema, inc_cg_col_schema, base_table_schema.is_aux_table(), is_oracle_mode))) {
        is_equal = false;
        if (in_find_same_aux_table_retry_white_list_(ret)) {
          ret = OB_ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION;
          LOG_USER_ERROR(OB_ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION, "COLUMN_STORAGE_FORMAT");
          LOG_WARN("column conditions in column groups are not equal", K(ret), KPC(base_cg_col_schema), KPC(inc_cg_col_schema), K(base_table_schema.is_aux_table()), K(is_oracle_mode));
        } else {
          LOG_WARN("column conditions in column groups are not equal, and ret_code not in in_find_same_aux_table_retry_white_list", K(ret), KPC(base_cg_col_schema), KPC(inc_cg_col_schema), K(base_table_schema.is_aux_table()), K(is_oracle_mode));
        }
      }
    }
  }
  return ret;
}

int ObPartitionExchange::get_next_pair_column_schema_(ObTableSchema::const_column_iterator &base_iter_begin,
                                                      ObTableSchema::const_column_iterator &base_iter_end,
                                                      ObTableSchema::const_column_iterator &inc_iter_begin,
                                                      ObTableSchema::const_column_iterator &inc_iter_end,
                                                      const bool is_oracle_mode,
                                                      ObColumnSchemaV2 *&base_table_col_schema,
                                                      ObColumnSchemaV2 *&inc_table_col_schema)
{
  int ret = OB_SUCCESS;
  base_table_col_schema = NULL;
  inc_table_col_schema = NULL;
  if (OB_FAIL(get_next_need_check_column_(base_iter_begin, base_iter_end, is_oracle_mode, base_table_col_schema))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(get_next_need_check_column_(inc_iter_begin, inc_iter_end, is_oracle_mode, inc_table_col_schema))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("error unexpected happened when iter next column schema", K(ret));
        }
      } else {
        if (is_oracle_mode) {
          ret = OB_ERR_COLUMNS_NUMBER_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION;
        } else {
          ret = OB_TABLES_DIFFERENT_DEFINITIONS;
        }
        LOG_WARN("base table schema's columns format don't match the inc_table_schema", K(ret));
      }
    } else {
      LOG_WARN("error unexpected happened when iter next column schema", K(ret));
    }
  } else if (OB_FAIL(get_next_need_check_column_(inc_iter_begin, inc_iter_end, is_oracle_mode, inc_table_col_schema))) {
    if (OB_ITER_END == ret) {
      if (is_oracle_mode) {
        ret = OB_ERR_COLUMNS_NUMBER_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION;
      } else {
        ret = OB_TABLES_DIFFERENT_DEFINITIONS;
      }
      LOG_WARN("base table schema's columns format don't match the inc_table_schema", K(ret));
    } else {
      LOG_WARN("error unexpected happened when iter next column schema", K(ret));
    }
  } else if (OB_UNLIKELY(base_table_col_schema->is_hidden() != inc_table_col_schema->is_hidden())) {
    if (is_oracle_mode) {
      ret = OB_ERR_COLUMNS_NUMBER_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION;
    } else {
      ret = OB_TABLES_DIFFERENT_DEFINITIONS;
    }
    LOG_WARN("base table schema's columns format don't match the inc_table_schema", K(ret));
  }
  return ret;
}

int ObPartitionExchange::get_next_need_check_column_(ObTableSchema::const_column_iterator &iter_begin, ObTableSchema::const_column_iterator &iter_end, const bool is_oracle_mode, ObColumnSchemaV2 *&table_col_schema)
{
  int ret = OB_SUCCESS;
  bool found_col_schema = false;
  for (; OB_SUCC(ret) && !found_col_schema && iter_begin != iter_end; iter_begin++) {
    table_col_schema = *iter_begin;
    if (OB_ISNULL(table_col_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("fail to get column schema", K(ret));
    } else if (table_col_schema->is_prefix_column() || table_col_schema->is_func_idx_column()) {
    } else if (!is_oracle_mode) {
      found_col_schema = true;
    } else if (!table_col_schema->is_virtual_generated_column()) {
      found_col_schema = true;
    }
  }
  if (OB_SUCC(ret) && !found_col_schema) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObPartitionExchange::set_global_storage_index_unusable_(const uint64_t tenant_id,
                                                            const ObTableSchema &partitioned_data_table_schema,
                                                            const ObTableSchema &non_partitioned_data_table_schema,
                                                            ObDDLOperator &ddl_operator,
                                                            ObDDLSQLTransaction &trans,
                                                            ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < unused_pt_index_id_.count(); i++) {
      uint64_t pt_index_id = unused_pt_index_id_.at(i);
      if (OB_FAIL(update_index_status_(tenant_id,
                                       pt_index_id,
                                       INDEX_STATUS_UNUSABLE,
                                       partitioned_data_table_schema.get_in_offline_ddl_white_list(),
                                       ddl_operator,
                                       trans,
                                       schema_guard))) {
        LOG_WARN("fail to update partitioned data table index status", K(ret), K(tenant_id), K(pt_index_id), K(partitioned_data_table_schema.get_in_offline_ddl_white_list()));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < unused_nt_index_id_.count(); i++) {
      uint64_t nt_index_id = unused_nt_index_id_.at(i);
      if (OB_FAIL(update_index_status_(tenant_id,
                                       nt_index_id,
                                       INDEX_STATUS_UNUSABLE,
                                       non_partitioned_data_table_schema.get_in_offline_ddl_white_list(),
                                       ddl_operator,
                                       trans,
                                       schema_guard))) {
        LOG_WARN("fail to update non partitioned data table index status", K(ret), K(tenant_id), K(nt_index_id), K(non_partitioned_data_table_schema.get_in_offline_ddl_white_list()));
      }
    }
  }
  return ret;
}

int ObPartitionExchange::get_data_partition_and_index_(const ObTableSchema &partitioned_data_table_schema, const ObString &data_part_name, const ObPartition *&data_part, int64_t &data_partition_index)
{
  int ret = OB_SUCCESS;
  int64_t part_id = 0;
  data_partition_index = OB_INVALID_INDEX;
  ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
  if (OB_UNLIKELY(!partitioned_data_table_schema.is_valid() || data_part_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partitioned_data_table_schema.is_valid()), K(data_part_name));
  } else if (OB_FAIL(partitioned_data_table_schema.get_partition_by_name(data_part_name, data_part))) {
    LOG_WARN("get part by name failed", K(ret), K(data_part_name));
  } else if (OB_ISNULL(data_part)) {
    ret = OB_PARTITION_NOT_EXIST;
    LOG_WARN("partition not found", K(ret), K(data_part_name), K(partitioned_data_table_schema));
  } else if (FALSE_IT(part_id = data_part->get_part_id())) {
  } else if (OB_FAIL(partitioned_data_table_schema.get_partition_index_by_id(part_id, check_partition_mode, data_partition_index))) {
    LOG_WARN("fail to get partition index by id", K(ret), K(partitioned_data_table_schema), K(part_id));
  }
  return ret;
}

int ObPartitionExchange::get_data_subpartition_and_index_(const ObTableSchema &partitioned_data_table_schema,
                                                          const ObString &data_subpart_name,
                                                          const ObPartition *&data_part,
                                                          const ObSubPartition *&data_subpart,
                                                          int64_t &data_partition_index,
                                                          int64_t &data_subpartition_index)
{
  int ret = OB_SUCCESS;
  int64_t part_id = 0;
  int64_t subpart_id = 0;
  data_partition_index = OB_INVALID_INDEX;
  data_subpartition_index = OB_INVALID_INDEX;
  ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
  if (OB_UNLIKELY(!partitioned_data_table_schema.is_valid() || data_subpart_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partitioned_data_table_schema.is_valid()), K(data_subpart_name));
  } else if (OB_FAIL(partitioned_data_table_schema.get_subpartition_by_name(data_subpart_name, data_part, data_subpart))) {
    LOG_WARN("get sub part by name failed", K(ret), K(partitioned_data_table_schema), K(data_subpart_name));
  } else if (OB_ISNULL(data_part) || OB_ISNULL(data_subpart)) {
    ret = OB_PARTITION_NOT_EXIST;
    LOG_WARN("partition not found", K(ret), K(partitioned_data_table_schema), K(data_subpart_name));
  } else if (FALSE_IT(part_id = data_part->get_part_id())) {
  } else if (OB_FAIL(partitioned_data_table_schema.get_partition_index_by_id(part_id, check_partition_mode, data_partition_index))) {
    LOG_WARN("fail to get partition index", K(ret), K(partitioned_data_table_schema), K(part_id));
  } else if (FALSE_IT(subpart_id = data_subpart->get_sub_part_id())) {
  } else if (OB_FAIL(data_part->get_normal_subpartition_index_by_id(subpart_id, data_subpartition_index))) {
    LOG_WARN("fail to get normal subpartition index by id", K(ret), KPC(data_part), K(subpart_id));
  }
  return ret;
}

int ObPartitionExchange::exchange_data_table_partition_(const uint64_t tenant_id,
                                                        const ObTableSchema &partitioned_table_schema,
                                                        const ObTableSchema &non_partitioned_table_schema,
                                                        const ObPartition &part,
                                                        const bool is_oracle_mode,
                                                        ObDDLOperator &ddl_operator,
                                                        ObDDLSQLTransaction &trans,
                                                        ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !partitioned_table_schema.is_valid() || !non_partitioned_table_schema.is_valid() || !part.is_valid() || !used_table_to_tablet_id_map_.created())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(partitioned_table_schema.is_valid()), K(non_partitioned_table_schema.is_valid()), K(part.is_valid()), K(used_table_to_tablet_id_map_.created()));
  } else if (OB_FAIL(used_table_to_tablet_id_map_.set_refactored(partitioned_table_schema.get_table_id(), part.get_tablet_id()))) {
    LOG_WARN("fail to set refactored used table to tablet id map", K(ret), K(partitioned_table_schema.get_table_id()), K(part.get_tablet_id()));
  } else if (OB_FAIL(used_table_to_tablet_id_map_.set_refactored(non_partitioned_table_schema.get_table_id(), non_partitioned_table_schema.get_tablet_id()))) {
    LOG_WARN("fail to set refactored used table to tablet id map", K(ret), K(non_partitioned_table_schema.get_table_id()), K(non_partitioned_table_schema.get_tablet_id()));
  } else if (OB_FAIL(exchange_partition_map_relationship_(tenant_id,
                                                          part,
                                                          partitioned_table_schema,
                                                          non_partitioned_table_schema,
                                                          is_oracle_mode,
                                                          ddl_operator,
                                                          trans,
                                                          schema_guard))) {
    LOG_WARN("fail to exchange partition map relationship", K(ret), K(tenant_id), K(part), K(partitioned_table_schema), K(non_partitioned_table_schema), K(is_oracle_mode));
  }
  return ret;
}

int ObPartitionExchange::exchange_data_table_subpartition_(const uint64_t tenant_id,
                                                           const ObTableSchema &partitioned_table_schema,
                                                           const ObTableSchema &non_partitioned_table_schema,
                                                           const ObPartition &part,
                                                           const ObSubPartition &subpart,
                                                           const bool is_oracle_mode,
                                                           ObDDLOperator &ddl_operator,
                                                           ObDDLSQLTransaction &trans,
                                                           ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !partitioned_table_schema.is_valid() || !non_partitioned_table_schema.is_valid() || !part.is_valid() || !subpart.is_valid() || !used_table_to_tablet_id_map_.created())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(partitioned_table_schema.is_valid()), K(non_partitioned_table_schema.is_valid()), K(part.is_valid()), K(subpart.is_valid()), K(used_table_to_tablet_id_map_.created()));
  } else if (OB_FAIL(used_table_to_tablet_id_map_.set_refactored(partitioned_table_schema.get_table_id(), subpart.get_tablet_id()))) {
    LOG_WARN("fail to set refactored used table to tablet id map", K(ret), K(partitioned_table_schema.get_table_id()), K(subpart.get_tablet_id()));
  } else if (OB_FAIL(used_table_to_tablet_id_map_.set_refactored(non_partitioned_table_schema.get_table_id(), non_partitioned_table_schema.get_tablet_id()))) {
    LOG_WARN("fail to set refactored used table to tablet id map", K(ret), K(non_partitioned_table_schema.get_table_id()), K(non_partitioned_table_schema.get_tablet_id()));
  } else if (OB_FAIL(exchange_subpartition_map_relationship_(tenant_id,
                                                             part,
                                                             subpart,
                                                             partitioned_table_schema,
                                                             non_partitioned_table_schema,
                                                             is_oracle_mode,
                                                             ddl_operator,
                                                             trans,
                                                             schema_guard))) {
    LOG_WARN("fail to exchange subpartition map relationship", K(ret), K(tenant_id), K(part), K(subpart), K(partitioned_table_schema), K(non_partitioned_table_schema), K(is_oracle_mode));
  }
  return ret;
}

int ObPartitionExchange::exchange_auxiliary_table_partition_(const uint64_t tenant_id,
                                                             const int64_t  ori_data_partition_index,
                                                             const ObPartition &ori_data_part,
                                                             const bool is_oracle_mode,
                                                             ObDDLOperator &ddl_operator,
                                                             ObDDLSQLTransaction &trans,
                                                             ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const ObPartition *part = nullptr;
  const ObTableSchema *partitioned_table_schema = NULL;
  const ObTableSchema *non_partitioned_table_schema = NULL;
  bool is_matched = false;
  ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_INDEX == ori_data_partition_index || !ori_data_part.is_valid() || !used_pt_nt_id_map_.created() || !used_table_to_tablet_id_map_.created())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ori_data_partition_index), K(ori_data_part.is_valid()), K(is_oracle_mode), K(used_pt_nt_id_map_.created()), K(used_table_to_tablet_id_map_.created()));
  } else {
    common::hash::ObHashMap<uint64_t, uint64_t>::iterator iter;
    for (iter = used_pt_nt_id_map_.begin(); OB_SUCC(ret) && iter != used_pt_nt_id_map_.end(); ++iter) {
      uint64_t partitioned_table_id = iter->first;
      uint64_t non_partitioned_table_id = iter->second;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, partitioned_table_id, partitioned_table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(partitioned_table_id));
      } else if (OB_ISNULL(partitioned_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema should not be null", K(ret), K(tenant_id), K(partitioned_table_id));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, non_partitioned_table_id, non_partitioned_table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(non_partitioned_table_id));
      } else if (OB_ISNULL(non_partitioned_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema should not be null", K(ret), K(tenant_id), K(non_partitioned_table_id));
      } else {
        const schema::ObPartitionOption &pt_part_option = partitioned_table_schema->get_part_option();
        schema::ObPartitionFuncType pt_part_func_type = pt_part_option.get_part_func_type();
        if (OB_FAIL(partitioned_table_schema->get_partition_by_partition_index(ori_data_partition_index, check_partition_mode, part))) {
          LOG_WARN("get_partition_by_partition_index fail", K(ret), K(ori_data_partition_index), KPC(partitioned_table_schema));
        } else if (OB_ISNULL(part)) {
          ret = OB_PARTITION_NOT_EXIST;
          LOG_WARN("partition not found", K(ret), K(ori_data_partition_index), KPC(partitioned_table_schema));
        } else if (OB_FAIL(ddl_service_.check_same_partition(is_oracle_mode, ori_data_part, *part, pt_part_func_type, is_matched))) {
          LOG_WARN("fail to check ori_table_part and ori_aux_part is the same", K(ret), K(is_oracle_mode), K(ori_data_part), KPC(part), K(pt_part_func_type));
        } else if (OB_UNLIKELY(!is_matched)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part with the same offset not equal, maybe not the right index", K(ret), K(ori_data_part), KPC(part));
        } else if (OB_FAIL(used_table_to_tablet_id_map_.set_refactored(partitioned_table_id, part->get_tablet_id()))) {
          LOG_WARN("fail to set refactored used table to tablet id map", K(ret), K(partitioned_table_id), K(part->get_tablet_id()));
        } else if (OB_FAIL(used_table_to_tablet_id_map_.set_refactored(non_partitioned_table_id, non_partitioned_table_schema->get_tablet_id()))) {
          LOG_WARN("fail to set refactored used table to tablet id map", K(ret), K(non_partitioned_table_id), K(non_partitioned_table_schema->get_tablet_id()));
        } else if (OB_FAIL(exchange_partition_map_relationship_(tenant_id,
                                                                *part,
                                                                *partitioned_table_schema,
                                                                *non_partitioned_table_schema,
                                                                is_oracle_mode,
                                                                ddl_operator,
                                                                trans,
                                                                schema_guard))) {
          LOG_WARN("fail to exchange partition map relationship", K(ret), K(tenant_id), KPC(part), KPC(partitioned_table_schema), KPC(non_partitioned_table_schema), K(is_oracle_mode));
        }
      }
    }
  }
  return ret;
}

int ObPartitionExchange::exchange_auxiliary_table_subpartition_(const uint64_t tenant_id,
                                                                const int64_t  ori_data_partition_index,
                                                                const int64_t  ori_data_subpartition_index,
                                                                const ObPartition &ori_data_part,
                                                                const ObSubPartition &ori_data_subpart,
                                                                const bool is_oracle_mode,
                                                                ObDDLOperator &ddl_operator,
                                                                ObDDLSQLTransaction &trans,
                                                                ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const ObPartition *part = nullptr;
  const ObSubPartition *subpart = nullptr;
  const ObTableSchema *partitioned_table_schema = NULL;
  const ObTableSchema *non_partitioned_table_schema = NULL;
  bool is_matched = false;
  ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_INDEX == ori_data_partition_index ||
      OB_INVALID_INDEX == ori_data_subpartition_index || !ori_data_part.is_valid() || !ori_data_subpart.is_valid() || !used_table_to_tablet_id_map_.created())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ori_data_partition_index), K(ori_data_subpartition_index), K(ori_data_part.is_valid()), K(ori_data_subpart.is_valid()), K(used_table_to_tablet_id_map_.created()));
  } else {
    common::hash::ObHashMap<uint64_t, uint64_t>::iterator iter;
    for (iter = used_pt_nt_id_map_.begin(); OB_SUCC(ret) && iter != used_pt_nt_id_map_.end(); ++iter) {
      uint64_t partitioned_table_id = iter->first;
      uint64_t non_partitioned_table_id = iter->second;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, partitioned_table_id, partitioned_table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(partitioned_table_id));
      } else if (OB_ISNULL(partitioned_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema should not be null", K(ret), K(tenant_id), K(partitioned_table_id));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, non_partitioned_table_id, non_partitioned_table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(non_partitioned_table_id));
      } else if (OB_ISNULL(non_partitioned_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema should not be null", K(ret), K(tenant_id), K(non_partitioned_table_id));
      } else {
        const schema::ObPartitionOption &pt_part_option = partitioned_table_schema->get_part_option();
        schema::ObPartitionFuncType pt_part_func_type = pt_part_option.get_part_func_type();
        const schema::ObPartitionOption &pt_subpart_option = partitioned_table_schema->get_sub_part_option();
        schema::ObPartitionFuncType pt_subpart_func_type = pt_subpart_option.get_sub_part_func_type();
        if (OB_FAIL(partitioned_table_schema->get_partition_by_partition_index(ori_data_partition_index, check_partition_mode, part))) {
          LOG_WARN("get_partition_by_partition_index fail", K(ret), K(ori_data_partition_index), KPC(partitioned_table_schema));
        } else if (OB_ISNULL(part)) {
          ret = OB_PARTITION_NOT_EXIST;
          LOG_WARN("partition not found", K(ret), K(ori_data_partition_index), KPC(partitioned_table_schema));
        } else if (OB_FAIL(ddl_service_.check_same_partition(is_oracle_mode, ori_data_part, *part, pt_part_func_type, is_matched))) {
          LOG_WARN("fail to check ori_table_part and ori_aux_part is the same", K(ret), K(is_oracle_mode), K(ori_data_part), KPC(part), K(pt_part_func_type));
        } else if (!is_matched) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part with the same offset not equal, maybe not the right index", K(ret), K(ori_data_part), KPC(part));
        } else if (OB_FAIL(part->get_normal_subpartition_by_subpartition_index(ori_data_subpartition_index, subpart))) {
          LOG_WARN("fail to get src subpart by subpart index", K(ret), K(ori_data_subpartition_index));
        } else if (OB_ISNULL(subpart)) {
          ret = OB_PARTITION_NOT_EXIST;
          LOG_WARN("partition not found", K(ret), K(part), K(ori_data_subpartition_index), KPC(partitioned_table_schema));
        } else if (OB_FAIL(ddl_service_.check_same_subpartition(is_oracle_mode, ori_data_subpart, *subpart, pt_subpart_func_type, is_matched))) {
          LOG_WARN("fail to check ori_table_subpart and ori_aux_subpart is the same", K(ret), K(is_oracle_mode), K(ori_data_subpart), KPC(subpart), K(pt_subpart_func_type));
        } else if (!is_matched) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part with the same offset not equal, maybe not the right index", K(ret), K(ori_data_subpart), KPC(subpart));
        } else if (OB_FAIL(used_table_to_tablet_id_map_.set_refactored(partitioned_table_id, subpart->get_tablet_id()))) {
          LOG_WARN("fail to set refactored used table to tablet id map", K(ret), K(partitioned_table_id), K(subpart->get_tablet_id()));
        } else if (OB_FAIL(used_table_to_tablet_id_map_.set_refactored(non_partitioned_table_id, non_partitioned_table_schema->get_tablet_id()))) {
          LOG_WARN("fail to set refactored used table to tablet id map", K(ret), K(non_partitioned_table_id), K(non_partitioned_table_schema->get_tablet_id()));
        } else if (OB_FAIL(exchange_subpartition_map_relationship_(tenant_id,
                                                                  *part,
                                                                  *subpart,
                                                                  *partitioned_table_schema,
                                                                  *non_partitioned_table_schema,
                                                                  is_oracle_mode,
                                                                  ddl_operator,
                                                                  trans,
                                                                  schema_guard))) {
          LOG_WARN("fail to exchange subpartition map relationship", K(ret), K(tenant_id), KPC(part), KPC(subpart), KPC(partitioned_table_schema), KPC(non_partitioned_table_schema), K(is_oracle_mode));
        }
      }
    }
  }
  return ret;
}

int ObPartitionExchange::exchange_partition_map_relationship_(const uint64_t tenant_id,
                                                              const ObPartition &part,
                                                              const ObTableSchema &partitioned_table_schema,
                                                              const ObTableSchema &non_partitioned_table_schema,
                                                              const bool is_oracle_mode,
                                                              ObDDLOperator &ddl_operator,
                                                              ObDDLSQLTransaction &trans,
                                                              ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> exchange_table_ids;
  ObArray<ObTabletID> exchange_tablet_ids;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !part.is_valid() || !partitioned_table_schema.is_valid() || !non_partitioned_table_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(part), K(partitioned_table_schema.is_valid()), K(non_partitioned_table_schema.is_valid()));
  } else if (OB_FAIL(exchange_table_ids.push_back(non_partitioned_table_schema.get_table_id()))) {
    LOG_WARN("fail to push back table id", K(ret), K(non_partitioned_table_schema.get_table_id()), K(exchange_table_ids), K(non_partitioned_table_schema));
  } else if (OB_FAIL(exchange_table_ids.push_back(partitioned_table_schema.get_table_id()))) {
    LOG_WARN("fail to push back table id", K(ret), K(partitioned_table_schema.get_table_id()), K(exchange_table_ids), K(partitioned_table_schema));
  } else if (OB_FAIL(exchange_tablet_ids.push_back(part.get_tablet_id()))) {
    LOG_WARN("fail to push back partitioned table tablet id", K(ret), K(part.get_tablet_id()));
  } else if (OB_FAIL(exchange_tablet_ids.push_back(non_partitioned_table_schema.get_tablet_id()))) {
    LOG_WARN("fail to push back non_partitioned table tablet id", K(ret), K(non_partitioned_table_schema.get_tablet_id()));
  } else if (OB_UNLIKELY(2 != exchange_table_ids.count() || 2 != exchange_tablet_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the array length is incorrect", K(ret), K(exchange_table_ids.count()), K(exchange_tablet_ids.count()));
  } else {
    // drop exchange partition in partitioned table, and add new exchange partition in partitioned table
    HEAP_VARS_3((ObPartition, new_part),
                (ObTableSchema, new_nt_schema),
                (AlterTableSchema, alter_pt_drop_part_schema)) {
      if (OB_FAIL(new_part.assign(part))) {
        LOG_WARN("fail to assign part", K(ret), K(part));
      } else if (OB_FAIL(alter_pt_drop_part_schema.assign(partitioned_table_schema))) {
        LOG_WARN("fail to assign partitioned table schema", K(ret), K(partitioned_table_schema));
      } else if (FALSE_IT(alter_pt_drop_part_schema.reset_partition_schema())) {
      } else if (OB_FAIL(alter_pt_drop_part_schema.add_partition(part))) {
        LOG_WARN("fail to add partition", K(ret), K(part));
      } else if (FALSE_IT(alter_pt_drop_part_schema.set_part_level(partitioned_table_schema.get_part_level()))) {
      } else if (OB_FAIL(alter_pt_drop_part_schema.get_part_option().assign(partitioned_table_schema.get_part_option()))) {
        LOG_WARN("fail to assign part option", K(ret), K(partitioned_table_schema.get_part_option()));
      } else if (FALSE_IT(alter_pt_drop_part_schema.get_part_option().set_part_num(alter_pt_drop_part_schema.get_partition_num()))) {
      } else if (OB_FAIL(new_nt_schema.assign(non_partitioned_table_schema))) {
        LOG_WARN("fail to assign non_partitioned table schema", K(ret), K(non_partitioned_table_schema));
      } else {
        new_part.set_tablet_id(exchange_tablet_ids.at(1));
        new_nt_schema.set_tablet_id(exchange_tablet_ids.at(0));
        HEAP_VARS_2((ObTableSchema, new_pt_schema),
                    (AlterTableSchema, alter_pt_add_new_part_schema)) {
          int64_t new_partition_id = OB_INVALID_PARTITION_ID;
          if (OB_FAIL(new_pt_schema.assign(partitioned_table_schema))) {
            LOG_WARN("fail to assign partitioned table schema", K(ret), K(partitioned_table_schema));
          } else if (OB_FAIL(alter_pt_add_new_part_schema.assign(partitioned_table_schema))) {
            LOG_WARN("fail to assign partitioned table schema", K(ret), K(partitioned_table_schema));
          } else if (FALSE_IT(alter_pt_add_new_part_schema.reset_partition_schema())) {
          } else if (OB_FAIL(alter_pt_add_new_part_schema.add_partition(new_part))) {
            LOG_WARN("fail to add partition", K(ret), K(new_part));
          } else if (FALSE_IT(alter_pt_add_new_part_schema.set_part_level(partitioned_table_schema.get_part_level()))) {
          } else if (OB_FAIL(alter_pt_add_new_part_schema.get_part_option().assign(partitioned_table_schema.get_part_option()))) {
            LOG_WARN("fail to assign part option", K(ret), K(partitioned_table_schema.get_part_option()));
          } else if (FALSE_IT(alter_pt_add_new_part_schema.get_part_option().set_part_num(alter_pt_add_new_part_schema.get_partition_num()))) {
          } else if (OB_FAIL(ddl_service_.generate_object_id_for_partition_schema(alter_pt_add_new_part_schema))) {
            LOG_WARN("fail to generate object_id for partition schema", K(ret), K(alter_pt_add_new_part_schema));
          } else if (OB_FAIL(get_object_id_from_partition_schema_(alter_pt_add_new_part_schema, false/*get_subpart_only*/, new_partition_id))) {
            LOG_WARN("fail get object id from partition schema", K(ret), K(alter_pt_add_new_part_schema), K(new_partition_id));
          } else if (OB_FAIL(update_exchange_table_non_schema_attributes_(tenant_id,
                                                                          part.get_part_id(),
                                                                          new_partition_id,
                                                                          false/*is_exchange_subpartition*/,
                                                                          new_pt_schema,
                                                                          new_nt_schema,
                                                                          exchange_table_ids,
                                                                          exchange_tablet_ids,
                                                                          is_oracle_mode,
                                                                          ddl_operator,
                                                                          trans,
                                                                          schema_guard))) {
            LOG_WARN("fail to update exchange table non schema attributes", K(ret), K(part.get_part_id()), K(new_partition_id), K(new_pt_schema), K(new_nt_schema), K(exchange_table_ids), K(exchange_tablet_ids), K(is_oracle_mode));
          } else if (OB_FAIL(ddl_operator.exchange_table_partitions(partitioned_table_schema,
                                                                    alter_pt_add_new_part_schema,
                                                                    alter_pt_drop_part_schema,
                                                                    trans))) {
            LOG_WARN("failed to exchange partitions", K(ret), K(partitioned_table_schema), K(alter_pt_add_new_part_schema), K(alter_pt_drop_part_schema));
          } else if (OB_FAIL(update_exchange_table_level_attributes_(tenant_id,
                                                                     exchange_table_ids,
                                                                     exchange_tablet_ids,
                                                                     new_pt_schema,
                                                                     new_nt_schema,
                                                                     trans))) {
            LOG_WARN("fail to update exchange table level attributes", K(ret), K(tenant_id), K(exchange_table_ids), K(exchange_tablet_ids), K(new_pt_schema), K(new_nt_schema));
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionExchange::exchange_subpartition_map_relationship_(const uint64_t tenant_id,
                                                                 const ObPartition &part,
                                                                 const ObSubPartition &subpart,
                                                                 const ObTableSchema &partitioned_table_schema,
                                                                 const ObTableSchema &non_partitioned_table_schema,
                                                                 const bool is_oracle_mode,
                                                                 ObDDLOperator &ddl_operator,
                                                                 ObDDLSQLTransaction &trans,
                                                                 ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> exchange_table_ids;
  ObArray<ObTabletID> exchange_tablet_ids;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !part.is_valid() || !subpart.is_valid() || !partitioned_table_schema.is_valid() || !non_partitioned_table_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(part), K(subpart), K(partitioned_table_schema.is_valid()), K(non_partitioned_table_schema.is_valid()));
  } else if (OB_FAIL(exchange_table_ids.push_back(non_partitioned_table_schema.get_table_id()))) {
    LOG_WARN("fail to push back table id", K(ret), K(non_partitioned_table_schema.get_table_id()), K(exchange_table_ids), K(non_partitioned_table_schema));
  } else if (OB_FAIL(exchange_table_ids.push_back(partitioned_table_schema.get_table_id()))) {
    LOG_WARN("fail to push back table id", K(ret), K(partitioned_table_schema.get_table_id()), K(exchange_table_ids), K(partitioned_table_schema));
  } else if (OB_FAIL(exchange_tablet_ids.push_back(subpart.get_tablet_id()))) {
    LOG_WARN("fail to push back partitioned table tablet id", K(ret), K(subpart.get_tablet_id()));
  } else if (OB_FAIL(exchange_tablet_ids.push_back(non_partitioned_table_schema.get_tablet_id()))) {
    LOG_WARN("fail to push back non_partitioned table tablet id", K(ret), K(non_partitioned_table_schema.get_table_id()));
  } else if (OB_UNLIKELY(2 != exchange_table_ids.count() || 2 != exchange_tablet_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the array length is incorrect", K(ret), K(exchange_table_ids.count()), K(exchange_tablet_ids.count()));
  } else {
    // drop exchange subpartition in partitioned table, and add new exchange subpartition in partitioned table
    HEAP_VARS_4((ObSubPartition, new_subpart),
                (ObPartition, dummy_part),
                (ObTableSchema, new_nt_schema),
                (AlterTableSchema, alter_pt_drop_subpart_schema)) {
      dummy_part.set_sub_part_num(1);
      dummy_part.set_part_id(part.get_part_id());
      if (OB_FAIL(new_subpart.assign(subpart))) {
        LOG_WARN("fail to assign subpartition schema", K(ret), K(subpart));
      } else if (OB_FAIL(dummy_part.set_part_name(part.get_part_name()))) {
        LOG_WARN("failed to set part name", K(ret), K(part.get_part_name()));
      } else if (OB_FAIL(dummy_part.add_partition(subpart))){
        LOG_WARN("failed to add subpart", K(ret), K(subpart));
      } else if (OB_FAIL(alter_pt_drop_subpart_schema.assign(partitioned_table_schema))) {
        LOG_WARN("fail to assign partitioned table schema", K(ret), K(partitioned_table_schema));
      } else if (FALSE_IT(alter_pt_drop_subpart_schema.reset_partition_schema())) {
      } else if (OB_FAIL(alter_pt_drop_subpart_schema.add_partition(dummy_part))) {
        LOG_WARN("failed to add partition", K(ret), K(dummy_part));
      } else if (FALSE_IT(alter_pt_drop_subpart_schema.set_part_level(partitioned_table_schema.get_part_level()))) {
      } else if (OB_FAIL(alter_pt_drop_subpart_schema.get_part_option().assign(partitioned_table_schema.get_part_option()))) {
        LOG_WARN("fail to assign part option", K(ret), K(partitioned_table_schema.get_part_option()));
      } else if (OB_FAIL(alter_pt_drop_subpart_schema.get_sub_part_option().assign(partitioned_table_schema.get_sub_part_option()))) {
        LOG_WARN("fail to assign subpart option", K(ret), K(partitioned_table_schema.get_sub_part_option()));
      } else if (FALSE_IT(alter_pt_drop_subpart_schema.get_part_option().set_part_num(alter_pt_drop_subpart_schema.get_partition_num()))) {
      } else if (OB_FAIL(new_nt_schema.assign(non_partitioned_table_schema))) {
        LOG_WARN("fail to assign non_partitioned table schema", K(ret), K(non_partitioned_table_schema));
      } else {
        new_subpart.set_tablet_id(exchange_tablet_ids.at(1));
        new_nt_schema.set_tablet_id(exchange_tablet_ids.at(0));
        HEAP_VARS_2((ObTableSchema, new_pt_schema),
                    (AlterTableSchema, alter_pt_add_new_subpart_schema)) {
          dummy_part.reset();
          dummy_part.set_sub_part_num(1);
          dummy_part.set_part_id(part.get_part_id());
          int64_t new_subpart_id = OB_INVALID_PARTITION_ID;
          if (OB_FAIL(new_pt_schema.assign(partitioned_table_schema))) {
            LOG_WARN("fail to assign partitioned table schema", K(ret), K(partitioned_table_schema));
          } else if (OB_FAIL(dummy_part.set_part_name(part.get_part_name()))) {
            LOG_WARN("failed to set part name", K(ret), K(part.get_part_name()));
          } else if (OB_FAIL(dummy_part.add_partition(new_subpart))){
            LOG_WARN("failed to add subpart", K(ret), K(new_subpart));
          } else if (OB_FAIL(alter_pt_add_new_subpart_schema.assign(partitioned_table_schema))) {
            LOG_WARN("fail to assign partitioned table schema", K(ret), K(partitioned_table_schema));
          } else if (FALSE_IT(alter_pt_add_new_subpart_schema.reset_partition_schema())) {
          } else if (FALSE_IT(alter_pt_add_new_subpart_schema.set_part_level(partitioned_table_schema.get_part_level()))) {
          } else if (OB_FAIL(alter_pt_add_new_subpart_schema.get_sub_part_option().assign(partitioned_table_schema.get_sub_part_option()))) {
            LOG_WARN("fail to assign subpart option", K(ret), K(partitioned_table_schema.get_sub_part_option()));
          } else if (OB_FAIL(alter_pt_add_new_subpart_schema.add_partition(dummy_part))) {
            LOG_WARN("fail to add subpartition", K(ret), K(dummy_part));
          } else if (OB_FAIL(alter_pt_add_new_subpart_schema.get_part_option().assign(partitioned_table_schema.get_part_option()))) {
            LOG_WARN("fail to assign subpart option", K(ret), K(partitioned_table_schema.get_part_option()));
          } else if (FALSE_IT(alter_pt_add_new_subpart_schema.set_part_num(alter_pt_add_new_subpart_schema.get_partition_num()))) {
          } else if (OB_FAIL(ddl_service_.generate_object_id_for_partition_schema(alter_pt_add_new_subpart_schema, true/*gen_subpart_only*/))) {
            LOG_WARN("fail to generate object_id for partition schema", K(ret), K(alter_pt_add_new_subpart_schema));
          } else if (OB_FAIL(get_object_id_from_partition_schema_(alter_pt_add_new_subpart_schema, true/*gen_subpart_only*/, new_subpart_id))) {
            LOG_WARN("fail get object id from partition schema", K(ret), K(alter_pt_add_new_subpart_schema), K(new_subpart_id));
          } else if (OB_FAIL(update_exchange_table_non_schema_attributes_(tenant_id,
                                                                          subpart.get_sub_part_id(),
                                                                          new_subpart_id,
                                                                          true/*is_exchange_subpartition*/,
                                                                          new_pt_schema,
                                                                          new_nt_schema,
                                                                          exchange_table_ids,
                                                                          exchange_tablet_ids,
                                                                          is_oracle_mode,
                                                                          ddl_operator,
                                                                          trans,
                                                                          schema_guard))) {
            LOG_WARN("fail update exchange table non schema attributes", K(ret), K(subpart.get_sub_part_id()), K(new_subpart_id), K(new_pt_schema), K(new_nt_schema), K(exchange_table_ids), K(exchange_tablet_ids), K(is_oracle_mode));
          } else if (OB_FAIL(ddl_operator.exchange_table_subpartitions(partitioned_table_schema,
                                                                       alter_pt_add_new_subpart_schema,
                                                                       alter_pt_drop_subpart_schema,
                                                                       trans))) {
            LOG_WARN("failed to exchange subpartitions", K(ret), K(partitioned_table_schema), K(alter_pt_add_new_subpart_schema), K(alter_pt_drop_subpart_schema));
          } else if (OB_FAIL(update_exchange_table_level_attributes_(tenant_id,
                                                                     exchange_table_ids,
                                                                     exchange_tablet_ids,
                                                                     new_pt_schema,
                                                                     new_nt_schema,
                                                                     trans))) {
            LOG_WARN("fail to update exchange table level attributes", K(ret), K(tenant_id), K(exchange_table_ids), K(exchange_tablet_ids), K(new_pt_schema), K(new_nt_schema));
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionExchange::update_exchange_table_non_schema_attributes_(const uint64_t tenant_id,
                                                                      const int64_t old_partition_id,
                                                                      const int64_t new_partition_id,
                                                                      const bool is_exchange_subpartition,
                                                                      const ObTableSchema &partitioned_table_schema,
                                                                      const ObTableSchema &non_partitioned_table_schema,
                                                                      const ObIArray<uint64_t> &exchange_table_ids,
                                                                      const ObIArray<ObTabletID> &exchange_tablet_ids,
                                                                      const bool is_oracle_mode,
                                                                      ObDDLOperator &ddl_operator,
                                                                      ObDDLSQLTransaction &trans,
                                                                      ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == old_partition_id || OB_INVALID_ID == new_partition_id || exchange_table_ids.count() != exchange_tablet_ids.count() || 2 != exchange_table_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(old_partition_id), K(new_partition_id), K(partitioned_table_schema), K(non_partitioned_table_schema), K(exchange_table_ids), K(exchange_tablet_ids));
  } else {
    // modify inner table __all_tablet_to_ls, __all_sequence_value or __all_sequence_value, __all_table_stat, __all_column_stat, __all_histogram_stat, __all_monitor_modified
    if (OB_SUCC(ret)) {
      if (OB_FAIL(update_table_to_tablet_id_mapping_(tenant_id, exchange_table_ids, exchange_tablet_ids, trans))) {
        LOG_WARN("fail to update table to tablet id mapping", K(ret), K(tenant_id), K(exchange_table_ids), K(exchange_tablet_ids));
      } else if (!partitioned_table_schema.is_aux_table() && !non_partitioned_table_schema.is_aux_table()) {
        // TODO: After confirming the specific behavior of self increasing columns in MySQL mode and identity in Oracle mode, supplement it.
        // if (is_oracle_mode) {
        //   if (OB_FAIL(update_identity_column_information_(tenant_id, partitioned_table_schema, non_partitioned_table_schema, is_oracle_mode, ddl_operator, trans, schema_guard))) {
        //     LOG_WARN("failed to update identity column information", K(ret), K(tenant_id), K(partitioned_table_schema), K(non_partitioned_table_schema), K(is_oracle_mode));
        //   }
        // } else if (OB_FAIL(update_autoinc_column_information_(tenant_id, partitioned_table_schema, non_partitioned_table_schema, ddl_operator, trans))) {
        //   LOG_WARN("failed to update autoinc column information", K(ret), K(tenant_id), K(partitioned_table_schema), K(non_partitioned_table_schema));
        // }
        if (OB_SUCC(ret)) {
          if (!is_exchange_subpartition) {
            if (OB_FAIL(sync_exchange_partition_stats_info_(tenant_id,
                                                            partitioned_table_schema.get_table_id(),
                                                            StatLevel::PARTITION_LEVEL,
                                                            non_partitioned_table_schema.get_table_id(),
                                                            new_partition_id,
                                                            exchange_tablet_ids.at(1),
                                                            non_partitioned_table_schema,
                                                            trans))) {
              LOG_WARN("fail to sync exchange partition stats info", K(ret), K(partitioned_table_schema), K(non_partitioned_table_schema), K(new_partition_id), K(exchange_tablet_ids));
            }
          } else if (OB_FAIL(sync_exchange_partition_stats_info_(tenant_id,
                                                                partitioned_table_schema.get_table_id(),
                                                                StatLevel::SUBPARTITION_LEVEL,
                                                                non_partitioned_table_schema.get_table_id(),
                                                                new_partition_id,
                                                                exchange_tablet_ids.at(1),
                                                                non_partitioned_table_schema,
                                                                trans))) {
            LOG_WARN("fail to sync exchange subpartition stats info", K(ret), K(partitioned_table_schema), K(non_partitioned_table_schema), K(new_partition_id), K(exchange_tablet_ids));
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(sync_exchange_partition_stats_info_(tenant_id,
                                                            non_partitioned_table_schema.get_table_id(),
                                                            StatLevel::TABLE_LEVEL,
                                                            old_partition_id,
                                                            non_partitioned_table_schema.get_table_id(),
                                                            exchange_tablet_ids.at(0),
                                                            partitioned_table_schema,
                                                            trans))) {
              LOG_WARN("fail to sync exchange partition stats info", K(ret), K(partitioned_table_schema), K(non_partitioned_table_schema), K(old_partition_id), K(exchange_tablet_ids));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionExchange::update_exchange_table_level_attributes_(const uint64_t tenant_id,
                                                                 const ObIArray<uint64_t> &exchange_table_ids,
                                                                 const ObIArray<ObTabletID> &exchange_tablet_ids,
                                                                 ObTableSchema &partitioned_table_schema,
                                                                 ObTableSchema &non_partitioned_table_schema,
                                                                 ObDDLSQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObArray<common::ObTabletID> pt_tablet_ids;
  ObArray<common::ObTabletID> nt_tablet_ids;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || 2 != exchange_table_ids.count() || 2 != exchange_tablet_ids.count() || !partitioned_table_schema.is_valid() || !non_partitioned_table_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(exchange_table_ids.count()), K(exchange_tablet_ids.count()), K(partitioned_table_schema.is_valid()), K(non_partitioned_table_schema.is_valid()));
  } else if (OB_FAIL(nt_tablet_ids.push_back(exchange_tablet_ids.at(0)))) {
    LOG_WARN("failed to push back tablet id", K(ret), K(exchange_tablet_ids.at(0)));
  } else if (OB_FAIL(pt_tablet_ids.push_back(exchange_tablet_ids.at(1)))) {
    LOG_WARN("failed to push back tablet id", K(ret), K(exchange_tablet_ids.at(1)));
  } else {
    // modify inner table __all_tablet_to_table_history and __all_table
    share::ObTabletTablePair pair;
    ObArray<share::ObTabletTablePair> pt_pairs;
    ObArray<share::ObTabletTablePair> nt_pairs;
    for (int64_t i = 0; OB_SUCC(ret) && i < exchange_tablet_ids.count(); i++) {
      if (OB_FAIL(pair.init(exchange_tablet_ids.at(i), exchange_table_ids.at(i)))) {
        LOG_WARN("fail to init tablet to table pair", K(ret), K(exchange_tablet_ids.at(i)), K(exchange_table_ids.at(i)));
      } else if (exchange_table_ids.at(i) == partitioned_table_schema.get_table_id()) {
        if (OB_FAIL(pt_pairs.push_back(pair))) {
          LOG_WARN("fail to push back tablet table pair", K(ret), K(pair), K(pt_pairs));
        }
      } else if (exchange_table_ids.at(i) == non_partitioned_table_schema.get_table_id()) {
        if (OB_FAIL(nt_pairs.push_back(pair))) {
          LOG_WARN("fail to push back tablet table pair", K(ret), K(pair), K(nt_pairs));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exchange table id is error", K(ret), K(exchange_table_ids.at(i)), K(partitioned_table_schema.get_table_id()), K(non_partitioned_table_schema.get_table_id()));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(refresh_table_schema_version_(tenant_id, non_partitioned_table_schema))) {
        LOG_WARN("fail to refresh table schema version", K(ret), K(non_partitioned_table_schema));
      } else if (OB_FAIL(refresh_table_schema_version_(tenant_id, partitioned_table_schema))) {
        LOG_WARN("fail to refresh table schema version", K(ret), K(partitioned_table_schema));
      } else if (OB_FAIL(ObTabletToTableHistoryOperator::create_tablet_to_table_history(trans, tenant_id, partitioned_table_schema.get_schema_version(), pt_pairs))) {
        LOG_WARN("fail to create tablet to table history", K(ret), K(tenant_id), K(partitioned_table_schema));
      } else if (OB_FAIL(ObTabletToTableHistoryOperator::create_tablet_to_table_history(trans, tenant_id, non_partitioned_table_schema.get_schema_version(), nt_pairs))) {
        LOG_WARN("fail to create tablet to table history", K(ret), K(tenant_id), K(non_partitioned_table_schema));
      } else if (OB_FAIL(update_table_attribute_(non_partitioned_table_schema, trans))) {
        LOG_WARN("fail to update table attribute", K(ret), K(non_partitioned_table_schema));
      } else if (OB_FAIL(update_table_attribute_(partitioned_table_schema, trans))) {
        LOG_WARN("fail to update table attribute", K(ret), K(partitioned_table_schema));
      } else if (partitioned_table_schema.is_aux_table() && non_partitioned_table_schema.is_aux_table()) {
        if (OB_FAIL(build_single_table_rw_defensive_(tenant_id, nt_tablet_ids, non_partitioned_table_schema.get_schema_version(), trans))) {
          LOG_WARN("failed to build rw defensive", K(ret), K(tenant_id), K(nt_tablet_ids), K(non_partitioned_table_schema.get_schema_version()));
        } else if (OB_FAIL(build_single_table_rw_defensive_(tenant_id, pt_tablet_ids, partitioned_table_schema.get_schema_version(), trans))) {
          LOG_WARN("failed to build rw defensive", K(ret), K(tenant_id), K(pt_tablet_ids), K(partitioned_table_schema.get_schema_version()));
        }
      }
    }
  }
  return ret;
}

int ObPartitionExchange::update_table_to_tablet_id_mapping_(const uint64_t tenant_id,
                                                            const ObIArray<uint64_t> &table_ids,
                                                            const ObIArray<ObTabletID> &tablet_ids,
                                                            ObDDLSQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id) || table_ids.count() != tablet_ids.count() || 2 != table_ids.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(table_ids.count()), K(tablet_ids.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); i++) {
      if (OB_FAIL(ObTabletToLSTableOperator::update_table_to_tablet_id_mapping(trans, tenant_id, table_ids.at(i), tablet_ids.at(i)))) {
        LOG_WARN("fail to update table to tablet id mapping", K(ret), K(tenant_id), K(table_ids.at(i)), K(tablet_ids.at(i)));
      }
    }
  }
  return ret;
}

int ObPartitionExchange::refresh_table_schema_version_(const uint64_t tenant_id, ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObMultiVersionSchemaService &multi_schema_service = ddl_service_.get_schema_service();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(multi_schema_service.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    table_schema.set_schema_version(new_schema_version);
  }
  return ret;
}

int ObPartitionExchange::update_table_attribute_(const ObTableSchema &table_schema,
                                                 ObDDLSQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = NULL;
  ObSchemaOperationType operation_type = OB_DDL_EXCHANGE_PARTITION;
  ObMultiVersionSchemaService &multi_schema_service = ddl_service_.get_schema_service();
  if (OB_ISNULL(schema_service = multi_schema_service.get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get schema_service is null", K(ret));
  } else if (OB_FAIL(schema_service->get_table_sql_service().update_table_attribute(trans,
                                                                                    table_schema,
                                                                                    operation_type,
                                                                                    false/*update_object_status_ignore_version*/,
                                                                                    nullptr/*ddl_stmt_str*/))) {
    LOG_WARN("failed to update table schema attribute", K(ret), K(table_schema), K(operation_type));
  }
  return ret;
}

int ObPartitionExchange::push_data_table_schema_version_(const uint64_t tenant_id,
                                                         const ObTableSchema &table_schema,
                                                         const common::ObString *ddl_stmt_str,
                                                         const uint64_t exchange_data_table_id,
                                                         int64_t &new_schema_version,
                                                         ObDDLSQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id;
  ObArray<common::ObTabletID> tablet_ids;
  ObSchemaOperationType operation_type = OB_DDL_EXCHANGE_PARTITION;
  ObSchemaService *schema_service = NULL;
  ObMultiVersionSchemaService &multi_schema_service = ddl_service_.get_schema_service();
  new_schema_version = OB_INVALID_VERSION;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == exchange_data_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(exchange_data_table_id));
  } else if (OB_FAIL(used_table_to_tablet_id_map_.get_refactored(exchange_data_table_id, tablet_id))) {
    LOG_WARN("get_refactored tablet id from used_table_to_tablet_id_map failed", K(ret), K(exchange_data_table_id));
  } else if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
    LOG_WARN("failed to push back tablet id", K(ret), K(tablet_id));
  } else if (OB_ISNULL(schema_service = multi_schema_service.get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get schema_service is null", K(ret));
  } else {
    ObRefreshSchemaStatus schema_status;
    schema_status.tenant_id_ = tenant_id;
    HEAP_VAR(ObTableSchema, new_table_schema) {
      if (OB_FAIL(schema_service->get_table_schema_from_inner_table(schema_status, table_schema.get_table_id(), trans, new_table_schema))) {
        LOG_WARN("get_table_schema failed", K(ret), K(schema_status), K(table_schema.get_table_id()));
      } else if (OB_FAIL(refresh_table_schema_version_(tenant_id, new_table_schema))) {
        LOG_WARN("fail to refresh table schema version", K(ret), K(new_table_schema));
      } else if (OB_FAIL(schema_service->get_table_sql_service().update_table_schema_version(trans,
                                                                                             new_table_schema,
                                                                                             operation_type,
                                                                                             ddl_stmt_str))) {
        LOG_WARN("failed to update table schema version", K(ret), K(new_table_schema), K(operation_type));
      } else if (OB_FAIL(build_single_table_rw_defensive_(tenant_id, tablet_ids, new_table_schema.get_schema_version(), trans))) {
        LOG_WARN("failed to build rw defensive", K(ret), K(tenant_id), K(tablet_ids), K(new_table_schema.get_schema_version()));
      } else {
        new_schema_version = new_table_schema.get_schema_version();
      }
    }
  }
  return ret;
}

int ObPartitionExchange::get_local_storage_index_and_lob_table_schemas_(const ObTableSchema &table_schema,
                                                                        const bool is_pt_schema,
                                                                        const bool is_oracle_mode,
                                                                        ObIArray<const ObTableSchema*> &table_schemas,
                                                                        ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  ObSEArray<uint64_t, 20> aux_table_ids;
  table_schemas.reset();
  if (OB_FAIL(!table_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_schema), K(table_schema.is_valid()));
  } else if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple index infos failed", K(ret));
  } else {
    const uint64_t tenant_id = table_schema.get_tenant_id();
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); i++) {
      if (OB_FAIL(aux_table_ids.push_back(simple_index_infos.at(i).table_id_))) {
        LOG_WARN("fail to push back index table id", K(ret), K(simple_index_infos.at(i).table_id_));
      }
    }
    if (OB_SUCC(ret)) {
      uint64_t mtid = table_schema.get_aux_lob_meta_tid();
      uint64_t ptid = table_schema.get_aux_lob_piece_tid();
      if (!((mtid != OB_INVALID_ID && ptid != OB_INVALID_ID) || (mtid == OB_INVALID_ID && ptid == OB_INVALID_ID))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Expect meta tid and piece tid both valid or both invalid", K(ret), K(mtid), K(ptid));
      } else if (OB_INVALID_ID != mtid && OB_FAIL(aux_table_ids.push_back(mtid))) {
        LOG_WARN("fail to push back lob meta tid", K(ret), K(mtid));
      } else if (OB_INVALID_ID != ptid && OB_FAIL(aux_table_ids.push_back(ptid))) {
        LOG_WARN("fail to push back lob piece tid", K(ret), K(ptid));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < aux_table_ids.count(); i++) {
          const ObTableSchema *aux_table_schema = NULL;
          if (OB_FAIL(schema_guard.get_table_schema(tenant_id, aux_table_ids.at(i), aux_table_schema))) {
            LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(aux_table_ids.at(i)));
          } else if (OB_ISNULL(aux_table_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table schema should not be null", K(ret));
          } else if (OB_FAIL(check_auxiliary_schema_conditions_(aux_table_schema, is_oracle_mode))) {
            LOG_WARN("fail to check auxiliary schema conditions", K(ret), K(aux_table_schema), K(is_oracle_mode));
          } else if (aux_table_schema->is_index_table() && aux_table_schema->is_global_index_table()) {
            if (is_pt_schema) {
              if (OB_FAIL(unused_pt_index_id_.push_back(aux_table_schema->get_table_id()))) {
                LOG_WARN("failed to push back", K(ret), K(aux_table_schema->get_table_id()));
              }
            } else if (OB_FAIL(unused_nt_index_id_.push_back(aux_table_schema->get_table_id()))) {
              LOG_WARN("failed to push back", K(ret), K(aux_table_schema->get_table_id()));
            }
          } else if (OB_FAIL(table_schemas.push_back(aux_table_schema))) {
            LOG_WARN("failed to push back table schema", K(ret), K(aux_table_schema));
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionExchange::check_auxiliary_schema_conditions_(const ObTableSchema *table_schema, const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  if (!table_schema->is_index_table() && !table_schema->is_aux_lob_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is not index table or lob table", K(ret), K(table_schema->is_index_table()), K(table_schema->is_aux_lob_table()));
  } else if (OB_UNLIKELY(table_schema->is_index_table() && table_schema->is_index_local_storage() && INDEX_STATUS_AVAILABLE != table_schema->get_index_status())) {
    if (is_oracle_mode) {
      ret = OB_ERR_INDEX_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION;
    } else {
      ret = OB_TABLES_DIFFERENT_DEFINITIONS;
    }
    LOG_WARN("there are unavailable index table", K(ret), K(table_schema->is_index_table()), K(table_schema->get_table_id()), K(table_schema->get_index_status()));
  }
  return ret;
}
// it is required that the data and order of extended_type_info to be exactly the same in the mysql mode
int ObPartitionExchange::compare_column_extended_type_info_(const common::ObIArray<common::ObString> &l_extended_type_info, const common::ObIArray<common::ObString> &r_extended_type_info, bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  if (OB_UNLIKELY(l_extended_type_info.count() != r_extended_type_info.count())) {
    is_equal = false;
    LOG_WARN("column extended type info count of exchanging partition tables are not equal", K(ret), K(l_extended_type_info.count()), K(r_extended_type_info.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < l_extended_type_info.count(); i++) {
      if (0 != l_extended_type_info.at(i).compare(r_extended_type_info.at(i))) {
        is_equal = false;
        LOG_WARN("column extended type info count of exchanging partition tables are not equal", K(ret), K(l_extended_type_info.at(i)), K(r_extended_type_info.at(i)));
      }
    }
  }
  return ret;
}

bool ObPartitionExchange::in_supported_table_type_white_list_(const ObTableSchema &table_schema)
{
  return table_schema.is_user_table() && !table_schema.is_ctas_tmp_table();
}

bool ObPartitionExchange::in_find_same_aux_table_retry_white_list_(const int ret_code)
{
  return OB_ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION == ret_code ||
         OB_ERR_COLUMNS_NUMBER_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION == ret_code ||
         OB_ERR_COLUMN_TYPE_OR_SIZE_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION == ret_code ||
         OB_ERR_FOREIGN_KEY_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION == ret_code ||
         OB_ERR_INDEX_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION == ret_code ||
         OB_TABLES_DIFFERENT_DEFINITIONS == ret_code;
}

int ObPartitionExchange::generate_auxiliary_table_mapping_(const ObTableSchema &partitioned_data_table_schema,
                                                           const ObTableSchema &non_partitioned_data_table_schema,
                                                           const ObString &exchange_partition_name,
                                                           const ObPartitionLevel exchange_partition_level,
                                                           const bool is_oracle_mode,
                                                           ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObArray<const ObTableSchema*> partitioned_table_schemas;
  ObArray<const ObTableSchema*> non_partitioned_table_schemas;
  ObArray<bool> used_nt_schema_flag;
  bool is_equal = false;
  if (OB_UNLIKELY(!used_pt_nt_id_map_.created() || exchange_partition_name.empty() || PARTITION_LEVEL_ZERO == exchange_partition_level)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(used_pt_nt_id_map_.created()), K(exchange_partition_name), K(exchange_partition_level));
  } else if (OB_FAIL(get_local_storage_index_and_lob_table_schemas_(partitioned_data_table_schema, true/*is_partitioned_table_schema*/, is_oracle_mode, partitioned_table_schemas, schema_guard))) {
    LOG_WARN("fail to get local storage index and lob table schemas", K(ret), K(partitioned_data_table_schema), K(is_oracle_mode));
  } else if (OB_FAIL(get_local_storage_index_and_lob_table_schemas_(non_partitioned_data_table_schema, false/*is_partitioned_table_schema*/, is_oracle_mode, non_partitioned_table_schemas, schema_guard))) {
    LOG_WARN("fail to get local storage index and lob table schemas", K(ret), K(non_partitioned_data_table_schema), K(is_oracle_mode));
  } else if (OB_UNLIKELY(partitioned_table_schemas.count() != non_partitioned_table_schemas.count())) {
    if (is_oracle_mode) {
      ret = OB_ERR_INDEX_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION;
    } else {
      ret = OB_TABLES_DIFFERENT_DEFINITIONS;
    }
    LOG_WARN("pt schemas count and nt schemas count are not equal", K(ret), K(partitioned_table_schemas.count()), K(non_partitioned_table_schemas.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < non_partitioned_table_schemas.count(); i++) {
      if (OB_FAIL(used_nt_schema_flag.push_back(false))) {
        LOG_WARN("failed to push back", K(ret), K(i), K(used_nt_schema_flag));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < partitioned_table_schemas.count(); i++) {
      // for each partitioned table, find a one-to-one corresponding table in the non partitioned table
      if (OB_FAIL(generate_local_storage_index_and_lob_table_mapping_(*partitioned_table_schemas.at(i), non_partitioned_table_schemas, exchange_partition_name, exchange_partition_level, is_oracle_mode, used_nt_schema_flag))) {
        LOG_WARN("fail to generate used aux table id mapping", K(ret), KPC(partitioned_table_schemas.at(i)), K(non_partitioned_table_schemas.count()), K(exchange_partition_name), K(exchange_partition_level), K(is_oracle_mode), K(used_nt_schema_flag.count()));
      }
    }
  }
  return ret;
}

int ObPartitionExchange::generate_local_storage_index_and_lob_table_mapping_(const ObTableSchema &partitioned_table_schema,
                                                                             ObIArray<const ObTableSchema*> &non_partitioned_table_schemas,
                                                                             const ObString &exchange_partition_name,
                                                                             const ObPartitionLevel exchange_partition_level,
                                                                             const bool is_oracle_mode,
                                                                             ObIArray<bool> &used_nt_schema_flag)
{
  int ret = OB_SUCCESS;
  bool find_related_nt_schema = false;
  if (OB_UNLIKELY((!partitioned_table_schema.is_index_local_storage() && !partitioned_table_schema.is_aux_lob_table()) || non_partitioned_table_schemas.count() != used_nt_schema_flag.count() || exchange_partition_name.empty() || PARTITION_LEVEL_ZERO == exchange_partition_level)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partitioned_table_schema), K(non_partitioned_table_schemas.count()), K(used_nt_schema_flag.count()), K(exchange_partition_name), K(exchange_partition_level));
  } else if (partitioned_table_schema.is_index_local_storage()) {
    if (!is_oracle_mode) {
      if (OB_FAIL(generate_local_storage_index_table_mapping_in_mysql_mode_(partitioned_table_schema, non_partitioned_table_schemas, exchange_partition_name, exchange_partition_level, used_nt_schema_flag, find_related_nt_schema))) {
        LOG_WARN("fail to generate local storage index table mapping in mysql mode", K(ret), K(partitioned_table_schema), K(non_partitioned_table_schemas.count()), K(exchange_partition_name), K(exchange_partition_level), K(used_nt_schema_flag.count()), K(find_related_nt_schema));
      } else if (!find_related_nt_schema) {
        ret = OB_TABLES_DIFFERENT_DEFINITIONS;
        LOG_WARN("can't find related nt schema in mysql mode", K(ret), K(partitioned_table_schema), K(non_partitioned_table_schemas.count()), K(used_nt_schema_flag.count()), K(find_related_nt_schema));
      }
    } else if (OB_FAIL(generate_local_storage_index_table_mapping_in_oracle_mode_(partitioned_table_schema, non_partitioned_table_schemas, exchange_partition_name, exchange_partition_level, used_nt_schema_flag, find_related_nt_schema))) {
      LOG_WARN("fail to generate local storage index table mapping in oracle mode", K(ret), K(partitioned_table_schema), K(non_partitioned_table_schemas.count()), K(exchange_partition_name), K(exchange_partition_level), K(used_nt_schema_flag.count()), K(find_related_nt_schema));
    } else if (!find_related_nt_schema) {
      ret = OB_ERR_INDEX_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION;
      LOG_WARN("can't find related nt schema in mysql mode", K(ret), K(partitioned_table_schema), K(non_partitioned_table_schemas.count()), K(used_nt_schema_flag.count()), K(find_related_nt_schema));
    }
  } else if (OB_FAIL(generate_lob_table_mapping_(partitioned_table_schema, non_partitioned_table_schemas, exchange_partition_name, exchange_partition_level, is_oracle_mode, used_nt_schema_flag, find_related_nt_schema))){
    LOG_WARN("fail to generate lob table mapping", K(ret), K(partitioned_table_schema), K(non_partitioned_table_schemas.count()), K(exchange_partition_name), K(exchange_partition_level), K(is_oracle_mode), K(used_nt_schema_flag.count()), K(find_related_nt_schema));
  } else if (!find_related_nt_schema) {
    if (is_oracle_mode) {
      ret = OB_ERR_INDEX_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION;
    } else {
      ret = OB_TABLES_DIFFERENT_DEFINITIONS;
    }
    LOG_WARN("can't find related nt_schema", K(ret), K(partitioned_table_schema), K(non_partitioned_table_schemas.count()), K(used_nt_schema_flag), K(is_oracle_mode));
  }
  return ret;
}

int ObPartitionExchange::generate_local_storage_index_table_mapping_in_mysql_mode_(const ObTableSchema &partitioned_table_schema,
                                                                                   ObIArray<const ObTableSchema*> &non_partitioned_table_schemas,
                                                                                   const ObString &exchange_partition_name,
                                                                                   const ObPartitionLevel exchange_partition_level,
                                                                                   ObIArray<bool> &used_nt_schema_flag,
                                                                                   bool &find_related_nt_schema)
{
  int ret = OB_SUCCESS;
  find_related_nt_schema = false;
  if (OB_UNLIKELY(!partitioned_table_schema.is_index_local_storage() || non_partitioned_table_schemas.count() != used_nt_schema_flag.count() || exchange_partition_name.empty() || PARTITION_LEVEL_ZERO == exchange_partition_level)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partitioned_table_schema), K(partitioned_table_schema.is_index_local_storage()), K(non_partitioned_table_schemas.count()), K(used_nt_schema_flag.count()), K(exchange_partition_name), K(exchange_partition_level));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !find_related_nt_schema && i < non_partitioned_table_schemas.count(); i++) {
      ObString pt_index_name;
      ObString nt_index_name;
      if (OB_ISNULL(non_partitioned_table_schemas.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", K(ret));
      } else if (!non_partitioned_table_schemas.at(i)->is_index_local_storage() || used_nt_schema_flag.at(i)) {
      } else if (OB_FAIL(partitioned_table_schema.get_index_name(pt_index_name))) {
        LOG_WARN("fail to get index name", K(ret), K(partitioned_table_schema));
      } else if (OB_FAIL(non_partitioned_table_schemas.at(i)->get_index_name(nt_index_name))) {
        LOG_WARN("fail to get index name", K(ret), KPC(non_partitioned_table_schemas.at(i)));
      } else if (0 == pt_index_name.compare(nt_index_name)) {
        if (OB_FAIL(check_table_conditions_in_common_(partitioned_table_schema, *non_partitioned_table_schemas.at(i), exchange_partition_name, exchange_partition_level, false /*is mysql mode*/))) {
          LOG_WARN("fail to check table conditions in common", K(ret), K(partitioned_table_schema), KPC(non_partitioned_table_schemas.at(i)), K(exchange_partition_name), K(exchange_partition_level));
        } else if (OB_FAIL(check_table_all_column_conditions_(partitioned_table_schema, *non_partitioned_table_schemas.at(i), false /*is mysql mode*/))) {
          LOG_WARN("fail to check table all column conditions", K(ret), K(partitioned_table_schema.get_table_id()), K(non_partitioned_table_schemas.at(i)->get_table_id()));
        } else {
          find_related_nt_schema = true;
          used_nt_schema_flag.at(i) = true;
          if (OB_FAIL(used_pt_nt_id_map_.set_refactored(partitioned_table_schema.get_table_id(), non_partitioned_table_schemas.at(i)->get_table_id()))) {
            LOG_WARN("fail to set refactored pt nt schema mapping", K(ret), K(partitioned_table_schema), K(non_partitioned_table_schemas.at(i)->get_table_id()));
          }
        }
      } else {
        LOG_WARN("index table name are different", K(ret), K(pt_index_name), K(nt_index_name));
      }
    }
  }
  return ret;
}

int ObPartitionExchange::generate_local_storage_index_table_mapping_in_oracle_mode_(const ObTableSchema &partitioned_table_schema,
                                                                                    ObIArray<const ObTableSchema*> &non_partitioned_table_schemas,
                                                                                    const ObString &exchange_partition_name,
                                                                                    const ObPartitionLevel exchange_partition_level,
                                                                                    ObIArray<bool> &used_nt_schema_flag,
                                                                                    bool &find_related_nt_schema)
{
  int ret = OB_SUCCESS;
  find_related_nt_schema = false;
  if (OB_UNLIKELY(!partitioned_table_schema.is_index_local_storage() || non_partitioned_table_schemas.count() != used_nt_schema_flag.count() || exchange_partition_name.empty() || PARTITION_LEVEL_ZERO == exchange_partition_level)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partitioned_table_schema), K(partitioned_table_schema.is_index_local_storage()), K(non_partitioned_table_schemas.count()), K(used_nt_schema_flag.count()), K(exchange_partition_name), K(exchange_partition_level));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !find_related_nt_schema && i < non_partitioned_table_schemas.count(); i++) {
      if (OB_ISNULL(non_partitioned_table_schemas.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", K(ret));
      } else if (!non_partitioned_table_schemas.at(i)->is_index_local_storage() || used_nt_schema_flag.at(i)) {
      } else if (OB_FAIL(check_table_conditions_in_common_(partitioned_table_schema, *non_partitioned_table_schemas.at(i), exchange_partition_name, exchange_partition_level, true /*is oracle mode*/))) {
        if (in_find_same_aux_table_retry_white_list_(ret)) {
          LOG_WARN("all column conditions of exchanging partition tables are not equal, and retry find the matched table", K(ret), K(partitioned_table_schema.get_table_id()), K(non_partitioned_table_schemas.at(i)->get_table_id()), K(exchange_partition_name), K(exchange_partition_level));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("all column conditions of exchanging partition tables are not equal, and ret_code not in in_find_same_aux_table_retry_white_list", K(ret), K(partitioned_table_schema.get_table_id()), K(non_partitioned_table_schemas.at(i)->get_table_id()), K(exchange_partition_name), K(exchange_partition_level));
        }
      } else if (OB_FAIL(check_table_all_column_conditions_(partitioned_table_schema, *non_partitioned_table_schemas.at(i), true /*is oracle mode*/))) {
        // uncertain if other non_partitioned tables match the partitioned table, so try matching other non_partitioned tables
        if (in_find_same_aux_table_retry_white_list_(ret)) {
          LOG_WARN("all column conditions of exchanging partition tables are not equal, and retry find the matched table", K(ret), K(partitioned_table_schema.get_table_id()), K(non_partitioned_table_schemas.at(i)->get_table_id()), K(exchange_partition_name), K(exchange_partition_level));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("all column conditions of exchanging partition tables are not equal, and ret_code not in in_find_same_aux_table_retry_white_list", K(ret), K(partitioned_table_schema.get_table_id()), K(non_partitioned_table_schemas.at(i)->get_table_id()), K(exchange_partition_name), K(exchange_partition_level));
        }
      } else {
        find_related_nt_schema = true;
        used_nt_schema_flag.at(i) = true;
        if (OB_FAIL(used_pt_nt_id_map_.set_refactored(partitioned_table_schema.get_table_id(), non_partitioned_table_schemas.at(i)->get_table_id()))) {
          LOG_WARN("fail to set refactored pt nt schema mapping", K(ret), K(partitioned_table_schema), K(non_partitioned_table_schemas.at(i)->get_table_id()));
        }
      }
    }
  }
  return ret;
}

int ObPartitionExchange::generate_lob_table_mapping_(const ObTableSchema &partitioned_table_schema,
                                                     ObIArray<const ObTableSchema*> &non_partitioned_table_schemas,
                                                     const ObString &exchange_partition_name,
                                                     const ObPartitionLevel exchange_partition_level,
                                                     const bool is_oracle_mode,
                                                     ObIArray<bool> &used_nt_schema_flag,
                                                     bool &find_related_nt_schema)
{
  int ret = OB_SUCCESS;
  bool is_equal = false;
  find_related_nt_schema = false;
  if (OB_UNLIKELY(!partitioned_table_schema.is_aux_lob_table() || non_partitioned_table_schemas.count() != used_nt_schema_flag.count() || exchange_partition_name.empty() || PARTITION_LEVEL_ZERO == exchange_partition_level)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partitioned_table_schema), K(partitioned_table_schema.is_aux_lob_table()), K(non_partitioned_table_schemas.count()), K(used_nt_schema_flag.count()), K(exchange_partition_name), K(exchange_partition_level), K(is_oracle_mode));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !find_related_nt_schema && i < non_partitioned_table_schemas.count(); i++) {
      if (OB_ISNULL(non_partitioned_table_schemas.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", K(ret));
      } else if (!non_partitioned_table_schemas.at(i)->is_aux_lob_table() || used_nt_schema_flag.at(i)) {
      } else if ((partitioned_table_schema.is_aux_lob_meta_table() && non_partitioned_table_schemas.at(i)->is_aux_lob_meta_table()) || (partitioned_table_schema.is_aux_lob_piece_table() && non_partitioned_table_schemas.at(i)->is_aux_lob_piece_table())) {
        find_related_nt_schema = true;
        used_nt_schema_flag.at(i) = true;
        if (OB_FAIL(used_pt_nt_id_map_.set_refactored(partitioned_table_schema.get_table_id(), non_partitioned_table_schemas.at(i)->get_table_id()))) {
          LOG_WARN("fail to set refactored pt nt schema mapping", K(ret), K(partitioned_table_schema), K(non_partitioned_table_schemas.at(i)->get_table_id()));
        }
      }
    }
  }
  return ret;
}

int ObPartitionExchange::update_index_status_(const uint64_t tenant_id,
                                              const uint64_t table_id,
                                              const share::schema::ObIndexStatus status,
                                              const bool in_offline_ddl_white_list,
                                              ObDDLOperator &ddl_operator,
                                              ObDDLSQLTransaction &trans,
                                              ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *index_schema = NULL;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == table_id || status <= INDEX_STATUS_NOT_FOUND || status >= INDEX_STATUS_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(table_id), K(status), K(in_offline_ddl_white_list));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, index_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(table_id), KPC(index_schema));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(tenant_id), K(table_id));
  } else if (OB_UNLIKELY(!index_schema->is_index_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the state that needs to be modified is not the index table", K(ret), K(tenant_id), K(table_id), K(index_schema->is_index_table()));
  } else if (OB_FAIL(ddl_operator.update_index_status(
    tenant_id,
    index_schema->get_data_table_id(),
    index_schema->get_table_id(),
    status,
    in_offline_ddl_white_list,
    trans,
    nullptr /* ddl_stmt_str */))) {
    LOG_WARN("update_index_status failed", K(ret), K(tenant_id), K(index_schema->get_data_table_id()), K(index_schema->get_table_id()), K(status), K(in_offline_ddl_white_list));
  }
  return ret;
}

int ObPartitionExchange::build_single_table_rw_defensive_(const uint64_t tenant_id,
                                                          const ObArray<common::ObTabletID> &tablet_ids,
                                                          const int64_t schema_version,
                                                          ObDDLSQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || tablet_ids.empty() || schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(tablet_ids), K(schema_version));
  } else if (OB_UNLIKELY(!ddl_service_.is_inited())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", K(ret));
  } else if (OB_LIKELY(data_version_ >= DATA_VERSION_4_3_2_0)) {
    const int64_t abs_timeout_us = THIS_WORKER.is_timeout_ts_valid() ? THIS_WORKER.get_timeout_ts()
                                                                     : ObTimeUtility::current_time() + GCONF.rpc_timeout;
    if (OB_FAIL(ObTabletBindingMdsHelper::modify_tablet_binding_for_rw_defensive(tenant_id, tablet_ids, schema_version, abs_timeout_us, trans))) {
      LOG_WARN("failed to modify tablet binding", K(ret), K(abs_timeout_us));
    }
  } else {
    ObArray<ObBatchUnbindTabletArg> args;
    if (OB_FAIL(build_modify_tablet_binding_args_v1_(
      tenant_id, tablet_ids, schema_version, args, trans))) {
      LOG_WARN("failed to build reuse index args", K(ret));
    }
    ObArenaAllocator allocator("DDLRWDefens");
    for (int64_t i = 0; OB_SUCC(ret) && i < args.count(); i++) {
      int64_t pos = 0;
      int64_t size = args[i].get_serialize_size();
      char *buf = nullptr;
      allocator.reuse();
      if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate", K(ret));
      } else if (OB_FAIL(args[i].serialize(buf, size, pos))) {
        LOG_WARN("failed to serialize arg", K(ret));
      } else if (OB_FAIL(trans.register_tx_data(args[i].tenant_id_, args[i].ls_id_, transaction::ObTxDataSourceType::UNBIND_TABLET_NEW_MDS, buf, pos))) {
        LOG_WARN("failed to register tx data", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionExchange::build_modify_tablet_binding_args_v1_(const uint64_t tenant_id,
                                                              const ObArray<ObTabletID> &tablet_ids,
                                                              const int64_t schema_version,
                                                              ObIArray<ObBatchUnbindTabletArg> &modify_args,
                                                              ObDDLSQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObArray<LSTabletID> tablets;
  if (OB_FAIL(get_tablets_(tenant_id, tablet_ids, tablets, trans))) {
    LOG_WARN("failed to get tablet ids of orig table", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tablets.count(); i++) {
    const ObLSID &ls_id = tablets[i].first;
    int64_t j = 0;
    for (; j < modify_args.count(); j++) {
      if (modify_args.at(j).ls_id_ == ls_id && modify_args.at(j).tenant_id_ == tenant_id) {
        break;
      }
    }
    if (j == modify_args.count()) {
      ObBatchUnbindTabletArg modify_arg;
      modify_arg.tenant_id_ = tenant_id;
      modify_arg.ls_id_ = ls_id;
      modify_arg.schema_version_ = schema_version;
      if (OB_FAIL(modify_args.push_back(modify_arg))) {
        LOG_WARN("failed to push back modify arg", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (0 <= j && j < modify_args.count()) {
        ObBatchUnbindTabletArg &modify_arg = modify_args.at(j);
        const ObTabletID &tablet_id = tablets[i].second;
        if (OB_FAIL(modify_arg.hidden_tablet_ids_.push_back(tablet_id))) {
          LOG_WARN("failed to push back", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Array idx out of bounds", K(ret), K(j), K(modify_args.count()));
      }
    }
  }
  LOG_DEBUG("build modify tablet binding args", K(ret), K(modify_args));
  return ret;
}

int ObPartitionExchange::get_tablets_(const uint64_t tenant_id,
                                      const ObArray<common::ObTabletID> &tablet_ids,
                                      ObIArray<LSTabletID> &tablets,
                                      ObDDLSQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObArray<ObLSID> ls_ids;
  tablets.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || tablet_ids.count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(tablet_ids));
  } else if (OB_FAIL(ObTabletToLSTableOperator::batch_get_ls(trans, tenant_id, tablet_ids, ls_ids))) {
    LOG_WARN("failed to batch get ls", K(ret));
  } else if (OB_UNLIKELY(tablet_ids.count() != ls_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet ids ls ids", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
    if (OB_FAIL(tablets.push_back({ls_ids[i], tablet_ids[i]}))) {
      LOG_WARN("failed to push back tablet id and ls id", K(ret));
    }
  }
  return ret;
}

int ObPartitionExchange::adapting_cdc_changes_in_exchange_partition_(const uint64_t tenant_id,
                                                                     const uint64_t partitioned_table_id,
                                                                     const uint64_t non_partitioned_table_id,
                                                                     ObDDLSQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == partitioned_table_id || OB_INVALID_ID == non_partitioned_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(partitioned_table_id), K(non_partitioned_table_id));
  } else if (OB_FAIL(used_pt_nt_id_map_.set_refactored(partitioned_table_id, non_partitioned_table_id))) {
    LOG_WARN("fail to set refactored pt nt schema mapping", K(ret), K(partitioned_table_id), K(non_partitioned_table_id));
  } else {
    ObChangeTabletToTableArg arg;
    share::ObLSID ls_id(share::ObLSID::SYS_LS_ID);
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    arg.base_table_id_ = partitioned_table_id;
    arg.inc_table_id_ = non_partitioned_table_id;
    common::hash::ObHashMap<uint64_t, uint64_t>::iterator iter_table;
    for (iter_table = used_pt_nt_id_map_.begin(); OB_SUCC(ret) && iter_table != used_pt_nt_id_map_.end(); ++iter_table) {
      ObTabletID tmp_tablet;
      if (OB_FAIL(arg.table_ids_.push_back(iter_table->first))) {
        LOG_WARN("failed to push back table id", K(ret), K(iter_table->first));
      } else if (OB_FAIL(used_table_to_tablet_id_map_.get_refactored(iter_table->second, tmp_tablet))) {
        LOG_WARN("get_refactored tablet id from used_table_to_tablet_id_map failed", K(ret), K(iter_table->second), K(tmp_tablet));
      } else if (OB_FAIL(arg.tablet_ids_.push_back(tmp_tablet))) {
        LOG_WARN("failed to push back tablet id", K(ret), K(tmp_tablet));
      } else if (OB_FAIL(arg.table_ids_.push_back(iter_table->second))) {
        LOG_WARN("failed to push back table id", K(ret), K(iter_table->second));
      } else if (OB_FAIL(used_table_to_tablet_id_map_.get_refactored(iter_table->first, tmp_tablet))) {
        LOG_WARN("get_refactored tablet id from used_table_to_tablet_id_map failed", K(ret), K(iter_table->first), K(tmp_tablet));
      } else if (OB_FAIL(arg.tablet_ids_.push_back(tmp_tablet))) {
        LOG_WARN("failed to push back tablet id", K(ret), K(tmp_tablet));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(arg.table_ids_.count() != arg.tablet_ids_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exchange partitions num are different tables num", K(ret), K(arg.table_ids_.count()), K(arg.tablet_ids_.count()));
      } else {
        int64_t pos = 0;
        int64_t size = arg.get_serialize_size();
        ObArenaAllocator allocator;
        char *buf = nullptr;
        if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate", K(ret));
        } else if (OB_FAIL(arg.serialize(buf, size, pos))) {
          LOG_WARN("failed to serialize arg", K(ret));
        } else if (OB_FAIL(trans.register_tx_data(arg.tenant_id_, arg.ls_id_, transaction::ObTxDataSourceType::CHANGE_TABLET_TO_TABLE_MDS, buf, pos))) {
          LOG_WARN("failed to register tx data", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPartitionExchange::update_autoinc_column_information_(const uint64_t tenant_id,
                                                            const ObTableSchema &partitioned_table_schema,
                                                            const ObTableSchema &non_partitioned_table_schema,
                                                            ObDDLOperator &ddl_operator,
                                                            ObDDLSQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !partitioned_table_schema.is_valid() || !non_partitioned_table_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(partitioned_table_schema), K(non_partitioned_table_schema));
  } else {
    const ObColumnSchemaV2 *pt_col_schema = NULL;
    const ObColumnSchemaV2 *nt_col_schema = NULL;
    if (0 != partitioned_table_schema.get_autoinc_column_id() && 0 != non_partitioned_table_schema.get_autoinc_column_id()) {
      if (OB_ISNULL(pt_col_schema = partitioned_table_schema.get_column_schema(partitioned_table_schema.get_autoinc_column_id()))) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("fail to get column schema", K(ret), K(partitioned_table_schema), K(partitioned_table_schema.get_autoinc_column_id()));
      } else if (OB_ISNULL(nt_col_schema = non_partitioned_table_schema.get_column_schema(non_partitioned_table_schema.get_autoinc_column_id()))) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("fail to get column schema", K(ret), K(partitioned_table_schema), K(partitioned_table_schema.get_autoinc_column_id()));
      } else {
        uint64_t pt_sequence_value = 0;
        uint64_t nt_sequence_value = 0;
        uint64_t pt_column_id = pt_col_schema->get_column_id();
        uint64_t nt_column_id = nt_col_schema->get_column_id();
        uint64_t new_sequence_value = 0;
        ObAutoincrementService &auto_inc_service = ObAutoincrementService::get_instance();
        if (OB_FAIL(ddl_operator.get_target_auto_inc_sequence_value(tenant_id, partitioned_table_schema.get_table_id(), pt_column_id, pt_sequence_value, trans))) {
          LOG_WARN("get sequence value failed", K(ret), K(tenant_id), K(partitioned_table_schema.get_table_id()), K(pt_column_id), K(pt_sequence_value));
        } else if (OB_FAIL(ddl_operator.get_target_auto_inc_sequence_value(tenant_id, non_partitioned_table_schema.get_table_id(), nt_column_id, nt_sequence_value, trans))) {
          LOG_WARN("get sequence value failed", K(ret), K(tenant_id), K(non_partitioned_table_schema.get_table_id()), K(nt_column_id), K(nt_sequence_value));
        } else if (FALSE_IT(new_sequence_value = max(pt_sequence_value, nt_sequence_value))) {
        } else if (OB_FAIL(ddl_operator.set_target_auto_inc_sync_value(tenant_id, partitioned_table_schema.get_table_id(), pt_column_id, new_sequence_value, new_sequence_value - 1, trans))) {
          LOG_WARN("set sequence value failed", K(ret), K(tenant_id), K(partitioned_table_schema.get_table_id()), K(pt_column_id), K(new_sequence_value));
        } else if (OB_FAIL(ddl_operator.set_target_auto_inc_sync_value(tenant_id, non_partitioned_table_schema.get_table_id(), nt_column_id, new_sequence_value, new_sequence_value - 1, trans))) {
          LOG_WARN("set sequence value failed", K(ret), K(tenant_id), K(non_partitioned_table_schema.get_table_id()), K(nt_column_id), K(new_sequence_value));
        }
      }
    }
  }
  return ret;
}

int ObPartitionExchange::update_identity_column_information_(const uint64_t tenant_id,
                                                             const ObTableSchema &partitioned_table_schema,
                                                             const ObTableSchema &non_partitioned_table_schema,
                                                             const bool is_oracle_mode,
                                                             ObDDLOperator &ddl_operator,
                                                             ObDDLSQLTransaction &trans,
                                                             ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !partitioned_table_schema.is_valid() || !non_partitioned_table_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(partitioned_table_schema), K(non_partitioned_table_schema));
  } else {
    ObTableSchema::const_column_iterator pt_iter_begin = partitioned_table_schema.column_begin();
    ObTableSchema::const_column_iterator pt_iter_end = partitioned_table_schema.column_end();
    ObTableSchema::const_column_iterator nt_iter_begin = non_partitioned_table_schema.column_begin();
    ObTableSchema::const_column_iterator nt_iter_end = non_partitioned_table_schema.column_end();
    ObColumnSchemaV2 *pt_col_schema = NULL;
    ObColumnSchemaV2 *nt_col_schema = NULL;
    ObArenaAllocator allocator(lib::ObLabel("ExchangePart"));
    while (OB_SUCC(ret) && OB_SUCC(get_next_pair_column_schema_(pt_iter_begin, pt_iter_end, nt_iter_begin, nt_iter_end, is_oracle_mode, pt_col_schema, nt_col_schema))) {
      if (OB_ISNULL(pt_col_schema) || OB_ISNULL(nt_col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to column schema", K(ret), KPC(pt_col_schema), KPC(nt_col_schema));
      } else if (pt_col_schema->is_identity_column() && nt_col_schema->is_identity_column()) {
        const ObSequenceSchema *pt_sequence_schema = NULL;
        const ObSequenceSchema *nt_sequence_schema = NULL;
        ObSequenceSchema pt_tmp_sequence_schema;
        ObSequenceSchema nt_tmp_sequence_schema;
        common::number::ObNumber pt_next_value;
        common::number::ObNumber nt_next_value;
        common::number::ObNumber max_next_value;
        if (OB_FAIL(schema_guard.get_sequence_schema(pt_col_schema->get_tenant_id(),
                                                     pt_col_schema->get_sequence_id(),
                                                     pt_sequence_schema))) {
          LOG_WARN("get sequence schema fail", K(ret), KPC(pt_col_schema));
        } else if (OB_ISNULL(pt_sequence_schema)) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("fail to get sequence schema", K(ret), K(pt_col_schema->get_tenant_id()), K(pt_col_schema->get_sequence_id()), KPC(pt_col_schema));
        } else if (OB_FAIL(schema_guard.get_sequence_schema(nt_col_schema->get_tenant_id(),
                                                            nt_col_schema->get_sequence_id(),
                                                            nt_sequence_schema))) {
          LOG_WARN("get sequence schema fail", K(ret), KPC(nt_col_schema));
        } else if (OB_ISNULL(nt_sequence_schema)) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("fail to get sequence schema", K(ret), K(nt_col_schema->get_tenant_id()), K(nt_col_schema->get_sequence_id()), KPC(nt_col_schema));
        } else if (OB_FAIL(ddl_operator.get_target_sequence_sync_value(tenant_id,
                                                                       pt_sequence_schema->get_sequence_id(),
                                                                       trans,
                                                                       allocator,
                                                                       pt_next_value))) {
          LOG_WARN("fail to get target sequence sync value", K(ret), KPC(pt_col_schema));
        } else if (OB_FAIL(ddl_operator.get_target_sequence_sync_value(tenant_id,
                                                                       nt_sequence_schema->get_sequence_id(),
                                                                       trans,
                                                                       allocator,
                                                                       nt_next_value))) {
          LOG_WARN("fail to get target sequence sync value", K(ret), KPC(nt_col_schema));
        } else if (OB_FAIL(pt_tmp_sequence_schema.assign(*pt_sequence_schema))) {
          LOG_WARN("fail to assign sequence schema", K(ret));
        } else if (OB_FAIL(nt_tmp_sequence_schema.assign(*nt_sequence_schema))) {
          LOG_WARN("fail to assign sequence schema", K(ret));
        } else {
          if (1 == pt_next_value.compare(nt_next_value)) {
            max_next_value = pt_next_value;
          } else {
            max_next_value = nt_next_value;
          }
          if (OB_FAIL(pt_tmp_sequence_schema.set_start_with(max_next_value))) {
            LOG_WARN("fail to set sequence start with", K(ret), K(max_next_value));
          } else if (OB_FAIL(nt_tmp_sequence_schema.set_start_with(max_next_value))) {
            LOG_WARN("fail to set sequence start with", K(ret), K(max_next_value));
          } else if (OB_FAIL(ddl_operator.alter_target_sequence_start_with(pt_tmp_sequence_schema, trans))) {
            LOG_WARN("fail to alter target sequence start with", K(ret), K(max_next_value), K(pt_tmp_sequence_schema));
          } else if (OB_FAIL(ddl_operator.alter_target_sequence_start_with(nt_tmp_sequence_schema, trans))) {
            LOG_WARN("fail to alter target sequence start with", K(ret), K(max_next_value), K(nt_tmp_sequence_schema));
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to update identity column information", K(ret), K(tenant_id), K(partitioned_table_schema), K(non_partitioned_table_schema), K(is_oracle_mode));
    }
  }
  return ret;
}

int ObPartitionExchange::sync_exchange_partition_stats_info_(const uint64_t tenant_id,
                                                             const uint64_t new_table_id,
                                                             const uint64_t new_stat_level,
                                                             const int64_t old_partition_id,
                                                             const int64_t new_partition_id,
                                                             const ObTabletID &tablet_id,
                                                             const ObTableSchema &orig_table_schema,
                                                             ObDDLSQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql_string;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == new_table_id || StatLevel::INVALID_LEVEL == new_stat_level || OB_INVALID_ID == old_partition_id ||
                  OB_INVALID_ID == new_partition_id || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(new_table_id), K(new_stat_level), K(old_partition_id), K(new_partition_id), K(tablet_id));
  } else if (OB_FAIL(sql_string.assign_fmt("UPDATE %s SET table_id = %ld, partition_id = %ld, object_type = %ld"
        " WHERE tenant_id = %ld and table_id = %ld and partition_id = %ld",
        OB_ALL_TABLE_STAT_TNAME, new_table_id, new_partition_id, new_stat_level,
        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id), orig_table_schema.get_table_id(), old_partition_id))) {
    LOG_WARN("fail to assign sql string", K(ret), K(new_table_id), K(new_partition_id), K(new_stat_level), K(tenant_id), K(orig_table_schema.get_table_id()), K(old_partition_id));
  } else if (OB_FAIL(trans.write(tenant_id, sql_string.ptr(), affected_rows))) {
    LOG_WARN("fail to update __all_table_stat", K(ret), K(sql_string));
  } else if (OB_FAIL(update_table_all_monitor_modified_(tenant_id, new_table_id, tablet_id, orig_table_schema, trans))) {
    LOG_WARN("fail to update table __all_monitor_modified", K(ret), K(tenant_id), K(new_table_id), K(tablet_id), K(orig_table_schema));
  } else {
    ObTableSchema::const_column_iterator iter = orig_table_schema.column_begin();
    ObTableSchema::const_column_iterator iter_end = orig_table_schema.column_end();
    for (; OB_SUCC(ret) && iter != iter_end; iter++) {
      const ObColumnSchemaV2 *col = *iter;
      ObSqlString column_sql_string;
      ObSqlString histogram_sql_string;
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col is NULL", K(ret));
      } else if (col->get_column_id() < OB_APP_MIN_COLUMN_ID) {
        // bypass hidden column
      } else if (col->is_udt_hidden_column()) {
        // bypass udt hidden column
      } else if (OB_FAIL(column_sql_string.assign_fmt("UPDATE %s SET table_id = %ld, partition_id = %ld, column_id = %ld, object_type = %ld"
          " WHERE tenant_id = %ld and table_id = %ld and partition_id = %ld and column_id = %ld",
          OB_ALL_COLUMN_STAT_TNAME, new_table_id, new_partition_id, col->get_column_id(), new_stat_level,
          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id), orig_table_schema.get_table_id(), old_partition_id, col->get_column_id()))) {
        LOG_WARN("fail to assign sql string", K(ret), K(new_table_id), K(new_partition_id), K(col->get_column_id()), K(new_stat_level), K(tenant_id), K(orig_table_schema.get_table_id()), K(old_partition_id), K(col->get_column_id()));
      } else if (OB_FAIL(histogram_sql_string.assign_fmt("UPDATE %s SET table_id = %ld, partition_id = %ld, column_id = %ld, object_type = %ld"
          " WHERE tenant_id = %ld and table_id = %ld and partition_id = %ld and column_id = %ld",
          OB_ALL_HISTOGRAM_STAT_TNAME, new_table_id, new_partition_id, col->get_column_id(), new_stat_level,
          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id), orig_table_schema.get_table_id(), old_partition_id, col->get_column_id()))) {
        LOG_WARN("fail to assign sql string", K(ret), K(new_table_id), K(new_partition_id), K(col->get_column_id()), K(new_stat_level), K(tenant_id), K(orig_table_schema.get_table_id()), K(old_partition_id), K(col->get_column_id()));
      } else if (OB_FAIL(trans.write(tenant_id, column_sql_string.ptr(), affected_rows))) {
        LOG_WARN("fail to update __all_column_stat", K(ret), K(tenant_id), K(column_sql_string));
      } else if (OB_FAIL(trans.write(tenant_id, histogram_sql_string.ptr(), affected_rows))) {
        LOG_WARN("fail to update __all_histogram_stat", K(ret), K(tenant_id), K(histogram_sql_string));
      }
    }
  }
  return ret;
}

int ObPartitionExchange::update_table_all_monitor_modified_(const uint64_t tenant_id, const uint64_t new_table_id, const ObTabletID &tablet_id, const ObTableSchema &orig_table_schema, ObDDLSQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString monitor_modified_read_sql_string;
  ObSqlString monitor_modified_insert_sql_string;
  ObSqlString monitor_modified_delete_sql_string;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == new_table_id || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(new_table_id), K(tablet_id));
  } else if (OB_FAIL(monitor_modified_read_sql_string.assign_fmt("SELECT last_inserts, last_updates, last_deletes, inserts, updates, deletes FROM %s WHERE tenant_id = %ld and table_id = %ld and tablet_id = %ld",
             OB_ALL_MONITOR_MODIFIED_TNAME, ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id), orig_table_schema.get_table_id(), tablet_id.id()))) {
    LOG_WARN("fail to assign sql string", K(ret), K(tenant_id), K(orig_table_schema.get_table_id()), K(tablet_id));
  } else {
    bool need_update = false;
    int64_t last_inserts = 0;
    int64_t last_updates = 0;
    int64_t last_deletes = 0;
    int64_t inserts = 0;
    int64_t updates = 0;
    int64_t deletes = 0;
    common::sqlclient::ObMySQLResult *result = NULL;
    ObSQLClientRetryWeak sql_client_retry_weak(GCTX.sql_proxy_);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, monitor_modified_read_sql_string.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(monitor_modified_read_sql_string));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail query sql", K(ret));
      } else if (OB_FAIL(result->next())) {
        if (common::OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get next result", K(ret), K(tenant_id));
        }
      } else {
        need_update = true;
        EXTRACT_INT_FIELD_MYSQL(*result, "last_inserts", last_inserts, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "last_updates", last_updates, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "last_deletes", last_deletes, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "inserts", inserts, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "updates", updates, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "deletes", deletes, int64_t);
      }
    }
    if (OB_SUCC(ret) && need_update) {
      if (OB_FAIL(monitor_modified_insert_sql_string.assign_fmt("INSERT INTO %s(tenant_id, table_id, tablet_id, last_inserts, last_updates, last_deletes, inserts, updates, deletes) VALUES (%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld) ON DUPLICATE KEY UPDATE tenant_id = %ld, table_id = %ld, tablet_id = %ld", OB_ALL_MONITOR_MODIFIED_TNAME,
        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id), new_table_id, tablet_id.id(), last_inserts, last_updates, last_deletes, inserts, updates, deletes, ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id), new_table_id, tablet_id.id()))) {
        LOG_WARN("fail to assign sql string", K(ret));
      } else if (OB_FAIL(monitor_modified_delete_sql_string.assign_fmt("DELETE FROM %s WHERE tenant_id = %ld AND table_id = %ld AND tablet_id = %ld", OB_ALL_MONITOR_MODIFIED_TNAME,
        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id), orig_table_schema.get_table_id(), tablet_id.id()))) {
        LOG_WARN("fail to assign sql string", K(ret));
      } else if (OB_FAIL(trans.write(tenant_id, monitor_modified_insert_sql_string.ptr(), affected_rows))) {
        LOG_WARN("fail to insert __all_monitor_modified", K(ret), K(monitor_modified_insert_sql_string));
      } else if (OB_FAIL(trans.write(tenant_id, monitor_modified_delete_sql_string.ptr(), affected_rows))) {
        LOG_WARN("fail to delete __all_monitor_modified", K(ret), K(monitor_modified_delete_sql_string));
      }
    }
  }
  return ret;
}

int ObPartitionExchange::get_object_id_from_partition_schema_(ObPartitionSchema &partition_schema, const bool get_subpart_only, int64_t &object_id)
{
  int ret = OB_SUCCESS;
  object_id = OB_INVALID_PARTITION_ID;
  int64_t partition_num = partition_schema.get_partition_num();
  ObPartitionLevel part_level = partition_schema.get_part_level();
  ObPartition** part_array = partition_schema.get_part_array();
  if (OB_UNLIKELY(1 != partition_num)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition num is not equal 1", K(ret), K(partition_num));
  } else if (OB_ISNULL(part_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part_array is empty", K(ret));
  } else {
    ObPartition* part = part_array[0];
    if (OB_ISNULL(part)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part is null", K(ret));
    } else if (!get_subpart_only) {
      object_id = part->get_part_id();
    } else if (OB_ISNULL(part->get_subpart_array()) || 1 != part->get_subpartition_num()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sub_part_array is null or invalid subpartition num", K(ret));
    } else {
      ObSubPartition *subpart = part->get_subpart_array()[0];
      if (OB_ISNULL(subpart)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpart is null", K(ret));
      } else {
        object_id = subpart->get_sub_part_id();
      }
    }
  }
  return ret;
}

bool ObChangeTabletToTableArg::is_valid() const { return OB_INVALID_TENANT_ID != tenant_id_ && ls_id_.is_valid() && OB_INVALID_ID != base_table_id_
                                                  && OB_INVALID_ID != inc_table_id_ &&  tablet_ids_.count() > 0 && table_ids_.count() > 0 && tablet_ids_.count() == table_ids_.count(); }

int ObChangeTabletToTableArg::assign(const ObChangeTabletToTableArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tablet_ids_.assign(other.tablet_ids_))) {
    LOG_WARN("fail to assign", K(ret), K(other.tablet_ids_));
  } else if (OB_FAIL(table_ids_.assign(other.table_ids_))) {
    LOG_WARN("fail to assign", K(ret), K(other.table_ids_));
  } else {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    base_table_id_ = other.base_table_id_;
    inc_table_id_ = other.inc_table_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObChangeTabletToTableArg, tenant_id_, ls_id_, base_table_id_, inc_table_id_, table_ids_, tablet_ids_);

}//end namespace rootserver
}//end namespace oceanbase
