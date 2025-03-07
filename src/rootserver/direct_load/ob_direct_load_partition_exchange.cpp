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

#include "rootserver/direct_load/ob_direct_load_partition_exchange.h"
#include "rootserver/ob_ddl_service.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
namespace rootserver
{
ObDirectLoadPartitionExchange::ObDirectLoadPartitionExchange(
    ObDDLService &ddl_service, const uint64_t data_version)
  : ObPartitionExchange(ddl_service, data_version, false/*exchange_part_id*/)
{
}

ObDirectLoadPartitionExchange::~ObDirectLoadPartitionExchange()
{
}

int ObDirectLoadPartitionExchange::exchange_multipart_table_partitions(
    const uint64_t tenant_id,
    ObDDLSQLTransaction &trans,
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema &base_table_schema,
    const ObTableSchema &inc_table_schema,
    const ObIArray<ObTabletID> &base_table_tablet_ids,
    const ObIArray<ObTabletID> &inc_table_tablet_ids)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  int64_t schema_version = OB_INVALID_VERSION;
  const ObPartitionLevel exchange_partition_level = base_table_schema.get_part_level();
  bool is_subpartition = (PARTITION_LEVEL_TWO == exchange_partition_level);
  ObDDLOperator ddl_operator(ddl_service_.get_schema_service(), ddl_service_.get_sql_proxy());

  if (data_version_ < DATA_VERSION_4_3_3_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3.3.0 does not support insert overwrite partition", KR(ret));
    FORWARD_USER_ERROR_MSG(ret, "version lower than 4.3.2.0 does not support insert overwrite partition");
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, schema_version))) {
    LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id), K(schema_version));
  } else if (OB_FAIL(base_table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("check_if_oracle_compat_mode failed", KR(ret), K(is_oracle_mode));
  } else if (OB_FAIL(check_multipart_exchange_conditions(schema_guard,
                                                         base_table_schema,
                                                         inc_table_schema,
                                                         base_table_tablet_ids,
                                                         inc_table_tablet_ids,
                                                         is_oracle_mode))) {
    LOG_WARN("failed to check multipart exchange conditions", KR(ret), K(base_table_schema),
        K(inc_table_schema), K(base_table_tablet_ids), K(inc_table_tablet_ids), K(is_oracle_mode));
  } else if (OB_FAIL(inner_init(base_table_schema,
                                inc_table_schema,
                                exchange_partition_level,
                                is_oracle_mode,
                                schema_guard))) {
    LOG_WARN("failed to inner init", KR(ret), K(base_table_schema), K(inc_table_schema),
        K(exchange_partition_level), K(is_oracle_mode));
  } else if (OB_FAIL(exchange_data_table_partitions(tenant_id,
                                                    base_table_schema,
                                                    inc_table_schema,
                                                    base_table_tablet_ids,
                                                    inc_table_tablet_ids,
                                                    is_oracle_mode,
                                                    is_subpartition,
                                                    ddl_operator,
                                                    trans,
                                                    schema_guard))) {
    LOG_WARN("failed to exchange data table partitions",
        KR(ret), K(tenant_id), K(base_table_schema), K(inc_table_schema),
        K(base_table_tablet_ids), K(inc_table_tablet_ids), K(is_oracle_mode), K(is_subpartition));
  } else if (OB_FAIL(exchange_auxiliary_table_partitions(tenant_id,
                                                         base_table_schema,
                                                         inc_table_schema,
                                                         base_table_tablet_ids,
                                                         inc_table_tablet_ids,
                                                         is_oracle_mode,
                                                         is_subpartition,
                                                         ddl_operator,
                                                         trans,
                                                         schema_guard))) {
    LOG_WARN("failed to exchange auxiliary table partitions",
        KR(ret), K(tenant_id), K(base_table_schema), K(inc_table_schema),
        K(base_table_tablet_ids), K(inc_table_tablet_ids), K(is_oracle_mode), K(is_subpartition));
  } else {
    int64_t new_inc_schema_version = OB_INVALID_VERSION;
    int64_t new_base_schema_version = OB_INVALID_VERSION;
    if (OB_FAIL(push_data_table_schema_version_(tenant_id, inc_table_schema,
        nullptr/*ddl_stmt_str*/, base_table_schema.get_table_id(), new_inc_schema_version, trans))) {
      LOG_WARN("failed to push data table schema version",
          KR(ret), K(tenant_id), K(inc_table_schema), K(base_table_schema.get_table_id()));
    } else if (OB_FAIL(push_data_table_schema_version_(tenant_id, base_table_schema,
        nullptr/*ddl_stmt_str*/, inc_table_schema.get_table_id(), new_base_schema_version, trans))) {
      LOG_WARN("failed to push data table schema version",
          KR(ret), K(tenant_id), K(base_table_schema), K(inc_table_schema.get_table_id()));
    } else if (OB_FAIL(adapting_cdc_changes_in_exchange_partition_(tenant_id,
        base_table_schema.get_table_id(), inc_table_schema.get_table_id(), trans))) {
      LOG_WARN("failed to adapting cdc changes in exchange_partition",
          KR(ret), K(tenant_id), K(base_table_schema.get_table_id()), K(inc_table_schema.get_table_id()));
    } else {
      LOG_INFO("succeed to exchange direct load table partitions",
          K(base_table_schema.get_table_name_str()), K(base_table_schema.get_table_id()),
          K(inc_table_schema.get_table_name_str()), K(inc_table_schema.get_table_id()));
    }
  }

  return ret;
}

int ObDirectLoadPartitionExchange::check_multipart_exchange_conditions(
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema &base_table_schema,
    const ObTableSchema &inc_table_schema,
    const ObIArray<ObTabletID> &base_tablet_ids,
    const ObIArray<ObTabletID> &inc_tablet_ids,
    const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  const ObPartitionLevel exchange_part_level = base_table_schema.get_part_level();
  const ObString &part_name = base_table_schema.get_table_name_str();
  if (OB_UNLIKELY(!base_table_schema.is_partitioned_table() || !inc_table_schema.is_partitioned_table())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("both base_table_schema and inc_table_schema should be partitioned tables",
        KR(ret), K(base_table_schema.is_partitioned_table()), K(inc_table_schema.is_partitioned_table()));
  } else if (OB_UNLIKELY(base_table_schema.get_part_level() != inc_table_schema.get_part_level())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the partition level of base_table_schema and inc_table_schema should be the same",
        KR(ret), K(base_table_schema.get_part_level()), K(inc_table_schema.get_part_level()));
  } else if (OB_UNLIKELY(base_tablet_ids.count() != inc_tablet_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the count of base_tablet_ids and inc_tablet_ids should be equal",
        KR(ret), K(base_tablet_ids.count()), K(inc_tablet_ids.count()));
  } else if (OB_FAIL(check_data_table_partition_exchange_conditions_(
      base_table_schema, inc_table_schema, base_tablet_ids, inc_tablet_ids, exchange_part_level, is_oracle_mode))) {
    LOG_WARN("failed to check data table partition exchange conditions",
        KR(ret), K(base_table_schema), K(inc_table_schema), K(part_name), K(exchange_part_level), K(is_oracle_mode));
  } else if (data_version_ < DATA_VERSION_4_3_5_1) {
    ObPartitionFuncType part_type = PARTITION_FUNC_TYPE_MAX;
    if (ObPartitionLevel::PARTITION_LEVEL_ONE == exchange_part_level) {
      part_type = base_table_schema.get_part_option().get_part_func_type();
    } else if (ObPartitionLevel::PARTITION_LEVEL_TWO == exchange_part_level) {
      part_type = base_table_schema.get_sub_part_option().get_part_func_type();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected partition level", KR(ret), K(exchange_part_level));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!((PARTITION_FUNC_TYPE_RANGE == part_type)
                          || (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type)
                          || (PARTITION_FUNC_TYPE_LIST == part_type)
                          || (PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type)))) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("insert overwrite partition does not support hash/key partitions", KR(ret), K(part_type));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "insert overwrite partition to table with hash/key partition type is");
    }
  }
  return ret;
}

int ObDirectLoadPartitionExchange::check_table_conditions_in_common_(
    const ObTableSchema &base_table_schema,
    const ObTableSchema &inc_table_schema,
    const ObPartitionLevel exchange_partition_level,
    const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  HEAP_VARS_2((ObTableSchema, new_base_table_schema),
              (ObTableSchema, new_inc_table_schema)) {
    if (OB_FAIL(new_base_table_schema.assign(base_table_schema))) {
      LOG_WARN("failed to assign base table schema", KR(ret), K(base_table_schema));
    } else if (OB_FALSE_IT(new_base_table_schema.set_in_offline_ddl_white_list(true))) {
    } else if (OB_FAIL(new_inc_table_schema.assign(inc_table_schema))) {
      LOG_WARN("failed to assign inc table schema", KR(ret), K(inc_table_schema));
    } else if (OB_FALSE_IT(new_inc_table_schema.set_in_offline_ddl_white_list(true))) {
    } else if (OB_FALSE_IT(new_inc_table_schema.set_table_mode(new_base_table_schema.get_table_mode()))) {
      // hidden table has different table mode
    } else if (OB_FAIL(ObPartitionExchange::check_table_conditions_in_common_(
        new_base_table_schema,
        new_inc_table_schema,
        exchange_partition_level,
        is_oracle_mode))) {
      LOG_WARN("failed to check table conditions in common", KR(ret),
          K(new_base_table_schema), K(new_inc_table_schema), K(exchange_partition_level), K(is_oracle_mode));
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase