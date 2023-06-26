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

#include "ob_core_meta_table.h"
#include "share/ls/ob_ls_table_operator.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_column_schema.h"
#include "lib/string/ob_string.h"  // for ObString

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;

namespace rootserver
{
ObCoreMetaTable::ObCoreMetaTable()
  : inited_(false),
    lst_operator_(NULL),
    schema_guard_(NULL)
{
}

ObCoreMetaTable::~ObCoreMetaTable()
{
}

int ObCoreMetaTable::init(ObLSTableOperator &lst_operator,
                          ObSchemaGetterGuard *schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (NULL == schema_guard) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_guard is null", KR(ret));
  } else {
    lst_operator_ = &lst_operator;
    schema_guard_ = schema_guard;
    inited_ = true;
  }
  return ret;
}

int ObCoreMetaTable::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init, allocator is null", KR(ret));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!start_to_read_) {
    const ObTableSchema *table_schema = NULL;
    const uint64_t table_id = OB_ALL_VIRTUAL_CORE_META_TABLE_TID;
    ObLSInfo ls_info;
    if (OB_FAIL(schema_guard_->get_table_schema(OB_SYS_TENANT_ID, table_id, table_schema))
        || NULL == table_schema) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("get_table_schema failed", K(table_id), KR(ret));
    } else if (OB_FAIL(lst_operator_->get(GCONF.cluster_id, OB_SYS_TENANT_ID,
               SYS_LS, share::ObLSTable::DEFAULT_MODE, ls_info))) {
      LOG_WARN("lst_operator get failed", KR(ret), K(ls_info));
    } else {
      ObArray<Column> columns;
      FOREACH_CNT_X(replica, ls_info.get_replicas(), OB_SUCCESS == ret) {
        columns.reuse();
        if (OB_FAIL(get_full_row(table_schema, *replica, columns))) {
          LOG_WARN("get_full_row failed", "table_schema", *table_schema,
              "replica", *replica, KR(ret));
        } else if (OB_FAIL(project_row(columns, cur_row_))) {
          LOG_WARN("project_row failed", K(columns), KR(ret));
        } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
          LOG_WARN("add_row failed", K(cur_row_), KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get_next_row failed", KR(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObCoreMetaTable::get_full_row(const ObTableSchema *table,
                                  const ObLSReplica &replica,
                                  ObIArray<Column> &columns)
{
  int ret = OB_SUCCESS;
  char *ip = NULL;
  char *zone = NULL;
  ObString member_list;
  ObString learner_list;
  ObSqlString member_list_str;
  ObSqlString learner_list_str;
  const char* replica_status = ob_replica_status_str(replica.get_replica_status());
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (NULL == table) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is null", KR(ret));
  } else if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", K(replica), KR(ret));
  } else {
    if (NULL == (ip = static_cast<char *>(allocator_->alloc(OB_MAX_SERVER_ADDR_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc ip buf failed", "size", OB_MAX_SERVER_ADDR_SIZE, KR(ret));
    } else if (NULL == (zone = static_cast<char *>(allocator_->alloc(MAX_ZONE_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc zone buf failed", "size", MAX_ZONE_LENGTH, KR(ret));
    } else if (false == replica.get_server().ip_to_string(ip, OB_MAX_SERVER_ADDR_SIZE)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("convert server ip to string failed", KR(ret), "server", replica.get_server());
    } else if (OB_FAIL(databuff_printf(zone, MAX_ZONE_LENGTH, "%s", replica.get_zone().ptr()))) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf failed", "buf_len", MAX_ZONE_LENGTH, "src_len", strlen(replica.get_zone().ptr()), KR(ret));
    } else if (OB_FAIL(ObLSReplica::member_list2text(replica.get_member_list(), member_list_str))) {
      LOG_WARN("member_list2text failed", K(replica), KR(ret));
    } else if (OB_FAIL(replica.get_learner_list().transform_to_string(learner_list_str))) {
      LOG_WARN("failed to transform GlobalLearnerList to ObString", KR(ret), K(replica));
    } else if (OB_FAIL(ob_write_string(*allocator_, member_list_str.string(), member_list))) {
      LOG_WARN("failed to construct member list", KR(ret), K(member_list_str));
    } else if (OB_FAIL(ob_write_string(*allocator_, learner_list_str.string(), learner_list))) {
      LOG_WARN("failed to construct learner list", KR(ret), K(learner_list_str));
    } else {
      ADD_COLUMN(set_int, table, "tenant_id", static_cast<int64_t>(OB_SYS_TENANT_ID), columns);
      ADD_COLUMN(set_int, table, "ls_id", ObLSID::SYS_LS_ID, columns);
      ADD_COLUMN(set_varchar, table, "svr_ip", ip, columns);
      ADD_COLUMN(set_int, table, "svr_port", replica.get_server().get_port(), columns);
      ADD_COLUMN(set_int, table, "sql_port", replica.get_sql_port(), columns);
      ADD_COLUMN(set_int, table, "role", replica.get_role(), columns);
      ADD_COLUMN(set_varchar, table, "member_list", member_list, columns);
      ADD_COLUMN(set_int, table, "proposal_id", replica.get_proposal_id(), columns);
      ADD_COLUMN(set_int, table, "replica_type", replica.get_replica_type(), columns);
      ADD_COLUMN(set_varchar, table, "replica_status", replica_status, columns);
      ADD_COLUMN(set_int, table, "restore_status", replica.get_restore_status(), columns);
      ADD_COLUMN(set_int, table, "memstore_percent", replica.get_memstore_percent(), columns);
      ADD_COLUMN(set_int, table, "unit_id", static_cast<int64_t>(replica.get_unit_id()), columns);
      ADD_COLUMN(set_varchar, table, "zone", zone, columns);
      ADD_COLUMN(set_int, table, "paxos_replica_number", replica.get_paxos_replica_number(), columns);
      ADD_COLUMN(set_int, table, "data_size", replica.get_data_size(), columns);
      ADD_COLUMN(set_int, table, "required_size", replica.get_required_size(), columns);
      ADD_TEXT_COLUMN(ObLongTextType, table, "learner_list", learner_list, columns);
      ADD_COLUMN(set_int, table, "rebuild", replica.get_rebuild(), columns);
    }

    if (OB_FAIL(ret)) {
      if (NULL != ip) {
        allocator_->free(ip);
        ip = NULL;
      }
      if (NULL != zone) {
        allocator_->free(zone);
        zone = NULL;
      }
    }
  }
  return ret;
}

}//end namespace rootserver
}//end namespace oceanbase
