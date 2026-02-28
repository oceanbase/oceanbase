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
#include "rootserver/parallel_ddl/ob_create_materialized_view_helper.h"
#include "rootserver/parallel_ddl/ob_create_view_helper.h"
#include "rootserver/ddl_task/ob_ddl_scheduler.h"
#include "storage/compaction/ob_compaction_schedule_util.h"
#include "storage/mview/ob_mview_sched_job_utils.h"
#include "rootserver/ob_balance_group_ls_stat_operator.h"
#include "rootserver/ob_table_creator.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "rootserver/ddl_task/ob_sys_ddl_util.h" // for ObSysDDLSchedulerUtil#include "rootserver/ddl_task/ob_ddl_scheduler.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;

int ObCreateMaterializedViewHelper::append_generate_schema_for_mv_()
{
    int ret = OB_SUCCESS;
    if (OB_FAIL(check_inner_stat_())) {
      LOG_WARN("fail to check inner stat", KR(ret));
    } else if (OB_ISNULL(new_view_schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new view schema is null", KR(ret));
    } else if (OB_UNLIKELY(!new_view_schema_->is_materialized_view())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("not mv schema", KR(ret), KPC(new_view_schema_));
    } else if (OB_FAIL(ObResolverUtils::check_schema_valid_for_mview(*new_view_schema_))) {
      LOG_WARN("failed to check schema valid for mview", KR(ret), KPC(new_view_schema_));
    } else if (arg_.mv_ainfo_.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("there should be mv ainfo", KR(ret), K(arg_.mv_ainfo_.count()));
    } else {
      new_view_schema_->get_view_schema().set_mv_additional_info(&(arg_.mv_ainfo_.at(0)));
    }
    return ret;
}

int ObCreateMaterializedViewHelper::generate_container_table_schema_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(new_view_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new view schema is null", KR(ret));
  } else {
    SMART_VAR(ObTableSchema, container_table_schema) {
    if (arg_.mv_ainfo_.count() >= 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("container table should be less than two", KR(ret), K(arg_.mv_ainfo_.count()));
    } else if (!new_view_schema_->is_materialized_view()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("only mv table is expected", KR(ret), KPC(new_view_schema_));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < arg_.mv_ainfo_.count(); i ++) {
      container_table_schema.reset();
      char buf[OB_MAX_TABLE_NAME_LENGTH];
      memset(buf, 0, OB_MAX_TABLE_NAME_LENGTH);

      if (OB_SUCC(ret)) {
        if (OB_FAIL(container_table_schema.assign(arg_.mv_ainfo_.at(i).container_table_schema_))) {
          LOG_WARN("fail to assign index schema", KR(ret));
        } else if (OB_FAIL(databuff_printf(buf, OB_MAX_TABLE_NAME_LENGTH, "__mv_container_%ld", new_view_schema_->get_table_id()))) {
          LOG_WARN("fail to print table name", KR(ret));
        } else if (OB_FAIL(container_table_schema.set_table_name(buf))) {
          LOG_WARN("fail to set table_name", KR(ret));
        } else {
          container_table_schema.set_database_id(new_view_schema_->get_database_id());
        }
      }

      if (OB_SUCC(ret)) {
        ObArray<ObSchemaType> conflict_schema_types;
        if (!arg_.is_alter_view_
            && OB_FAIL(schema_guard_wrapper_.check_oracle_object_exist(
                       container_table_schema.get_database_id(),
                       arg_.schema_.get_session_id(),
                       container_table_schema.get_table_name_str(), TABLE_SCHEMA,
                       INVALID_ROUTINE_TYPE, arg_.if_not_exist_))) {
          LOG_WARN("fail to check oracle_object exist", KR(ret), K(container_table_schema));
        }
      }
      if (OB_SUCC(ret)) { // check same table_name
        uint64_t synonym_id = OB_INVALID_ID;
        uint64_t table_id = OB_INVALID_ID;
        ObTableType table_type = MAX_TABLE_TYPE;
        int64_t schema_version = OB_INVALID_VERSION;
        ObSchemaGetterGuard::CheckTableType check_type = ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES;

        if (FAILEDx(schema_guard_wrapper_.get_synonym_id(container_table_schema.get_database_id(),
                                                         container_table_schema.get_table_name_str(),
                                                         synonym_id))) {
          LOG_WARN("fail to get synonymn_id", KR(ret), K_(tenant_id), K(container_table_schema.get_database_id()),
                                              K(container_table_schema.get_table_name_str()));
        } else if (OB_UNLIKELY(OB_INVALID_ID != synonym_id)) {
          ret = OB_ERR_EXIST_OBJECT;
          LOG_WARN("Name is already used by an existing object",
                   KR(ret), K_(tenant_id), K(container_table_schema.get_database_id()),
                   K(container_table_schema.get_table_name_str()), K(synonym_id));
        } else if (OB_FAIL(schema_guard_wrapper_.get_table_id(container_table_schema.get_database_id(),
                                                              container_table_schema.get_session_id(),
                                                              container_table_schema.get_table_name_str(),
                                                              table_id,
                                                              table_type,
                                                              schema_version))) {
          LOG_WARN("check table exist failed", KR(ret), K(container_table_schema));
        } else if (OB_UNLIKELY(OB_INVALID_ID != table_id)) {
          ret = OB_ERR_TABLE_EXIST;
          LOG_WARN("table exist", KR(ret), K(container_table_schema));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(inner_generate_table_schema_(arg_, container_table_schema))) {
          LOG_WARN("fail to generate container table schema", KR(ret));
        } else {
          //table_schema.get_view_schema().set_container_table_id(container_table_schema.get_table_id());
          new_view_schema_->set_data_table_id(container_table_schema.get_table_id());
        }
        if (FAILEDx(new_tables_.push_back(container_table_schema))) {
          LOG_WARN("fail to push back table", KR(ret));
        }
      }
    }
    }
  }
  return ret;
}

int ObCreateMaterializedViewHelper::create_schemas_for_mv_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(new_tables_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table cnt", KR(ret), "table_cnt", new_tables_.count());
  } else if (OB_ISNULL(new_view_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new view schema is null", KR(ret));
  } else {
    ObMViewInfo mview_info;
    ObSchemaGetterGuard schema_guard;
    uint64_t tenant_data_version = OB_INVALID_VERSION;
    ObTableSchema &container_table_schema = new_tables_.at(0);
    const obrpc::ObMVAdditionalInfo *mv_additional_info = new_view_schema_->get_view_schema().get_mv_additional_info();
    if (OB_ISNULL(mv_additional_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mv additional info is NULL", KR(ret));
    } else if (OB_FAIL(ObMViewSchedJobUtils::add_mview_info_and_refresh_job(get_trans_(),
                                                                     tenant_id_,
                                                                     new_view_schema_->get_table_id(),
                                                                     database_schema_->get_database_name_str(),
                                                                     new_view_schema_->get_table_name_str(),
                                                                     &(mv_additional_info->mv_refresh_info_),
                                                                     new_view_schema_->get_schema_version(),
                                                                     mview_info))) {
      LOG_WARN("fail to start mview refresh job", KR(ret));
    } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
      LOG_WARN("fail to get min data version", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(ddl_service_->start_mview_complete_refresh_task(get_trans_(),
                                                                       schema_guard,
                                                                       *new_view_schema_,
                                                                       container_table_schema,
                                                                       &arg_.dep_infos_,
                                                                       allocator_,
                                                                       tenant_data_version,
                                                                       mview_info,
                                                                       ddl_stmt_str_,
                                                                       task_record_))) {
      LOG_WARN("failed to start mview complete refresh task", KR(ret));
    }
  }
  return ret;
}

int ObCreateMaterializedViewHelper::generate_aux_table_schemas_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(new_tables_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table cnt", KR(ret), "table_cnt", new_tables_.count());
  } else {
    if (OB_FAIL(inner_generate_aux_table_schema_(arg_))) {
      LOG_WARN("fail to generate aus table schema", KR(ret));
    }
  }
  return ret;
}

int ObCreateMaterializedViewHelper::generate_schemas_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ObCreateViewHelper::generate_schemas_())) {
    LOG_WARN("fail to generate view schema", KR(ret));
  } else if (OB_ISNULL(new_view_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new view schema is null", KR(ret));
  } else if (!new_view_schema_->is_materialized_view()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only mv table is expected", KR(ret), KPC(new_view_schema_));
  } else if (OB_FAIL(append_generate_schema_for_mv_())) {
    LOG_WARN("fail to generate mv schema", KR(ret));
  } else if (OB_FAIL(ObTableHelper::generate_schemas_())) {
    LOG_WARN("fail to generate container table schema", KR(ret));
  }
  return ret;
}

int ObCreateMaterializedViewHelper::operate_schemas_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ObCreateViewHelper::create_schemas_())) {
    LOG_WARN("fail to create view schemas", KR(ret));
  } else if (OB_UNLIKELY(new_tables_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table cnt", KR(ret), "table_cnt", new_tables_.count());
  // create container table's schema
  } else if (OB_FAIL(create_tables_(nullptr /*ddl_stmt*/))) {
    LOG_WARN("fail to create tables", KR(ret));
  // create tablets for container table
  } else if (OB_FAIL(create_tablets_())) {
    LOG_WARN("fail to create tablet for mv", KR(ret));
  } else if (OB_FAIL(create_schemas_for_mv_())) {
    LOG_WARN("fail to create schema for mv", KR(ret));
  }
  return ret;
}


int ObCreateMaterializedViewHelper::calc_schema_version_cnt_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ObCreateViewHelper::calc_schema_version_cnt_())) {
    LOG_WARN("fail to calc schema version cnt", KR(ret));
  } else {
    schema_version_cnt_ += new_tables_.count();
    if (new_tables_.count() >= 2) {
      // lob table update data table version
      schema_version_cnt_ ++;
    }
  }
  return ret;
}

int ObCreateMaterializedViewHelper::construct_and_adjust_result_(int &return_ret)
{
  int ret = return_ret;
  if (FAILEDx(ObCreateViewHelper::construct_and_adjust_result_(ret))) {
    LOG_WARN("fail to construnct and adjust result", KR(ret));
  } else if (task_record_.is_valid() && OB_FAIL(ObSysDDLSchedulerUtil::schedule_ddl_task(task_record_))) {
    LOG_WARN("fail to schedule ddl task", KR(ret), K_(task_record));
  } else {
    res_.task_id_ = task_record_.task_id_;
  }
  return ret;
}
