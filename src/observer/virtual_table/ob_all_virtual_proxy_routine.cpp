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

#define USING_LOG_PREFIX SERVER

#include "observer/virtual_table/ob_all_virtual_proxy_routine.h"
#include "share/schema/ob_routine_info.h" // ObRoutineInfo
#include "sql/resolver/ob_resolver_utils.h" // ObResolverUtils
#include "sql/resolver/ob_schema_checker.h" // ObSchemaChecker

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;

namespace observer
{
ObAllVirtualProxyRoutine::ObProxyRoutineKey::ObProxyRoutineKey()
    : tenant_name_(),
      database_name_(),
      package_name_(),
      routine_name_()
{
}

int ObAllVirtualProxyRoutine::ObProxyRoutineKey::init(
    const common::ObString &tenant_name,
    const common::ObString &database_name,
    const common::ObString &package_name,
    const common::ObString &routine_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tenant_name.empty()
      || database_name.empty()
      || routine_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_name), K(database_name), K(routine_name));
  } else {
    tenant_name_ = tenant_name;
    database_name_ = database_name;
    package_name_ = package_name; // package_name can be empty
    routine_name_ = routine_name;
  }
  return ret;
}

void ObAllVirtualProxyRoutine::ObProxyRoutineKey::reset()
{
  tenant_name_.reset();
  database_name_.reset();
  package_name_.reset();
  routine_name_.reset();
}

ObAllVirtualProxyRoutine::ObAllVirtualProxyRoutine()
    : ObVirtualTableIterator(),
      is_inited_(false),
      inner_idx_(0),
      valid_routine_keys_(),
      routine_infos_(),
      tenant_schema_guard_(),
      schema_service_(NULL)
{
}

int ObAllVirtualProxyRoutine::init(
    share::schema::ObMultiVersionSchemaService &schema_service,
    common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", KR(ret));
  } else {
    schema_service_ = &schema_service;
    allocator_ = allocator;
    inner_idx_ = 0;
    valid_routine_keys_.reset();
    routine_infos_.reset();
    tenant_schema_guard_.reset();
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualProxyRoutine::inner_open()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP_(schema_service));
  } else {
    const int64_t ROW_KEY_COUNT = 4;
    ObRowkey start_key;
    ObRowkey end_key;
    ObString tenant_name;
    ObString database_name;
    ObString package_name;
    ObString routine_name;
    ObProxyRoutineKey routine_key;
    const ObObj *start_key_obj_ptr = NULL;
    const ObObj *end_key_obj_ptr = NULL;
    const ObRoutineInfo *routine_info = NULL;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    const ObTenantSchema *tenant_schema = NULL;
    sql::ObSchemaChecker schema_checker;
    ObSEArray<ObProxyRoutineKey, 1> input_routine_keys;
    ObSEArray<const share::schema::ObIRoutineInfo *, 8> routine_infos;

    for (int64_t i = 0; (OB_SUCC(ret)) && (i < key_ranges_.count()); ++i) {
      start_key = key_ranges_.at(i).start_key_;
      end_key = key_ranges_.at(i).end_key_;
      if (OB_UNLIKELY(ROW_KEY_COUNT != start_key.get_obj_cnt()
          || ROW_KEY_COUNT != end_key.get_obj_cnt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, "row key count not match");
        LOG_WARN("row key count not match", KR(ret), K(ROW_KEY_COUNT),
            "start key count", start_key.get_obj_cnt(), "end key count", end_key.get_obj_cnt());
      } else {
        start_key_obj_ptr = start_key.get_obj_ptr();
        end_key_obj_ptr = end_key.get_obj_ptr();

        for (int64_t j = 0; (OB_SUCC(ret)) && (j < 4); ++j) {
          if (OB_UNLIKELY(start_key_obj_ptr[j].is_min_value()
              || end_key_obj_ptr[j].is_max_value()
              || !start_key_obj_ptr[j].is_varchar_or_char()
              || !end_key_obj_ptr[j].is_varchar_or_char()
              || (start_key_obj_ptr[j] != end_key_obj_ptr[j]))) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                "tenant_name, database_name, package_name and routine_name (must all be specified)");
            LOG_WARN("invalid keys", KR(ret),
                "start key obj", start_key_obj_ptr[j], "end key obj", end_key_obj_ptr[j]);
          } else {
            switch (j) {
              case 0: {// tenant_name
                tenant_name = end_key_obj_ptr[j].get_string();
                break;
              }
              case 1: {// database_name
                database_name = end_key_obj_ptr[j].get_string();
                break;
              }
              case 2: {// package_name
                package_name = end_key_obj_ptr[j].get_string();
                break;
              }
              case 3: {// routine_name
                routine_name = end_key_obj_ptr[j].get_string();
                break;
              }
            }//end switch
          }
        }//end for j
        LOG_TRACE("finish to get keys", KR(ret),
            K(tenant_name), K(database_name), K(package_name), K(routine_name));

        // check all tenant_names in batch query and change schema guard by tenant
        if (OB_SUCC(ret)) {
          if (input_routine_keys.count() > 0
              && OB_UNLIKELY(0 != input_routine_keys.at(0).get_tenant_name().case_compare(tenant_name))) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "different tenant_names in a batch query");
            LOG_WARN("unexpected different tenant_names", KR(ret), K(input_routine_keys), K(tenant_name));
          } else if (OB_FAIL(routine_key.init(
              tenant_name,
              database_name,
              package_name,
              routine_name))) {
            LOG_WARN("fail to init routine_key", KR(ret), K(tenant_name),
                K(database_name), K(package_name), K(routine_name));
          } else if (OB_FAIL(input_routine_keys.push_back(routine_key))) {
            LOG_WARN("failed to push back input_routine_keys", KR(ret), K(routine_key));
          }
        }
      }
    } // end for key_ranges_

    if (OB_FAIL(ret) || input_routine_keys.empty()) {
      // skip
    } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(
        OB_SYS_TENANT_ID, 
        tenant_schema_guard_))) {
      LOG_WARN("fail to get sys tenant schema guard", KR(ret));
    } else if (OB_FAIL(tenant_schema_guard_.get_tenant_info(tenant_name, tenant_schema))) {
      LOG_WARN("fail to get tenant info", KR(ret), K(tenant_name));
    } else if (OB_ISNULL(tenant_schema)) {
      LOG_TRACE("tenant not exist", K(tenant_name)); // skip
    } else {
      tenant_id = tenant_schema->get_tenant_id();
      if (OB_UNLIKELY(!is_valid_tenant_id(effective_tenant_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid effective_tenant_id", KR(ret), K_(effective_tenant_id));
      } else if (!is_sys_tenant(effective_tenant_id_)) {
        LOG_TRACE("only sys tenant is allowed", K(tenant_id), K_(effective_tenant_id)); // skip
      } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, tenant_schema_guard_))) {
        LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_checker.init(tenant_schema_guard_))) {
        LOG_WARN("fail to init schema_checker", KR(ret));
      } else {
        ARRAY_FOREACH_N(input_routine_keys, idx, cnt) {
          const ObProxyRoutineKey &input_key = input_routine_keys.at(idx);
          routine_infos.reset();
          routine_info = NULL;
          if (OB_FAIL(sql::ObResolverUtils::get_candidate_routines(
              schema_checker,
              tenant_id,
              input_key.get_database_name(),
              input_key.get_database_name(),
              input_key.get_package_name(),
              input_key.get_routine_name(),
              ROUTINE_PROCEDURE_TYPE,
              routine_infos))) {
            LOG_WARN("failed to get routine", KR(ret), K(tenant_id), K(input_key));
            ret = OB_SUCCESS; // if database, package or routine not exist, return nothing
          } else if (routine_infos.count() <= 0
              || OB_ISNULL(routine_info = static_cast<const ObRoutineInfo*>(routine_infos.at(0)))) {
            LOG_TRACE("routine does not exist", K(input_key)); // skip
          } else if (OB_FAIL(valid_routine_keys_.push_back(input_key))) {
            LOG_WARN("failed to push back valid_routine_keys_", KR(ret), K(input_key));
          } else if (OB_FAIL(routine_infos_.push_back(routine_info))) {
            LOG_WARN("failed to push back routine_infos_", KR(ret), K(routine_info));
          } else {
            LOG_TRACE("succ to push back routine_info", K(input_key), KPC(routine_info));
          }
        } // end for input_table_ids_
      }
    }
  }
  return ret;
}

int ObAllVirtualProxyRoutine::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllVirtualProxyRoutine not init", KR(ret));
  } else if (OB_FAIL(inner_get_next_row_())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", KR(ret), K_(cur_row));
    }
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualProxyRoutine::inner_get_next_row_()
{
  int ret = OB_SUCCESS;
  const ObRoutineInfo *routine_info = NULL;
  if (OB_UNLIKELY(inner_idx_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner_idx_ can't be smaller than 0", KR(ret), K_(inner_idx));
  } else if (inner_idx_ >= routine_infos_.count()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(routine_info = routine_infos_.at(inner_idx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("routine info is NULL", KR(ret), K_(inner_idx));
  } else if (OB_UNLIKELY(routine_infos_.count() != valid_routine_keys_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("routine infos count not equel to input key count",
        KR(ret), K_(routine_infos), K_(valid_routine_keys));
  } else if (OB_FAIL(fill_row_(valid_routine_keys_.at(inner_idx_), *routine_info))) {
    LOG_WARN("fail to fill row", KR(ret), K_(inner_idx), K_(valid_routine_keys), KPC(routine_info));
  } else {
    LOG_TRACE("succ to fill row", KR(ret), K_(inner_idx), K_(valid_routine_keys), KPC(routine_info));
    ++inner_idx_;
  }
  return ret;
}

int ObAllVirtualProxyRoutine::fill_row_(
    const ObProxyRoutineKey &routine_key,
    const share::schema::ObRoutineInfo &routine_info)
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  const int64_t col_count = output_column_ids_.count();
  ObCollationType coll_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
  if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur row cell is NULL", KR(ret));
  } else {
    uint64_t cell_idx = 0;
    uint64_t col_id = OB_INVALID_ID;
    const int64_t routine_partition_id = 0;
    const ObString routine_svr_ip("");
    const int64_t routine_svr_port = 0;

    for (int64_t i = 0; (OB_SUCC(ret)) && (i < col_count); ++i) {
      col_id = output_column_ids_.at(i);
      switch (col_id) {
      case TENANT_NAME: {
          cells[cell_idx].set_varchar(routine_key.get_tenant_name());
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
      case DATABASE_NAME: {
          cells[cell_idx].set_varchar(routine_key.get_database_name());
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
      case PACKAGE_NAME_FOR_ROUTINE: {
          cells[cell_idx].set_varchar(routine_key.get_package_name());
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
      case ROUTINE_NAME: {
          cells[cell_idx].set_varchar(routine_info.get_routine_name());
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
      case ROUTINE_ID: {
          cells[cell_idx].set_int(routine_info.get_routine_id());
          break;
        }
      case ROUTINE_TYPE: {
          cells[cell_idx].set_int(routine_info.get_routine_type());
          break;
        }
      case SCHEMA_VERSION: {
          cells[cell_idx].set_int(static_cast<int64_t>(routine_info.get_schema_version()));
          break;
        }
      case ROUTINE_SQL: {
          cells[i].set_lob_value(
              ObLongTextType, routine_info.get_route_sql().ptr(),
              static_cast<int32_t>(routine_info.get_route_sql().length()));
          cells[i].set_collation_type(
              ObCharset::get_default_collation(
              ObCharset::get_default_charset()));
          break;
        }
      case SPARE1: { // int, unused
          cells[cell_idx].set_int(0);
          break;
        }
      case SPARE2: { // int, unused
          cells[cell_idx].set_int(0);
          break;
        }
      case SPARE3: { // int, unused
          cells[cell_idx].set_int(0);
          break;
        }
      case SPARE4: {// varchar, unused
          cells[cell_idx].set_varchar("");
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
      case SPARE5: {// varchar, unused
          cells[cell_idx].set_varchar("");
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
      case SPARE6: {// varchar, unused
          cells[cell_idx].set_varchar("");
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
      default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column id", KR(ret),
              K(cell_idx), K(output_column_ids_), K(col_id));
          break;
        }
      } // end switch
      if (OB_SUCC(ret)) {
        cell_idx++;
      }
    } // end for
  }
  return ret;
}

} // end of namespace observer
} // end of namespace oceanbase