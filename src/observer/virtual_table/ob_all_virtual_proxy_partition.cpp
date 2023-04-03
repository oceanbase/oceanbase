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

#include "ob_all_virtual_proxy_partition.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_get_compat_mode.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace observer
{

ObAllVirtualProxyPartition::ObAllVirtualProxyPartition()
    : ObAllVirtualProxyBaseIterator(),
      is_inited_(false),
      iter_(),
      part_func_type_(PARTITION_FUNC_TYPE_MAX)
{
}

ObAllVirtualProxyPartition::~ObAllVirtualProxyPartition()
{
}

int ObAllVirtualProxyPartition::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllVirtualProxyPartition doesn't init",
        KR(ret), K_(allocator), K_(schema_service));
  } else if (key_ranges_.count() != 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "batch query");
    LOG_WARN("do not support batch query", KR(ret),
        "key_ranges_ count", key_ranges_.count());
  } else {
    ObRowkey start_key = key_ranges_.at(0).start_key_;
    ObRowkey end_key = key_ranges_.at(0).end_key_;
    const ObObj *start_key_obj_ptr = start_key.get_obj_ptr();;
    const ObObj *end_key_obj_ptr = end_key.get_obj_ptr();
    ObString tenant_name;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    uint64_t table_id = OB_INVALID_ID;
    void *tmp_ptr = NULL;
    const ObTenantSchema *tenant_schema = NULL;
    const ObSimpleTableSchemaV2 *table_schema = NULL;

    if (OB_UNLIKELY(start_key.get_obj_cnt() != ROW_KEY_COUNT
        || end_key.get_obj_cnt() != ROW_KEY_COUNT)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "row key count not match");
      LOG_WARN("row key count not match", KR(ret), K(ROW_KEY_COUNT),
          "start key count", start_key.get_obj_cnt(), "end key count", end_key.get_obj_cnt());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && (i < ROW_KEY_COUNT); ++i) {
        if (TENANT_NAME_IDX == i
            && (!start_key_obj_ptr[i].is_varchar_or_char()
            || !end_key_obj_ptr[i].is_varchar_or_char()
            || start_key_obj_ptr[i] != end_key_obj_ptr[i])) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant_name and table_id (must all be specified)");
          LOG_WARN("invalid tenant_name", KR(ret),
              "start key obj", start_key_obj_ptr[i], "end key obj", end_key_obj_ptr[i]);
        } else if (TABLE_ID_IDX == i
            && (!start_key_obj_ptr[i].is_int()
            || !end_key_obj_ptr[i].is_int()
            || start_key_obj_ptr[i] != end_key_obj_ptr[i])) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant_name and table_id (must all be specified)");
          LOG_WARN("invalid table_id", KR(ret),
              "start key obj", start_key_obj_ptr[i], "end key obj", end_key_obj_ptr[i]);
        } else {
          switch (i) {
          case TENANT_NAME_IDX: {
              tenant_name = start_key_obj_ptr[i].get_string();
              break;
            }
          case TABLE_ID_IDX: {
              table_id = static_cast<uint64_t>(start_key_obj_ptr[i].get_int());
              break;
            }
          case PART_ID_IDX: {// unused, return all part
              break;
            }
          default: {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid key", K(i), KR(ret));
              break;
            }
          } // end switch
        }
      } // end for
    }

    if (FAILEDx(schema_service_->get_tenant_schema_guard(
        OB_SYS_TENANT_ID,
        tenant_schema_guard_))) {
      LOG_WARN("fail to get schema guard of sys tenant", KR(ret));
    } else if (OB_FAIL(tenant_schema_guard_.get_tenant_info(tenant_name, tenant_schema))) {
      LOG_WARN("fail to get tenant info", KR(ret), K(tenant_name));
    } else if (OB_ISNULL(tenant_schema)) {
      LOG_TRACE("tenant not exist", K(tenant_name)); // skip
    } else {
      tenant_id = tenant_schema->get_tenant_id();
      if (OB_UNLIKELY(!is_valid_tenant_id(effective_tenant_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid effective_tenant_id", KR(ret), K_(effective_tenant_id));
      } else if (!is_sys_tenant(effective_tenant_id_) && (tenant_id != effective_tenant_id_)) {
        LOG_TRACE("unprivileged tenant", K(tenant_id), K_(effective_tenant_id)); // skip
      } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, tenant_schema_guard_))) { // switch guard
        LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
      } else if (OB_FAIL(tenant_schema_guard_.get_simple_table_schema(
                         tenant_id, table_id, table_schema))) {
        LOG_WARN("get table schema failed", KR(ret), K(tenant_id), K(table_id));
      } else if (OB_ISNULL(table_schema)) {
        if (OB_FAIL(check_schema_version(tenant_schema_guard_, tenant_id))) {
          LOG_WARN("fail to check schema version", KR(ret), K(tenant_id));
        } else {
          LOG_TRACE("table not exist", K(tenant_id), K(table_id)); // skip
        }
      } else {
        iter_.init(*table_schema, CHECK_PARTITION_MODE_NORMAL);
        input_tenant_name_ = tenant_name;
        part_func_type_ = table_schema->get_part_option().get_part_func_type();
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObAllVirtualProxyPartition::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  const ObPartition *partition = NULL;
  if (!is_inited_) {
    ret = OB_ITER_END; // maybe table or part not exists
  } else if (OB_FAIL(iter_.next(partition))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next partition", KR(ret));
    }
  } else if (OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition is null", KR(ret));
  } else if (OB_FAIL(fill_row_(*partition))) {
    LOG_WARN("fail to fill cells", KR(ret), KPC(partition));
  }
  return ret;
}

int ObAllVirtualProxyPartition::fill_row_(const ObPartition &partition)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  ObCollationType coll_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
  const int64_t table_id = partition.get_table_id();
  const uint64_t tenant_id = partition.get_tenant_id();
  bool is_oracle_mode = false;
  if (OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur row cell is NULL", KR(ret));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(
      tenant_id,
      table_id,
      is_oracle_mode))) {
    LOG_WARN("fail to get oracle mode", KR(ret), K(partition));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch(col_id) {
    case TENANT_NAME: {
        cells[i].set_varchar(input_tenant_name_);
        cells[i].set_collation_type(coll_type);
        break;
      }
    case TABLE_ID: {
        cells[i].set_int(partition.get_table_id());
        break;
      }
    case PART_ID: {
        cells[i].set_int(partition.get_part_id());
        break;
      }
    case PART_NAME: {
        cells[i].set_varchar(partition.get_part_name());
        cells[i].set_collation_type(coll_type);
        break;
      }
    case STATUS: {
        cells[i].set_int(partition.get_status());
        break;
      }
    case LOW_BOUND_VAL: {
        cells[i].set_varchar(""); // we do not store low bound in this version
        cells[i].set_collation_type(coll_type);
        break;
      }
    case LOW_BOUND_VAL_BIN: {
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      }
    case HIGH_BOUND_VAL: {
        if (OB_FAIL(get_partition_value_str(is_oracle_mode, part_func_type_, partition, cells[i]))) {
          LOG_WARN("fail to get str", K(partition), KR(ret));
        } else {
          cells[i].set_collation_type(coll_type);
        }
        break;
      }
    case HIGH_BOUND_VAL_BIN: {
        if (OB_FAIL(get_partition_value_bin_str(part_func_type_, partition, cells[i]))) {
          LOG_WARN("fail to get str", K(partition), KR(ret));
        } else {
          cells[i].set_collation_type(coll_type);
        }
        break;
      }
    case PART_POSITION: {
        if (is_hash_like_part(part_func_type_)) {
          cells[i].set_int(partition.get_part_idx());
        } else {
          cells[i].set_int(0);
        }
        break;
      }
    case TABLET_ID: {
        cells[i].set_int(partition.get_tablet_id().id());
        break;
      }
    case SUB_PART_NUM: {
        cells[i].set_int(partition.get_sub_part_num());
        break;
      }
    case SUB_PART_SPACE: {
        cells[i].set_int(0); // not used
        break;
      }
    case SUB_PART_INTERVAL: {
        if (OB_FAIL(get_obj_str(partition.get_sub_part_interval(), cells[i]))) {
          LOG_WARN("fail to get str", K(partition.get_sub_part_interval()), KR(ret));
        } else {
          cells[i].set_collation_type(coll_type);
        }
        break;
      }
    case SUB_PART_INTERVAL_BIN: {
        if (OB_FAIL(get_obj_bin_str(partition.get_sub_part_interval(), cells[i]))) {
          LOG_WARN("fail to get bin str", K(partition.get_sub_part_interval()), KR(ret));
        } else {
          cells[i].set_collation_type(coll_type);
        }
        break;
      }
    case SUB_INTERVAL_START: {
        if (OB_FAIL(get_obj_str(partition.get_sub_interval_start(), cells[i]))) {
          LOG_WARN("fail to get str", K(partition.get_sub_part_interval()), KR(ret));
        } else {
          cells[i].set_collation_type(coll_type);
        }
        break;
      }
    case SUB_INTERVAL_START_BIN: {
        if (OB_FAIL(get_obj_bin_str(partition.get_sub_interval_start(), cells[i]))) {
          LOG_WARN("fail to get bin str", K(partition.get_sub_part_interval()), KR(ret));
        } else {
          cells[i].set_collation_type(coll_type);
        }
        break;
      }
    case SPARE1: {
        cells[i].set_int(0);
        break;
      }
    case SPARE2: {
        cells[i].set_int(0);
        break;
      }
    case SPARE3: {
        cells[i].set_int(0);
        break;
      }
    case SPARE4: {
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      }
    case SPARE5: {
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      }
    case SPARE6: {
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      }
    default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column id", K(i), KR(ret));
        break;
      }
    }
  }
  return ret;
}


} // end of namespace observer
} // end of namespace oceanbase
