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

#include "ob_all_virtual_proxy_partition.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase {
namespace observer {

ObAllVirtualProxyPartition::ObAllVirtualProxyPartition()
    : ObAllVirtualProxyBaseIterator(), iter_(NULL), part_func_type_(PARTITION_FUNC_TYPE_MAX)
{}

ObAllVirtualProxyPartition::~ObAllVirtualProxyPartition()
{
  if (NULL != allocator_ && NULL != iter_) {
    allocator_->free(iter_);
    iter_ = NULL;
  }
}

int ObAllVirtualProxyPartition::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member doesn't init", K_(allocator), K(ret));
  } else if (key_ranges_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "only one table_id can be specified");
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "schema_service is null", K(ret));
  } else if (OB_FAIL(schema_service_->get_schema_guard(full_schema_guard_))) {
    SERVER_LOG(WARN, "fail to get schema guard", K(ret));
  } else {
    ObRowkey start_key = key_ranges_.at(0).start_key_;
    ObRowkey end_key = key_ranges_.at(0).end_key_;
    const ObObj* start_key_obj_ptr = start_key.get_obj_ptr();
    ;
    const ObObj* end_key_obj_ptr = end_key.get_obj_ptr();
    uint64_t table_id = OB_INVALID_ID;
    int32_t part_id = OB_INVALID_INDEX;
    void* tmp_ptr = NULL;
    const ObTableSchema* table_schema = NULL;

    if (start_key.get_obj_cnt() < ROW_KEY_COUNT || end_key.get_obj_cnt() < ROW_KEY_COUNT ||
        OB_ISNULL(start_key_obj_ptr) || OB_ISNULL(end_key_obj_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "table_id must be specified");
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && (i < ROW_KEY_COUNT); ++i) {
        if (!start_key_obj_ptr[i].is_int() || !end_key_obj_ptr[i].is_int() ||
            start_key_obj_ptr[i] != end_key_obj_ptr[i]) {
          if (TABLE_ID_IDX == i) {
            ret = OB_ERR_UNEXPECTED;
            LOG_USER_ERROR(OB_ERR_UNEXPECTED, "table_id must be specified");
          } else {
            // part id do not spec, jump it
            continue;
          }
        }
        switch (i) {
          case TABLE_ID_IDX:
            table_id = static_cast<uint64_t>(start_key_obj_ptr[i].get_int());
            break;
          case PART_ID_IDX:
            part_id = static_cast<int32_t>(start_key_obj_ptr[i].get_int());
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid key", K(i), K(ret));
            break;
        }
      }
    }

    if (OB_SUCC(ret)) {
      UNUSED(part_id);  // do not use part_id yet, we will return all first part
      bool check_dropped_schema = false;
      if (OB_FAIL(get_table_schema(full_schema_guard_, table_id, table_schema))) {
        SERVER_LOG(WARN, "fail to get table schema", K(ret), K(table_id));
      } else if (OB_ISNULL(table_schema)) {
        // skip
      } else if (NULL == (tmp_ptr = allocator_->alloc(sizeof(ObPartIteratorV2)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SERVER_LOG(ERROR, "fail to alloc", K(ret));
      } else if (NULL == (iter_ = new (tmp_ptr) ObPartIteratorV2(*table_schema, check_dropped_schema))) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "placement new fail", K(ret));
      } else {
        part_func_type_ = table_schema->get_part_option().get_part_func_type();
      }
    }
  }
  return ret;
}

int ObAllVirtualProxyPartition::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  const ObPartition* partition = NULL;
  if (OB_ISNULL(iter_)) {
    ret = OB_ITER_END;  // maybe table or part not exists
  } else if (OB_FAIL(iter_->next(partition))) {
    // usually, OB_ITER_END
  } else if (OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "partition is null", K(ret));
  } else {
    ret = fill_cells(*partition);
  }
  return ret;
}

int ObAllVirtualProxyPartition::fill_cells(const ObPartition& partition)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj* cells = cur_row_.cells_;
  ObCollationType coll_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
  if (OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case TABLE_ID:
        cells[i].set_int(partition.get_table_id());
        break;
      case PART_ID:
        cells[i].set_int(partition.get_part_id());
        break;

      case TENANT_ID:
        cells[i].set_int(partition.get_tenant_id());
        break;
      case PART_NAME:
        cells[i].set_varchar(partition.get_part_name());
        cells[i].set_collation_type(coll_type);
        break;
      case STATUS:
        cells[i].set_int(partition.get_status());
        break;
      case LOW_BOUND_VAL:
        cells[i].set_varchar("");  // we do not store low bound in this version
        cells[i].set_collation_type(coll_type);
        break;
      case LOW_BOUND_VAL_BIN:
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      case HIGH_BOUND_VAL:
        if (OB_FAIL(get_partition_value_str(part_func_type_, partition, cells[i]))) {
          SERVER_LOG(WARN, "fail to get str", K(partition), K(ret));
        } else {
          cells[i].set_collation_type(coll_type);
        }
        break;
      case HIGH_BOUND_VAL_BIN:
        if (OB_FAIL(get_partition_value_bin_str(part_func_type_, partition, cells[i]))) {
          SERVER_LOG(WARN, "fail to get str", K(partition), K(ret));
        } else {
          cells[i].set_collation_type(coll_type);
        }
        break;
      case PART_IDX:
        cells[i].set_int(partition.get_part_idx());
        break;

      case SUB_PART_NUM:
        cells[i].set_int(partition.get_sub_part_num());
        break;
      case SUB_PART_SPACE:
        cells[i].set_int(partition.get_sub_part_space());
        break;
      case SUB_PART_INTERVAL:
        if (OB_FAIL(get_obj_str(partition.get_sub_part_interval(), cells[i]))) {
          SERVER_LOG(WARN, "fail to get str", K(partition.get_sub_part_interval()), K(ret));
        } else {
          cells[i].set_collation_type(coll_type);
        }
        break;
      case SUB_PART_INTERVAL_BIN:
        if (OB_FAIL(get_obj_bin_str(partition.get_sub_part_interval(), cells[i]))) {
          SERVER_LOG(WARN, "fail to get bin str", K(partition.get_sub_part_interval()), K(ret));
        } else {
          cells[i].set_collation_type(coll_type);
        }
        break;
      case SUB_INTERVAL_START:
        if (OB_FAIL(get_obj_str(partition.get_sub_interval_start(), cells[i]))) {
          SERVER_LOG(WARN, "fail to get str", K(partition.get_sub_part_interval()), K(ret));
        } else {
          cells[i].set_collation_type(coll_type);
        }
        break;
      case SUB_INTERVAL_START_BIN:
        if (OB_FAIL(get_obj_bin_str(partition.get_sub_interval_start(), cells[i]))) {
          SERVER_LOG(WARN, "fail to get bin str", K(partition.get_sub_part_interval()), K(ret));
        } else {
          cells[i].set_collation_type(coll_type);
        }
        break;

      case SPARE1:
        cells[i].set_int(0);
        break;
      case SPARE2:
        cells[i].set_int(0);
        break;
      case SPARE3:
        cells[i].set_int(0);
        break;
      case SPARE4:
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      case SPARE5:
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      case SPARE6:
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid column id", K(i), K(ret));
        break;
    }
  }
  return ret;
}

}  // end of namespace observer
}  // end of namespace oceanbase
