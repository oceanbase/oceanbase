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

#include "ob_all_virtual_proxy_sub_partition.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase {
namespace observer {

ObAllVirtualProxySubPartition::ObAllVirtualProxySubPartition()
    : ObAllVirtualProxyBaseIterator(),
      part_iter_(),
      subpart_iter_(),
      part_func_type_(PARTITION_FUNC_TYPE_MAX),
      is_sub_part_template_(true),
      table_(NULL),
      is_inited_(false)
{}

ObAllVirtualProxySubPartition::~ObAllVirtualProxySubPartition()
{}

int ObAllVirtualProxySubPartition::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member doesn't init", K_(allocator), K(ret));
  } else if (key_ranges_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "only one table_id can be specified");
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member doesn't init", KP_(schema_service), K(ret));
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
    int32_t sub_part_id = OB_INVALID_INDEX;

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
          case SUB_PART_ID_IDX:
            sub_part_id = static_cast<int32_t>(start_key_obj_ptr[i].get_int());
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid key", K(i), K(ret));
            break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      UNUSED(part_id);      // do not use part_id yet, we will return all sub part
      UNUSED(sub_part_id);  // do not use part_id yet, we will return all sub part
      if (OB_FAIL(get_table_schema(full_schema_guard_, table_id, table_))) {
        SERVER_LOG(WARN, "fail to get table schema", K(ret), K(table_id));
      } else if (OB_ISNULL(table_) || PARTITION_LEVEL_TWO != table_->get_part_level()) {
        // skip
      } else {
        is_sub_part_template_ = table_->is_sub_part_template();
        part_func_type_ = table_->get_sub_part_option().get_part_func_type();
        bool check_dropped_schema = false;
        if (is_sub_part_template_) {
          (void)subpart_iter_.init(*table_, check_dropped_schema);
        } else {
          const ObPartition* part = NULL;
          (void)part_iter_.init(*table_, check_dropped_schema);
          if (OB_FAIL(part_iter_.next(part))) {
            SERVER_LOG(WARN, "get part failed", K(ret), K(table_id));
          } else {
            (void)subpart_iter_.init(*table_, *part, check_dropped_schema);
          }
        }
        if (OB_SUCC(ret)) {
          is_inited_ = true;
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualProxySubPartition::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  const ObSubPartition* sub_partition = NULL;
  if (!is_inited_) {
    ret = OB_ITER_END;  // maybe table or part not exists
  } else if (OB_FAIL(subpart_iter_.next(sub_partition))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to iter sub_partition", K(ret));
    } else if (is_sub_part_template_) {
      // Templated secondary partition, iter end
    } else {
      // Non-templated secondary partition
      const ObPartition* partition = NULL;
      if (OB_FAIL(part_iter_.next(partition))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "get part failed", K(ret));
        } else { /* iter end */
        }
      } else if (OB_ISNULL(partition) || OB_ISNULL(table_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "partition or table is null", K(ret));
      } else {
        bool check_dropped_partition = false;
        (void)subpart_iter_.init(*table_, *partition, check_dropped_partition);
        if (OB_FAIL(subpart_iter_.next(sub_partition))) {
          SERVER_LOG(WARN, "fail to iter sub_partition", K(ret), KPC(partition));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(sub_partition)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "sub_partition is null", K(ret));
  } else {
    ret = fill_cells(*sub_partition);
  }
  return ret;
}

int ObAllVirtualProxySubPartition::fill_cells(const ObSubPartition& sub_partition)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj* cells = cur_row_.cells_;
  ObCollationType coll_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
  if (OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case TABLE_ID:
        cells[i].set_int(sub_partition.get_table_id());
        break;
      case PART_ID:
        cells[i].set_int(sub_partition.get_part_id());
        break;
      case SUB_PART_ID:
        cells[i].set_int(sub_partition.get_sub_part_id());
        break;

      case TENANT_ID:
        cells[i].set_int(sub_partition.get_tenant_id());
        break;
      case PART_NAME:
        cells[i].set_varchar(sub_partition.get_part_name());
        cells[i].set_collation_type(coll_type);
        break;
      case STATUS:
        cells[i].set_int(sub_partition.get_status());
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
        if (OB_FAIL(get_partition_value_str(part_func_type_, sub_partition, cells[i]))) {
          SERVER_LOG(WARN, "fail to get str", K(sub_partition), K(ret));
        } else {
          cells[i].set_collation_type(coll_type);
        }
        break;
      case HIGH_BOUND_VAL_BIN:
        if (OB_FAIL(get_partition_value_bin_str(part_func_type_, sub_partition, cells[i]))) {
          SERVER_LOG(WARN, "fail to get str", K(sub_partition), K(ret));
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
