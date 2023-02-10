// Copyright 2010-2016 Alibaba Inc. All Rights Reserved.
// Author:
//   zhenling.zzg
// this file defines implementation of __all_virtual_plan_table
#include "sql/monitor/ob_sql_plan_manager.h"
#include "observer/omt/ob_multi_tenant.h"
#include "ob_all_virtual_plan_table.h"
#include "lib/utility/utility.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;
namespace oceanbase {
namespace observer {

ObAllVirtualPlanTable::ObAllVirtualPlanTable() :
    ObVirtualTableScannerIterator(),
    plan_table_mgr_(nullptr),
    start_id_(INT64_MAX),
    end_id_(INT64_MIN),
    cur_id_(0)
{
}

ObAllVirtualPlanTable::~ObAllVirtualPlanTable()
{
  reset();
}

int ObAllVirtualPlanTable::inner_reset()
{
  int ret = OB_SUCCESS;
  if (nullptr == plan_table_mgr_) {
     //do nothing
  } else {
    start_id_ = INT64_MIN;
    end_id_ = INT64_MAX;
    int64_t start_idx = plan_table_mgr_->get_start_idx();
    int64_t end_idx = plan_table_mgr_->get_end_idx();
    start_id_ = MAX(start_id_, start_idx);
    end_id_ = MIN(end_id_, end_idx);
    if (start_id_ >= end_id_) {
      SERVER_LOG(DEBUG, "sql_plan_mgr_ iter end", K(start_id_), K(end_id_));
    } else if (is_reverse_scan()) {
      cur_id_ = end_id_ - 1;
    } else {
      cur_id_ = start_id_;
    }
  }
  return ret;
}

void ObAllVirtualPlanTable::reset()
{
  ObVirtualTableScannerIterator::reset();
  inner_reset();
}

int ObAllVirtualPlanTable::inner_open()
{
  int ret = OB_SUCCESS;
  ret = inner_reset();
  return ret;
}

int ObAllVirtualPlanTable::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObRaQueue::Ref ref;
  void *rec = NULL;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "invalid argument", KP(allocator_), K(ret));
  } else if (nullptr == plan_table_mgr_ ||
             cur_id_ < start_id_ ||
             cur_id_ >= end_id_) {
    ret = OB_ITER_END;
  }
  //fetch next record
  if (OB_SUCC(ret)) {
    do {
      ref.reset();
      ret = plan_table_mgr_->get(cur_id_, rec, &ref);
      if (OB_ENTRY_NOT_EXIST == ret) {
        if (is_reverse_scan()) {
          cur_id_ -= 1;
        } else {
          cur_id_ += 1;
        }
      }
    } while (OB_ENTRY_NOT_EXIST == ret &&
             cur_id_ < end_id_ &&
             cur_id_ >= start_id_);
    if (OB_SUCC(ret) && NULL == rec) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected null rec", K(cur_id_), K(ret));
    }
  }

  //fill new row with record
  if (OB_SUCC(ret)) {
    ObSqlPlanItemRecord *record = static_cast<ObSqlPlanItemRecord*> (rec);
    if (OB_FAIL(fill_cells(*record))) {
      SERVER_LOG(WARN, "failed to fill cells", K(ret));
    } else {
      //finish fetch one row
      row = &cur_row_;
      plan_table_mgr_->revert(&ref);
      SERVER_LOG(DEBUG, "get next row succ", K(cur_id_));
    }
  }

  // move to next slot
  if (OB_SUCC(ret)) {
    if (!is_reverse_scan()) {
      // forwards
      cur_id_++;
    } else {
      // backwards
      cur_id_--;
    }
  }
  return ret;
}

int ObAllVirtualPlanTable::fill_cells(ObSqlPlanItemRecord &record)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;

  if (OB_ISNULL(cells)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(cells));
  }
  for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < col_count; cell_idx++) {
    uint64_t col_id = output_column_ids_.at(cell_idx);
    switch(col_id) {
    case STATEMENT_ID: {
      cells[cell_idx].set_varchar(record.data_.sql_id_, record.data_.sql_id_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case PLAN_ID: {
      cells[cell_idx].set_int(record.data_.plan_id_);
      break;
    }
    case TIMESTAMP: {
      cells[cell_idx].set_timestamp(record.data_.gmt_create_);
      break;
    }
    case REMARKS: {
      cells[cell_idx].set_varchar(record.data_.remarks_, record.data_.remarks_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case OPERATION: {
      cells[cell_idx].set_varchar(record.data_.operation_, record.data_.operation_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case OPTIONS: {
      cells[cell_idx].set_varchar(record.data_.options_, record.data_.options_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case OBJECT_NODE: {
      cells[cell_idx].set_varchar(record.data_.object_node_, record.data_.object_node_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case OBJECT_OWNER: {
      cells[cell_idx].set_varchar(record.data_.object_owner_, record.data_.object_owner_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case OBJECT_NAME: {
      cells[cell_idx].set_varchar(record.data_.object_name_, record.data_.object_name_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case OBJECT_ALIAS: {
      cells[cell_idx].set_varchar(record.data_.object_alias_, record.data_.object_alias_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case OBJECT_INSTANCE: {
      cells[cell_idx].set_int(record.data_.object_id_);
      break;
    }
    case OBJECT_TYPE: {
      cells[cell_idx].set_varchar(record.data_.object_type_, record.data_.object_type_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case OPTIMZIER: {
      cells[cell_idx].set_varchar(record.data_.optimizer_, record.data_.optimizer_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case ID: {
      cells[cell_idx].set_int(record.data_.id_);
      break;
    }
    case PARENT_ID: {
      cells[cell_idx].set_int(record.data_.parent_id_);
      break;
    }
    case DEPTH: {
      cells[cell_idx].set_int(record.data_.depth_);
      break;
    }
    case POSITION: {
      cells[cell_idx].set_int(record.data_.position_);
      break;
    }
    case SEARCH_COLUMNS: {
      cells[cell_idx].set_int(record.data_.search_columns_);
      break;
    }
    case COST: {
      cells[cell_idx].set_int(record.data_.cost_);
      break;
    }
    case CARDINALITY: {
      cells[cell_idx].set_int(record.data_.cardinality_);
      break;
    }
    case BYTES: {
      cells[cell_idx].set_int(record.data_.bytes_);
      break;
    }
    case ROWSET: {
      cells[cell_idx].set_int(record.data_.rowset_);
      break;
    }
    case OTHER_TAG: {
      cells[cell_idx].set_varchar(record.data_.other_tag_, record.data_.other_tag_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case PARTITION_START: {
      cells[cell_idx].set_varchar(record.data_.partition_start_, record.data_.partition_start_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case PARTITION_STOP: {
      cells[cell_idx].set_varchar(record.data_.partition_stop_, record.data_.partition_stop_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case PARTITION_ID: {
      cells[cell_idx].set_int(record.data_.partition_id_);
      break;
    }
    case OTHER: {
      cells[cell_idx].set_varchar(record.data_.other_, record.data_.other_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case DISTRIBUTION: {
      cells[cell_idx].set_varchar(record.data_.distribution_, record.data_.distribution_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case CPU_COST: {
      cells[cell_idx].set_int(record.data_.cpu_cost_);
      break;
    }
    case IO_COST: {
      cells[cell_idx].set_int(record.data_.io_cost_);
      break;
    }
    case TEMP_SPACE: {
      cells[cell_idx].set_int(record.data_.temp_space_);
      break;
    }
    case ACCESS_PREDICATES: {
      cells[cell_idx].set_varchar(record.data_.access_predicates_, record.data_.access_predicates_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case FILTER_PREDICATES: {
      cells[cell_idx].set_varchar(record.data_.filter_predicates_, record.data_.filter_predicates_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case STARTUP_PREDICATES: {
      cells[cell_idx].set_varchar(record.data_.startup_predicates_, record.data_.startup_predicates_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case PROJECTION: {
      cells[cell_idx].set_varchar(record.data_.projection_, record.data_.projection_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case SPECIAL_PREDICATES: {
      cells[cell_idx].set_varchar(record.data_.special_predicates_, record.data_.special_predicates_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case TIME: {
      cells[cell_idx].set_int(record.data_.time_);
      break;
    }
    case QBLOCK_NAME: {
      cells[cell_idx].set_varchar(record.data_.qblock_name_, record.data_.qblock_name_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case OTHER_XML: {
      cells[cell_idx].set_varchar(record.data_.other_xml_, record.data_.other_xml_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(col_id));
      break;
    }
    }
  }
  return ret;
}

} //namespace observer
} //namespace oceanbase
