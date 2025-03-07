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

#define USING_LOG_PREFIX SQL_OPT
#include "ob_stat_define.h"

namespace oceanbase
{
namespace common
{
const int64_t ObColumnStatParam::DEFAULT_HISTOGRAM_BUCKET_NUM = 254;

void ObAnalyzeSampleInfo::set_percent(double percent)
{
  is_sample_ = true;
  sample_type_ = PercentSample;
  sample_value_ = percent;
}

void ObAnalyzeSampleInfo::set_rows(double row_num)
{
  is_sample_ = true;
  sample_type_ = RowSample;
  sample_value_ = row_num;
}

bool ObColumnStatParam::is_valid_opt_col_type(const ObObjType type, bool is_online_stat)
{
  bool ret = false;
  // currently, we only support the following type to collect histogram
  ColumnTypeClass type_class = ob_obj_type_class(type);
  if (type_class == ColumnTypeClass::ObIntTC ||
      type_class == ColumnTypeClass::ObUIntTC ||
      type_class == ColumnTypeClass::ObFloatTC ||
      type_class == ColumnTypeClass::ObDoubleTC ||
      type_class == ColumnTypeClass::ObNumberTC ||
      type_class == ColumnTypeClass::ObDateTimeTC ||
      type_class == ColumnTypeClass::ObDateTC ||
      type_class == ColumnTypeClass::ObTimeTC ||
      type_class == ColumnTypeClass::ObYearTC ||
      type_class == ColumnTypeClass::ObStringTC ||
      type_class == ColumnTypeClass::ObRawTC ||
      type_class == ColumnTypeClass::ObOTimestampTC ||
      type_class == ColumnTypeClass::ObBitTC ||
      type_class == ColumnTypeClass::ObEnumSetTC ||
      type_class == ColumnTypeClass::ObIntervalTC ||
      type_class == ColumnTypeClass::ObDecimalIntTC ||
      (!is_online_stat && lib::is_mysql_mode() && type_class == ColumnTypeClass::ObTextTC) ||
      type_class == ColumnTypeClass::ObMySQLDateTC ||
      type_class == ColumnTypeClass::ObMySQLDateTimeTC) {
    ret = true;
  }
  return ret;
}

bool ObColumnStatParam::is_valid_avglen_type(const ObObjType type)
{
  bool ret = false;
  // currently, we only support the following type to collect avg len
  ColumnTypeClass type_class = ob_obj_type_class(type);
  if (is_valid_opt_col_type(type) ||
      type_class == ColumnTypeClass::ObTextTC ||
      type_class == ColumnTypeClass::ObRowIDTC ||
      type_class == ColumnTypeClass::ObLobTC) {
    ret = true;
  }
  return ret;
}

bool ObColumnStatParam::is_valid_refine_min_max_type(const ObObjType type)
{
  bool ret = false;
  ColumnTypeClass type_class = ob_obj_type_class(type);
  if (type_class == ColumnTypeClass::ObIntTC ||
      type_class == ColumnTypeClass::ObUIntTC ||
      type_class == ColumnTypeClass::ObFloatTC ||
      type_class == ColumnTypeClass::ObDoubleTC ||
      type_class == ColumnTypeClass::ObNumberTC ||
      type_class == ColumnTypeClass::ObDateTimeTC ||
      type_class == ColumnTypeClass::ObDateTC ||
      type_class == ColumnTypeClass::ObTimeTC ||
      type_class == ColumnTypeClass::ObYearTC ||
      type_class == ColumnTypeClass::ObOTimestampTC ||
      type_class == ColumnTypeClass::ObDecimalIntTC) {
    ret = true;
  }
  return ret;
}

bool StatTable::operator<(const StatTable &other) const
{
  return stale_percent_ < other.stale_percent_;
}

int StatTable::assign(const StatTable &other)
{
  int ret = OB_SUCCESS;
  database_id_ = other.database_id_;
  table_id_ = other.table_id_;
  stale_percent_ = other.stale_percent_;
  consecutive_failed_count_ = other.consecutive_failed_count_;
  return partition_stat_infos_.assign(other.partition_stat_infos_);
}

/**
 * @brief
 *  The order to gather tables
 *  1. gather user tables
 *     if table is a 'big table', make it at last
 *     if table is first time to gather, then gather it at first
 *     else, gather them according to the last gather duration
 *  2. gather sys tables
 *     so as to user tables
 * @param other
 * @return true
 * @return false
 */
bool ObStatTableWrapper::operator<(const ObStatTableWrapper &other) const
{
  bool bret = true;
  if (this == &other) {
    bret = false;
  } else if (table_type_ == ObStatTableType::ObSysTable && other.table_type_ == ObStatTableType::ObUserTable) {
    bret = false;
  } else if ((table_type_ == ObStatTableType::ObUserTable && other.table_type_ == ObStatTableType::ObUserTable) ||
             (table_type_ == ObStatTableType::ObSysTable && other.table_type_ == ObStatTableType::ObSysTable)) {
    if (is_big_table_ && !other.is_big_table_) {
      bret = false;
    } else if ((is_big_table_ && other.is_big_table_) ||
               (!is_big_table_ && !other.is_big_table_)) {
      if (stat_type_ == other.stat_type_) {
        bret = last_gather_duration_ < other.last_gather_duration_;
      } else if (stat_type_ == ObStatType::ObStale && other.stat_type_ == ObStatType::ObFirstTimeToGather) {
        bret = false;
      }
    }
  }
  return bret;
}

int ObStatTableWrapper::assign(const ObStatTableWrapper &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(stat_table_.assign(other.stat_table_))) {
    LOG_WARN("failed to assign stat table");
  } else {
    table_type_ = other.table_type_;
    stat_type_ = other.stat_type_;
    is_big_table_ = other.is_big_table_;
    last_gather_duration_ = other.last_gather_duration_;
  }
  return ret;
}

int ObTableStatParam::assign(const ObTableStatParam &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  db_name_ = other.db_name_;
  db_id_ = other.db_id_;
  tab_name_ = other.tab_name_;
  table_id_ = other.table_id_;
  part_level_ = other.part_level_;
  part_name_ = other.part_name_;
  sample_info_.is_sample_ = other.sample_info_.is_sample_;
  sample_info_.is_block_sample_ = other.sample_info_.is_block_sample_;
  sample_info_.sample_type_ = other.sample_info_.sample_type_;
  sample_info_.sample_value_ = other.sample_info_.sample_value_;
  method_opt_ = other.method_opt_;
  degree_ = other.degree_;
  global_stat_param_ = other.global_stat_param_;
  part_stat_param_ = other.part_stat_param_;
  subpart_stat_param_ = other.subpart_stat_param_;
  granularity_ = other.granularity_;
  cascade_ = other.cascade_;
  stat_tab_ = other.stat_tab_;
  stat_id_ = other.stat_id_;
  stat_own_ = other.stat_own_;
  no_invalidate_ = other.no_invalidate_;
  force_ = other.force_;
  is_subpart_name_ = other.is_subpart_name_;
  stat_category_ = other.stat_category_;
  tab_group_ = other.tab_group_;
  stattype_ = other.stattype_;
  need_approx_ndv_ = other.need_approx_ndv_;
  is_index_stat_ = other.is_index_stat_;
  data_table_name_ = other.data_table_name_;
  is_global_index_ = other.is_global_index_;
  global_part_id_ = other.global_part_id_;
  duration_time_ = other.duration_time_;
  global_tablet_id_ = other.global_tablet_id_;
  global_data_part_id_ = other.global_data_part_id_;
  data_table_id_ = other.data_table_id_;
  need_estimate_block_ = other.need_estimate_block_;
  is_temp_table_ = other.is_temp_table_;
  allocator_ = other.allocator_;
  ref_table_type_ = other.ref_table_type_;
  is_async_gather_ = other.is_async_gather_;
  async_full_table_size_ = other.async_full_table_size_;
  async_partition_ids_ = other.async_partition_ids_;
  hist_sample_info_.is_sample_ = other.hist_sample_info_.is_sample_;
  hist_sample_info_.is_block_sample_ = other.hist_sample_info_.is_block_sample_;
  hist_sample_info_.sample_type_ = other.hist_sample_info_.sample_type_;
  hist_sample_info_.sample_value_ = other.hist_sample_info_.sample_value_;
  is_auto_gather_ = other.is_auto_gather_;
  is_auto_sample_size_ = other.is_auto_sample_size_;
  need_refine_min_max_ = other.need_refine_min_max_;
  auto_sample_row_cnt_ = other.auto_sample_row_cnt_;
  consumer_group_id_ = other.consumer_group_id_;
  min_iops_ = other.min_iops_;
  max_iops_ = other.max_iops_;
  weight_iops_ = other.weight_iops_;
  if (OB_FAIL(part_infos_.assign(other.part_infos_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(subpart_infos_.assign(other.subpart_infos_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(approx_part_infos_.assign(other.approx_part_infos_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(column_params_.assign(other.column_params_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(no_regather_partition_ids_.assign(other.no_regather_partition_ids_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(all_part_infos_.assign(other.all_part_infos_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(all_subpart_infos_.assign(other.all_subpart_infos_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(column_group_params_.assign(other.column_group_params_))) {
    LOG_WARN("failed to assign", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObTableStatParam::assign_common_property(const ObTableStatParam &other)
{
  int ret = OB_SUCCESS;
  sample_info_.is_sample_ = other.sample_info_.is_sample_;
  sample_info_.is_block_sample_ = other.sample_info_.is_block_sample_;
  sample_info_.sample_type_ = other.sample_info_.sample_type_;
  sample_info_.sample_value_ = other.sample_info_.sample_value_;
  method_opt_ = other.method_opt_;
  degree_ = other.degree_;
  global_stat_param_ = other.global_stat_param_;
  part_stat_param_ = other.part_stat_param_;
  subpart_stat_param_ = other.subpart_stat_param_;
  granularity_ = other.granularity_;
  cascade_ = other.cascade_;
  stat_tab_ = other.stat_tab_;
  stat_id_ = other.stat_id_;
  stat_own_ = other.stat_own_;
  no_invalidate_ = other.no_invalidate_;
  force_ = other.force_;
  stat_category_ = other.stat_category_;
  stattype_ = other.stattype_;
  need_approx_ndv_ = other.need_approx_ndv_;
  duration_time_ = other.duration_time_;
  allocator_ = other.allocator_;
  online_sample_percent_ = other.online_sample_percent_;
  hist_sample_info_.is_sample_ = other.hist_sample_info_.is_sample_;
  hist_sample_info_.is_block_sample_ = other.hist_sample_info_.is_block_sample_;
  hist_sample_info_.sample_type_ = other.hist_sample_info_.sample_type_;
  hist_sample_info_.sample_value_ = other.hist_sample_info_.sample_value_;
  is_auto_gather_ = other.is_auto_gather_;
  is_auto_sample_size_ = other.is_auto_sample_size_;
  need_refine_min_max_ = other.need_refine_min_max_;
  auto_sample_row_cnt_ = other.auto_sample_row_cnt_;
  consumer_group_id_ = other.consumer_group_id_;
  min_iops_ = other.min_iops_;
  max_iops_ = other.max_iops_;
  weight_iops_ = other.weight_iops_;
  return ret;
}

int ObOptStatGatherParam::assign(const ObOptStatGatherParam &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  db_name_ = other.db_name_;
  tab_name_ = other.tab_name_;
  table_id_ = other.table_id_;
  stat_level_ = other.stat_level_;
  need_histogram_ = other.need_histogram_;
  sample_info_.is_sample_ = other.sample_info_.is_sample_;
  sample_info_.is_block_sample_ = other.sample_info_.is_block_sample_;
  sample_info_.sample_type_ = other.sample_info_.sample_type_;
  sample_info_.sample_value_ = other.sample_info_.sample_value_;
  degree_ = other.degree_;
  allocator_ = other.allocator_;
  partition_id_block_map_ = other.partition_id_block_map_;
  gather_start_time_ = other.gather_start_time_;
  stattype_ = other.stattype_;
  is_split_gather_ = other.is_split_gather_;
  max_duration_time_ = other.max_duration_time_;
  need_approx_ndv_ = other.need_approx_ndv_;
  data_table_name_ = other.data_table_name_;
  global_part_id_ = other.global_part_id_;
  gather_vectorize_ = other.gather_vectorize_;
  sepcify_scn_ = other.sepcify_scn_;
  hist_sample_info_.is_sample_ = other.hist_sample_info_.is_sample_;
  hist_sample_info_.is_block_sample_ = other.hist_sample_info_.is_block_sample_;
  hist_sample_info_.sample_type_ = other.hist_sample_info_.sample_type_;
  hist_sample_info_.sample_value_ = other.hist_sample_info_.sample_value_;
  is_auto_sample_size_ = other.is_auto_sample_size_;
  need_refine_min_max_ = other.need_refine_min_max_;
  auto_sample_row_cnt_ = other.auto_sample_row_cnt_;
  data_table_id_ = other.data_table_id_;
  is_global_index_ = other.is_global_index_;
  part_level_ = other.part_level_;
  consumer_group_id_ = other.consumer_group_id_;
  if (OB_FAIL(partition_infos_.assign(other.partition_infos_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(column_params_.assign(other.column_params_))) {
    LOG_WARN("failed to assign", K(ret));
  } else {/*do nothing*/}
  return ret;
}

bool ObTableStatParam::is_specify_partition() const
{
  bool is_specify = false;
  if (part_level_ == share::schema::PARTITION_LEVEL_ZERO) {
    //do nothing
  } else if (part_level_ == share::schema::PARTITION_LEVEL_ONE) {
    is_specify = part_infos_.count() != all_part_infos_.count();
  } else if (part_level_ == share::schema::PARTITION_LEVEL_TWO) {
    is_specify = (part_infos_.count() + approx_part_infos_.count() != all_part_infos_.count()) ||
                 (subpart_infos_.count() != all_subpart_infos_.count());
  }
  return is_specify;
}

bool ObTableStatParam::is_specify_column() const
{
  bool is_specify = false;
  for (int64_t i = 0; !is_specify && i < column_params_.count(); ++i) {
    is_specify = column_params_.at(i).is_valid_opt_col() && !column_params_.at(i).need_basic_stat();
  }
  return is_specify;
}

int64_t ObTableStatParam::get_need_gather_column() const
{
  int64_t valid_column = 0;
  for (int64_t i = 0; i < column_params_.count(); ++i) {
    if (column_params_.at(i).need_basic_stat()) {
      ++ valid_column;
    }
  }
  return valid_column;
}

int64_t ObOptStatGatherParam::get_need_gather_column() const
{
  int64_t valid_column = 0;
  for (int64_t i = 0; i < column_params_.count(); ++i) {
    if (column_params_.at(i).need_basic_stat()) {
      ++ valid_column;
    }
  }
  return valid_column;
}

int AsyncStatTable::assign(const AsyncStatTable &other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  return partition_ids_.assign(other.partition_ids_);
}

OB_SERIALIZE_MEMBER(ObOptDmlStat,
                    tenant_id_,
                    table_id_,
                    tablet_id_,
                    insert_row_count_,
                    update_row_count_,
                    delete_row_count_);


class BasicStatAuditItem : public AuditBaseItem
{
public:
  BasicStatAuditItem(ObIAllocator &allocator, bool is_index) :
   part_ids_(allocator),
   cost_time_(0),
   is_index_(is_index) {}
  virtual ~BasicStatAuditItem() {}

  virtual int64_t get_cost_time() const { return cost_time_; }
  DECLARE_VIRTUAL_TO_STRING;
public:
  ObFixedArray<uint64_t, ObIAllocator> part_ids_;
  int64_t cost_time_;
  bool is_index_;
};

DEF_TO_STRING(BasicStatAuditItem)
{
  int64_t pos = 0;
  uint64_t start_part = -1;
  uint64_t part_span = 0;
  J_OBJ_START();
  J_KV("TYPE", is_index_ ? "Index" : "Basic");
  J_COMMA();
  if (part_ids_.empty()) {
    J_KV("PART", "ALL");
  } else {
    J_NAME("PART");
    J_COLON();
    J_ARRAY_START();
    for (int64_t i = 0; pos < buf_len && i < part_ids_.count(); ++i) {
      if (start_part == -1) {
        start_part = part_ids_.at(i);
        part_span = 0;
      } else if (start_part + part_span + 1 == part_ids_.at(i)) {
        ++part_span;
      } else {
        if (part_span > 0) {
          BUF_PRINTF("%lu-%lu", start_part, start_part + part_span);
        } else {
          BUF_PRINTF("%lu", start_part);
        }
        J_COMMA();
        start_part = part_ids_.at(i);
        part_span = 0;
      }
    }
    if (pos < buf_len && part_span != -1) {
      if (part_span > 0) {
        BUF_PRINTF("%lu-%lu", start_part, start_part + part_span);
      } else {
        BUF_PRINTF("%lu", start_part);
      }
    }
    J_ARRAY_END();
  }
  J_COMMA();
  J_KV("COST", cost_time_);
  J_OBJ_END();
  return pos;
}

class HistogramAuditItem : public AuditBaseItem
{
public:
  HistogramAuditItem(uint64_t part_id,
                     uint64_t topk_cost,
                     uint64_t hybird_cost) :
    part_id_(part_id),
    topk_cost_time_(topk_cost),
    hybrid_cost_time_(hybird_cost) {}
  virtual ~HistogramAuditItem() {}

  virtual int64_t get_cost_time() const { return topk_cost_time_ + hybrid_cost_time_; }

  DECLARE_VIRTUAL_TO_STRING;
public:
  uint64_t part_id_;
  int64_t topk_cost_time_;
  int64_t hybrid_cost_time_;
};

DEF_TO_STRING(HistogramAuditItem)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("TYPE", "Hist");
  J_COMMA();
  if (part_id_ == UINT64_MAX) {
    J_KV("PART", "ALL");
  } else {
    J_KV("PART", part_id_);
  }
  J_COMMA();
  if (topk_cost_time_ > 1000) {
    J_KV("TOPK_COST", topk_cost_time_);
    J_COMMA();
  }
  if (hybrid_cost_time_ > 1000) {
    J_KV("HYBRID_COST", hybrid_cost_time_);
  }
  J_OBJ_END();
  return pos;
}

class RefineMinMaxAuditItem : public AuditBaseItem
{
public:
  static const int64_t REFINE_MIN_MAX_USE_INDEX = 1;
  static const int64_t REFINE_MIN_MAX_USE_SKIP_INDEX = 2;
public:
  RefineMinMaxAuditItem(int64_t type, uint64_t part_id, int64_t cost_time)
    : type_(type),
      part_id_(part_id),
      cost_time_(cost_time) {}
  virtual ~RefineMinMaxAuditItem() {}

  virtual int64_t get_cost_time() const { return cost_time_; }
  DECLARE_VIRTUAL_TO_STRING;
public:
  int64_t type_;
  uint64_t part_id_;
  int64_t cost_time_;
};

DEF_TO_STRING(RefineMinMaxAuditItem)
{
  int64_t pos = 0;
  J_OBJ_START();
  if (type_ == REFINE_MIN_MAX_USE_INDEX) {
    J_KV("TYPE", "RefineI");
  } else if (type_ == REFINE_MIN_MAX_USE_SKIP_INDEX) {
    J_KV("TYPE", "RefineSI");
  } else {
    J_KV("TYPE", "Refine");
  }
  J_COMMA();
  if (part_id_ == UINT64_MAX) {
    J_KV("PART", "ALL");
  } else {
    J_KV("PART", part_id_);
  }
  J_COMMA();
  J_KV("COST", cost_time_);
  J_OBJ_END();
  return pos;
}

class FlushStatsAuditItem : public AuditBaseItem
{
public:
  FlushStatsAuditItem(int64_t cost_time) :
    cost_time_(cost_time) {}
  virtual ~FlushStatsAuditItem() {}

  virtual int64_t get_cost_time() const { return cost_time_; }

  TO_STRING_KV("TYPE", "Flush", "COST", cost_time_);
public:
  int64_t cost_time_;
};

ObOptStatGatherAudit::~ObOptStatGatherAudit()
{
  for (int64_t i = 0; i < audit_items_.count(); ++i) {
    AuditBaseItem* item = audit_items_.at(i);
    if (OB_NOT_NULL(item)) {
      item->~AuditBaseItem();
    }
  }
}

int64_t ObOptStatGatherAudit::inner_to_string(const ObIArray<AuditBaseItem*> &items, char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  if (!failed_refine_parts_.empty()) {
    J_KV("FRP", failed_refine_parts_);
  }
  if (!items.empty()) {
    J_NAME("STAT");
    J_COLON();
    J_ARRAY_START();
    for (int64_t i = 0; pos < buf_len && i < items.count(); ++i) {
      AuditBaseItem* item = items.at(i);
      if (OB_NOT_NULL(item)) {
        pos += item->to_string(buf + pos, buf_len - pos);
        if (i < items.count() - 1) {
          J_COMMA();
        }
      }
    }
    J_ARRAY_END();
  }
  J_OBJ_END();
  return pos;
}

struct AuditBaseItemCmp
{
  inline bool operator()(const AuditBaseItem * left, const AuditBaseItem * right)
  {
    bool bret = false;
    if (left != nullptr && right != nullptr) {
      bret = (left->get_cost_time() > right->get_cost_time());
    }
    return bret;
  }
};

DEF_TO_STRING(ObOptStatGatherAudit)
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  pos = inner_to_string(audit_items_, buf, buf_len);
  if (OB_UNLIKELY(pos >= buf_len - 1)) {
    if (!audit_items_.empty()) {
      ObSEArray<AuditBaseItem*, 4> items;
      if (OB_SUCC(items.assign(audit_items_))) {
        lib::ob_sort(&items.at(0),
                     &items.at(0) + items.count(),
                     AuditBaseItemCmp());
        pos = inner_to_string(items, buf, buf_len);
      }
    }
  }
  return pos;
}

int ObOptStatGatherAudit::add_basic_estimate_audit(const ObIArray<PartInfo> & parts, bool is_index, int64_t cost_time)
{
  int ret = OB_SUCCESS;
  BasicStatAuditItem *item = NULL;
  void *ptr = NULL;
  if (need_audit(cost_time)) {
    if (OB_ISNULL(ptr = allocator_.alloc(sizeof(BasicStatAuditItem)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate audit item", K(ret));
    } else if (OB_FALSE_IT(item = new(ptr) BasicStatAuditItem(allocator_, is_index))) {
    } else if (OB_FAIL(item->part_ids_.init(parts.count()))) {
      LOG_WARN("failed to init fixed array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < parts.count(); ++i) {
        if (OB_FAIL(item->part_ids_.push_back(parts.at(i).part_id_))) {
          LOG_WARN("failed to push back part ids", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        item->cost_time_ = cost_time;
        if (OB_FAIL(audit_items_.push_back(item))) {
          LOG_WARN("failed to push back audit item", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObOptStatGatherAudit::add_histogram_estimate_audit(uint64_t part_id, int64_t topk_cost, int64_t hybrid_cost)
{
  int ret = OB_SUCCESS;
  HistogramAuditItem *item = NULL;
  void *ptr = NULL;
  if (need_audit(topk_cost + hybrid_cost)) {
    if (OB_ISNULL(ptr = allocator_.alloc(sizeof(HistogramAuditItem)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate audit item", K(ret));
    } else if (OB_FALSE_IT(item = new(ptr) HistogramAuditItem(part_id, topk_cost, hybrid_cost))) {
    } else if (OB_FAIL(audit_items_.push_back(item))) {
      LOG_WARN("failed to push back audit item", K(ret));
    }
  }
  return ret;
}

int ObOptStatGatherAudit::add_refine_estimate_audit(bool use_skip_index, uint64_t part_id, int64_t cost_time)
{
  int ret = OB_SUCCESS;
  RefineMinMaxAuditItem *item = NULL;
  void *ptr = NULL;
  if (need_audit(cost_time)) {
    int64_t type = use_skip_index ? RefineMinMaxAuditItem::REFINE_MIN_MAX_USE_SKIP_INDEX :
                     RefineMinMaxAuditItem::REFINE_MIN_MAX_USE_INDEX;
    if (OB_ISNULL(ptr = allocator_.alloc(sizeof(RefineMinMaxAuditItem)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate audit item", K(ret));
    } else if (OB_FALSE_IT(item = new(ptr) RefineMinMaxAuditItem(type, part_id, cost_time))) {
    } else if (OB_FAIL(audit_items_.push_back(item))) {
      LOG_WARN("failed to push back audit item", K(ret));
    }
  }
  return ret;
}

int ObOptStatGatherAudit::add_flush_stats_audit(int64_t cost_time)
{
  int ret = OB_SUCCESS;
  FlushStatsAuditItem *item = NULL;
  void *ptr = NULL;
  if (need_audit(cost_time)) {
    if (OB_ISNULL(ptr = allocator_.alloc(sizeof(FlushStatsAuditItem)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate audit item", K(ret));
    } else if (OB_FALSE_IT(item = new(ptr) FlushStatsAuditItem(cost_time))) {
    } else if (OB_FAIL(audit_items_.push_back(item))) {
      LOG_WARN("failed to push back audit item", K(ret));
    }
  }
  return ret;
}

}
}
