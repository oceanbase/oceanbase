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

#ifndef OB_STAT_DEFINE_H
#define OB_STAT_DEFINE_H

#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "common/object/ob_object.h"
#include "common/rowkey/ob_rowkey_info.h"
#include "share/schema/ob_schema_struct.h"
#include "share/stat/ob_opt_table_stat.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_opt_osg_column_stat.h"

namespace oceanbase
{
namespace common
{
class ObOptTableStat;
class ObOptColumnStat;
struct BlockNumStat;

typedef hash::ObHashMap<int64_t, BlockNumStat *, common::hash::NoPthreadDefendMode> PartitionIdBlockMap;
typedef common::hash::ObHashMap<ObOptTableStat::Key, ObOptTableStat *, common::hash::NoPthreadDefendMode> TabStatIndMap;
typedef common::hash::ObHashMap<ObOptColumnStat::Key, ObOptOSGColumnStat *, common::hash::NoPthreadDefendMode> OSGColStatIndMap;
typedef common::hash::ObHashMap<ObOptColumnStat::Key, ObOptColumnStat *, common::hash::NoPthreadDefendMode> ColStatIndMap;

enum StatOptionFlags
{
  OPT_ESTIMATE_PERCENT = 1,
  OPT_BLOCK_SAMPLE     = 1 << 1,
  OPT_METHOD_OPT       = 1 << 2,
  OPT_DEGREE           = 1 << 3,
  OPT_GRANULARITY      = 1 << 4,
  OPT_CASCADE          = 1 << 5,
  OPT_STATTAB          = 1 << 6,
  OPT_STATID           = 1 << 7,
  OPT_STATOWN          = 1 << 8,
  OPT_NO_INVALIDATE    = 1 << 9,
  OPT_STATTYPE         = 1 << 10,
  OPT_FORCE            = 1 << 11,
  OPT_APPROXIMATE_NDV  = 1 << 12,
  OPT_ESTIMATE_BLOCK   = 1 << 13,
  OPT_STAT_OPTION_ALL  = (1 << 14) -1
};
const static double OPT_DEFAULT_STALE_PERCENT = 0.1;
const static int64_t OPT_DEFAULT_STATS_RETENTION = 31;
const static int64_t OPT_STATS_MAX_VALUE_CHAR_LEN = 128;
const int64_t MAX_AUTO_GATHER_FULL_TABLE_ROWS = 100000000;
const int64_t MAGIC_SAMPLE_SIZE = 5500;
const int64_t MAGIC_MAX_AUTO_SAMPLE_SIZE = 22000;
const int64_t MAGIC_MIN_SAMPLE_SIZE = 2500;
const int64_t MAGIC_BASE_SAMPLE_SIZE = 1000;
const double MAGIC_SAMPLE_CUT_RATIO = 0.00962;
const static int64_t MAX_NUM_OF_WRITE_STATS = 2000;
const static int64_t DEFAULT_STAT_GATHER_VECTOR_BATCH_SIZE = 256;
const static int64_t MIN_GATHER_WORK_ARANA_SIZE = 10 * 1024L * 1024L; //10M
const int64_t MAX_OPT_STATS_PROCESS_RPC_TIMEOUT = 300000000;//one optimizer stats processing rpc time should not exceed 300 seconds

enum StatLevel
{
  INVALID_LEVEL,
  TABLE_LEVEL,
  PARTITION_LEVEL,
  SUBPARTITION_LEVEL,
};

enum StatTypeLocked
{
  NULL_TYPE             = 0,
  DATA_TYPE             = 1,
  CACHE_TYPE            = 1 << 1,
  TABLE_ALL_TYPE        = 1 << 2,
  PARTITION_ALL_TYPE    = 1 << 3
};

// enum ObGranularityType
// {
//   TABLE_LEVEL,        // 全表
//   PARTITION_LEVEL,     // 一级分区
//   SUBPARTITION_LEVEL   // 二级分区
// };

enum ColumnUsageFlag
{
  NON_FLAG           = 0,
  EQUALITY_PREDS     = 1,
  EQUIJOIN_PREDS     = 1 << 1,
  NONEQUIJOIN_PREDS  = 1 << 2,
  RANGE_PREDS        = 1 << 3,
  LIKE_PREDS         = 1 << 4,
  NULL_PREDS         = 1 << 5,
  DISTINCT_MEMBER    = 1 << 6,
  GROUPBY_MEMBER     = 1 << 7
};

enum ColumnAttrFlag
{
  IS_INDEX_COL      = 1,
  IS_HIDDEN_COL     = 1 << 1,
  IS_UNIQUE_COL     = 1 << 2,
  IS_NOT_NULL_COL   = 1 << 3
};

enum ColumnGatherFlag
{
  NO_NEED_STAT          = 0,
  VALID_OPT_COL         = 1,
  NEED_BASIC_STAT       = 1 << 1,
  NEED_AVG_LEN          = 1 << 2
};

enum ObGranularityType
{
  GRANULARITY_INVALID = 0,
  GRANULARITY_GLOBAL,
  GRANULARITY_PARTITION,
  GRANULARITY_SUBPARTITION,
  GRANULARITY_ALL,
  GRANULARITY_AUTO,
  GRANULARITY_GLOBAL_AND_PARTITION,
  GRANULARITY_APPROX_GLOBAL_AND_PARTITION
};

struct BlockNumStat
{
  BlockNumStat() :
    tab_macro_cnt_(0),
    tab_micro_cnt_(0),
    cg_macro_cnt_arr_(),
    cg_micro_cnt_arr_(),
    sstable_row_cnt_(0),
    memtable_row_cnt_(0)
  {
    cg_macro_cnt_arr_.set_attr(ObMemAttr(MTL_ID(), "BlockNumStat"));
    cg_micro_cnt_arr_.set_attr(ObMemAttr(MTL_ID(), "BlockNumStat"));
  }
  int64_t tab_macro_cnt_;
  int64_t tab_micro_cnt_;
  ObSEArray<int64_t, 1, common::ModulePageAllocator, true> cg_macro_cnt_arr_;
  ObSEArray<int64_t, 1, common::ModulePageAllocator, true> cg_micro_cnt_arr_;
  int64_t sstable_row_cnt_;
  int64_t memtable_row_cnt_;
  TO_STRING_KV(K(tab_macro_cnt_),
               K(tab_micro_cnt_),
               K(cg_macro_cnt_arr_),
               K(cg_micro_cnt_arr_),
               K(sstable_row_cnt_),
               K(memtable_row_cnt_))
};

//TODO@jiangxiu.wt: improve the expression of PartInfo, use the map is better.
struct PartInfo
{
  PartInfo() :
    part_name_(),
    tablet_id_(),
    part_id_(OB_INVALID_ID),
    part_stattype_(StatTypeLocked::NULL_TYPE),
    subpart_cnt_(0),
    first_part_id_(OB_INVALID_ID)
  {}

  ObString part_name_;
  ObTabletID tablet_id_;
  int64_t  part_id_;
  int64_t part_stattype_;
  int64_t subpart_cnt_;
  int64_t first_part_id_;//part id corresponding to the sub partition.

  TO_STRING_KV(K_(part_name),
               K_(tablet_id),
               K_(part_id),
               K_(part_stattype),
               K_(subpart_cnt),
               K_(first_part_id));
};

struct ObPartitionStatInfo;
struct StatTable
{
  StatTable() :
    database_id_(OB_INVALID_ID),
    table_id_(OB_INVALID_ID),
    stale_percent_(0.0),
    partition_stat_infos_()
  {
    partition_stat_infos_.set_attr(lib::ObMemAttr(MTL_ID(), "StatTable"));
  }
  StatTable(uint64_t database_id, uint64_t table_id) :
    database_id_(database_id),
    table_id_(table_id),
    stale_percent_(0.0),
    partition_stat_infos_()
  {
    partition_stat_infos_.set_attr(lib::ObMemAttr(MTL_ID(), "StatTable"));
  }
  bool operator<(const StatTable &other) const;
  int assign(const StatTable &other);
  TO_STRING_KV(K_(database_id),
               K_(table_id),
               K_(stale_percent),
               K_(partition_stat_infos));
  uint64_t database_id_;
  uint64_t table_id_;
  double stale_percent_;
  ObArray<ObPartitionStatInfo> partition_stat_infos_;
};

enum ObStatTableType {
  ObUserTable = 0,
  ObSysTable
};

enum ObStatType {
  ObFirstTimeToGather = 0,
  ObStale,
  ObNotStale
};

struct ObStatTableWrapper {
  ObStatTableWrapper() :
    stat_table_(),
    table_type_(ObUserTable),
    stat_type_(ObFirstTimeToGather),
    is_big_table_(false),
    last_gather_duration_(0)
    {}
  bool operator<(const ObStatTableWrapper &other) const;
  int assign(const ObStatTableWrapper &other);
  StatTable stat_table_;
  ObStatTableType table_type_;
  ObStatType stat_type_;
  bool is_big_table_;
  int64_t last_gather_duration_;
  TO_STRING_KV(K_(stat_table),
               K_(table_type),
               K_(stat_type),
               K_(is_big_table),
               K_(last_gather_duration));
};

struct ObGlobalStatParam
{
  ObGlobalStatParam()
  : need_modify_(true),
    gather_approx_(false),
    gather_histogram_(true)
  {}
  ObGlobalStatParam& operator=(const ObGlobalStatParam& other)
  {
    need_modify_ = other.need_modify_;
    gather_approx_ = other.gather_approx_;
    gather_histogram_ = other.gather_histogram_;
    return *this;
  }
  void set_gather_stat(const bool gather_approx)
  {
    need_modify_ = true;
    gather_approx_ = gather_approx;
    gather_histogram_ = true;
  }
  void reset_gather_stat()
  {
    need_modify_ = false;
    gather_approx_ = false;
    gather_histogram_ = false;
  }
  TO_STRING_KV(K_(need_modify),
               K_(gather_approx),
               K_(gather_histogram));
  bool need_modify_;
  bool gather_approx_;
  bool gather_histogram_;
};
struct ObPartitionStatParam
{
  ObPartitionStatParam(share::schema::ObPartitionFuncType type)
  : part_type_(type),
    need_modify_(true),
    can_use_approx_(false),
    gather_histogram_(true)
  {}
  ObPartitionStatParam& operator=(const ObPartitionStatParam& other)
  {
    part_type_ = other.part_type_;
    need_modify_ = other.need_modify_;
    gather_histogram_ = other.gather_histogram_;
    return *this;
  }
  void assign_without_part_type(const ObPartitionStatParam& other)
  {
    need_modify_ = other.need_modify_;
    gather_histogram_ = other.gather_histogram_;
  }
  void set_gather_stat(const bool can_use_approx = false)
  {
    need_modify_ = true;
    can_use_approx_ = can_use_approx;
    gather_histogram_ = true;
  }
  void reset_gather_stat()
  {
    need_modify_ = false;
    can_use_approx_ = false;
    gather_histogram_ = false;
  }
  TO_STRING_KV(K_(part_type),
               K_(need_modify),
               K_(can_use_approx),
               K_(gather_histogram));
  share::schema::ObPartitionFuncType part_type_;
  bool need_modify_;
  bool can_use_approx_;
  bool gather_histogram_;
};

enum SampleType
{
  RowSample,
  PercentSample,
  InvalidSample
};

struct ObAnalyzeSampleInfo
{
  ObAnalyzeSampleInfo()
    : is_sample_(false),
      is_block_sample_(false),
      sample_type_(SampleType::InvalidSample),
      sample_value_(0)
  {}

  virtual ~ObAnalyzeSampleInfo() {}

  void set_percent(double percent);
  void set_rows(double row_num);
  void set_is_block_sample(bool is_block_sample) { is_block_sample_ = is_block_sample; }

  TO_STRING_KV(K_(is_sample),
               K_(is_block_sample),
               K_(sample_type),
               K_(sample_value));
  bool is_sample_;
  bool is_block_sample_;
  SampleType sample_type_;
  double sample_value_;
};

struct ObColumnStatParam {
  inline void set_size_manual() { size_mode_ = 1; }
  inline void set_size_auto() { size_mode_ = 2; }
  inline void set_size_repeat() { size_mode_ = 3; }
  inline void set_size_skewonly() { size_mode_ = 4; }
  inline bool is_size_manual() const { return size_mode_ == 1; }
  inline bool is_size_auto() const { return size_mode_ == 2; }
  inline bool is_size_repeat() const { return size_mode_ == 3; }
  inline bool is_size_skewonly() const { return size_mode_ == 4; }
  inline void set_is_index_column() { column_attribute_ |= ColumnAttrFlag::IS_INDEX_COL; }
  inline void set_is_hidden_column() { column_attribute_ |= ColumnAttrFlag::IS_HIDDEN_COL; }
  inline void set_is_unique_column() { column_attribute_ |= ColumnAttrFlag::IS_UNIQUE_COL; }
  inline void set_is_not_null_column() { column_attribute_ |= ColumnAttrFlag::IS_NOT_NULL_COL; }
  inline bool is_index_column() const { return column_attribute_ & ColumnAttrFlag::IS_INDEX_COL; }
  inline bool is_hidden_column() const { return column_attribute_ & ColumnAttrFlag::IS_HIDDEN_COL; }
  inline bool is_unique_column() const { return column_attribute_ & ColumnAttrFlag::IS_UNIQUE_COL; }
  inline bool is_not_null_column() const { return column_attribute_ & ColumnAttrFlag::IS_NOT_NULL_COL; }
  inline void set_valid_opt_col() { gather_flag_ |= ColumnGatherFlag::VALID_OPT_COL; }
  inline void set_need_basic_stat() { gather_flag_ |= ColumnGatherFlag::NEED_BASIC_STAT; }
  inline void set_need_avg_len() { gather_flag_ |= ColumnGatherFlag::NEED_AVG_LEN; }
  inline bool is_valid_opt_col() const { return gather_flag_ & ColumnGatherFlag::VALID_OPT_COL; }
  inline bool need_basic_stat() const { return gather_flag_ & ColumnGatherFlag::NEED_BASIC_STAT; }
  inline bool need_avg_len() const { return gather_flag_ & ColumnGatherFlag::NEED_AVG_LEN; }
  inline bool need_col_stat() const { return gather_flag_ != ColumnGatherFlag::NO_NEED_STAT; }

  ObString column_name_;
  uint64_t column_id_;
  common::ObCollationType cs_type_;

  int64_t size_mode_;
  int64_t bucket_num_;
  int64_t column_attribute_;
  int64_t column_usage_flag_;
  int64_t gather_flag_;

  static bool is_valid_opt_col_type(const ObObjType type);
  static bool is_valid_avglen_type(const ObObjType type);
  static const int64_t DEFAULT_HISTOGRAM_BUCKET_NUM;

  TO_STRING_KV(K_(column_name),
               K_(column_id),
               K_(cs_type),
               K_(size_mode),
               K_(bucket_num),
               K_(column_attribute),
               K_(column_usage_flag),
               K_(gather_flag));
};

struct ObColumnGroupStatParam {
  ObColumnGroupStatParam() : column_group_id_(0), column_id_arr_() {}
  uint64_t column_group_id_;
  ObArray<uint64_t> column_id_arr_;
  TO_STRING_KV(K(column_group_id_), K(column_id_arr_));
};

struct ObTableStatParam {
  static const int64_t INVALID_GLOBAL_PART_ID = -2;
  static const int64_t DEFAULT_DATA_PART_ID = -1;

  ObTableStatParam() : tenant_id_(0),
    db_name_(),
    db_id_(OB_INVALID_ID),
    tab_name_(),
    table_id_(OB_INVALID_ID),
    part_level_(share::schema::ObPartitionLevel::PARTITION_LEVEL_ZERO),
    global_stat_param_(),
    part_stat_param_(share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_MAX),
    subpart_stat_param_(share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_MAX),
    part_name_(),
    part_infos_(),
    subpart_infos_(),
    approx_part_infos_(),
    sample_info_(),
    method_opt_(),
    degree_(1),
    granularity_(),
    cascade_(false),
    stat_tab_(),
    stat_id_(),
    stat_own_(),
    column_params_(),
    no_invalidate_(false),
    force_(false),
    no_regather_partition_ids_(),
    is_subpart_name_(false),
    stat_category_(),
    tab_group_(),
    stattype_(StatTypeLocked::NULL_TYPE),
    need_approx_ndv_(true),
    is_index_stat_(false),
    data_table_name_(),
    is_global_index_(false),
    global_part_id_(-1),
    all_part_infos_(),
    all_subpart_infos_(),
    duration_time_(-1),
    global_tablet_id_(0),
    global_data_part_id_(INVALID_GLOBAL_PART_ID),
    data_table_id_(INVALID_GLOBAL_PART_ID),
    need_estimate_block_(true),
    is_temp_table_(false),
    allocator_(NULL),
    ref_table_type_(share::schema::ObTableType::MAX_TABLE_TYPE),
    column_group_params_(),
    online_sample_percent_(1.)
  {}

  int assign(const ObTableStatParam &other);
  int assign_common_property(const ObTableStatParam &other);

  bool is_block_sample() const
  {
    return sample_info_.is_block_sample_;
  }
  
  bool is_index_param() const
  {
    return global_data_part_id_ != INVALID_GLOBAL_PART_ID;
  }

  bool is_specify_partition_gather() const;

  bool is_specify_column_gather() const;

  int64_t get_need_gather_column() const;

  bool need_gather_stats() const { return global_stat_param_.need_modify_ ||
                                          part_stat_param_.need_modify_ ||
                                          subpart_stat_param_.need_modify_; }

  uint64_t tenant_id_;

  ObString db_name_;
  uint64_t db_id_;

  ObString tab_name_;
  uint64_t table_id_;
  share::schema::ObPartitionLevel part_level_;
  ObGlobalStatParam global_stat_param_;
  ObPartitionStatParam part_stat_param_;
  ObPartitionStatParam subpart_stat_param_;

  ObString part_name_;
  ObSEArray<PartInfo, 4> part_infos_;
  ObSEArray<PartInfo, 4> subpart_infos_;
  ObSEArray<PartInfo, 4> approx_part_infos_;

  ObAnalyzeSampleInfo sample_info_;
  ObString method_opt_;
  int64_t degree_;

  ObString granularity_;
  bool cascade_;

  ObString stat_tab_;
  ObString stat_id_; // what does stat identifer for
  ObString stat_own_; // stat owner

  ObSEArray<ObColumnStatParam, 4> column_params_;

  bool no_invalidate_;
  bool force_;
  ObSEArray<int64_t, 4> no_regather_partition_ids_;

  bool is_subpart_name_;
  ObString stat_category_;
  ObString tab_group_;
  StatTypeLocked stattype_;
  bool need_approx_ndv_;
  bool is_index_stat_;
  ObString data_table_name_;
  bool is_global_index_;
  int64_t global_part_id_;
  ObSEArray<PartInfo, 4> all_part_infos_;
  ObSEArray<PartInfo, 4> all_subpart_infos_;
  int64_t duration_time_;
  uint64_t global_tablet_id_;
  int64_t global_data_part_id_; // used to check wether table is locked, while gathering index stats.
  int64_t data_table_id_; // the data table id for index schema
  bool need_estimate_block_;//need estimate macro/micro block count
  bool is_temp_table_;
  common::ObIAllocator *allocator_;
  share::schema::ObTableType ref_table_type_;
  ObArray<ObColumnGroupStatParam> column_group_params_;
  double online_sample_percent_;

  TO_STRING_KV(K(tenant_id_),
               K(db_name_),
               K(db_id_),
               K(tab_name_),
               K(table_id_),
               K(part_name_),
               K(part_infos_),
               K(subpart_infos_),
               K(approx_part_infos_),
               K(sample_info_),
               K(method_opt_),
               K(degree_),
               K(global_stat_param_),
               K(part_stat_param_),
               K(subpart_stat_param_),
               K(granularity_),
               K(cascade_),
               K(stat_tab_),
               K(stat_id_),
               K(stat_own_),
               K(column_params_),
               K(no_invalidate_),
               K(force_),
               K(no_regather_partition_ids_),
               K(is_subpart_name_),
               K(stat_category_),
               K(tab_group_),
               K(stattype_),
               K(need_approx_ndv_),
               K(is_index_stat_),
               K(data_table_name_),
               K(is_global_index_),
               K(global_part_id_),
               K(all_part_infos_),
               K(all_subpart_infos_),
               K(duration_time_),
               K(global_tablet_id_),
               K(global_data_part_id_),
               K(data_table_id_),
               K(need_estimate_block_),
               K(is_temp_table_),
               K(ref_table_type_),
               K(column_group_params_),
               K(online_sample_percent_));
};

struct ObOptStatGatherParam {
  ObOptStatGatherParam () :
    tenant_id_(0),
    db_name_(),
    tab_name_(),
    table_id_(OB_INVALID_ID),
    stat_level_(INVALID_LEVEL),
    need_histogram_(false),
    sample_info_(),
    degree_(1),
    partition_infos_(),
    column_params_(),
    column_group_params_(),
    allocator_(NULL),
    partition_id_block_map_(NULL),
    gather_start_time_(0),
    stattype_(StatTypeLocked::NULL_TYPE),
    is_split_gather_(false),
    max_duration_time_(0),
    need_approx_ndv_(true),
    data_table_name_(),
    global_part_id_(-1),
    gather_vectorize_(DEFAULT_STAT_GATHER_VECTOR_BATCH_SIZE),
    sepcify_scn_(0),
    use_column_store_(false),
    is_specify_partition_(false)
  {}
  int assign(const ObOptStatGatherParam &other);
  int64_t get_need_gather_column() const;
  uint64_t tenant_id_;
  ObString db_name_;
  ObString tab_name_;
  uint64_t table_id_;
  StatLevel stat_level_;
  bool need_histogram_;
  ObAnalyzeSampleInfo sample_info_;
  int64_t degree_;
  ObSEArray<PartInfo, 4> partition_infos_;
  ObSEArray<ObColumnStatParam, 4> column_params_;
  ObArray<ObColumnGroupStatParam> column_group_params_;
  common::ObIAllocator *allocator_;
  const PartitionIdBlockMap *partition_id_block_map_;
  int64_t gather_start_time_;
  StatTypeLocked stattype_;
  bool is_split_gather_;
  int64_t max_duration_time_;
  bool need_approx_ndv_;
  ObString data_table_name_;
  int64_t global_part_id_;
  int64_t gather_vectorize_;
  uint64_t sepcify_scn_;
  bool use_column_store_;
  bool is_specify_partition_;

  TO_STRING_KV(K(tenant_id_),
               K(db_name_),
               K(tab_name_),
               K(table_id_),
               K(stat_level_),
               K(need_histogram_),
               K(sample_info_),
               K(degree_),
               K(partition_infos_),
               K(column_params_),
               K(column_group_params_),
               K(gather_start_time_),
               K(stattype_),
               K(is_split_gather_),
               K(max_duration_time_),
               K(need_approx_ndv_),
               K(data_table_name_),
               K(global_part_id_),
               K(gather_vectorize_),
               K(sepcify_scn_),
               K(use_column_store_),
               K(is_specify_partition_));
};

struct ObOptStat
{
  ObOptStat() : table_stat_(NULL), column_stats_() {
    column_stats_.set_attr(lib::ObMemAttr(MTL_ID(), "ObOptStat"));
  }
  virtual ~ObOptStat();
  ObOptTableStat *table_stat_;
  // turn the column stat into pointer
  ObArray<ObOptColumnStat *> column_stats_;
  TO_STRING_KV(K(table_stat_),
               K(column_stats_));
};

struct ObHistogramParam
{
  ObHistogramParam():
    epc_(0),
    minval_(NULL),
    maxval_(NULL),
    bkvals_(),
    novals_(),
    chvals_(),
    eavals_(),
    rpcnts_(),
    eavs_(0)
  {}
  int64_t epc_;                       //Number of buckets in histogram
  const ObObj *minval_;               //Minimum value
  const ObObj *maxval_;               //Maximum value
  ObSEArray<int64_t, 4> bkvals_;      //Array of bucket numbers
  ObSEArray<int64_t, 4> novals_;      //Array of normalized end point values
  ObSEArray<ObString, 4> chvals_;     //Array of dumped end point values
  ObSEArray<ObString, 4> eavals_;     //Array of end point actual values
  ObSEArray<int64_t, 4> rpcnts_;      //Array of end point value frequencies
  int64_t eavs_;                      //A number indicating whether actual end point values are needed
                                      //  in the histogram. If using the PREPARE_COLUMN_VALUES Procedures,
                                      //  this field will be automatically filled.
  TO_STRING_KV(K(epc_),
               K(minval_),
               K(maxval_),
               K(bkvals_),
               K(novals_),
               K(chvals_),
               K(eavals_),
               K(rpcnts_),
               K(eavs_));
};

struct ObSetTableStatParam
{
  ObTableStatParam table_param_;

  int64_t numrows_;
  int64_t numblks_;
  int64_t avgrlen_;
  int64_t flags_;
  int64_t cachedblk_;
  int64_t cachehit_;
  int64_t nummacroblks_;
  int64_t nummicroblks_;

  TO_STRING_KV(K(table_param_),
               K(numrows_),
               K(numblks_),
               K(avgrlen_),
               K(flags_),
               K(cachedblk_),
               K(cachehit_),
               K(nummacroblks_),
               K(nummicroblks_));
};

struct ObSetColumnStatParam
{
  ObSetColumnStatParam():
  table_param_(),
  distcnt_(0),
  density_(0.0),
  nullcnt_(0),
  hist_param_(),
  avgclen_(0),
  flags_(0),
  col_meta_()
  {}
  ObTableStatParam table_param_;
  int64_t distcnt_;
  double density_;
  int64_t nullcnt_;
  ObHistogramParam hist_param_;
  int64_t avgclen_;
  int64_t flags_;
  common::ObObjMeta col_meta_;

  TO_STRING_KV(K(table_param_),
               K(distcnt_),
               K(density_),
               K(nullcnt_),
               K(hist_param_),
               K(avgclen_),
               K(flags_),
               K(col_meta_));

};

struct ObSetSystemStatParam
{
  ObSetSystemStatParam()
  :tenant_id_(OB_INVALID_ID),
  name_(),
  value_(0)
  { }

  int64_t tenant_id_;
  ObString name_;
  int64_t value_;

  TO_STRING_KV(K(tenant_id_),
               K(name_),
               K(value_)
    );
};

struct ObPartitionStatInfo
{
  ObPartitionStatInfo():
    partition_id_(-1),
    row_cnt_(0),
    is_stat_locked_(false),
    is_no_dml_modified_(false),
    is_no_stale_(false)
  {}

  ObPartitionStatInfo(int64_t partition_id, int64_t row_cnt, bool is_locked, bool is_no_dml_modified):
    partition_id_(partition_id),
    row_cnt_(row_cnt),
    is_stat_locked_(is_locked),
    is_no_dml_modified_(is_no_dml_modified),
    is_no_stale_(false)
  {}

  int64_t partition_id_;
  int64_t row_cnt_;
  bool is_stat_locked_;
  bool is_no_dml_modified_;
  bool is_no_stale_;
  bool is_regather() const { return !is_stat_locked_ && !is_no_dml_modified_ && !is_no_stale_; }

  TO_STRING_KV(K(partition_id_),
               K(row_cnt_),
               K(is_stat_locked_),
               K(is_no_dml_modified_),
               K(is_no_stale_));
};

enum ObOptDmlStatType {
  TABLET_OPT_INSERT_STAT = 1,
  TABLET_OPT_UPDATE_STAT,
  TABLET_OPT_DELETE_STAT
};

struct ObOptDmlStat
{
  OB_UNIS_VERSION(1);
public:
  ObOptDmlStat ():
    tenant_id_(0),
    table_id_(common::OB_INVALID_ID),
    tablet_id_(0),
    insert_row_count_(0),
    update_row_count_(0),
    delete_row_count_(0)
  {}
  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t tablet_id_;
  int64_t insert_row_count_;
  int64_t update_row_count_;
  int64_t delete_row_count_;
  TO_STRING_KV(K(tenant_id_),
               K(table_id_),
               K(tablet_id_),
               K(insert_row_count_),
               K(update_row_count_),
               K(delete_row_count_));
};

enum OSG_TYPE
{
  NORMAL_OSG = 0,
  GATHER_OSG = 1,
  MERGE_OSG = 2
};

struct OSGPartInfo {
  OB_UNIS_VERSION(1);
public:
  OSGPartInfo() : part_id_(OB_INVALID_ID), tablet_id_(OB_INVALID_ID) {}
  ObObjectID part_id_;
  ObTabletID tablet_id_;
  TO_STRING_KV(K_(part_id), K_(tablet_id));
};
typedef common::hash::ObHashMap<ObObjectID, OSGPartInfo, common::hash::NoPthreadDefendMode> OSGPartMap;

}
}

#endif // OB_STAT_DEFINE_H
