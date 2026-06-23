/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_CATALOG_STAT_DEFINE_H
#define OB_CATALOG_STAT_DEFINE_H

#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array_wrap.h"
#include "lib/string/ob_string.h"
#include "share/stat/ob_stat_define.h"
#include "share/stat/catalog/ob_opt_catalog_table_stat.h"
#include "share/stat/catalog/ob_opt_catalog_column_stat.h"
#include "share/ob_catalog_ext_partition_info.h"
#include "sql/engine/cmd/ob_load_data_parser.h"

namespace oceanbase
{
namespace common
{

/**
 * @brief ObCatalogColumnStatParam
 * Catalog table column statistics parameter structure
 * Dedicated for catalog tables only, separate from ObColumnStatParam (which is for internal tables)
 *
 * Key differences from ObColumnStatParam:
 * - Focused on catalog table needs only
 * - Excludes internal table specific fields (if any)
 * - All related methods should be catalog-specific
 */
struct ObCatalogColumnStatParam
{
  int assign(const ObCatalogColumnStatParam &other);

  // Column identifiers
  ObString column_name_;
  common::ObCollationType cs_type_;

  // Statistics gathering options
  int64_t size_mode_;
  int64_t bucket_num_;
  int64_t column_attribute_;
  int64_t gather_flag_;
  ObObjType column_type_;
  ObNdvScaleAlgo ndv_scale_algo_;

  // Helper methods (same as ObColumnStatParam)
  inline void set_size_manual() { size_mode_ = 1; }
  inline void set_size_auto() { size_mode_ = 2; }
  inline void set_size_repeat() { size_mode_ = 3; }
  inline void set_size_skewonly() { size_mode_ = 4; }
  inline bool is_size_manual() const { return size_mode_ == 1; }
  inline bool is_size_auto() const { return size_mode_ == 2; }
  inline bool is_size_repeat() const { return size_mode_ == 3; }
  inline bool is_size_skewonly() const { return size_mode_ == 4; }

  inline void set_is_unique_column() { column_attribute_ |= ColumnAttrFlag::IS_UNIQUE_COL; }
  inline void set_is_not_null_column() { column_attribute_ |= ColumnAttrFlag::IS_NOT_NULL_COL; }
  inline void set_is_text_column() { column_attribute_ |= ColumnAttrFlag::IS_TEXT_COL; }
  inline void set_is_string_column() { column_attribute_ |= ColumnAttrFlag::IS_STRING_COL; }
  inline bool is_unique_column() const { return column_attribute_ & ColumnAttrFlag::IS_UNIQUE_COL; }
  inline bool is_virtual_column() const { return column_attribute_ & ColumnAttrFlag::IS_VIRTUAL_COL; }
  inline bool is_not_null_column() const { return column_attribute_ & ColumnAttrFlag::IS_NOT_NULL_COL; }
  inline bool is_text_column() const { return column_attribute_ & ColumnAttrFlag::IS_TEXT_COL; }
  inline bool is_string_column() const { return column_attribute_ & ColumnAttrFlag::IS_STRING_COL; }
  inline void unset_text_column() { column_attribute_ &= ~ColumnAttrFlag::IS_TEXT_COL; }
  inline void set_is_virtual_col() { column_attribute_ |= ColumnAttrFlag::IS_VIRTUAL_COL; }

  inline void set_valid_opt_col() { gather_flag_ |= ColumnGatherFlag::VALID_OPT_COL; }
  inline void set_need_basic_stat() { gather_flag_ |= ColumnGatherFlag::NEED_BASIC_STAT; }
  inline void set_need_avg_len() { gather_flag_ |= ColumnGatherFlag::NEED_AVG_LEN; }
  inline void set_need_refine_min_max() { gather_flag_ |= ColumnGatherFlag::NEED_REFINE_MIN_MAX; }
  inline void set_need_cs_refine_min_max() { gather_flag_ |= ColumnGatherFlag::NEED_CS_REFINE_MIN_MAX; }
  inline bool is_valid_opt_col() const { return gather_flag_ & ColumnGatherFlag::VALID_OPT_COL; }
  inline bool need_basic_stat() const { return gather_flag_ & ColumnGatherFlag::NEED_BASIC_STAT; }
  inline bool need_avg_len() const { return gather_flag_ & ColumnGatherFlag::NEED_AVG_LEN; }
  inline bool need_col_stat() const { return gather_flag_ != ColumnGatherFlag::NO_NEED_STAT; }
  inline void unset_need_basic_stat() { gather_flag_ &= ~ColumnGatherFlag::NEED_BASIC_STAT; }
  inline bool need_refine_min_max() const { return gather_flag_ & ColumnGatherFlag::NEED_REFINE_MIN_MAX; }
  inline bool need_cs_refine_min_max() const { return gather_flag_ & ColumnGatherFlag::NEED_CS_REFINE_MIN_MAX; }

  static bool is_valid_opt_col_type(const ObObjType type, bool is_online_stat = false);
  static bool is_valid_avglen_type(const ObObjType type);
  static bool is_valid_refine_min_max_type(const ObObjType type);
  static const int64_t DEFAULT_HISTOGRAM_BUCKET_NUM;

  TO_STRING_KV(K_(column_name),
               K_(cs_type),
               K_(size_mode),
               K_(bucket_num),
               K_(column_attribute),
               K_(gather_flag),
               K_(ndv_scale_algo));
};

struct ObCatalogTableIdentity
{
  ObCatalogTableIdentity()
      : tenant_id_(0), catalog_id_(OB_INVALID_ID), catalog_name_(), db_name_(), tab_name_()
  {
  }

  int assign(const ObCatalogTableIdentity &other);

  uint64_t tenant_id_;
  uint64_t catalog_id_;
  ObString catalog_name_;
  ObString db_name_;
  ObString tab_name_;

  TO_STRING_KV(K_(tenant_id),
               K_(catalog_id),
               K_(catalog_name),
               K_(db_name),
               K_(tab_name));
};

struct ObCatalogExternalAccessInfo
{
  ObCatalogExternalAccessInfo()
      : location_(), access_info_(),
        format_type_(sql::ObExternalFileFormat::INVALID_FORMAT),
        lake_table_format_(share::ObLakeTableFormat::INVALID)
  {
  }

  int assign(const ObCatalogExternalAccessInfo &other);

  ObString location_;
  ObString access_info_;
  sql::ObExternalFileFormat::FormatType format_type_;
  share::ObLakeTableFormat lake_table_format_;

  TO_STRING_KV(K_(location),
               K_(access_info),
               K_(format_type),
               K_(lake_table_format));
};

/**
 * @brief ObCatalogFilteredPartStats
 * Accumulated statistics of filtered partitions (partitions skipped due to freshness)
 * These are accumulated during filtering and passed to gather phase for global stats derivation
 */
struct ObCatalogFilteredPartStats
{
  ObCatalogFilteredPartStats()
      : filtered_file_count_(0), filtered_data_size_(0),
        filtered_last_analyzed_(0), filtered_schema_version_(0)
  {
  }

  void reset()
  {
    filtered_file_count_ = 0;
    filtered_data_size_ = 0;
    filtered_last_analyzed_ = 0;
    filtered_schema_version_ = 0;
  }

  int assign(const ObCatalogFilteredPartStats &other);

  int64_t filtered_file_count_;
  int64_t filtered_data_size_;
  int64_t filtered_last_analyzed_;
  int64_t filtered_schema_version_;

  TO_STRING_KV(K_(filtered_file_count),
               K_(filtered_data_size),
               K_(filtered_last_analyzed),
               K_(filtered_schema_version));
};

struct ObCatalogAnalyzeSampleInfo
{
  enum SampleMode
  {
    ROW = 0,
    BLOCK,
    FAST,
    FILE,
    INVALID_MODE
  };

  ObCatalogAnalyzeSampleInfo()
      : is_sample_(false),
        mode_(ROW),
        percent_(0.0),
        seed_(-1)
  {
  }

  int assign(const ObCatalogAnalyzeSampleInfo &other);
  void reset();
  void set_percent(double percent);
  void set_sample_mode(const SampleMode mode);
  bool is_specify_sample() const;
  bool is_row_sample() const { return ROW == mode_; }
  bool is_block_sample() const { return BLOCK == mode_; }
  bool is_fast_sample() const { return FAST == mode_; }
  bool is_file_sample() const { return FILE == mode_; }
  bool is_block_family_sample() const { return is_block_sample() || is_fast_sample(); }
  bool is_valid_mode() const { return mode_ >= ROW && mode_ < INVALID_MODE; }

  TO_STRING_KV(K_(is_sample),
               K_(mode),
               K_(percent),
               K_(seed));

  bool is_sample_;
  SampleMode mode_;
  double percent_;
  int64_t seed_;
};

struct ObCatalogGatherOptions
{
  ObCatalogGatherOptions()
      : sample_info_(), method_opt_(), degree_(1), granularity_(),
        granularity_type_(GRANULARITY_AUTO), stat_own_(), force_(false),
        stattype_(StatTypeLocked::NULL_TYPE), need_approx_ndv_(true),
        need_refine_min_max_(false), duration_time_(-1), consumer_group_id_(0)
  {
  }

  int assign(const ObCatalogGatherOptions &other);

  ObCatalogAnalyzeSampleInfo sample_info_;
  ObString method_opt_;
  int64_t degree_;
  ObString granularity_;
  ObGranularityType granularity_type_;
  ObString stat_own_;
  bool force_;
  StatTypeLocked stattype_;
  bool need_approx_ndv_;
  bool need_refine_min_max_;
  int64_t duration_time_;
  int64_t consumer_group_id_;

  // Helper
  bool is_specify_sample() const { return sample_info_.is_specify_sample(); }

  TO_STRING_KV(K_(sample_info),
               K_(method_opt),
               K_(degree),
               K_(granularity),
               K_(granularity_type),
               K_(stat_own),
               K_(force),
               K_(stattype),
               K_(need_approx_ndv),
               K_(need_refine_min_max),
               K_(duration_time),
               K_(consumer_group_id));
};

/**
 * @brief ObCatalogTableStatParam
 * Catalog table statistics parameter structure
 * Dedicated for catalog tables only, separate from ObTableStatParam (which is for internal tables)
 *
 * Sub-structures:
 * - table_identity_:   Table identification (tenant, catalog, db, table)
 * - external_info_:    External table access information (location, format, etc.)
 * - filtered_stats_:   Accumulated stats of filtered partitions during freshness check
 * - gather_options_:   Statistics gathering options (sample, degree, granularity, etc.)
 */
struct ObCatalogTableStatParam
{
  ObCatalogTableStatParam()
      : table_identity_(), external_info_(),
        part_name_(), part_cols_(),
        part_level_(share::schema::ObPartitionLevel::PARTITION_LEVEL_ZERO), global_stat_param_(),
        part_stat_param_(share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_MAX),
        part_infos_(), all_part_infos_(), column_params_(), gather_options_(),
        global_modified_ts_(-1), allocator_(NULL), filtered_stats_()
  {
  }

  int assign(const ObCatalogTableStatParam &other);

  // Table identity (tenant, catalog, db, table)
  ObCatalogTableIdentity table_identity_;

  // External table access information
  ObCatalogExternalAccessInfo external_info_;

  // Partition information
  ObString part_name_;
  ObSEArray<ObString, 4> part_cols_;
  share::schema::ObPartitionLevel part_level_;
  ObGlobalStatParam global_stat_param_;
  ObPartitionStatParam part_stat_param_;
  ObSEArray<ObCatalogExtPartitionInfo, 4> part_infos_;
  ObSEArray<ObCatalogExtPartitionInfo, 4> all_part_infos_;

  // Column parameters (catalog-specific)
  ObSEArray<ObCatalogColumnStatParam, 4> column_params_;

  // Statistics gathering options
  ObCatalogGatherOptions gather_options_;

  // Store the table modified time, store the root path modified time of non-part table.
  int64_t global_modified_ts_;

  common::ObIAllocator *allocator_;

  // Filtered partitions statistics
  ObCatalogFilteredPartStats filtered_stats_;

  // Helper methods
  bool is_specify_partition() const;
  bool is_specify_column() const;
  int64_t get_need_gather_column() const;
  bool need_gather_stats() const { return global_stat_param_.need_modify_ || part_stat_param_.need_modify_; }
  bool is_specify_sample() const { return gather_options_.is_specify_sample(); }
  int64_t get_global_modified_ts() { return global_modified_ts_; }

  TO_STRING_KV(K_(table_identity),
               K_(external_info),
               K_(part_name),
               K_(part_cols),
               K_(part_level),
               K_(global_stat_param),
               K_(part_stat_param),
               K_(part_infos),
               K_(all_part_infos),
               K_(column_params),
               K_(gather_options),
               K_(global_modified_ts),
               K_(filtered_stats));
};

struct ObOptCatalogStatGatherParam
{
  ObOptCatalogStatGatherParam()
      : table_identity_(), external_info_(),
        stat_level_(INVALID_LEVEL), part_cols_(), partition_infos_(), column_params_(),
        allocator_(NULL), gather_start_time_(0), max_duration_time_(0),
        stattype_(StatTypeLocked::NULL_TYPE), is_split_gather_(false),
        need_approx_ndv_(true), need_refine_min_max_(false), degree_(1),
        is_specify_partition_(false), gather_vectorize_(DEFAULT_STAT_GATHER_VECTOR_BATCH_SIZE),
        consumer_group_id_(0), sample_info_()
  {
  }

  int assign(const ObOptCatalogStatGatherParam &other);

  // Table identity (reused from ObCatalogTableIdentity)
  ObCatalogTableIdentity table_identity_;

  // External table access information (reused from ObCatalogExternalAccessInfo)
  ObCatalogExternalAccessInfo external_info_;

  // Statistics level
  StatLevel stat_level_;

  // Partition information (catalog tables use partition_value, not partition_id)
  ObArray<ObString> part_cols_;
  ObSEArray<ObCatalogExtPartitionInfo, 4> partition_infos_;

  // Column parameters (input: which columns to gather stats for, catalog-specific)
  ObArray<ObCatalogColumnStatParam> column_params_;

  // Memory allocator
  common::ObIAllocator *allocator_;

  // Time parameters
  int64_t gather_start_time_;
  int64_t max_duration_time_;

  // Statistics type
  StatTypeLocked stattype_;

  // Gathering options
  bool is_split_gather_;
  bool need_approx_ndv_;
  bool need_refine_min_max_;
  int64_t degree_;
  bool is_specify_partition_;
  int64_t gather_vectorize_;
  int64_t consumer_group_id_;

  ObCatalogAnalyzeSampleInfo sample_info_;

  TO_STRING_KV(K_(table_identity),
               K_(external_info),
               K_(stat_level),
               K_(part_cols),
               K_(partition_infos),
               K_(column_params),
               K_(gather_start_time),
               K_(stattype),
               K_(is_split_gather),
               K_(max_duration_time),
               K_(need_approx_ndv),
               K_(need_refine_min_max),
               K_(degree),
               K_(is_specify_partition),
               K_(gather_vectorize),
               K_(consumer_group_id),
               K_(sample_info));
};

/**
 * @brief ObOptCatalogStat
 * Catalog table statistics structure
 * Corresponds to ObOptStat for internal tables
 *
 * Key differences from ObOptStat:
 * - Uses ObOptCatalogTableStat instead of ObOptTableStat
 * - Uses ObOptCatalogColumnStat instead of ObOptColumnStat
 */
struct ObOptCatalogStat
{
  ObOptCatalogStat() : table_stat_(NULL), column_stats_()
  {
    column_stats_.set_attr(lib::ObMemAttr(MTL_ID(), "ObOptCatStat"));
  }
  virtual ~ObOptCatalogStat();
  share::ObOptCatalogTableStat *table_stat_;
  ObArray<share::ObOptCatalogColumnStat *> column_stats_;
  TO_STRING_KV(K(table_stat_), K(column_stats_));
};

/**
 * @brief ObLoadCatalogTableStatParam
 * Parameters for loading catalog table statistics
 * Used to reduce function parameter count (from 8 to 6)
 */
struct ObLoadCatalogTableStatParam
{
  ObLoadCatalogTableStatParam()
      : tenant_id_(OB_INVALID_ID), catalog_id_(OB_INVALID_ID), table_id_(OB_INVALID_ID)
  {
  }

  ObLoadCatalogTableStatParam(uint64_t tenant_id, uint64_t catalog_id, uint64_t table_id)
      : tenant_id_(tenant_id), catalog_id_(catalog_id), table_id_(table_id)
  {
  }

  bool is_valid() const
  {
    return OB_INVALID_ID != tenant_id_ && OB_INVALID_ID != catalog_id_
           && OB_INVALID_ID != table_id_;
  }

  uint64_t tenant_id_;
  uint64_t catalog_id_;
  uint64_t table_id_;

  TO_STRING_KV(K(tenant_id_), K(catalog_id_), K(table_id_));
};

} // end namespace common
} // end namespace oceanbase

#endif // OB_CATALOG_STAT_DEFINE_H
