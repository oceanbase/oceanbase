/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#ifndef OB_EXTERNAL_TABLE_ACCESS_SERVICE_H_
#define OB_EXTERNAL_TABLE_ACCESS_SERVICE_H_


#include "share/ob_i_tablet_scan.h"
#include "lib/file/ob_file.h"
#include "common/row/ob_row_iterator.h"
#include "storage/access/ob_dml_param.h"
#include "common/storage/ob_io_device.h"
#include "share/external_table/ob_hdfs_storage_info.h"
#include "sql/ob_sql_context.h"
#include "sql/engine/table/ob_file_prebuffer.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"
#include "sql/engine/basic/ob_lake_table_reader_profile.h"
#include "sql/engine/expr/ob_expr_get_path.h"

namespace oceanbase
{
namespace common
{
class ObIVector;
}

namespace share
{
class ObExternalTablePartInfoArray;
}

namespace sql {
class ObExprRegexpSessionVariables;
class ObDecompressor;
class ObIcebergDeleteBitmapBuilder;
// help.aliyun.com   maxcompute user-guide time-zone-configuration-operations
// 后续应该优化为系统配置项
const int32_t ODPS_JAVA_SDK_NO_OVERSEA_OFFSET = (8 * 60 * 60);
class ObExternalDataAccessDriver
{
public:
  ObExternalDataAccessDriver() : storage_type_(common::OB_STORAGE_MAX_TYPE), access_info_(nullptr), device_handle_(nullptr) {}
  ~ObExternalDataAccessDriver();
  int init(const common::ObString &location, const ObString &access_info);
  int open(const char *url);
  bool is_opened() const;
  int get_file_size(const common::ObString &url, int64_t &file_size);

  int get_file_sizes(const ObString &location, const ObIArray<ObString> &urls, ObIArray<int64_t> &file_sizes);
  int pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size);
  int get_directory_list(const common::ObString &path,
                         common::ObIArray<common::ObString> &file_urls,
                         common::ObIAllocator &allocator);
  common::ObStorageType get_storage_type() { return storage_type_; }
  void close();

  const char dummy_empty_char = '\0';
private:
  common::ObStorageType storage_type_;
  share::ObExternalTableStorageInfo backup_storage_info_;
  share::ObHDFSStorageInfo hdfs_storage_info_;
  common::ObObjectStorageInfo *access_info_;
  ObIODevice* device_handle_;
  ObIOFd fd_;
};

class ObExternalStreamFileReader final
{
public:
  // ObExternalStreamFileReader();
  ~ObExternalStreamFileReader();
  void reset();

  int init(const common::ObString &location,
           const ObString &access_info,
           ObCSVGeneralFormat::ObCSVCompression compression_format,
           ObIAllocator &allocator);

  int open(const ObString &filename);
  int64_t get_file_size() const;
  void close();

  /**
   * read data into buffer. decompress source data if need
   */
  int read(char *buf, int64_t buf_len, int64_t &read_size);
  bool eof();

  common::ObStorageType get_storage_type() { return data_access_driver_.get_storage_type(); }

  ObExternalDataAccessDriver &get_data_access_driver() { return data_access_driver_; }

private:
  int read_from_driver(char *buf, int64_t buf_len, int64_t &read_size);
  int read_decompress(char *buf, int64_t buf_len, int64_t &read_size);
  int read_compressed_data(); // read data from driver into compressed buffer

  /**
   * create the decompressor if need
   *
   * It's no need to create new decompressor if the compression_format is the seem as decompressor's.
   */
  int create_decompressor(ObCSVGeneralFormat::ObCSVCompression compression_format);

private:
  ObExternalDataAccessDriver data_access_driver_;
  bool    is_file_end_ = true;
  int64_t file_offset_ = 0;
  int64_t file_size_   = 0;

  ObDecompressor *decompressor_ = nullptr;
  char *  compressed_data_      = nullptr; /// compressed data buffer
  int64_t compress_data_size_   = 0;       /// the valid data size in compressed data buffer
  int64_t consumed_data_size_   = 0;       /// handled buffer size in the compressed data buffer
  int64_t uncompressed_size_    = 0;       /// decompressed size from compressed data

  ObIAllocator *allocator_ = nullptr;

  /// the compression format specified in `create external table` statement
  ObCSVGeneralFormat::ObCSVCompression compression_format_ = ObCSVGeneralFormat::ObCSVCompression::NONE;

  static const char * MEMORY_LABEL;
  static const int64_t COMPRESSED_DATA_BUFFER_SIZE;
};

class ObExternalIteratorState
{
public:
  ObExternalIteratorState() :
    file_idx_(0),
    part_id_(0),
    cur_file_id_(0),
    cur_line_number_(0),
    cur_file_url_(),
    part_list_val_(),
    batch_first_row_line_num_(0),
    is_delete_file_loaded_(false) {}

  virtual void reuse() {
    file_idx_ = 0;
    part_id_ = 0;
    cur_file_id_ = 0;
    cur_line_number_ = 0;
    cur_file_url_.reset();
    part_list_val_.reset();
    batch_first_row_line_num_ = 0;
    is_delete_file_loaded_ = false;
  }
  DECLARE_VIRTUAL_TO_STRING;
  int64_t file_idx_;
  int64_t part_id_;
  int64_t cur_file_id_;
  int64_t cur_line_number_;
  ObString cur_file_url_;
  ObNewRow part_list_val_;
  int64_t batch_first_row_line_num_;
  bool is_delete_file_loaded_;
};

struct ObExternalTableAccessOptions
{
  ObExternalTableAccessOptions() :
    enable_prebuffer_(false), enable_page_cache_(false), enable_disk_cache_(false), cache_options_()
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      enable_prebuffer_ = tenant_config->_enable_external_table_prefetch;
      enable_page_cache_ = tenant_config->_enable_external_table_memory_cache;
      enable_disk_cache_ = tenant_config->_enable_external_table_disk_cache;
    }
  }
  ObExternalTableAccessOptions(const bool enable_page_cache, const bool enable_disk_cache,
                               const ObFilePreBuffer::CacheOptions &options) :
    enable_prebuffer_(false),
    enable_page_cache_(enable_page_cache), enable_disk_cache_(enable_disk_cache),
    cache_options_(options)
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      enable_prebuffer_ = tenant_config->_enable_external_table_prefetch;
    }
  }
  static ObExternalTableAccessOptions defaults()
  {
    ObExternalTableAccessOptions options;
    options.cache_options_ = ObFilePreBuffer::CacheOptions::defaults();
    return options;
  }
  static ObExternalTableAccessOptions lazy_defaults()
  {
    ObExternalTableAccessOptions options;
    options.cache_options_ = ObFilePreBuffer::CacheOptions::lazy_defaults();
    return options;
  }
  static ObExternalTableAccessOptions disable_cache_defaults()
  {
    return ObExternalTableAccessOptions(/*enable_page_cache=*/false,
                                        /*enable_disk_cache=*/false,
                                        ObFilePreBuffer::CacheOptions::defaults());
  }
  TO_STRING_KV(K_(enable_prebuffer),
               K_(enable_page_cache),
               K_(enable_disk_cache),
               K_(cache_options));

  bool enable_prebuffer_;
  bool enable_page_cache_;
  bool enable_disk_cache_;
  ObFilePreBuffer::CacheOptions cache_options_;
};

class ObDiagnosisInfoProvider {
public:
  virtual ~ObDiagnosisInfoProvider() {}
  virtual bool is_diagnosis_supported() const { return false; }
  virtual int64_t get_cur_line_num() const { return 0; }
  virtual ObString get_cur_file_url() const { return ObString(); }
};

struct ObColumnDefaultValue {
  bool is_null_;
  void* batch_data_or_nulls_;
  int32_t *batch_len_;

  ObColumnDefaultValue() :
    is_null_(false), batch_data_or_nulls_(nullptr), batch_len_(nullptr) {}

  int assign(const ObColumnDefaultValue &other);
  TO_STRING_KV(K_(is_null), K_(batch_data_or_nulls), K_(batch_len));
};

class ObExternalTableRowIterator : public common::ObNewRowIterator, public ObDiagnosisInfoProvider {
public:
  friend class ObExternalTablePushdownFilter;
  ObExternalTableRowIterator() :
    scan_param_(nullptr), line_number_expr_(NULL), file_id_expr_(NULL), file_name_expr_(NULL),
    delete_bitmap_(nullptr), delete_bitmap_builder_(nullptr), reader_profile_(),
    need_partition_info_(false), file_column_exprs_(allocator_), mapping_column_ids_(allocator_),
    file_meta_column_exprs_(allocator_), column_need_conv_(allocator_)
  {}
  virtual int init(const storage::ObTableScanParam *scan_param);
  ~ObExternalTableRowIterator();

  static const std::string ICEBERG_ID_KEY;

  static int set_default_batch(const ObDatumMeta &datum_type,
                               const ObColumnDefaultValue &col_def,
                               ObIVector *vec);

protected:
  int init_exprs(const storage::ObTableScanParam *scan_param);
  int gen_ip_port(common::ObIAllocator &allocator);
  int calc_file_partition_list_value(const int64_t part_id,
                                     common::ObIAllocator &allocator,
                                     common::ObNewRow &value);
  int calc_file_part_list_value_by_array(const int64_t part_id,
                                        common::ObIAllocator &allocator,
                                        const share::ObExternalTablePartInfoArray *partition_array,
                                        common::ObNewRow &value);
  int fill_file_partition_expr(ObExpr *expr, common::ObNewRow &value, const int64_t row_count);
  int calc_exprs_for_rowid(const int64_t read_count, ObExternalIteratorState &state,
                           const bool update_state = true);
  bool is_dummy_file(const ObString &file_url);
  static inline bool text_type_length_is_valid_at_runtime(ObObjType type, int64_t text_data_length) {
    bool is_valid = false;
    if (ObTinyTextType == type && text_data_length <= OB_MAX_TINYTEXT_LENGTH) {
      is_valid = true;
    } else if (ObTextType == type && text_data_length <= OB_MAX_TEXT_LENGTH) {
      is_valid = true;
    } else if (ObMediumTextType == type && text_data_length <= OB_MAX_MEDIUMTEXT_LENGTH) {
      is_valid = true;
    } else if (ObLongTextType == type && text_data_length <= OB_MAX_LONGTEXT_LENGTH) {
      is_valid = true;
    } else if (ObJsonType == type && text_data_length <= OB_MAX_LONGTEXT_LENGTH) {
      is_valid = true;
    }
    return is_valid;
  }

  static inline bool varchar_length_is_valid_at_runtime(ObObjType type, ObCharsetType charset, int32_t judge_length, int32_t max_length) {
    return (type == ObVarcharType) && ((CHARSET_UTF8MB4 == charset && judge_length <= max_length)
              || (CHARSET_BINARY == charset && judge_length <= max_length));
  }

  OB_INLINE bool is_iceberg_lake_table() const
  {
    return scan_param_->lake_table_format_ == share::ObLakeTableFormat::ICEBERG;
  }

  OB_INLINE bool is_hive_lake_table() const
  {
    return scan_param_->lake_table_format_ == share::ObLakeTableFormat::HIVE;
  }

  OB_INLINE bool is_lake_table() const
  {
    return is_iceberg_lake_table() || is_hive_lake_table();
  }
  bool is_abs_url(ObString url)
  {
    ObString dst("://");
    if (0
        == ObCharset::instr(ObCollationType::CS_TYPE_UTF8MB4_BIN,
                            url.ptr(),
                            url.length(),
                            dst.ptr(),
                            dst.length())) {
      return false;
    } else {
      return true;
    }
  }

  int init_for_iceberg(ObExternalTableAccessOptions *options);
  int init_default_batch(ExprFixedArray &file_column_exprs);
  int build_delete_bitmap(const ObString &data_file_path, const int64_t task_idx);
  int check_can_skip_conv();
  ObExpr* get_column_expr_by_id(int64_t file_column_expr_idx);
  int generate_mapping_column_id(ObExpr* ext_file_column_expr,
                                int64_t file_column_expr_idx,
                                ObIArray<std::pair<uint64_t, uint64_t>> &mapping_column_ids);
protected:
  const storage::ObTableScanParam *scan_param_;
  //external table column exprs
  common::ObSEArray<ObExpr*, 16> column_exprs_;
  common::ObSEArray<bool, 16> column_sel_mask_;
  //hidden columns
  ObExpr *line_number_expr_;
  ObExpr *file_id_expr_;
  ObExpr *file_name_expr_;
  common::ObString ip_port_;
  ObRoaringBitmap* delete_bitmap_;
  common::ObArray<ObColumnDefaultValue> colid_default_value_arr_;
  ObIcebergDeleteBitmapBuilder* delete_bitmap_builder_;
  ObLakeTableReaderProfile reader_profile_;
  bool need_partition_info_;
  ExprFixedArray file_column_exprs_; //column value from file
  // file column expr index -> 1. column id 2. column expr index
  ObFixedArray<std::pair<uint64_t, uint64_t>, ObIAllocator> mapping_column_ids_;
  ExprFixedArray file_meta_column_exprs_; //column value from file meta
  common::ObFixedArray<bool, ObIAllocator> column_need_conv_;
private:
  ObArenaAllocator allocator_; // used by delete map now
};

class ObExternalTableAccessService : public common::ObITabletScan
{
public:
  ObExternalTableAccessService() {}
  virtual int table_scan(common::ObVTableScanParam &param, ObNewRowIterator *&result) override;
  virtual int table_rescan(ObVTableScanParam &param, ObNewRowIterator *result) override;
  virtual int reuse_scan_iter(const bool switch_param, ObNewRowIterator *iter) override;
  virtual int revert_scan_iter(ObNewRowIterator *iter) override;

private:

  DISALLOW_COPY_AND_ASSIGN(ObExternalTableAccessService);
};


}
}

#endif // OB_EXTERNAL_TABLE_ACCESS_SERVICE_H_
