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

#ifndef SRC_SQL_ENGINE_BASIC_OB_SELECT_INTO_OP_H_
#define SRC_SQL_ENGINE_BASIC_OB_SELECT_INTO_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_arrow_basic.h"
#include "lib/file/ob_file.h"
#include "share/backup/ob_backup_struct.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "sql/engine/table/ob_odps_jni_table_row_iter.h"

#ifdef OB_BUILD_CPP_ODPS
#include <odps/odps_tunnel.h>
#include <odps/odps_api.h>
#endif

#ifdef OB_BUILD_JNI_ODPS
#include "sql/engine/connector/ob_odps_jni_writer.h"
#include <arrow/api.h>
#include "sql/engine/basic/ob_arrow_basic.h"
#endif

#include <parquet/api/writer.h>
#include "sql/engine/basic/ob_select_into_basic.h"
#include "sql/engine/basic/ob_external_file_writer.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/engine/table/ob_odps_table_utils.h"

namespace oceanbase
{
namespace sql
{
class ObSelectIntoOpInput : public ObOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObSelectIntoOpInput(ObExecContext &ctx, const ObOpSpec &spec)
  : ObOpInput(ctx, spec),
    task_id_(common::OB_INVALID_ID),
    sqc_id_(common::OB_INVALID_ID)
  {}
  virtual ~ObSelectIntoOpInput() = default;
  virtual int init(ObTaskInfo &task_info) override
  {
    UNUSED(task_info);
    return common::OB_SUCCESS;
  }
  virtual void reset() override {}
  virtual void set_task_id(int64_t task_id) { task_id_ = task_id; }
  virtual void set_sqc_id(int64_t sqc_id) { sqc_id_ = sqc_id; }
  int64_t get_task_id() const { return task_id_; }
  int64_t get_sqc_id() const { return sqc_id_; }

  int64_t task_id_;
  int64_t sqc_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSelectIntoOpInput);
};

class ObSelectIntoSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObSelectIntoSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      into_type_(T_INTO_OUTFILE),
      user_vars_(alloc),
      outfile_name_(),
      field_str_(),
      line_str_(),
      closed_cht_(),
      is_optional_(false),
      select_exprs_(alloc),
      is_single_(true),
      max_file_size_(DEFAULT_MAX_FILE_SIZE),
      escaped_cht_(),
      cs_type_(CS_TYPE_INVALID),
      parallel_(1),
      file_partition_expr_(NULL),
      buffer_size_(DEFAULT_BUFFER_SIZE),
      is_overwrite_(false),
      external_properties_(alloc),
      external_partition_(alloc),
      alias_names_(alloc)
  {
  }

  ObItemType into_type_;
  common::ObFixedArray<common::ObString, common::ObIAllocator> user_vars_;
  common::ObObj outfile_name_;
  common::ObObj field_str_; // FARM COMPAT WHITELIST FOR filed_str_: renamed
  common::ObObj line_str_;
  // 431以下版本select into无法并行执行, 不会序列化算子, 修改closed_cht_类型不会导致升级兼容性问题
  common::ObObj closed_cht_; // FARM COMPAT WHITELIST FOR closed_cht_: change type
  bool is_optional_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> select_exprs_;
  bool is_single_;
  int64_t max_file_size_;
  common::ObObj escaped_cht_;
  common::ObCollationType cs_type_;
  int64_t parallel_;
  sql::ObExpr* file_partition_expr_;
  int64_t buffer_size_;
  bool is_overwrite_;
  ObExternalFileFormat::StringData external_properties_;
  ObExternalFileFormat::StringData external_partition_;
  ObExternalFileFormat::StringList alias_names_;
  static const int64_t DEFAULT_MAX_FILE_SIZE = 256LL * 1024 * 1024;
  static const int64_t DEFAULT_BUFFER_SIZE = 1LL * 1024 * 1024;
};

class ObSelectIntoOp : public ObOperator
{
public:
  ObSelectIntoOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
      top_limit_cnt_(INT64_MAX),
      is_first_(true),
      field_str_(),
      line_str_(),
      cs_type_(CS_TYPE_INVALID),
      basic_url_(),
      access_info_(NULL),
      file_location_(IntoFileLocation::SERVER_DISK),
      write_offset_(0),
      data_writer_(NULL),
      char_enclose_(0),
      char_escape_('\\'),
      has_enclose_(false),
      is_optional_(false),
      has_escape_(true),
      has_lob_(false),
      has_json_(false),
      has_coll_(false),
      print_params_(),
      escape_printer_(),
      do_partition_(false),
      json_buf_(NULL),
      json_buf_len_(0),
      shared_buf_(NULL),
      shared_buf_len_(0),
      use_shared_buf_(false),
      has_compress_(false),
      partition_map_(),
      curr_partition_num_(0),
      external_properties_(),
      format_type_(ObExternalFileFormat::FormatType::CSV_FORMAT),
      is_odps_cpp_table_(false),
      is_odps_java_table_(false),
#ifdef OB_BUILD_CPP_ODPS
      upload_(NULL),
      record_writer_(NULL),
#endif
#ifdef OB_BUILD_JNI_ODPS
      uploader_(),
      arrow_schema_(nullptr),
#endif
      block_id_(0),
      need_commit_(true),
      arrow_alloc_(),
      parquet_writer_schema_(nullptr),
      orc_schema_(nullptr),
      array_helpers_()
  {
  }

  // cs_type of ObString in ObEscapeInfo should be dst_cs_type
  struct ObEscapePrinter
  {
    ObEscapePrinter():
      enclose_(), escape_(), zero_(), field_terminator_(), line_terminator_(), convert_replacer_(),
      need_enclose_(false), do_encode_(false), do_escape_(false), print_hex_(false),
      ignore_convert_failed_(false) {}
    int operator() (const ObString &src_str, const ob_wc_t &unicode_value) {
      int ret = OB_SUCCESS;
      ObString dst_str = src_str;
      int result_len = 0;
      char tmp_buf[ObCharset::MAX_MB_LEN];
      if (do_encode_) {
        ret = ObCharset::wc_mb(coll_type_, unicode_value, tmp_buf, ObCharset::MAX_MB_LEN, result_len);
        if (OB_SUCC(ret)) {
          dst_str = ObString(result_len, tmp_buf);
        } else if (ret == OB_ERR_INCORRECT_STRING_VALUE && ignore_convert_failed_) {
          dst_str = convert_replacer_;
          ret = OB_SUCCESS;
        }
      }
      if (OB_FAIL(ret) || !do_escape_ || print_hex_) {
      } else if (dst_str.compare(zero_) == 0
                 || dst_str.compare(enclose_) == 0
                 || dst_str.compare(escape_) == 0
                 || (!need_enclose_ && (dst_str.compare(field_terminator_) == 0
                                        || dst_str.compare(line_terminator_) == 0))) {
        ret = databuff_memcpy(buf_, buf_len_, pos_, escape_.length(), escape_.ptr());
      }
      if (OB_FAIL(ret)) {
      } else if (print_hex_) {
        ret = hex_print(dst_str.ptr(), dst_str.length(), buf_, buf_len_, pos_);
      } else if (do_escape_ && dst_str.compare(zero_) == 0) {
        char zero = '0';
        ret = databuff_memcpy(buf_, buf_len_, pos_, 1, &zero);
      } else {
        ret = databuff_memcpy(buf_, buf_len_, pos_, dst_str.length(), dst_str.ptr());
      }
      return ret;
    }
    ObString enclose_;
    ObString escape_;
    ObString zero_;
    ObString field_terminator_;
    ObString line_terminator_;
    ObString convert_replacer_;
    ObCollationType coll_type_;
    bool need_enclose_;
    bool do_encode_;
    bool do_escape_;
    bool print_hex_;
    bool ignore_convert_failed_;
    char *buf_;
    int64_t buf_len_;
    int64_t pos_;
  };

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;

private:
  int init_env_common();
  int init_csv_env();
  void set_csv_format_options();
#ifdef OB_BUILD_CPP_ODPS
  int init_odps_tunnel();
  int into_odps();
  int into_odps_batch(const ObBatchRows &brs);
  int odps_commit_upload();
  int set_odps_column_value_mysql(apsara::odps::sdk::ODPSTableRecord &table_record,
                                  const ObDatum &datum,
                                  const ObDatumMeta &datum_meta,
                                  const ObObjMeta &obj_meta,
                                  uint32_t col_idx,
                                  const bool is_strict_mode,
                                  const ObDateSqlMode date_sql_mode);
  int set_odps_column_value_oracle(apsara::odps::sdk::ODPSTableRecord &table_record,
                                   const ObDatum &datum,
                                   const ObDatumMeta &datum_meta,
                                   const ObObjMeta &obj_meta,
                                   uint32_t col_idx);
#endif
#ifdef OB_BUILD_JNI_ODPS
  int init_odps_jni_tunnel();
  int into_odps_jni();
  int into_odps_jni_batch(const ObBatchRows &brs);
  int odps_jni_commit_upload();

  int create_odps_schema();

  int into_odps_jni_batch_one_col(int64_t col_idx, ObOdpsJniConnector::OdpsType odps_type, arrow::Field &arrow_field,
      ObDatumMeta &meta, ObObjMeta &obj_meta, ObIVector &expr_vector, arrow::ArrayBuilder *builder,
      const ObBatchRows &brs, int &act_cnt, ObIAllocator &alloc, const bool is_strict_mode, const ObDateSqlMode date_sql_mode);

  int set_odps_column_value_mysql_jni(arrow::ArrayBuilder *builder,
                                                ObOdpsJniConnector::OdpsType odps_type,
                                                const ObDatum &datum,
                                                const ObDatumMeta &datum_meta,
                                                const ObObjMeta &obj_meta,
                                                arrow::Field &arrow_field,
                                                uint32_t col_idx,
                                                const bool is_strict_mode,
                                                const ObDateSqlMode date_sql_mode);

  int set_odps_column_value_oracle_jni(arrow::ArrayBuilder *builder,
                                                 ObOdpsJniConnector::OdpsType odps_type,
                                                 const ObDatum &datum,
                                                 const ObDatumMeta &datum_meta,
                                                 const ObObjMeta &obj_meta,
                                                 arrow::Field &arrow_field,
                                                 uint32_t col_idx);
#endif

  int decimal_or_number_to_int64(const ObDatum &datum, const ObDatumMeta &datum_meta, int64_t &res);
  int decimal_to_string(const ObDatum &datum,
                        const ObDatumMeta &datum_meta,
                        std::string &res,
                        ObIAllocator &allocator);
  int get_row_str(const int64_t buf_len, bool is_first_row, char *buf, int64_t &pos);
  int into_dumpfile(ObExternalFileWriter *data_writer);
  int into_outfile(ObExternalFileWriter *data_writer);
  int into_outfile_batch_csv(const ObBatchRows &brs, ObExternalFileWriter *data_writer);
  int extract_fisrt_wchar_from_varhcar(const ObObj &obj, int32_t &wchar);
  int print_wchar_to_buf(char *buf,
                         const int64_t buf_len,
                         int64_t &pos,
                         int32_t wchar,
                         ObString &str,
                         ObCollationType coll_type);
  int print_field(const ObObj &obj, ObCsvFileWriter &data_writer);
  int print_lob_field(const ObObj &obj,
                      const ObExpr &expr,
                      const ObDatum &datum,
                      ObCsvFileWriter &data_writer);
  int get_buf(char* &buf, int64_t &buf_len, int64_t &pos, ObCsvFileWriter &data_writer);
  int use_shared_buf(ObCsvFileWriter &data_writer, char* &buf, int64_t &buf_len, int64_t &pos);
  int resize_buf(char* &buf,
                 int64_t &buf_len,
                 int64_t &pos,
                 int64_t curr_pos,
                 bool is_json = false);
  int resize_or_flush_shared_buf(ObCsvFileWriter &data_writer,
                                 char* &buf,
                                 int64_t &buf_len,
                                 int64_t &pos);
  int check_buf_sufficient(ObCsvFileWriter &data_writer,
                           char* &buf,
                           int64_t &buf_len,
                           int64_t &pos,
                           int64_t str_len);
  int write_obj_to_file(const ObObj &obj, ObCsvFileWriter &data_writer, bool need_escape = false);
  int print_str_or_json_with_escape(const ObObj &obj, ObCsvFileWriter &data_writer);
  int print_normal_obj_without_escape(const ObObj &obj, ObCsvFileWriter &data_writer);
  int print_json_to_json_buf(const ObObj &obj,
                             char* &buf,
                             int64_t &buf_len,
                             int64_t &pos,
                             ObCsvFileWriter &data_writer);
  int write_single_char_to_file(const char *wchar, ObCsvFileWriter &data_writer);
  int write_lob_to_file(const ObObj &obj,
                        const ObExpr &expr,
                        const ObDatum &datum,
                        ObCsvFileWriter &data_writer);
  int into_varlist();
  int calc_next_file_path(ObExternalFileWriter &data_writer);
  int calc_first_file_path(ObString &path);
  int calc_file_path_with_partition(ObString partition, ObExternalFileWriter &data_writer);
  int check_csv_file_size(ObCsvFileWriter &data_writer);
  int split_file(ObExternalFileWriter &data_writer);
  int prepare_escape_printer();
  int check_has_lob_or_json();
  int calc_url_and_set_access_info();
  int create_shared_buffer_for_data_writer();
  int create_the_only_data_writer(ObExternalFileWriter *&data_writer);
  int new_data_writer(ObExternalFileWriter *&data_writer);
  int get_data_writer_for_partition(const ObString &partition_str, ObExternalFileWriter *&data_writer);
  char *get_json_buf() { return json_buf_; }
  int64_t get_json_buf_len() { return json_buf_len_; }
  char *get_shared_buf() { return shared_buf_; }
  int64_t get_shared_buf_len() { return shared_buf_len_; }

  // methods for handling parquet
  int init_parquet_env();
  int get_parquet_logical_type(
      std::shared_ptr<const parquet::LogicalType> &logical_type,
      const ObObjType &obj_type, const int32_t precision, const int32_t scale);
  int get_parquet_physical_type(parquet::Type::type &physical_type, const ObObjType &obj_type);
  int calc_parquet_decimal_length(int precision);
  int setup_parquet_schema();
  int into_outfile_batch_parquet(const ObBatchRows &brs_, ObExternalFileWriter *data_writer);
  int check_parquet_file_size(ObParquetFileWriter &data_writer);
  int build_parquet_cell(parquet::RowGroupWriter* rg_writer,
                         const ObDatumMeta &datum_meta,
                         const ObObjMeta &obj_meta,
                         const common::ObIVector* expr_vector,
                         int64_t col_idx,
                         int64_t row_idx,
                         int64_t row_offset,
                         int64_t &value_offset,
                         int16_t* definition_levels,
                         ObIAllocator &allocator,
                         void* value_batch,
                         const bool is_strict_mode,
                         const ObDateSqlMode date_sql_mode);
  int calc_parquet_decimal_array(const common::ObIVector* expr_vector,
                                 int row_idx,
                                 const ObDatumMeta &datum_meta,
                                 int parquet_decimal_length,
                                 uint8_t* parquet_flba_ptr);
  int calc_byte_array(const common::ObIVector* expr_vector,
                      int row_idx,
                      const ObDatumMeta &datum_meta,
                      const ObObjMeta &obj_meta,
                      ObIAllocator &allocator,
                      char* &buf,
                      uint32_t &res_len);
  int oracle_timestamp_to_int96(const common::ObIVector* expr_vector,
                                int64_t row_idx,
                                const ObDatumMeta &datum_meta,
                                parquet::Int96 &res);
  int init_orc_env();
  int setup_orc_schema();
  int orc_type_mapping_of_ob_type(ObDatumMeta& meta, int max_length, std::unique_ptr<orc::Type>& orc_type);
  int create_orc_schema(std::unique_ptr<orc::Type> &schema);
  int into_outfile_batch_orc(const ObBatchRows &brs, ObExternalFileWriter *data_writer);
  int build_orc_cell(const ObDatumMeta &datum_meta,
                     const ObObjMeta &obj_meta,
                     const common::ObIVector* expr_vector,
                     int64_t col_idx,
                     int64_t row_idx,
                     int64_t row_offset,
                     orc::ColumnVectorBatch* col_vector_batch,
                     ObIAllocator &allocator,
                     const bool is_strict_mode,
                     const ObDateSqlMode date_sql_mode);
  int check_orc_file_size(ObOrcFileWriter &data_writer);
  int get_data_from_expr_vector(const common::ObIVector* expr_vector,
                                int row_idx,
                                ObObjType type,
                                int64_t &value,
                                const bool is_strict_mode,
                                const ObDateSqlMode date_sql_mode);
  bool file_need_split(int64_t file_size);
  int check_oracle_number(ObObjType obj_type, int16_t &precision, int8_t scale);
  static bool day_number_checker(int32_t days);

  #ifdef OB_BUILD_CPP_ODPS
  int recursive_fill_list_record(shared_ptr<apsara::odps::sdk::ODPSArray> odps_array,
                                 ObODPSArrayHelper *array_helper,
                                 ObIAllocator &alloc,
                                 const bool is_strict_mode,
                                 const ObDateSqlMode date_sql_mode);
  #endif
  #ifdef OB_BUILD_JNI_ODPS
  int recursive_fill_list_builder(arrow::ListBuilder *list_builder,
                                  arrow::ArrayBuilder *child_builder,
                                  ObODPSArrayHelper *array_helper,
                                  ObIAllocator &alloc,
                                  const bool is_strict_mode,
                                  const ObDateSqlMode date_sql_mode);
  #endif
public:
  static int check_secure_file_path(ObString file_name);
private:
  int64_t top_limit_cnt_;
  bool is_first_;
  ObObj field_str_;
  ObObj line_str_;
  ObObj file_name_;
  common::ObCollationType cs_type_;
  ObString basic_url_; // url without partition expr
  common::ObObjectStorageInfo *access_info_;
  share::ObBackupStorageInfo backup_info_;
  share::ObHDFSStorageInfo hdfs_info_;
  IntoFileLocation file_location_;
  int64_t write_offset_;
  ObExternalFileWriter* data_writer_;
  char char_enclose_;
  char char_escape_;
  bool has_enclose_;
  bool is_optional_;
  bool has_escape_;
  bool has_lob_;
  bool has_json_;
  bool has_coll_;
  common::ObObjPrintParams print_params_;
  ObEscapePrinter escape_printer_;
  bool do_partition_;
  char *json_buf_;  //json需要多一个buffer用来放转义前的string
  int64_t json_buf_len_;
  char *shared_buf_;
  int64_t shared_buf_len_;
  bool use_shared_buf_;
  bool has_compress_;
  typedef common::hash::ObHashMap<common::ObString, ObExternalFileWriter*, hash::NoPthreadDefendMode> ObPartitionWriterMap;
  ObPartitionWriterMap partition_map_;
  int curr_partition_num_;
  ObExternalFileFormat external_properties_;
  ObExternalFileFormat::FormatType format_type_;
  bool is_odps_cpp_table_;
  bool is_odps_java_table_;
#ifdef OB_BUILD_CPP_ODPS
  apsara::odps::sdk::IUploadPtr upload_;
  apsara::odps::sdk::IRecordWriterPtr record_writer_;
#endif
#ifdef OB_BUILD_JNI_ODPS
  ObOdpsJniUploaderMgr::OdpsUploader uploader_;
  std::shared_ptr<arrow::Schema> arrow_schema_;
#endif
  uint32_t block_id_;
  bool need_commit_;
  // Handle parquet variables
  ObArrowMemPool arrow_alloc_;
  std::shared_ptr<parquet::schema::GroupNode> parquet_writer_schema_;
  static const int64_t SHARED_BUFFER_SIZE = 2LL * 1024 * 1024;
  static const int64_t MAX_OSS_FILE_SIZE = 5LL * 1024 * 1024 * 1024;
  static const int32_t ODPS_DATE_MIN_VAL = -719162; // '0001-1-1'

  orc::WriterOptions options_;
  ObOrcMemPool orc_alloc_;
  std::unique_ptr<orc::Type> orc_schema_;
  ObSEArray<ObODPSArrayHelper*, 8> array_helpers_;
};


}
}
#endif /* SRC_SQL_ENGINE_BASIC_OB_SELECT_INTO_OP_H_ */
