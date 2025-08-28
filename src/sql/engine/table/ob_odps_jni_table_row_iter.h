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

#ifndef __SQL_OB_ODPS_JNI_TABLE_ROW_ITER_H__
#define __SQL_OB_ODPS_JNI_TABLE_ROW_ITER_H__
#ifdef OB_BUILD_JNI_ODPS

#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_se_array.h"

#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include "sql/engine/connector/ob_jni_scanner.h"
#include "sql/engine/connector/ob_jni_writer.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "sql/engine/table/ob_odps_table_utils.h"

namespace arrow {
class Array;
class Field;
}  // namespace arrow
namespace oceanbase {
namespace sql {

inline bool is_fixed_odps_type(const ObJniConnector::OdpsType type)
{
  return type == ObJniConnector::OdpsType::BOOLEAN ||
         type == ObJniConnector::OdpsType::TINYINT ||
         type == ObJniConnector::OdpsType::SMALLINT ||
         type == ObJniConnector::OdpsType::INT ||
         type == ObJniConnector::OdpsType::BIGINT ||
         type == ObJniConnector::OdpsType::FLOAT ||
         type == ObJniConnector::OdpsType::DOUBLE ||
         type == ObJniConnector::OdpsType::DECIMAL ||
         type == ObJniConnector::OdpsType::DATE ||
         type == ObJniConnector::OdpsType::DATETIME ||
         type == ObJniConnector::OdpsType::TIMESTAMP_NTZ ||
         type == ObJniConnector::OdpsType::TIMESTAMP;
}

inline bool is_variety_odps_type(const ObJniConnector::OdpsType type)
{
  return type == ObJniConnector::OdpsType::CHAR ||
         type == ObJniConnector::OdpsType::VARCHAR ||
         type == ObJniConnector::OdpsType::STRING ||
         type == ObJniConnector::OdpsType::BINARY ||
         type == ObJniConnector::OdpsType::JSON;
}

inline bool is_array_odps_type(const ObJniConnector::OdpsType type)
{
  return type == ObJniConnector::OdpsType::ARRAY;
}

class ObODPSJNITableRowIterator : public ObExternalTableRowIterator {
public:
  using MirrorOdpsJniColumn = ObJniConnector::MirrorOdpsJniColumn;
  using OdpsJNIColumn = ObJniConnector::OdpsJNIColumn;
  using OdpsJNIPartition = ObJniConnector::OdpsJNIPartition;
  using OdpsType = ObJniConnector::OdpsType;

  struct StateValues {
    StateValues()
        : task_idx_(-1),
          part_id_(0),
          start_(0),
          step_(0),
          count_(0),
          cur_line_number_(0),
          is_from_gi_pump_(false),
          odps_jni_scanner_(NULL),
          part_spec_()
    {}
    int reuse();
    TO_STRING_KV(K(task_idx_), K(part_id_), K(start_), K(step_), K(count_), K(cur_line_number_), K(is_from_gi_pump_),
        K(part_spec_));
    int64_t task_idx_;
    int64_t part_id_;
    int64_t start_;
    int64_t step_;
    int64_t count_;
    int64_t cur_line_number_;
    bool is_from_gi_pump_;
    JNIScannerPtr odps_jni_scanner_;
    ObString part_spec_;
    ObNewRow part_list_val_;
  };

  struct OdpsDecoder {
    OdpsDecoder(ObIAllocator& alloc, bool is_root, const MirrorOdpsJniColumn &odps_column)
    : alloc_(alloc),
      odps_column_(odps_column),
      is_root_(is_root),
      datums_(nullptr),
      array_(nullptr) {}

    virtual int init(JniScanner::JniTableMeta& column_meta, ObEvalCtx &ctx, const ObExpr &expr, ObODPSArrayHelper *array_helper);
    virtual int decode(ObEvalCtx &ctx, const ObExpr &expr, int64_t offset, int64_t size);

    ObIAllocator& alloc_;

    const MirrorOdpsJniColumn &odps_column_;
    ObObjType type_;
    ObPrecision type_precision_;
    ObScale type_scale_;
    ObCollationType type_collation_;
    int32_t type_length_;
    long null_map_ptr_;
    // expr.extra_ : decimalint for cast mode
    // root decoder set data from odps to datums_ directly
    // non root decoder, like element of array type, set data from odps to array_.
    bool is_root_;
    ObDatum *datums_;
    ObIArrayType *array_;
  };

  struct OdpsFixedTypeDecoder : public OdpsDecoder {
    OdpsFixedTypeDecoder(ObIAllocator& alloc, bool is_root, const MirrorOdpsJniColumn &odps_column, 
      ObSQLSessionInfo* session_ptr, ObTime& ob_time, int timezone_ret, int64_t timezone_offset)
    : OdpsDecoder(alloc, is_root, odps_column),
      column_addr_(0),
      session_ptr_(session_ptr),
      ob_time_(ob_time),
      timezone_ret_(timezone_ret),
      timezone_offset_(timezone_offset) {}
    virtual int init(JniScanner::JniTableMeta& column_meta, ObEvalCtx &ctx, const ObExpr &expr, ObODPSArrayHelper *array_helper);
    virtual int decode(ObEvalCtx &ctx, const ObExpr &expr, int64_t offset, int64_t size);

    long column_addr_;
    ObSQLSessionInfo* session_ptr_;
    ObTime& ob_time_;
    int timezone_ret_;
    int timezone_offset_;
  };

  struct OdpsVarietyTypeDecoder : public OdpsDecoder {
    OdpsVarietyTypeDecoder(ObIAllocator& alloc, bool is_root, const MirrorOdpsJniColumn &odps_column)
    : OdpsDecoder(alloc, is_root, odps_column),
      offsets_(nullptr),
      base_addr_(nullptr) {}
    virtual int init(JniScanner::JniTableMeta& column_meta, ObEvalCtx &ctx, const ObExpr &expr, ObODPSArrayHelper *array_helper);
    virtual int decode(ObEvalCtx &ctx, const ObExpr &expr, int64_t offset, int64_t size);

    int *offsets_;
    char *base_addr_;
  };

  struct OdpsArrayTypeDecoder : public OdpsDecoder {
    OdpsArrayTypeDecoder(ObIAllocator& alloc, bool is_root, const MirrorOdpsJniColumn &odps_column)
    : OdpsDecoder(alloc, is_root, odps_column),
      offsets_(nullptr),
      child_decoder_(nullptr) {}
    virtual int init(JniScanner::JniTableMeta& column_meta, ObEvalCtx &ctx, const ObExpr &expr, ObODPSArrayHelper *array_helper);
    virtual int decode(ObEvalCtx &ctx, const ObExpr &expr, int64_t offset, int64_t size);

    int64_t *offsets_;
    OdpsDecoder *child_decoder_;
  };

public:
  static const int64_t ODPS_BLOCK_DOWNLOAD_SIZE = 1 << 18;
  static const char *NON_PARTITION_FLAG;

public:
  ObODPSJNITableRowIterator()
      : odps_format_(),
        state_(),
        is_part_table_(false),
        bit_vector_cache_(NULL),
        batch_size_(-1),
        inited_columns_and_types_(false),
        is_empty_external_file_exprs_(false),
        read_rounds_(0),
        read_rows_(0),
        real_row_idx_(0),
        timezone_str_(nullptr),
        timezone_ret_(OB_SUCCESS),
        timezone_offset_(0),
        session_ptr_(nullptr),
        ob_time_(DT_TYPE_ORACLE_TTZ),
        predicate_buf_(nullptr),
        predicate_buf_len_(0),
        pd_storage_filters_(nullptr),
        array_helpers_()
  {
    mem_attr_ = ObMemAttr(MTL_ID(), "OdpsJniRowIter");
    malloc_alloc_.set_attr(mem_attr_);
    arena_alloc_.set_attr(mem_attr_);
    expr_attr_ = ObMemAttr(MTL_ID(), "JniFillExpr");
    task_alloc_.set_attr(expr_attr_);
    column_exprs_alloc_.set_attr(expr_attr_);
  }

  virtual ~ObODPSJNITableRowIterator()
  {
    if (NULL != bit_vector_cache_) {
      malloc_alloc_.free(bit_vector_cache_);
    }

    if (OB_NOT_NULL(odps_jni_schema_scanner_)) {
      odps_jni_schema_scanner_->do_close();
      odps_jni_schema_scanner_.reset();
    }
    for (int64_t i = 0; i < array_helpers_.count(); ++i) {
      if (array_helpers_.at(i) != nullptr) {
        array_helpers_.at(i)->~ObODPSArrayHelper();
        arena_alloc_.free(array_helpers_.at(i));
      }
    }

    mirror_nonpart_column_list_.reset();
    mirror_partition_column_list_.reset();
    partition_specs_.reset();
    sorted_column_ids_.reset();
    obexpr_odps_nonpart_col_idsmap_.reset();
    obexpr_odps_part_col_idsmap_.reset();
    arena_alloc_.clear();

    is_part_table_ = false;

    inited_columns_and_types_ = false;
    is_empty_external_file_exprs_ = false;

    reset();
  }

  virtual void reset() override;

  virtual int get_next_row() override
  {
    int ret = OB_NOT_SUPPORTED;
    return ret;
  }
  virtual int init(const storage::ObTableScanParam *scan_param) override;

  virtual int get_next_row(ObNewRow *&row) override
  {
    UNUSED(row);
    return common::OB_ERR_UNEXPECTED;
  }

  virtual int get_next_rows(int64_t &count, int64_t capacity) override;
  int init_required_mini_params(const ObSQLSessionInfo* session_ptr_in);
  int init_jni_schema_scanner(const ObODPSGeneralFormat &odps_format, const ObSQLSessionInfo* session_ptr_in);
  int init_jni_meta_scanner(const ObODPSGeneralFormat &odps_format, const ObSQLSessionInfo* session_ptr_in);
  int close_schema_scanner();
  int init_storage_api_meta_param(
      const ExprFixedArray &ext_file_column_expr, const ObString &part_list_str, int64_t split_block_size);
  int prepare_data_expr(const ExprFixedArray &ext_file_column_expr);
  int prepare_partition_expr(const ExprFixedArray &ext_file_column_expr);
  int prepare_bit_vector();
  int calc_exprs_for_rowid(const int64_t read_count);
  inline ObIArray<OdpsJNIPartition> &get_partition_specs()
  {
    return partition_specs_;
  }
  inline bool is_part_table()
  {
    return is_part_table_;
  }
  int get_file_total_row_count(int64_t &count);
  int get_file_total_size(int64_t &size);
  int get_split_count(int64_t &count);
  int get_session_id(ObIAllocator &alloc, ObString &sid);
  int get_serilize_session(ObIAllocator &alloc, ObString &session_str);
  int init_empty_require_column();
  int init_part_spec(const ObString& part_spec);
  int construct_predicate_using_white_filter(const ObDASScanCtDef &das_ctdef,
                                             ObDASScanRtDef *das_rtdef,
                                             ObExecContext &exec_ctx);
private:
  int init_all_columns_name_as_odps_params();
  int init_data_tunnel_reader_params(int64_t start, int64_t step, const ObString &part_spec);
  int init_data_storage_reader_params(int64_t start_split, int64_t end_split, int64_t start_rows, int64_t row_count,
      const ObString &session_str, int64_t capacity);
  int inner_get_next_row();
  int pull_and_prepare_column_exprs(const ExprFixedArray &ext_file_column_expr);
  int pull_data_columns();
  int pull_partition_columns();
  int extract_mirror_odps_columns(
      ObSEArray<ObString, 8> &mirror_columns, ObSEArray<MirrorOdpsJniColumn, 8> &mirror_target_columns);
  int extract_mirror_odps_column(ObString &mirror_column_string, ObIArray<MirrorOdpsJniColumn> &mirror_columns);
  int get_int_from_mirror_column_string(ObString &mirror_column_string, int &int_val);
  int get_type_expr_from_mirror_column_string(ObString &mirror_column_string, ObString &type_expr);
  int check_type_static(MirrorOdpsJniColumn &odps_column, const ObExpr *ob_type_expr, ObODPSArrayHelper *array_helper);
  int check_type_static(MirrorOdpsJniColumn &odps_column,
                        const ObObjType ob_type,
                        const int32_t ob_type_length,
                        const int32_t ob_type_precision,
                        const int32_t ob_type_scale,
                        ObODPSArrayHelper *array_helper);
  int next_task_storage(const int64_t capacity);
  int next_task_storage_row_without_data_getter(const int64_t capacity);

  int build_storage_task_state(int64_t task_idx, int64_t part_id, int64_t start_split_idx, int64_t end_split_idx,
      int64_t start, int64_t step, const ObString &session_id, int64_t capacity);
  int next_task_tunnel_without_data_getter(const int64_t capacity);
  int next_task_tunnel(const int64_t capacity);
  int build_tunnel_partition_task_state(
      int64_t task_idx, int64_t part_id, const ObString &part_spec, int64_t start, int64_t step, int64_t capacity);
  int fill_column_offheaptable(
      ObEvalCtx &ctx, const ObExpr &expr, const MirrorOdpsJniColumn &odps_column, int64_t num_rows, int32_t column_idx);
  int fill_column_arrow(ObEvalCtx &ctx, const ObExpr &expr, const std::shared_ptr<arrow::Array> &array,
      const std::shared_ptr<arrow::Field> &field, const MirrorOdpsJniColumn &column,int64_t num_rows, int64_t column_idx);
  int fill_column_exprs_storage(const ExprFixedArray &column_exprs, ObEvalCtx &ctx, int64_t num_rows);
  int fill_column_exprs_tunnel(const ExprFixedArray &column_exprs, ObEvalCtx &ctx, int64_t num_rows);

  int get_next_rows_tunnel(int64_t &count, int64_t capacity);
  int get_next_rows_storage_api(int64_t &count, int64_t capacity);

  // flush table count
public:
  int fetch_partition_row_count(const ObString &part_spec, int64_t &row_count);
  int pull_partition_info();

private:
  int extract_odps_partition_specs(ObSEArray<ObString, 8> &partition_specs);
  int create_odps_decoder(ObEvalCtx &ctx,
                          const ObExpr &expr,
                          const MirrorOdpsJniColumn &odps_column,
                          bool is_root,
                          ObODPSArrayHelper *array_helper,
                          OdpsDecoder *&decoder);
  int fill_array_arrow(const std::shared_ptr<arrow::Array>& array,
                       const std::shared_ptr<arrow::Field>& element_field,
                       ObODPSArrayHelper *array_helper);

  int construct_predicate_using_white_filter(const ObDASScanCtDef &das_ctdef,
                                             ObExecContext &exec_ctx,
                                             sql::ObPushdownFilterExecutor &filter);
  int init_access_exprs(const ObDASScanCtDef &das_ctdef, ObIArray<ObExpr*> &access_exprs, bool &is_valid);
  int get_mirror_column(const ObIArray<ObExpr*> &access_exprs,
                        const ObIArray<ObExpr*> &file_column_exprs,
                        const ObExpr* column_expr,
                        MirrorOdpsJniColumn *&mirror_column);
  
  int print_predicate_string(const ObIArray<ObExpr*> &access_exprs,
                             const ObIArray<ObExpr*> &file_column_exprs,
                             sql::ObPushdownFilterExecutor &filter,
                             ObObjPrintParams &print_params,
                             ObIAllocator &alloc,
                             char *&buf,
                             int64_t &length,
                             ObSqlString &predicate,
                             bool &can_pushdown,
                             bool is_root = false);
  int check_type_for_pushdown(const MirrorOdpsJniColumn &mirror_column,
                              const ObObjMeta &obj_meta,
                              ObWhiteFilterOperatorType cmp_type,
                              bool &is_valid);
  bool is_zero_time(const ObObj &obj);

private:
  common::ObMemAttr mem_attr_;
  common::ObMemAttr expr_attr_;
  common::ObMalloc malloc_alloc_;
  common::ObArenaAllocator arena_alloc_;
  common::ObArenaAllocator column_exprs_alloc_;  // op一次读取生命周期 下一次读取重置
  common::ObArenaAllocator task_alloc_;          // task级别的生命周期 取task重置
  ObODPSGeneralFormat odps_format_;
  StateValues state_;
  bool is_part_table_;
  ObBitVector *bit_vector_cache_;
  // -1 means not inited, 0 means call get_next_row(), > 0 means call get_next_rows().
  int64_t batch_size_;
  // only used for get next task and recall inner_get_next_row() when current task was iter end.
  bool inited_columns_and_types_;
  bool is_empty_external_file_exprs_;
  // Only used in reading empty file columns expr, hold the temp remanant records.
  int64_t read_rounds_;
  int64_t read_rows_;
  int64_t real_row_idx_;
  common::hash::ObHashMap<ObString, ObString> odps_params_map_;
  JNIScannerPtr odps_jni_schema_scanner_;

  // mirror column list is using for pre-static checking
  ObSEArray<MirrorOdpsJniColumn, 8> mirror_nonpart_column_list_;
  ObSEArray<MirrorOdpsJniColumn, 8> mirror_partition_column_list_;

  ObSEArray<OdpsJNIPartition, 8> partition_specs_;

  // non-partition column.
  struct ExternalPair {
    int64_t ob_col_idx_;
    int64_t odps_col_idx_;
    struct Compare {
      bool operator()(ExternalPair &l, ExternalPair &r)
      {
        return l.odps_col_idx_ < r.odps_col_idx_;
      }
    };
    TO_STRING_KV(K_(ob_col_idx), K_(odps_col_idx));
  };
  ObSEArray<ExternalPair, 8> sorted_column_ids_;
  // total_column_ids_ contains the all column with parittion column and
  ObSEArray<ExternalPair, 8> obexpr_odps_part_col_idsmap_;
  // obexpr_odps_nonpart_col_idsmap_ only contains the normal column index.
  ObSEArray<ExternalPair, 8> obexpr_odps_nonpart_col_idsmap_;
  ObString timezone_str_;
  int timezone_ret_;
  int64_t timezone_offset_;
  ObSQLSessionInfo* session_ptr_;
  ObTime ob_time_;
  char *predicate_buf_;
  int64_t predicate_buf_len_;
  sql::ObPushdownFilterExecutor *pd_storage_filters_;
  ObSEArray<ObODPSArrayHelper*, 8> array_helpers_;

  static const int64_t MAX_PARAMS_SIZE = 16;
  static const char *DATETIME_PREFIX;
  static const char *TIMESTAMP_PREFIX;
  static const char *TIMESTAMP_NTZ_PREFIX;
};

class ObOdpsPartitionJNIScannerMgr {
public:
  ObOdpsPartitionJNIScannerMgr() : inited_(false), ref_(0)
  {}
  static int fetch_row_count(ObExecContext &exec_ctx, uint64_t tenant_id, const ObString &properties,
      ObIArray<ObExternalFileInfo> &external_table_files, bool &use_partition_gi);

  static int fetch_row_count(ObExecContext &exec_ctx, const ObString part_spec, const ObString &properties, int64_t &row_count);

  static int fetch_storage_row_count(ObExecContext &exec_ctx, 
    const ObString part_spec, const ObString &properties, int64_t &row_count);

  static int fetch_storage_row_count(ObExecContext &exec_ctx, const ObString &properties, int64_t &row_count);
  static int fetch_storage_api_total_task(ObExecContext &exec_ctx, const ExprFixedArray &ext_file_column_expr, const ObString &part_list_str,
      const ObDASScanCtDef &das_ctdef, ObDASScanRtDef *das_rtdef, int64_t parallel, ObString &session_str, int64_t &split_count, 
      ObIAllocator &range_allocator);
  static int fetch_storage_api_split_by_row(ObExecContext &exec_ctx, const ExprFixedArray &ext_file_column_expr, const ObString &part_list_str,
      const ObDASScanCtDef &das_ctdef, ObDASScanRtDef *das_rtdef, int64_t parallel, ObString &session_str, int64_t &total_row_count,
      ObIAllocator &range_allocator);

public:
  int init_map(const int64_t bucket_size)
  {
    int ret = OB_SUCCESS;
    inited_ = true;
    return ret;
  }

  ObIAllocator &get_allocator()
  {
    return arena_alloc_;
  }

public:
  int reset();
  inline bool is_jni_scanner_mgr_inited()
  {
    return inited_;
  }
  inline int64_t inc_ref()
  {
    return ATOMIC_FAA(&ref_, 1);
  }
  inline int64_t dec_ref()
  {
    return ATOMIC_SAF(&ref_, 1);
  }

private:
  bool inited_;
  common::ObArenaAllocator arena_alloc_;
  int64_t ref_;
};

class ObOdpsJniUploaderMgr {
public:
  struct OdpsUploader {
    OdpsUploader() : writer_ptr(nullptr)
    {}

    JNIWriterPtr writer_ptr;
  };

  ObOdpsJniUploaderMgr()
      : inited_(false),
        ref_(0),
        block_num_(0),
        init_parallel_(0),
        need_commit_(true),
        write_arena_alloc_("IntoOdps", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
  {}

  ~ObOdpsJniUploaderMgr()
  {
    if (OB_NOT_NULL(session_holder_ptr_.get())) {
      (void)session_holder_ptr_->do_close();
      session_holder_ptr_.reset();
    }
  }

  inline int64_t inc_ref()
  {
    return ATOMIC_FAA(&ref_, 1);
  }
  inline int64_t dec_ref()
  {
    return ATOMIC_SAF(&ref_, 1);
  }
  inline int64_t inc_block()
  {
    return ATOMIC_FAA(&block_num_, 1);
  }
  inline int64_t block_num()
  {
    return block_num_;
  }
  inline void set_fail()
  {
    ATOMIC_STORE(&need_commit_, false);
  }

  inline bool could_commit()
  {
    return ATOMIC_LOAD(&need_commit_);
  }

  static int create_writer_params_map(ObIAllocator &alloc, const sql::ObODPSGeneralFormat &odps_format,
      const ObString &external_partition, bool is_overwrite, common::hash::ObHashMap<ObString, ObString> &params_map);

  int init_writer_params_in_px(
      const ObString &properties, const ObString &external_partition, bool is_overwrite, int64_t parallel);

  int get_odps_uploader_in_px(int task_id, const common::ObFixedArray<ObExpr *, common::ObIAllocator> &select_exprs,
      OdpsUploader &odps_uploader);

  void release_hold_session();
  
  int commit_session(int64_t num_block);
  int append_block_id(long block_id);

  int reset();

private:
  bool inited_;
  int64_t ref_;
  int64_t block_num_;
  int64_t init_parallel_;
  bool need_commit_;
  common::ObArenaAllocator write_arena_alloc_;  // construct first and descruct last
  ObString sid_;
  common::hash::ObHashMap<ObString, ObString> odps_params_map_;
  static const int64_t MAX_PARAMS_SIZE = 16;
  JNIWriterPtr session_holder_ptr_;
  lib::ObMutex lock_;

private:
  int get_writer_sid(
      ObIAllocator &alloc, const common::hash::ObHashMap<ObString, ObString> &odps_params_map, ObString &sid);
};

}  // namespace sql
}  // namespace oceanbase
#endif
#endif /* __SQL_OB_ODPS_JNI_TABLE_ROW_ITER_H__ */