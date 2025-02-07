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

namespace oceanbase
{
namespace sql
{

class ObODPSJNITableRowIterator : public ObExternalTableRowIterator {
public:
  struct StateValues {
    StateValues() :
      task_idx_(-1),
      part_id_(0),
      start_(0),
      step_(0),
      count_(0),
      cur_line_number_(0),
      is_from_gi_pump_(false),
      odps_jni_scanner_(NULL){}
    int reuse();
    TO_STRING_KV(K(task_idx_),
                 K(part_id_),
                 K(start_),
                 K(step_),
                 K(count_),
                 K(cur_line_number_),
                 K(is_from_gi_pump_),
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

  struct MirrorOdpsJniColumn{
    MirrorOdpsJniColumn():
      name_(""),
      type_(""),
      precision_(-1),
      scale_(-1),
      length_(-1),
      type_size_(-1),
      type_expr_("") {}

    // simple primitive
    MirrorOdpsJniColumn(ObString name, ObString type, int32_t type_size,
                        ObString type_expr)
        : name_(name), type_(type), type_size_(type_size),
          type_expr_(type_expr) {}

    // char/varchar (default is -1 (in java side))
    MirrorOdpsJniColumn(ObString name, ObString type, int32_t length,
                        int32_t type_size, ObString type_expr)
        : name_(name), type_(type), length_(length), type_size_(type_size),
          type_expr_(type_expr) {}

    // decimal
    MirrorOdpsJniColumn(ObString name, ObString type, int32_t precision,
                        int32_t scale, int32_t type_size, ObString type_expr)
        : name_(name), type_(type), precision_(precision), scale_(scale),
          type_size_(type_size), type_expr_(type_expr) {}

    ObString name_;
    ObString type_;
    int32_t precision_ = -1;
    int32_t scale_ = -1;
    int32_t length_ = -1;
    int32_t type_size_; // type size is useful to calc offset on memory
    ObString type_expr_;

    TO_STRING_KV(K(name_), K(type_), K(precision_), K(scale_), K(length_),
                 K(type_size_), K(type_expr_));
  };

  struct OdpsJNIColumn {
    OdpsJNIColumn():
      name_(""),
      type_("") {}

    OdpsJNIColumn(ObString name, ObString type) :
      name_(name),
      type_(type) {}

    ObString name_;
    ObString type_;
    TO_STRING_KV(K(name_), K(type_));
  };

  struct OdpsJNIPartition {
    OdpsJNIPartition() {}

    OdpsJNIPartition(ObString partition_spec, int record_count)
        : partition_spec_(partition_spec), record_count_(record_count) {}

    ObString partition_spec_;
    int record_count_;

    TO_STRING_KV(K(partition_spec_), K(record_count_));
  };

public:
  static const int64_t ODPS_BLOCK_DOWNLOAD_SIZE = 1 << 18;
  static const char* NON_PARTITION_FLAG;

public:
  ObODPSJNITableRowIterator() :
    odps_format_(),
    state_(),
    is_part_table_(false),
    total_count_(0),
    bit_vector_cache_(NULL),
    batch_size_(-1),
    inited_columns_and_types_(false),
    is_empty_external_file_exprs_(false),
    is_only_part_col_queried_(false),
    read_rounds_(0),
    real_row_idx_(0)
  {
    mem_attr_ = ObMemAttr(MTL_ID(), "OdpsJniRowIter");
    malloc_alloc_.set_attr(mem_attr_);
    arena_alloc_.set_attr(mem_attr_);
    expr_attr_ = ObMemAttr(MTL_ID(), "JniFillExpr");
    task_alloc_.set_attr(expr_attr_);
    column_exprs_alloc_.set_attr(expr_attr_);
  }

  virtual ~ObODPSJNITableRowIterator() {
    if (NULL != bit_vector_cache_) {
      malloc_alloc_.free(bit_vector_cache_);
    }

    if (OB_NOT_NULL(odps_jni_schema_scanner_)) {
      odps_jni_schema_scanner_->do_close();
      odps_jni_schema_scanner_.reset();
    }

    arena_alloc_.clear();

    mirror_column_list_.reset();
    partition_specs_.reset();
    total_column_ids_.reset();
    target_column_ids_.reset();

    is_part_table_ = false;

    inited_columns_and_types_ = false;
    is_empty_external_file_exprs_ = false;
    is_only_part_col_queried_ = false;

    reset();
  }

  virtual void reset() override;

  virtual int get_next_row() override {
    int ret = OB_NOT_SUPPORTED;
    return ret;
  }
  virtual int init(const storage::ObTableScanParam *scan_param) override;

  virtual int get_next_row(ObNewRow *&row) override {
    UNUSED(row);
    return common::OB_ERR_UNEXPECTED;
  }

  virtual int get_next_rows(int64_t &count, int64_t capacity) override;
  int init_required_params();
  int init_jni_schema_scanner(const ObODPSGeneralFormat& odps_format);

  int calc_exprs_for_rowid(const int64_t read_count);

  int fecth_partition_row_count(const ObString &part_spec, int64_t &row_count);
  int pull_partition_info();
  inline ObIArray<OdpsJNIPartition>& get_partition_specs() { return partition_specs_; }
  inline bool is_part_table() { return is_part_table_; }

private:
  int inner_get_next_row();
  int pull_columns();
  int check_type_static(MirrorOdpsJniColumn odps_column, const ObExpr *ob_type_expr);
  int prepare_expr();
  int init_columns_and_types();
  int next_task(const int64_t capacity);

  int extract_odps_partition_specs(ObSEArray<ObString, 8> &partition_specs);
  int extract_mirror_odps_columns(ObSEArray<ObString, 8> &mirror_columns);

  int fill_column_(ObEvalCtx &ctx, const ObExpr &expr,
                   const MirrorOdpsJniColumn &odps_column, int64_t num_rows,
                   int32_t column_idx);
  int fill_column_exprs_(const ExprFixedArray &column_exprs, ObEvalCtx &ctx,
                         int64_t num_rows);

private:
  common::ObMemAttr mem_attr_;
  common::ObMemAttr expr_attr_;
  common::ObMalloc malloc_alloc_;
  common::ObArenaAllocator arena_alloc_;
  common::ObArenaAllocator column_exprs_alloc_; // op一次读取生命周期 下一次读取重置
  common::ObArenaAllocator task_alloc_; // task级别的生命周期 取task重置
  ObODPSGeneralFormat odps_format_;
  StateValues state_;
  bool is_part_table_;
  int64_t total_count_;
  ObBitVector *bit_vector_cache_;
  // -1 means not inited, 0 means call get_next_row(), > 0 means call get_next_rows().
  int64_t batch_size_;
  // only used for get next task and recall inner_get_next_row() when current task was iter end.
  bool inited_columns_and_types_;
  bool is_empty_external_file_exprs_;
  bool is_only_part_col_queried_;
  // Only used in reading empty file columns expr, hold the temp remanant records.
  int64_t read_rounds_;
  int64_t real_row_idx_;
  common::hash::ObHashMap<ObString, ObString> odps_params_map_;
  JNIScannerPtr odps_jni_schema_scanner_;

  // mirror column list is using for pre-static checking
  ObSEArray<MirrorOdpsJniColumn, 8> mirror_column_list_;

  ObSEArray<OdpsJNIPartition, 8> partition_specs_;

  // total_column_ids_ contains the all column with parittion column and
  // non-partition column.
  ObSEArray<int64_t, 8> total_column_ids_;
  // target_column_ids_ only contains the normal column index.
  ObSEArray<int64_t, 8> target_column_ids_;

  static const int64_t MAX_PARAMS_SIZE = 16;
};


class ObOdpsPartitionJNIScannerMgr
{
public:
  ObOdpsPartitionJNIScannerMgr() : inited_(false), ref_(0) {}
  typedef common::hash::ObHashMap<int64_t, int64_t, common::hash::SpinReadWriteDefendMode> OdpsScannerMgrMap;

public:
  int init_map(const int64_t bucket_size) {
    int ret = OB_SUCCESS;
    if (!odps_part_id_to_scanner_map_.created()){
      ret = odps_part_id_to_scanner_map_.create(bucket_size, "OdpsTable",
                                                "OdpsJNITableMap");
      if (OB_SUCC(ret)) {
        inited_ = true;
      }
    }
    return ret;
  }

  OdpsScannerMgrMap &get_odps_scanner_map() {
    return odps_part_id_to_scanner_map_;
  }

  ObIAllocator &get_allocator() { return arena_alloc_; }
public:
  int get_jni_scanner(int64_t part_id, JNIScannerPtr &jni_scanner);
  int reset();
  inline bool is_jni_scanner_mgr_inited() { return inited_; }
  inline int64_t inc_ref() { return ATOMIC_FAA(&ref_, 1); }
  inline int64_t dec_ref()
  {
    return ATOMIC_SAF(&ref_, 1);
  }

public:
  static int fetch_row_count(uint64_t tenant_id,
                             const ObIArray<ObExternalFileInfo> &external_table_files,
                             const ObString &properties,
                             bool &use_partition_gi);
  static int fetch_row_count(const ObString part_spec,
                             const ObString &properties,
                             int64_t &row_count);

private:
  bool inited_;
  // TODO(bitao): consider how to use this map
  OdpsScannerMgrMap odps_part_id_to_scanner_map_;
  common::ObArenaAllocator arena_alloc_;
  int64_t ref_;
};


class ObOdpsJniUploaderMgr
{
public:

  struct OdpsUploader {
    OdpsUploader(): writer_ptr(nullptr){}

    JNIWriterPtr writer_ptr;
  };

  ObOdpsJniUploaderMgr(): inited_(false), ref_(0), need_commit_(true), write_arena_alloc_("IntoOdps", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()){}

  ~ObOdpsJniUploaderMgr()
  {
  }

  inline int64_t inc_ref()
  {
    return ATOMIC_FAA(&ref_, 1);
  }
  inline int64_t dec_ref()
  {
    return ATOMIC_SAF(&ref_, 1);
  }
  inline void set_fail()
  {
    ATOMIC_STORE(&need_commit_, false);
  }

  inline bool could_commit()
  {
    return ATOMIC_LOAD(&need_commit_);
  }

  static int create_writer_params_map(ObIAllocator& alloc,
                                      const sql::ObODPSGeneralFormat &odps_format,
                                      const ObString &external_partition,
                                      bool is_overwrite,
                                      common::hash::ObHashMap<ObString, ObString>& params_map);


  int init_writer_params_in_px(const ObString &properties,
                            const ObString &external_partition,
                            bool is_overwrite,
                            int64_t parallel);


  int get_odps_uploader_in_px(int task_id, const common::ObFixedArray<ObExpr*, common::ObIAllocator>& select_exprs, OdpsUploader &odps_uploader);

  void release_hold_session();

  int reset();
private:
  bool inited_;
  int64_t ref_;
  int64_t init_parallel_;
  bool need_commit_;
  common::ObArenaAllocator write_arena_alloc_; // construct first and descruct last
  ObString sid_;
  common::hash::ObHashMap<ObString, ObString> odps_params_map_;
  static const int64_t MAX_PARAMS_SIZE = 16;
  JNIWriterPtr session_holder_ptr_;
  lib::ObMutex lock_;

private:
  int get_writer_sid(ObIAllocator& alloc,
                            const common::hash::ObHashMap<ObString, ObString> &odps_params_map,
                            ObString& sid);
};

} // namespace sql
} // namespace oceanbase
#endif
#endif /* __SQL_OB_ODPS_JNI_TABLE_ROW_ITER_H__ */