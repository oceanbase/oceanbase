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

#ifndef OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JNI_SCANNER_H_
#define OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JNI_SCANNER_H_

#include <memory>
#include <map>
#include <set>

#include "sql/engine/expr/ob_expr.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/lock/ob_mutex.h"
#include "lib/jni_env/ob_jni_connector.h"
#include "ob_odps_jni_connector.h"

namespace arrow {
  class RecordBatch;
  class Array;
}

namespace oceanbase
{

namespace common
{
class ObSqlString;
}

namespace sql
{

class ObOdpsJniReader;
typedef std::shared_ptr<ObOdpsJniReader> JNIScannerPtr;

class ObOdpsJniReader: public ObOdpsJniConnector
{
public:
  using ObOdpsJniConnector::JniTableMeta;
  enum SplitMode {
    RETURN_ODPS_BATCH,
    RETURN_OB_BATCH
  };
  enum TransferMode {
    ARROW_TABLE,
    OFF_HEAP_TABLE
  };

public:
  ObOdpsJniReader(ObString factory_class, ObString scanner_type,
             const bool is_schema_scanner = false);

  virtual ~ObOdpsJniReader() = default;

  int do_init(common::hash::ObHashMap<ObString, ObString> &params);
  int do_open();
  int do_close();
  int do_get_next_split_by_ob(int64_t *read_rows, bool *eof, int capacity);
  int do_get_next_split_by_odps(int64_t *read_rows, bool *eof, int capacity);
  // int release_column(int32_t column_index);
  int release_table(const int64_t num_rows);
  int release_slice();
  // Note: these methods could add extra params before scanner open
  int add_extra_optional_params(
      common::hash::ObHashMap<ObString, ObString> &extra_params);

  // Note: every executing with the scanner api should check the status must be
  // `opened` and `inited`.
  bool is_inited() { return inited_; }
  bool is_opened() { return is_opened_; }

  ObString get_transfer_mode_str() {
    ObString transfer_option;
    if (transfer_mode_
      == ObOdpsJniReader::TransferMode::OFF_HEAP_TABLE) {
      transfer_option = ObString::make_string("offHeapTable");
    } else {
      transfer_option = ObString::make_string("arrowTable");
    }
    return transfer_option;
  }
  TransferMode get_transfer_mode() {
    return transfer_mode_;
  }
  SplitMode get_split_mode() {
    return split_mode_;
  }
  JniTableMeta& get_jni_table_meta() {
    return table_meta_;
  }

  const std::shared_ptr<arrow::RecordBatch>& get_cur_arrow_batch() {
    return cur_arrow_batch_;
  }
  // ------------ public methods for odps ------------
  int get_odps_partition_row_count(ObIAllocator &allocator,
                                   const ObString &partition_spec,
                                   int64_t &row_count);
  int get_odps_partition_specs(ObIAllocator &allocator,
                               ObSEArray<ObString, 4> &partition_specs);
  int get_odps_partition_phy_specs(ObIAllocator &allocator, ObSEArray<ObString, 4> &partition_specs);
  int get_odps_mirror_data_columns(ObIAllocator &allocator,
                              ObSEArray<ObString, 4> &mirror_colums,
                              const ObString& mode);
  int add_extra_optional_part_spec(const ObString& partition_spec);
  int get_file_total_row_count(int64_t &count);
  int get_file_total_size(int64_t &count);
  int get_split_count(int64_t& size);
  int get_session_id(ObIAllocator &allocator, ObString& str);
  int get_serilize_session(ObIAllocator& alloc, ObString& session_str);
  int get_project_timezone_info(ObIAllocator& alloc, ObString &project_timezone);
  int total_read_rows_ = 0;
  int remain_total_rows_ = 0;
  int start_offset_ = 0;
private:

  int init_jni_table_scanner_(JNIEnv *env);
  int init_jni_method_(JNIEnv *env);

private:
  jclass jni_scanner_cls_ = nullptr;
  jobject jni_scanner_obj_ = nullptr;
  jmethodID jni_scanner_open_ = nullptr;
  jmethodID jni_scanner_get_next_offheap_batch_ = nullptr;
  jmethodID jni_scanner_get_next_arrow_ = nullptr;
  jmethodID jni_scanner_close_ = nullptr;
  // jmethodID jni_scanner_release_column_ = nullptr;
  jmethodID jni_scanner_release_table_ = nullptr;
   // Get the list class and needed methods
  jmethodID size_mid_ = nullptr;
  jmethodID get_mid_ = nullptr;
  common::ObString jni_scanner_factory_class_;
  // Scanner type will be only used in `iceberg` scenes.
  // Default is empty str "".
  common::ObString scanner_type_;
  common::hash::ObHashMap<ObString, ObString> scanner_params_;
  common::hash::ObHashSet<ObString> skipped_log_params_;
  // In parallel mode, required params should re-add again.
  common::hash::ObHashSet<ObString> skipped_required_params_;
  // batch_size is for jni scanner to fetch data at once.
  JniTableMeta table_meta_;
  std::shared_ptr<arrow::RecordBatch> cur_arrow_batch_;
  std::shared_ptr<arrow::RecordBatch> cur_reader_;

  bool inited_;
  bool is_opened_;
  bool is_schema_scanner_;
  SplitMode split_mode_ = RETURN_ODPS_BATCH;
  TransferMode transfer_mode_ = ARROW_TABLE;
  static const int64_t MAX_PARAMS_SIZE = 16;
  int64_t MAX_PARAMS_SET_SIZE = 8;
  static const int64_t DEFAULT_BATCH_SIZE = 256;
private:
  DISALLOW_COPY_AND_ASSIGN(ObOdpsJniReader);
};

// TODO(bitao): add more jni scanner
// Current only support odps(MaxCompute) jni scanner
JNIScannerPtr create_odps_jni_scanner(const bool is_schema_scanner = false);

} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JNI_SCANNER_H_ */