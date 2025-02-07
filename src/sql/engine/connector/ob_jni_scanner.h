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
#include <jni.h>
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
#include "sql/engine/connector/ob_jni_connector.h"

namespace oceanbase
{

namespace common
{
class ObSqlString;
}

namespace sql
{

class JniScanner;
typedef std::shared_ptr<JniScanner> JNIScannerPtr;

class JniScanner: public ObJniConnector
{
public:
  class JniTableMeta {
    private:
      long *batch_meta_ptr_;
      int batch_meta_index_;

    public:
      JniTableMeta() {
        batch_meta_ptr_ = nullptr;
        batch_meta_index_ = 0;
      }

      JniTableMeta(long meta_addr) {
        batch_meta_ptr_ =
            static_cast<long *>(reinterpret_cast<void *>(meta_addr));
        batch_meta_index_ = 0;
      }

      void set_meta(long meta_addr) {
        batch_meta_ptr_ =
            static_cast<long *>(reinterpret_cast<void *>(meta_addr));
        batch_meta_index_ = 0;
      }

      long next_meta_as_long() {
        return batch_meta_ptr_[batch_meta_index_++];
      }

      void *next_meta_as_ptr() {
        return reinterpret_cast<void *>(batch_meta_ptr_[batch_meta_index_++]);
      }
  };

public:
  JniScanner(ObString factory_class, ObString scanner_type,
             const bool is_schema_scanner = false,
             int64_t batch_size = DEFAULT_BATCH_SIZE)
      : jni_scanner_factory_class_(factory_class), scanner_type_(scanner_type),
        scanner_params_(), skipped_log_params_(), skipped_required_params_(),
        batch_size_(batch_size), table_meta_(), inited_(false),
        is_opened_(false), is_schema_scanner_(is_schema_scanner),
        is_debug_(false) {}

  virtual ~JniScanner() = default;

  int do_init(common::hash::ObHashMap<ObString, ObString> &params);
  int do_open();
  int do_close();
  int do_get_next(int64_t *read_rows, bool *eof);
  int release_column(int32_t column_index);
  int release_table(const int64_t num_rows);
  // Note: these methods could add extra params before scanner open
  int add_extra_optional_params(
      common::hash::ObHashMap<ObString, ObString> &extra_params);
  void set_batch_size(int64_t batch_size) {
    batch_size_ = batch_size;
  }
  void set_debug_mode(bool is_debug) {
    is_debug_ = is_debug;
  }

  // Note: every executing with the scanner api should check the status must be
  // `opened` and `inited`.
  bool is_inited() { return inited_; }
  bool is_opened() { return is_opened_; }

  bool is_schema_scanner() { return is_schema_scanner_; }

  bool is_debug() { return is_debug_; }

  JniTableMeta& get_jni_table_meta() {
    return table_meta_;
  }
  // ------------ public methods for odps ------------
  int get_odps_partition_row_count(ObIAllocator &allocator,
                                   const ObString &partition_spec,
                                   int64_t &row_count);
  int get_odps_partition_specs(ObIAllocator &allocator,
                               ObSEArray<ObString, 8> &partition_specs);
  int get_odps_mirror_columns(ObIAllocator &allocator,
                              ObSEArray<ObString, 8> &mirror_colums);
  int add_extra_optional_part_spec(const ObString& partition_spec);

private:

  int init_jni_table_scanner_(JNIEnv *env);
  int init_jni_method_(JNIEnv *env);

private:
  jclass jni_scanner_cls_ = nullptr;
  jobject jni_scanner_obj_ = nullptr;
  jmethodID jni_scanner_open_ = nullptr;
  jmethodID jni_scanner_get_next_batch_ = nullptr;
  jmethodID jni_scanner_close_ = nullptr;
  jmethodID jni_scanner_release_column_ = nullptr;
  jmethodID jni_scanner_release_table_ = nullptr;

  common::ObString jni_scanner_factory_class_;
  // Scanner type will be only used in `iceberg` scenes.
  // Default is empty str "".
  common::ObString scanner_type_;
  common::hash::ObHashMap<ObString, ObString> scanner_params_;
  common::hash::ObHashSet<ObString> skipped_log_params_;
  // In parallel mode, required params should re-add again.
  common::hash::ObHashSet<ObString> skipped_required_params_;
  // batch_size is for jni scanner to fetch data at once.
  int64_t batch_size_;
  JniTableMeta table_meta_;
  bool inited_;
  bool is_opened_;
  bool is_schema_scanner_;
  bool is_debug_;

  static const int64_t MAX_PARAMS_SIZE = 16;
  int64_t MAX_PARAMS_SET_SIZE = 8;
  static const int64_t DEFAULT_BATCH_SIZE = 256;

private:
  DISALLOW_COPY_AND_ASSIGN(JniScanner);
};

// TODO(bitao): add more jni scanner
// Current only support odps(MaxCompute) jni scanner
JNIScannerPtr create_odps_jni_scanner(const bool is_schema_scanner = false);

} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JNI_SCANNER_H_ */