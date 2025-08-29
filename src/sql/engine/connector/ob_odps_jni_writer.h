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

#ifndef OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JNI_WRITER_H_
#define OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JNI_WRITER_H_

#include <memory>

#include "sql/engine/expr/ob_expr.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/lock/ob_mutex.h"
#include "lib/jni_env/ob_jni_connector.h"
#include "ob_odps_jni_connector.h"

namespace oceanbase {
namespace common {
class ObSqlString;
}

namespace sql {
class ObOdpsJniWriter;
typedef std::shared_ptr<ObOdpsJniWriter> JNIWriterPtr;
typedef lib::ObLockGuard<lib::ObMutex> LockGuard;

class ObOdpsJniWriter: public ObOdpsJniConnector {
public:
  ObOdpsJniWriter(ObString factory_class, int64_t batch_size = DEFAULT_BATCH_SIZE)
      : params_created_(false), is_opened_(false), jni_writer_factory_class_(factory_class)
  {}

  virtual ~ObOdpsJniWriter() = default;
  int init_params(const common::hash::ObHashMap<ObString, ObString> &params);

  int do_open();
  int get_session_id(ObIAllocator& alloc, ObString& sid);
  int do_open_record(int block_id);
  int do_write_next_brs(void *brs, int batch_size);
  int get_current_block_addr();
  int finish_write();
  int append_block_id(int64_t block_id);
  int commit_session(int64_t block_num);
  int do_close();
  intptr_t get_schema_ptr() { return schema_ptr_; }
  intptr_t get_array_ptr() { return array_ptr_; }
  intptr_t get_export_schema_ptr() { return export_schema_ptr_; }
  const common::ObIArray<OdpsType>& get_schema_from_odps() {return column_types_;}
  bool is_params_created() { return params_created_; }
  bool is_open() { return is_opened_; }


private:
  int init_jni_table_writer_(JNIEnv *env);
  int init_jni_method_(JNIEnv *env);

private:
  static const int64_t DEFAULT_BATCH_SIZE = 256;
  static const int64_t MAX_PARAMS_SIZE = 16;

  bool params_created_ = false;
  bool is_opened_ = false;
  jclass jni_writer_cls_ = nullptr;
  jobject jni_writer_obj_ = nullptr;
  jmethodID jni_writer_open_ = nullptr;
  jmethodID jni_writer_write_next_brs_ = nullptr;
  jmethodID jni_writer_close_ = nullptr;
  jmethodID jni_writer_get_session_id_ = nullptr;
  jmethodID jni_get_scheam_address_ = nullptr;
  jmethodID jni_get_array_address_ = nullptr;
  jmethodID jni_get_export_schema_address_ = nullptr;
  jmethodID jni_writer_get_odps_schema_ = nullptr;
  jmethodID jni_append_block_id_ = nullptr;
  jmethodID jni_commit_session_ = nullptr;

  lib::ObMutex lock_;
  common::ObString jni_writer_factory_class_;
  intptr_t schema_ptr_ = 0;
  intptr_t array_ptr_ = 0;
  intptr_t export_schema_ptr_ = 0;
  common::hash::ObHashMap<ObString, ObString> writer_params_;
  common::ObSEArray<OdpsType, 32> column_types_;


  DISALLOW_COPY_AND_ASSIGN(ObOdpsJniWriter);
};

JNIWriterPtr create_odps_jni_writer();

}  // namespace sql
}  // namespace oceanbase

#endif