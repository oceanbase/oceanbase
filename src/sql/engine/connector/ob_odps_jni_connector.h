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

#ifndef OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JNI_CONNECTOR_H_
#define OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JNI_CONNECTOR_H_
#include <jni.h>
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {
namespace common {
class ObSqlString;
}

namespace sql {

class ObOdpsJniConnector {
public:
  enum OdpsType {
    /**
     * 8字节有符号整型
     */
    BIGINT,
    /**
     * 双精度浮点
     */
    DOUBLE,
    /**
     * 布尔型
     */
    BOOLEAN,
    /**
     * 日期类型
     */
    DATETIME,
    /**
     * 字符串类型
     */
    STRING,
    /**
     * 精确小数类型
     */
    DECIMAL,
    /**
     * MAP类型
     */
    MAP,
    /**
     * ARRAY类型
     */
    ARRAY,
    /**
     * 空
     */
    VOID,
    /**
     * 1字节有符号整型
     */
    TINYINT,
    /**
     * 2字节有符号整型
     */
    SMALLINT,
    /**
     * 4字节有符号整型
     */
    INT,
    /**
     * 单精度浮点
     */
    FLOAT,
    /**
     * 固定长度字符串
     */
    CHAR,
    /**
     * 可变长度字符串
     */
    VARCHAR,
    /**
     * 时间类型
     */
    DATE,
    /**
     * 时间戳
     */
    TIMESTAMP,
    /**
     * 字节数组
     */
    BINARY,
    /**
     * 日期间隔
     */
    INTERVAL_DAY_TIME,
    /**
     * 年份间隔
     */
    INTERVAL_YEAR_MONTH,
    /**
     * 结构体
     */
    STRUCT,
    /**
     * JSON类型
     */
    JSON,
    /**
     * 时区无关的时间戳
     */
    TIMESTAMP_NTZ,
    /**
     * Unsupported types from external systems
     */
    UNKNOWN
  };

  struct MirrorOdpsJniColumn{
    MirrorOdpsJniColumn():
      type_(OdpsType::UNKNOWN),
      precision_(-1),
      scale_(-1),
      length_(-1),
      type_size_(-1),
      name_(""),
      type_expr_("") {}

    // simple primitive
    MirrorOdpsJniColumn(ObString name, OdpsType type, int32_t type_size,
                        ObString type_expr)
        : type_(type), type_size_(type_size), name_(name), type_expr_(type_expr) {}

    // char/varchar (default is -1 (in java side))
    MirrorOdpsJniColumn(ObString name, OdpsType type, int32_t length,
                        int32_t type_size, ObString type_expr)
        : type_(type), length_(length), type_size_(type_size), name_(name), type_expr_(type_expr) {}

    // decimal
    MirrorOdpsJniColumn(ObString name, OdpsType type, int32_t precision,
                        int32_t scale, int32_t type_size, ObString type_expr)
        : type_(type), precision_(precision), scale_(scale), type_size_(type_size), name_(name),
          type_expr_(type_expr) {}

    // array
    MirrorOdpsJniColumn(ObString name, OdpsType type)
    : type_(type), type_size_(-1),name_(name), type_expr_(), child_columns_() {}

    virtual ~MirrorOdpsJniColumn() {
      child_columns_.reset();
    }
    int assign(const MirrorOdpsJniColumn &other);
    ObOdpsJniConnector::OdpsType get_odps_type() const {
      return type_;
    }
    int32_t get_precision() const {
      return precision_;
    }
    int32_t get_scale() const {
      return scale_;
    }
    int32_t get_length() const {
      return length_;
    }
    int32_t get_child_columns_size() const {
      return child_columns_.count();
    }
    const MirrorOdpsJniColumn& get_child_column(int32_t index) const {
      return child_columns_.at(index);
    }
    const ObString get_name() const {
      return name_;
    }
    bool is_child_ = false;
    OdpsType type_;
    int32_t precision_ = -1;
    int32_t scale_ = -1;
    int32_t length_ = -1;
    int32_t type_size_; // type size is useful to calc offset on memory
    ObString name_;
    ObString type_expr_;
    ObArray<MirrorOdpsJniColumn> child_columns_;

    TO_STRING_KV(K(name_), K(type_), K(precision_), K(scale_), K(length_),
                 K(type_size_), K(type_expr_), K(child_columns_));
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
  static int check_jni_exception_(JNIEnv *env);
  static OdpsType get_odps_type_by_string(ObString type);
  static int get_jni_class(JNIEnv *env, const char *class_name, jclass &class_obj);
  static int get_jni_method(JNIEnv *env, jclass class_obj, const char *method_name, const char *method_signature, jmethodID &method_id);
  static int construct_jni_object(JNIEnv *env, jobject &object, jclass clazz, jmethodID constructorMethodID, ...);
  static int gen_jni_string(JNIEnv *env, const char *str, jstring &j_str);
  static ObObjType get_ob_type_by_odps_type_default(OdpsType type);
};

}
}


#endif