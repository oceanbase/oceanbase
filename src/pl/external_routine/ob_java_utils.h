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

#ifndef OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_JAVA_UTILS_H_
#define OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_JAVA_UTILS_H_

#include "object/ob_object.h"
#include "lib/jni_env/ob_java_env.h"
#include "lib/jni_env/ob_jni_connector.h"

namespace oceanbase
{

namespace common
{

class ObDatum;
class ObObj;

}

namespace pl
{

class ObJavaUtils
{
public:
  static int load_routine_jar(const ObString &jar, jobject &class_loader);
  static void delete_local_ref(jobject obj, JNIEnv *env = nullptr);
  static void delete_global_ref(jobject obj, JNIEnv *env = nullptr);
  static int exception_check(JNIEnv *env = nullptr);
};

class ObToJavaTypeMapperBase
{
public:
  explicit ObToJavaTypeMapperBase(JNIEnv &env)
    : env_(env)
  {  }

  virtual int operator()(const common::ObObj &obj, jobject &java_value) = 0;

  virtual ~ObToJavaTypeMapperBase()
  {
    if (OB_NOT_NULL(type_class_)) {
      env_.DeleteLocalRef(type_class_);
      type_class_ = nullptr;
    }
  }

  inline jclass &get_java_type_class() { return type_class_; }

protected:
  int init(const char* clazz_name)
  {
    int ret = OB_SUCCESS;

    jclass object_clazz = env_.FindClass(clazz_name);

    if (OB_ISNULL(object_clazz)) {
      ret = OB_ERR_UNEXPECTED;
      PL_LOG(WARN, "unexpected NULL object clazz", K(ret), K(clazz_name));
    } else {
      type_class_ = object_clazz;
    }

    return ret;
  }

private:
  // don't bother to maintain ref count
  DISALLOW_COPY_AND_ASSIGN(ObToJavaTypeMapperBase);

protected:
  JNIEnv &env_;
  jclass type_class_ = nullptr;
};

class ObToJavaNullTypeMapper final : public ObToJavaTypeMapperBase
{
public:
  explicit ObToJavaNullTypeMapper(JNIEnv &env) : ObToJavaTypeMapperBase(env) {  }

  int init() { return OB_SUCCESS; }

  int operator()(const common::ObObj &obj, jobject &java_value) override
  {
    int ret = OB_SUCCESS;

    // return null Object
    java_value = nullptr;

    return ret;
  }

  private:
  DISALLOW_COPY_AND_ASSIGN(ObToJavaNullTypeMapper);
};

class ObToJavaByteTypeMapper final : public ObToJavaTypeMapperBase
{
public:
  explicit ObToJavaByteTypeMapper(JNIEnv &env) : ObToJavaTypeMapperBase(env) {  }

  int init() { return ObToJavaTypeMapperBase::init("java/lang/Byte"); }

  int operator()(const common::ObObj &obj, jobject &java_value) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObToJavaByteTypeMapper);
};

class ObToJavaShortTypeMapper final : public ObToJavaTypeMapperBase
{
public:
  explicit ObToJavaShortTypeMapper(JNIEnv &env) : ObToJavaTypeMapperBase(env) {  }

  int init() { return ObToJavaTypeMapperBase::init("java/lang/Short"); }

  int operator()(const common::ObObj &obj, jobject &java_value) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObToJavaShortTypeMapper);
};

class ObToJavaIntegerTypeMapper final : public ObToJavaTypeMapperBase
{
public:
  explicit ObToJavaIntegerTypeMapper(JNIEnv &env) : ObToJavaTypeMapperBase(env) {  }

  int init() { return ObToJavaTypeMapperBase::init("java/lang/Integer"); }

  int operator()(const common::ObObj &obj, jobject &java_value) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObToJavaIntegerTypeMapper);
};

class ObToJavaLongTypeMapper final : public ObToJavaTypeMapperBase
{
public:
  explicit ObToJavaLongTypeMapper(JNIEnv &env) : ObToJavaTypeMapperBase(env) {  }

  int init() { return ObToJavaTypeMapperBase::init("java/lang/Long"); }

  int operator()(const common::ObObj &obj, jobject &java_value) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObToJavaLongTypeMapper);
};

class ObToJavaFloatTypeMapper final : public ObToJavaTypeMapperBase
{
public:
  explicit ObToJavaFloatTypeMapper(JNIEnv &env) : ObToJavaTypeMapperBase(env) {  }

  int init() { return ObToJavaTypeMapperBase::init("java/lang/Float"); }

  int operator()(const common::ObObj &obj, jobject &java_value) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObToJavaFloatTypeMapper);
};

class ObToJavaDoubleTypeMapper final : public ObToJavaTypeMapperBase
{
public:
  explicit ObToJavaDoubleTypeMapper(JNIEnv &env) : ObToJavaTypeMapperBase(env) {  }

  int init() { return ObToJavaTypeMapperBase::init("java/lang/Double"); }

  int operator()(const common::ObObj &obj, jobject &java_value) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObToJavaDoubleTypeMapper);
};

class ObToJavaBigDecimalTypeMapper final : public ObToJavaTypeMapperBase
{
public:
  explicit ObToJavaBigDecimalTypeMapper(JNIEnv &env) : ObToJavaTypeMapperBase(env) {  }

  int init() { return ObToJavaTypeMapperBase::init("java/math/BigDecimal"); }

  int operator()(const common::ObObj &obj, jobject &java_value) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObToJavaBigDecimalTypeMapper);
};

class ObToJavaStringTypeMapper final : public ObToJavaTypeMapperBase
{
public:
  ObToJavaStringTypeMapper(JNIEnv &env) : ObToJavaTypeMapperBase(env) {  }

  int init() { return ObToJavaTypeMapperBase::init("java/lang/String"); }

  int operator()(const common::ObObj &obj, jobject &java_value) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObToJavaStringTypeMapper);
};

class ObToJavaByteBufferTypeMapper final : public ObToJavaTypeMapperBase
{
public:
ObToJavaByteBufferTypeMapper(JNIEnv &env) : ObToJavaTypeMapperBase(env) {  }

  int init() { return ObToJavaTypeMapperBase::init("java/nio/ByteBuffer"); }

  int operator()(const common::ObObj &obj, jobject &java_value) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObToJavaByteBufferTypeMapper);
};

class ObFromJavaTypeMapperBase
{
public:
  ObFromJavaTypeMapperBase(JNIEnv &env, ObIAllocator &alloc)
    : env_(env),
      alloc_(alloc),
      type_class_(nullptr)
  {  }

  virtual int operator()(jobject java_value, ObObj &result) = 0;

  virtual ~ObFromJavaTypeMapperBase()
  {
    if (OB_NOT_NULL(type_class_)) {
      env_.DeleteLocalRef(type_class_);
      type_class_ = nullptr;
    }
  }

  inline jclass &get_java_type_class() { return type_class_; }

protected:
  int init(const char* clazz_name)
  {
    int ret = OB_SUCCESS;

    jclass object_clazz = env_.FindClass(clazz_name);

    if (OB_ISNULL(object_clazz)) {
      ret = OB_ERR_UNEXPECTED;
      PL_LOG(WARN, "unexpected NULL object clazz", K(ret), K(clazz_name));
    } else {
      type_class_ = object_clazz;
    }

    return ret;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaTypeMapperBase);

protected:
  JNIEnv &env_;
  ObIAllocator &alloc_;
  jclass type_class_ = nullptr;
};

class ObFromJavaByteTypeMapper final : public ObFromJavaTypeMapperBase
{
public:
  ObFromJavaByteTypeMapper(JNIEnv &env, ObIAllocator &alloc) : ObFromJavaTypeMapperBase(env, alloc) {  }

  int init() { return ObFromJavaTypeMapperBase::init("java/lang/Byte"); }

  int operator()(jobject java_value, ObObj &result) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaByteTypeMapper);
};

class ObFromJavaShortTypeMapper final : public ObFromJavaTypeMapperBase
{
public:
  ObFromJavaShortTypeMapper(JNIEnv &env, ObIAllocator &alloc) : ObFromJavaTypeMapperBase(env, alloc) {  }

  int init() { return ObFromJavaTypeMapperBase::init("java/lang/Short"); }

  int operator()(jobject java_value, ObObj &result) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaShortTypeMapper);
};

class ObFromJavaIntegerTypeMapper final : public ObFromJavaTypeMapperBase
{
public:
  ObFromJavaIntegerTypeMapper(JNIEnv &env, ObIAllocator &alloc) : ObFromJavaTypeMapperBase(env, alloc) {  }

  int init() { return ObFromJavaTypeMapperBase::init("java/lang/Integer"); }

  int operator()(jobject java_value, ObObj &result) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaIntegerTypeMapper);
};

class ObFromJavaLongTypeMapper final : public ObFromJavaTypeMapperBase
{
public:
  ObFromJavaLongTypeMapper(JNIEnv &env, ObIAllocator &alloc) : ObFromJavaTypeMapperBase(env, alloc) {  }

  int init() { return ObFromJavaTypeMapperBase::init("java/lang/Long"); }

  int operator()(jobject java_value, ObObj &result) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaLongTypeMapper);
};

class ObFromJavaFloatTypeMapper final : public ObFromJavaTypeMapperBase
{
public:
  ObFromJavaFloatTypeMapper(JNIEnv &env, ObIAllocator &alloc) : ObFromJavaTypeMapperBase(env, alloc) {  }

  int init() { return ObFromJavaTypeMapperBase::init("java/lang/Float"); }

  int operator()(jobject java_value, ObObj &result) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaFloatTypeMapper);
};

class ObFromJavaDoubleTypeMapper final : public ObFromJavaTypeMapperBase
{
public:
  ObFromJavaDoubleTypeMapper(JNIEnv &env, ObIAllocator &alloc) : ObFromJavaTypeMapperBase(env, alloc) {  }

  int init() { return ObFromJavaTypeMapperBase::init("java/lang/Double"); }

  int operator()(jobject java_value, ObObj &result) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaDoubleTypeMapper);
};

class ObFromJavaBigDecimalTypeMapper final : public ObFromJavaTypeMapperBase
{
public:
  ObFromJavaBigDecimalTypeMapper(JNIEnv &env, ObIAllocator &alloc) : ObFromJavaTypeMapperBase(env, alloc) {  }

  int init() { return ObFromJavaTypeMapperBase::init("java/math/BigDecimal"); }

  int operator()(jobject java_value, ObObj &result) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaBigDecimalTypeMapper);
};

class ObFromJavaStringTypeMapper final : public ObFromJavaTypeMapperBase
{
public:
  ObFromJavaStringTypeMapper(JNIEnv &env, ObIAllocator &alloc) : ObFromJavaTypeMapperBase(env, alloc) {  }

  int init() { return ObFromJavaTypeMapperBase::init("java/lang/String"); }

  int operator()(jobject java_value, ObObj &result) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaStringTypeMapper);
};

class ObFromJavaByteBufferTypeMapper final : public ObFromJavaTypeMapperBase
{
public:
  ObFromJavaByteBufferTypeMapper(JNIEnv &env, ObIAllocator &alloc) : ObFromJavaTypeMapperBase(env, alloc) {  }

  int init() { return ObFromJavaTypeMapperBase::init("java/nio/ByteBuffer"); }

  int operator()(jobject java_value, ObObj &result) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaByteBufferTypeMapper);
};

} // namespace pl
} // namespace oceanbase

#endif // OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_JAVA_UTILS_H_
