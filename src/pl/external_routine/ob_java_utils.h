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
#include "proto/ob_pl_java_udf.pb-c.h"
#include "sql/engine/expr/ob_expr_res_type.h"

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

  static void *protobuf_c_allocator_alloc(void *allocator_data, size_t size);
  static void protobuf_c_allocator_free(void *allocator_data, void *pointer);

  static int get_cached_class(JNIEnv &env, const char *name, jclass &result)
  {
    int ret = OB_SUCCESS;

    using ClassMap = common::hash::ObHashMap<ObString,
                                             jclass,
                                             common::hash::NoPthreadDefendMode,
                                             common::hash::hash_func<ObString>,
                                             common::hash::equal_to<ObString>,
                                             common::hash::SimpleAllocer<common::hash::ObHashTableNode<common::hash::HashMapPair<ObString, jclass>>>,
                                             common::hash::NormalPointer,
                                             common::ObMalloc,
                                             2 // EXTEND_RATIO
                                             >;
    static std::pair<ObLatchMutex, ClassMap> cached_class;

    ObString class_name(name);
    result = nullptr;

    ObLatchMutexGuard guard(cached_class.first, ObLatchIds::JAVA_CACHED_CLASS_LOCK);

    if (OB_UNLIKELY(!cached_class.second.created())) {
      if (OB_FAIL(cached_class.second.create(16, "JavaUDFStatic"))) {
        PL_LOG(WARN, "failed to create cached_class", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(cached_class.second.get_refactored(class_name, result))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;

        jclass clazz = env.FindClass(name);
        jclass global_clazz = nullptr;

        if (OB_FAIL(exception_check(&env))) {
          PL_LOG(WARN, "failed to FindClass", K(ret), K(name));
        } else if (OB_ISNULL(clazz)) {
          ret = OB_ERR_UNEXPECTED;
          PL_LOG(WARN, "unexpected NULL class", K(ret), K(clazz));
        } else if (OB_ISNULL(global_clazz = static_cast<jclass>(env.NewGlobalRef(clazz)))) {
          ret = OB_ERR_UNEXPECTED;
          PL_LOG(WARN, "unexpected NULL NewGlobalRef", K(ret), K(clazz));
        } else if (OB_FAIL(cached_class.second.set_refactored(class_name,
                                                              global_clazz))) {
          PL_LOG(WARN, "failed to set_refactored", K(ret), K(global_clazz));
          env.DeleteGlobalRef(global_clazz);
          global_clazz = nullptr;
        } else {
          result = global_clazz;
        }

        delete_local_ref(clazz, &env);
      } else {
        PL_LOG(WARN, "failed to get_refactored from cached_class", K(ret));
      }
    }

    return ret;
  }

  static int get_executor_class(JNIEnv &env, jclass &executor_class, jmethodID &execute)
  {
    int ret = OB_SUCCESS;

    static std::tuple<ObLatchMutex, jclass, jmethodID> cached_executor;

    executor_class = nullptr;
    execute = nullptr;

    ObLatchMutexGuard guard(std::get<0>(cached_executor), ObLatchIds::JAVA_EXECUTOR_CLASS_LOCK);

    if (OB_ISNULL(std::get<1>(cached_executor))) {
      jobject loader = nullptr;
      jmethodID load_class_method = nullptr;
      jstring class_name = nullptr;
      jobject class_obj = nullptr;
      jclass global_executor = nullptr;
      jmethodID execute_method = nullptr;

      if (OB_FAIL(get_executor_class_loader(env, loader, load_class_method))) {
        PL_LOG(WARN, "failed to get_executor_class_loader", K(ret), K(loader), K(load_class_method));
      } else if (OB_ISNULL(loader) || OB_ISNULL(load_class_method)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "unexpected NULL executor class loader or load class method", K(ret), K(loader), K(load_class_method));
      } else if (OB_ISNULL(class_name = env.NewStringUTF("com.oceanbase.internal.ObJavaUDFExecutor"))) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN,"unexpected NULL string", K(ret));
      } else if (FALSE_IT(class_obj = env.CallObjectMethod(loader, load_class_method, class_name))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(&env))) {
        PL_LOG(WARN, "failed to create call loadClass of ObJavaUDFExecutorClassLoader", K(ret));
      } else if (OB_ISNULL(class_obj)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN,"unexpected NULL ObJavaUDFExecutor class", K(ret));
      } else if (
          FALSE_IT(
              execute_method = env.GetStaticMethodID(
                  static_cast<jclass>(class_obj),
                  "execute",
                  "("
                    "Ljava/lang/Object;"       // obj, UDF object
                    "Ljava/lang/String;"       // methodName, java String type
                    "J"                        // sessionId, java long type
                    "J"                        // timeoutTs, java long type
                    "[Ljava/lang/Object;"      // argTypes, java Object[] type
                    "Ljava/nio/ByteBuffer;"    // argValues, java ByteBuffer type
                    "Ljava/lang/Object;"       // resultType, java Object type
                  ")"
                  "Ljava/nio/ByteBuffer;"      // return value, java ByteBuffer type
                ))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(&env))) {
        PL_LOG(WARN, "failed to find execute method in ObJavaUDFExecutor class", K(ret));
      } else if (OB_ISNULL(execute_method)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN,"unexpected NULL execute method of ObJavaUDFExecutor class", K(ret));
      } else if (OB_ISNULL(global_executor = static_cast<jclass>(env.NewGlobalRef(class_obj)))) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN,"unexpected NULL NewGlobalRef", K(ret));
      } else {
        std::get<1>(cached_executor) = global_executor;
        std::get<2>(cached_executor) = execute_method;
      }

      ObJavaUtils::delete_local_ref(class_obj, &env);
      ObJavaUtils::delete_local_ref(class_name, &env);
    }

    if (OB_SUCC(ret)) {
      executor_class = std::get<1>(cached_executor);
      execute = std::get<2>(cached_executor);
    }

    return ret;
  }

  static int get_executor_class_loader(JNIEnv &env, jobject &loader, jmethodID &load_class_method)
  {
    int ret = OB_SUCCESS;

    static std::tuple<ObLatchMutex, jobject, jmethodID> cached_loader;

    loader = nullptr;
    load_class_method = nullptr;

    ObLatchMutexGuard guard(std::get<0>(cached_loader), ObLatchIds::JAVA_EXECUTOR_CLASS_LOCK);

    if (OB_ISNULL(std::get<1>(cached_loader))) {
      jclass loader_class = nullptr;
      jmethodID init = nullptr;
      jobject loader_obj = nullptr;

      jstring security_manager_class_name = nullptr;
      jclass security_manager_class = nullptr;
      jmethodID sm_init = nullptr;

      jmethodID load_class_method_id = nullptr;
      jobject global_loader = nullptr;

      loader_class = env.FindClass("com/oceanbase/internal/ObJavaUDFExecutorClassLoader");

      if (OB_FAIL(ObJavaUtils::exception_check(&env))) {
        PL_LOG(WARN, "failed to find ObJavaUDFExecutorClassLoader class", K(ret));
      } else if (OB_ISNULL(loader_class)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "unexpected NULL ObJavaUDFExecutorClassLoader class", K(ret));
      } else if (FALSE_IT(init = env.GetMethodID(loader_class, "<init>", "()V"))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(&env))) {
        PL_LOG(WARN, "failed to find init method of ObJavaUDFExecutorClassLoader", K(ret));
      } else if (OB_ISNULL(init)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "unexpected NULL init method of ObJavaUDFExecutorClassLoader", K(ret));
      } else if (FALSE_IT(loader_obj = env.NewObject(loader_class, init))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(&env))) {
        PL_LOG(WARN, "failed to create new ObJavaUDFExecutorClassLoader", K(ret));
      } else if (OB_ISNULL(loader_obj)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "unexpected NULL ObJavaUDFExecutorClassLoader object", K(ret));
      } else if (FALSE_IT(load_class_method_id = env.GetMethodID(loader_class, "loadClass", "(Ljava/lang/String;)Ljava/lang/Class;"))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(&env))) {
        PL_LOG(WARN, "failed to GetMethodID loadClass from ObJavaUDFExecutorClassLoader", K(ret));
      } else if (OB_ISNULL(load_class_method_id)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "unexpected NULL loadClass method", K(ret));
      } else if (OB_ISNULL(global_loader = env.NewGlobalRef(loader_obj))) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "unexpected NULL NewGlobalRef for loader", K(ret));
      } else if (OB_ISNULL(security_manager_class_name = env.NewStringUTF("com.oceanbase.internal.ObJavaUDFSecurityManager"))) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "unexpected NULL String for ObJavaUDFSecurityManager class name", K(ret));
      } else if (FALSE_IT(security_manager_class = static_cast<jclass>(env.CallObjectMethod(loader_obj, load_class_method_id, security_manager_class_name)))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(&env))) {
        PL_LOG(WARN, "failed to call CallObjectMethod for ObJavaUDFSecurityManager class", K(ret));
      } else if (OB_ISNULL(security_manager_class)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "unexpected NULL ObJavaUDFSecurityManager class", K(ret));
      } else if (FALSE_IT(sm_init = env.GetStaticMethodID(security_manager_class, "init", "()V"))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(&env))) {
        PL_LOG(WARN, "failed to get methodID init for ObJavaUDFSecurityManager class", K(ret));
      } else if (OB_ISNULL(sm_init)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "unexpected NULL init method for ObJavaUDFSecurityManager class", K(ret));
      } else if (FALSE_IT(env.CallStaticVoidMethod(security_manager_class, sm_init))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(&env))) {
        PL_LOG(WARN, "failed to call init method for ObJavaUDFSecurityManager class", K(ret));
      } else {
        std::get<1>(cached_loader) = global_loader;
        std::get<2>(cached_loader) = load_class_method_id;
      }

      ObJavaUtils::delete_local_ref(security_manager_class_name, &env);
      ObJavaUtils::delete_local_ref(security_manager_class, &env);
      ObJavaUtils::delete_local_ref(loader_obj, &env);
      ObJavaUtils::delete_local_ref(loader_class, &env);
    }

    if (OB_SUCC(ret)) {
      loader = std::get<1>(cached_loader);
      load_class_method = std::get<2>(cached_loader);
    }

    return ret;
  }

  static int get_udf_loader_class(JNIEnv &env, jclass &loader_class, jmethodID &constructor, jmethodID &find_class_method)
  {
    int ret = OB_SUCCESS;

    static std::tuple<ObLatchMutex, jclass, jmethodID, jmethodID> cached_class_loader;

    loader_class = nullptr;
    constructor = nullptr;
    find_class_method = nullptr;

    ObLatchMutexGuard guard(std::get<0>(cached_class_loader), ObLatchIds::UDF_LOADER_CLASS_LOCK);

    if (OB_ISNULL(std::get<1>(cached_class_loader))) {
      jobject executor_classloader = nullptr;
      jmethodID executor_classloader_loadclass_method = nullptr;
      jstring loader_class_name = nullptr;
      jclass loader_class = nullptr;
      jclass global_class = nullptr;
      jmethodID tmp_constructor = nullptr;
      jmethodID tmp_find_class_method = nullptr;

      if (OB_FAIL(get_executor_class_loader(env,
                                            executor_classloader,
                                            executor_classloader_loadclass_method))) {
        PL_LOG(WARN, "failed to get executor classloader", K(ret));
      } else if (OB_ISNULL(executor_classloader) || OB_ISNULL(executor_classloader_loadclass_method)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN,
               "unexpected NULL executor classloader or executor classloader loadclass method",
               K(ret),
               K(executor_classloader),
               K(executor_classloader_loadclass_method));
      } else if (OB_ISNULL(loader_class_name = env.NewStringUTF("com.oceanbase.internal.ObJavaUDFClassLoader"))) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "unexpected NULL string for ObJavaUDFClassLoader class name", K(ret));
      } else if (FALSE_IT(loader_class =
                              static_cast<jclass>(env.CallObjectMethod(executor_classloader,
                                                                       executor_classloader_loadclass_method,
                                                                       loader_class_name)))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(&env))) {
        PL_LOG(WARN, "failed to find ObJavaUDFClassLoader class", K(ret));
      } else if (OB_ISNULL(loader_class)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "unexpected NULL ObJavaUDFClassLoader class", K(ret));
      } else if (OB_ISNULL(global_class = static_cast<jclass>(env.NewGlobalRef(loader_class)))) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "unexpected NULL NewGlobalRef", K(ret));
      } else if (FALSE_IT(tmp_constructor = env.GetMethodID(global_class, "<init>", "()V"))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(&env))) {
        PL_LOG(WARN, "failed to get constructor", K(ret));
      } else if (OB_ISNULL(tmp_constructor)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "unexpected NULL constructor", K(ret));
      } else if (FALSE_IT(tmp_find_class_method = env.GetMethodID(global_class, "findClass", "(Ljava/lang/String;)Ljava/lang/Class;"))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(&env))) {
        PL_LOG(WARN, "failed to get find_class_method", K(ret));
      } else if (OB_ISNULL(tmp_find_class_method)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "unexpected NULL find_class_method", K(ret));
      } else {
        std::get<1>(cached_class_loader) = global_class;
        std::get<2>(cached_class_loader) = tmp_constructor;
        std::get<3>(cached_class_loader) = tmp_find_class_method;
      }

      ObJavaUtils::delete_local_ref(loader_class_name, &env);
      ObJavaUtils::delete_local_ref(loader_class, &env);
    }

    if (OB_SUCC(ret)) {
      loader_class = std::get<1>(cached_class_loader);
      constructor = std::get<2>(cached_class_loader);
      find_class_method = std::get<3>(cached_class_loader);
    }

    return ret;
  }

  static int ob_string_to_jstring(JNIEnv &env, ObIAllocator &alloc, const ObString &str, jstring &result);

  static int trans_jar_to_classes(const ObString &jar_binary,
                                  ObIArray<std::pair<ObString, ObString>> &classes,
                                  JNIEnv &env,
                                  jobject &buffer_handle);

private:
  static int parse_binary_response(const char *buffer,
                                   int64_t buffer_size,
                                   int64_t &pos,
                                   ObString &content);

};

class ObToJavaTypeMapperBase
{
public:
  ObToJavaTypeMapperBase(JNIEnv &env, ObIAllocator &alloc, int64_t batch_size)
    : env_(env),
      alloc_(alloc),
      batch_size_(batch_size)
  {  }

  virtual int operator()(const common::ObObj &obj, int64_t idx) = 0;

  virtual ~ObToJavaTypeMapperBase()
  {  }

  inline jclass &get_java_type_class() { return type_class_; }

  inline ObPl__JavaUdf__Values *get_arg_values() { return &arg_; }

  TO_STRING_KV(K(OB_ISNULL(type_class_)), K_(batch_size));

protected:
  int init(const char* clazz_name)
  {
    int ret = OB_SUCCESS;

    jclass object_clazz = nullptr;

    if (OB_FAIL(ObJavaUtils::get_cached_class(env_, clazz_name, object_clazz))) {
      PL_LOG(WARN, "failed to get_cached_class", K(ret), K(clazz_name));
    } else {
      type_class_ = object_clazz;
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (0 >= batch_size_) {
      ret = OB_ERR_UNEXPECTED;
      PL_LOG(WARN, "unexpected batch_size", K(ret), KPC(this));
    } else {
      ob_pl__java_udf__values__init(&arg_);

      uint8 *buff = nullptr;
      size_t size = batch_size_ * sizeof(uint8);

      if (OB_ISNULL(buff = static_cast<uint8 *>(alloc_.alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PL_LOG(WARN, "failed to allocate memory for null_map", K(ret));
      } else {
        memset(buff, 0, size);
        arg_.null_map.data = buff;
        arg_.null_map.len = size;
      }
    }

    return ret;
  }

private:
  // don't bother to maintain ref count
  DISALLOW_COPY_AND_ASSIGN(ObToJavaTypeMapperBase);

protected:
  JNIEnv &env_;
  jclass type_class_ = nullptr;
  ObIAllocator &alloc_;
  int64_t batch_size_ = OB_INVALID_SIZE;
  ObPl__JavaUdf__Values arg_;
};

class ObToJavaByteTypeMapper final : public ObToJavaTypeMapperBase
{
public:
  ObToJavaByteTypeMapper(JNIEnv &env, ObIAllocator &alloc, int64_t batch_size)
    : ObToJavaTypeMapperBase(env, alloc, batch_size)
  {  }

  int init()
  {
    int ret = OB_SUCCESS;

    ob_pl__java_udf__byte_values__init(&values_);

    using ValueType = std::remove_pointer_t<decltype(values_.value.data)>;
    ValueType *buff = nullptr;

    if (OB_FAIL(ObToJavaTypeMapperBase::init("java/lang/Byte"))) {
      PL_LOG(WARN, "failed to init ObToJavaTypeMapperBase", K(ret), KPC(this));
    } else if (OB_ISNULL(buff = static_cast<ValueType*>(alloc_.alloc(batch_size_ * sizeof(ValueType))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PL_LOG(WARN, "failed to allocate memory for values buffer", K(ret), K(batch_size_));
    } else {
      values_.value.len = batch_size_;
      values_.value.data = buff;

      arg_.values_case = OB_PL__JAVA_UDF__VALUES__VALUES_BYTE_VALUES;
      arg_.byte_values = &values_;
    }

    return ret;
  }

  int operator()(const common::ObObj &obj, int64_t idx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObToJavaByteTypeMapper);

private:
  ObPl__JavaUdf__ByteValues values_;
};

class ObToJavaShortTypeMapper final : public ObToJavaTypeMapperBase
{
public:
  ObToJavaShortTypeMapper(JNIEnv &env, ObIAllocator &alloc, int64_t batch_size)
    : ObToJavaTypeMapperBase(env, alloc, batch_size)
  {  }

  int init()
  {
    int ret = OB_SUCCESS;

    ob_pl__java_udf__short_values__init(&values_);

    using ValueType = std::remove_pointer_t<decltype(values_.value)>;
    ValueType *buff = nullptr;

    if (OB_FAIL(ObToJavaTypeMapperBase::init("java/lang/Short"))) {
      PL_LOG(WARN, "failed to init ObToJavaTypeMapperBase", K(ret), KPC(this));
    } else if (OB_ISNULL(buff = static_cast<ValueType*>(alloc_.alloc(batch_size_ * sizeof(ValueType))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PL_LOG(WARN, "failed to allocate memory for values buffer", K(ret), K(batch_size_));
    } else {
      values_.n_value = batch_size_;
      values_.value = buff;

      arg_.values_case = OB_PL__JAVA_UDF__VALUES__VALUES_SHORT_VALUES;
      arg_.short_values = &values_;
    }

    return ret;
  }

  int operator()(const common::ObObj &obj, int64_t idx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObToJavaShortTypeMapper);

private:
  ObPl__JavaUdf__ShortValues values_;
};

class ObToJavaIntegerTypeMapper final : public ObToJavaTypeMapperBase
{
public:
  ObToJavaIntegerTypeMapper(JNIEnv &env, ObIAllocator &alloc, int64_t batch_size)
    : ObToJavaTypeMapperBase(env, alloc, batch_size)
  {  }

  int init()
  {
    int ret = OB_SUCCESS;

    ob_pl__java_udf__int_values__init(&values_);

    using ValueType = std::remove_pointer_t<decltype(values_.value)>;
    ValueType *buff = nullptr;

    if (OB_FAIL(ObToJavaTypeMapperBase::init("java/lang/Integer"))) {
      PL_LOG(WARN, "failed to init ObToJavaTypeMapperBase", K(ret), KPC(this));
    } else if (OB_ISNULL(buff = static_cast<ValueType*>(alloc_.alloc(batch_size_ * sizeof(ValueType))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PL_LOG(WARN, "failed to allocate memory for values buffer", K(ret), K(batch_size_));
    } else {
      values_.n_value = batch_size_;
      values_.value = buff;

      arg_.values_case = OB_PL__JAVA_UDF__VALUES__VALUES_INT_VALUES;
      arg_.int_values = &values_;
    }

    return ret;
  }

  int operator()(const common::ObObj &obj, int64_t idx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObToJavaIntegerTypeMapper);

private:
  ObPl__JavaUdf__IntValues values_;
};

class ObToJavaLongTypeMapper final : public ObToJavaTypeMapperBase
{
public:
  ObToJavaLongTypeMapper(JNIEnv &env, ObIAllocator &alloc, int64_t batch_size)
    : ObToJavaTypeMapperBase(env, alloc, batch_size)
  {  }

  int init()
  {
    int ret = OB_SUCCESS;

    ob_pl__java_udf__long_values__init(&values_);

    using ValueType = std::remove_pointer_t<decltype(values_.value)>;
    ValueType *buff = nullptr;

    if (OB_FAIL(ObToJavaTypeMapperBase::init("java/lang/Long"))) {
      PL_LOG(WARN, "failed to init ObToJavaTypeMapperBase", K(ret), KPC(this));
    } else if (OB_ISNULL(buff = static_cast<ValueType*>(alloc_.alloc(batch_size_ * sizeof(ValueType))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PL_LOG(WARN, "failed to allocate memory for values buffer", K(ret), K(batch_size_));
    } else {
      values_.n_value = batch_size_;
      values_.value = buff;

      arg_.values_case = OB_PL__JAVA_UDF__VALUES__VALUES_LONG_VALUES;
      arg_.long_values = &values_;
    }

    return ret;
  }

  int operator()(const common::ObObj &obj, int64_t idx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObToJavaLongTypeMapper);

private:
  ObPl__JavaUdf__LongValues values_;
};

class ObToJavaFloatTypeMapper final : public ObToJavaTypeMapperBase
{
public:
  ObToJavaFloatTypeMapper(JNIEnv &env, ObIAllocator &alloc, int64_t batch_size)
    : ObToJavaTypeMapperBase(env, alloc, batch_size)
  {  }

  int init()
  {
    int ret = OB_SUCCESS;

    ob_pl__java_udf__float_values__init(&values_);

    using ValueType = std::remove_pointer_t<decltype(values_.value)>;
    ValueType *buff = nullptr;

    if (OB_FAIL(ObToJavaTypeMapperBase::init("java/lang/Float"))) {
      PL_LOG(WARN, "failed to init ObToJavaTypeMapperBase", K(ret), KPC(this));
    } else if (OB_ISNULL(buff = static_cast<ValueType*>(alloc_.alloc(batch_size_ * sizeof(ValueType))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PL_LOG(WARN, "failed to allocate memory for values buffer", K(ret), K(batch_size_));
    } else {
      values_.n_value = batch_size_;
      values_.value = buff;

      arg_.values_case = OB_PL__JAVA_UDF__VALUES__VALUES_FLOAT_VALUES;
      arg_.float_values = &values_;
    }

    return ret;
  }

  int operator()(const common::ObObj &obj, int64_t idx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObToJavaFloatTypeMapper);

private:
  ObPl__JavaUdf__FloatValues values_;
};

class ObToJavaDoubleTypeMapper final : public ObToJavaTypeMapperBase
{
public:
  ObToJavaDoubleTypeMapper(JNIEnv &env, ObIAllocator &alloc, int64_t batch_size)
    : ObToJavaTypeMapperBase(env, alloc, batch_size)
  {  }

  int init()
  {
    int ret = OB_SUCCESS;

    ob_pl__java_udf__double_values__init(&values_);

    using ValueType = std::remove_pointer_t<decltype(values_.value)>;
    ValueType *buff = nullptr;

    if (OB_FAIL(ObToJavaTypeMapperBase::init("java/lang/Double"))) {
      PL_LOG(WARN, "failed to init ObToJavaTypeMapperBase", K(ret), KPC(this));
    } else if (OB_ISNULL(buff = static_cast<ValueType*>(alloc_.alloc(batch_size_ * sizeof(ValueType))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PL_LOG(WARN, "failed to allocate memory for values buffer", K(ret), K(batch_size_));
    } else {
      values_.n_value = batch_size_;
      values_.value = buff;

      arg_.values_case = OB_PL__JAVA_UDF__VALUES__VALUES_DOUBLE_VALUES;
      arg_.double_values = &values_;
    }

    return ret;
  }

  int operator()(const common::ObObj &obj, int64_t idx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObToJavaDoubleTypeMapper);

private:
  ObPl__JavaUdf__DoubleValues values_;
};

class ObToJavaBigDecimalTypeMapper final : public ObToJavaTypeMapperBase
{
public:
  ObToJavaBigDecimalTypeMapper(JNIEnv &env, ObIAllocator &alloc, int64_t batch_size)
    : ObToJavaTypeMapperBase(env, alloc, batch_size)
  {  }

  int init()
  {
    int ret = OB_SUCCESS;

    ob_pl__java_udf__big_decimal_values__init(&values_);

    using ValueType = std::remove_pointer_t<decltype(values_.value)>;
    ValueType *buff = nullptr;

    if (OB_FAIL(ObToJavaTypeMapperBase::init("java/math/BigDecimal"))) {
      PL_LOG(WARN, "failed to init ObToJavaTypeMapperBase", K(ret), KPC(this));
    } else if (OB_ISNULL(buff = static_cast<ValueType*>(alloc_.alloc(batch_size_ * sizeof(ValueType))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PL_LOG(WARN, "failed to allocate memory for values buffer", K(ret), K(batch_size_));
    } else {
      for (int64_t i = 0; i < batch_size_; ++i) {
        buff[i].len = 0;
        buff[i].data = nullptr;
      }

      values_.n_value = batch_size_;
      values_.value = buff;

      arg_.values_case = OB_PL__JAVA_UDF__VALUES__VALUES_BIG_DECIMAL_VALUES;
      arg_.big_decimal_values = &values_;
    }

    return ret;
  }

  int operator()(const common::ObObj &obj, int64_t idx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObToJavaBigDecimalTypeMapper);

private:
  ObPl__JavaUdf__BigDecimalValues values_;
};

class ObToJavaStringTypeMapper final : public ObToJavaTypeMapperBase
{
public:
  ObToJavaStringTypeMapper(JNIEnv &env, ObIAllocator &alloc, int64_t batch_size)
    : ObToJavaTypeMapperBase(env, alloc, batch_size)
  {  }

  int init()
  {
    int ret = OB_SUCCESS;

    ob_pl__java_udf__string_values__init(&values_);

    using ValueType = std::remove_pointer_t<decltype(values_.value)>;
    ValueType *buff = nullptr;

    if (OB_FAIL(ObToJavaTypeMapperBase::init("java/lang/String"))) {
      PL_LOG(WARN, "failed to init ObToJavaTypeMapperBase", K(ret), KPC(this));
    } else if (OB_ISNULL(buff = static_cast<ValueType*>(alloc_.alloc(batch_size_ * sizeof(ValueType))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PL_LOG(WARN, "failed to allocate memory for values buffer", K(ret), K(batch_size_));
    } else {
      for (int64_t i = 0; i < batch_size_; ++i) {
        buff[i].len = 0;
        buff[i].data = nullptr;
      }

      values_.n_value = batch_size_;
      values_.value = buff;

      arg_.values_case = OB_PL__JAVA_UDF__VALUES__VALUES_STRING_VALUES;
      arg_.string_values = &values_;
    }

    return ret;
  }

  int operator()(const common::ObObj &obj, int64_t idx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObToJavaStringTypeMapper);

private:
  ObPl__JavaUdf__StringValues values_;
};

class ObToJavaByteBufferTypeMapper final : public ObToJavaTypeMapperBase
{
public:
  ObToJavaByteBufferTypeMapper(JNIEnv &env, ObIAllocator &alloc, int64_t batch_size)
    : ObToJavaTypeMapperBase(env, alloc, batch_size)
  {  }

  int init()
  {
    int ret = OB_SUCCESS;

    ob_pl__java_udf__byte_buffer_values__init(&values_);

    using ValueType = std::remove_pointer_t<decltype(values_.value)>;
    ValueType *buff = nullptr;

    if (OB_FAIL(ObToJavaTypeMapperBase::init("java/nio/ByteBuffer"))) {
      PL_LOG(WARN, "failed to init ObToJavaTypeMapperBase", K(ret), KPC(this));
    } else if (OB_ISNULL(buff = static_cast<ValueType*>(alloc_.alloc(batch_size_ * sizeof(ValueType))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PL_LOG(WARN, "failed to allocate memory for values buffer", K(ret), K(batch_size_));
    } else {
      for (int64_t i = 0; i < batch_size_; ++i) {
        buff[i].len = 0;
        buff[i].data = nullptr;
      }

      values_.n_value = batch_size_;
      values_.value = buff;

      arg_.values_case = OB_PL__JAVA_UDF__VALUES__VALUES_BYTE_BUFFER_VALUES;
      arg_.byte_buffer_values = &values_;
    }

    return ret;
  }

  int operator()(const common::ObObj &obj, int64_t idx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObToJavaByteBufferTypeMapper);

private:
  ObPl__JavaUdf__ByteBufferValues values_;
};

class ObFromJavaTypeMapperBase
{
public:
  ObFromJavaTypeMapperBase(JNIEnv &env,
                           ObIAllocator &alloc,
                           int64_t batch_size,
                           const sql::ObExprResType &res_type,
                           sql::ObSQLSessionInfo &session)
    : env_(env),
      alloc_(alloc),
      type_class_(nullptr),
      batch_size_(batch_size),
      res_type_(res_type),
      session_(session)
  {  }

  virtual int operator()(const ObPl__JavaUdf__Values &values, ObIArray<ObObj> &result_array) = 0;

  virtual ~ObFromJavaTypeMapperBase()
  {  }

  inline jclass &get_java_type_class() { return type_class_; }

  inline void set_batch_size(int64_t batch_size) { batch_size_ = batch_size; }
  inline int64_t get_batch_size() const { return batch_size_; }

  TO_STRING_KV(K(OB_ISNULL(type_class_)));

protected:
  int init(const char* clazz_name)
  {
    int ret = OB_SUCCESS;

    jclass object_clazz = nullptr;

    if (OB_FAIL(ObJavaUtils::get_cached_class(env_,clazz_name, object_clazz))) {
      PL_LOG(WARN, "failed to get_cached_class", K(ret), K(clazz_name));
    } else {
      type_class_ = object_clazz;
    }

    return ret;
  }

protected:
  int convert(ObObj &src, ObObj &dest);

private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaTypeMapperBase);

protected:
  JNIEnv &env_;
  ObIAllocator &alloc_;
  jclass type_class_ = nullptr;
  int64_t batch_size_;
  sql::ObExprResType res_type_;
  sql::ObSQLSessionInfo &session_;
};

class ObFromJavaByteTypeMapper final : public ObFromJavaTypeMapperBase
{
public:
  ObFromJavaByteTypeMapper(JNIEnv &env,
                           ObIAllocator &alloc,
                           int64_t batch_size,
                           const sql::ObExprResType &res_type,
                           sql::ObSQLSessionInfo &session)
    : ObFromJavaTypeMapperBase(env, alloc, batch_size, res_type, session)
  {  }

  int init() { return ObFromJavaTypeMapperBase::init("java/lang/Byte"); }

  int operator()(const ObPl__JavaUdf__Values &values,
                 ObIArray<ObObj> &result_array) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaByteTypeMapper);
};

class ObFromJavaShortTypeMapper final : public ObFromJavaTypeMapperBase
{
public:
  ObFromJavaShortTypeMapper(JNIEnv &env,
                           ObIAllocator &alloc,
                           int64_t batch_size,
                           const sql::ObExprResType &res_type,
                           sql::ObSQLSessionInfo &session)
    : ObFromJavaTypeMapperBase(env, alloc, batch_size, res_type, session)
  {  }

  int init() { return ObFromJavaTypeMapperBase::init("java/lang/Short"); }

  int operator()(const ObPl__JavaUdf__Values &values,
                 ObIArray<ObObj> &result_array) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaShortTypeMapper);
};

class ObFromJavaIntegerTypeMapper final : public ObFromJavaTypeMapperBase
{
public:
  ObFromJavaIntegerTypeMapper(JNIEnv &env,
                              ObIAllocator &alloc,
                              int64_t batch_size,
                              const sql::ObExprResType &res_type,
                              sql::ObSQLSessionInfo &session)
    : ObFromJavaTypeMapperBase(env, alloc, batch_size, res_type, session)
  {  }

  int init() { return ObFromJavaTypeMapperBase::init("java/lang/Integer"); }

  int operator()(const ObPl__JavaUdf__Values &values,
                 ObIArray<ObObj> &result_array) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaIntegerTypeMapper);
};

class ObFromJavaLongTypeMapper final : public ObFromJavaTypeMapperBase
{
public:
  ObFromJavaLongTypeMapper(JNIEnv &env,
                           ObIAllocator &alloc,
                           int64_t batch_size,
                           const sql::ObExprResType &res_type,
                           sql::ObSQLSessionInfo &session)
    : ObFromJavaTypeMapperBase(env, alloc, batch_size, res_type, session)
  {  }

  int init() { return ObFromJavaTypeMapperBase::init("java/lang/Long"); }

  int operator()(const ObPl__JavaUdf__Values &values,
                 ObIArray<ObObj> &result_array) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaLongTypeMapper);
};

class ObFromJavaFloatTypeMapper final : public ObFromJavaTypeMapperBase
{
public:
  ObFromJavaFloatTypeMapper(JNIEnv &env,
                            ObIAllocator &alloc,
                            int64_t batch_size,
                            const sql::ObExprResType &res_type,
                            sql::ObSQLSessionInfo &session)
    : ObFromJavaTypeMapperBase(env, alloc, batch_size, res_type, session)
  {  }

  int init() { return ObFromJavaTypeMapperBase::init("java/lang/Float"); }

  int operator()(const ObPl__JavaUdf__Values &values,
                 ObIArray<ObObj> &result_array) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaFloatTypeMapper);
};

class ObFromJavaDoubleTypeMapper final : public ObFromJavaTypeMapperBase
{
public:
  ObFromJavaDoubleTypeMapper(JNIEnv &env,
                             ObIAllocator &alloc,
                             int64_t batch_size,
                             const sql::ObExprResType &res_type,
                             sql::ObSQLSessionInfo &session)
    : ObFromJavaTypeMapperBase(env, alloc, batch_size, res_type, session)
  {  }

  int init() { return ObFromJavaTypeMapperBase::init("java/lang/Double"); }

  int operator()(const ObPl__JavaUdf__Values &values,
                 ObIArray<ObObj> &result_array) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaDoubleTypeMapper);
};

class ObFromJavaBigDecimalTypeMapper final : public ObFromJavaTypeMapperBase
{
public:
  ObFromJavaBigDecimalTypeMapper(JNIEnv &env,
                                 ObIAllocator &alloc,
                                 int64_t batch_size,
                                 const sql::ObExprResType &res_type,
                                 sql::ObSQLSessionInfo &session)
    : ObFromJavaTypeMapperBase(env, alloc, batch_size, res_type, session)
  {  }

  int init() { return ObFromJavaTypeMapperBase::init("java/math/BigDecimal"); }

  int operator()(const ObPl__JavaUdf__Values &values,
                 ObIArray<ObObj> &result_array) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaBigDecimalTypeMapper);
};

class ObFromJavaStringTypeMapper final : public ObFromJavaTypeMapperBase
{
public:
  ObFromJavaStringTypeMapper(JNIEnv &env,
                             ObIAllocator &alloc,
                             int64_t batch_size,
                             const sql::ObExprResType &res_type,
                             sql::ObSQLSessionInfo &session)
    : ObFromJavaTypeMapperBase(env, alloc, batch_size, res_type, session)
  {  }

  int init() { return ObFromJavaTypeMapperBase::init("java/lang/String"); }

  int operator()(const ObPl__JavaUdf__Values &values,
                 ObIArray<ObObj> &result_array) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaStringTypeMapper);
};

class ObFromJavaByteBufferTypeMapper final : public ObFromJavaTypeMapperBase
{
public:
  ObFromJavaByteBufferTypeMapper(JNIEnv &env,
                                 ObIAllocator &alloc,
                                 int64_t batch_size,
                                 const sql::ObExprResType &res_type,
                                 sql::ObSQLSessionInfo &session)
    : ObFromJavaTypeMapperBase(env, alloc, batch_size, res_type, session)
  {  }

  int init() { return ObFromJavaTypeMapperBase::init("java/nio/ByteBuffer"); }

  int operator()(const ObPl__JavaUdf__Values &values,
                 ObIArray<ObObj> &result_array) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFromJavaByteBufferTypeMapper);
};

} // namespace pl
} // namespace oceanbase

#endif // OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_JAVA_UTILS_H_
