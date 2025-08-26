/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include <apache-arrow/arrow/c/abi.h>

#include "lib/guard/ob_shared_guard.h"
#include "plugin/interface/ob_plugin_external_intf.h"
#include "lib/jni_env/ob_java_helper.h"
#include "plugin/external_table/ob_external_jni_utils.h"

namespace oceanbase {
namespace plugin {

class ObJavaLogLabelSetter final
{
public:
  ObJavaLogLabelSetter();
  ~ObJavaLogLabelSetter();
};

using ObJavaLogGuard = common::ObSharedGuard<ObJavaLogLabelSetter>;

/**
 * Help to call functions in Java
 */
class ObJavaDataSourceFactoryHelper final
{
public:
  ObJavaDataSourceFactoryHelper() = default;
  ~ObJavaDataSourceFactoryHelper() = default;

  int init();

  int list_plugins(ObIAllocator &allocator, ObIArray<ObString> &data_source_names);
  int create_data_source(const ObString &parameters,
                         const ObString &plugin_name,
                         ObJavaGlobalRef &data_source_object);
  int export_arrow_stream(jobject record_batch_reader_object, ArrowArrayStream &arrow_stream);
  int import_record_batch(struct ArrowArray *array_c, struct ArrowSchema *schema_c, jobject &vector_schema_root_ret);
private:
  ObJavaGlobalRef data_source_factory_class_;
  jmethodID create_data_source_method_ = nullptr;
  jmethodID export_arrow_stream_method_ = nullptr;
  jmethodID import_record_batch_method_ = nullptr;
};

class ObJavaExternalPlugin final : public ObIExternalDescriptor
{
public:
  ObJavaExternalPlugin() = default;
  ~ObJavaExternalPlugin() override;

  const char *name() const { return "java"; }

  int init(ObPluginParam *) override;
  int deinit(ObPluginParam *) override;

  /// @copydoc ObIExternalDescriptor::validate_properties
  int validate_properties(ObIAllocator &allocator,
                          const external::ObTableSchema &table_schema,
                          const ObString &plugin_name,
                          ObString &parameters) override;

  /// @copydoc ObIExternalDescriptor::create_data_engine
  int create_data_engine(ObIAllocator &allocator,
                         const ObString &parameters,
                         const ObString &plugin_name,
                         ObExternalDataEngine *&data_engine) override;

private:
  ObJavaDataSourceFactoryHelper data_source_factory_;
};

class ObJavaExternalDataEngine final : public ObExternalDataEngine
{
public:
  ObJavaExternalDataEngine(const ObString &parameters,
                           ObJavaGlobalRef &&data_source_object,
                           ObJavaDataSourceFactoryHelper &data_source_factory,
                           ObJavaLogGuard &java_log_guard);
  ~ObJavaExternalDataEngine() override;

  /// @copydoc ObExternalDataEngine::init
  int init() override;
  /// @copydoc ObExternalDataEngine::destroy
  void destroy() override;

  int display(ObIAllocator &allocator, ObString &display_string);

  /// @copydoc ObExternalDataEngine::split_task
  int split_task(ObIAllocator &allocator, int64_t parallelism, ObIArray<ObString> &tasks) override;
  /// @copydoc ObExternalDataEngine::can_pushdown_filters
  int pushdown_filters(ObIAllocator &allocator,
                       const ParamStore &param_store,
                       const ObIArray<sql::ObRawExpr *> &filters,
                       ObIArray<ObString> &pushdown_filters) override;
  /// @copydoc ObExternalDataEngine::open_scanner
  int open_scanner(ObIAllocator &allocator,
                   const ObExternalTableScanParam &param,
                   ObExternalScanner *&scanner) override;

private:
  /**
   * create HashMap<String, Object> according to table scan param.
   * @param[out] map_ret type HashMap<String, Object>
   */
  int create_scanner_parameters(const ObExternalTableScanParam &param, JNIEnv *jni_env, jobject &map_ret);
  int create_filters(const ObExternalTableScanParam &scan_param, JNIEnv *jni_env, jobject &filters_ret);
  int create_param_store_jobject(
      const ObExternalTableScanParam &param, JNIEnv *jni_env, jobject &param_store_jobject_ret);
private:
  ObJavaGlobalRef data_source_object_;

  jmethodID create_scanner_method_       = nullptr;
  jmethodID split_task_method_           = nullptr;
  jmethodID pushdown_filters_method_     = nullptr;
  jmethodID display_string_method_       = nullptr;

  ObJavaDataSourceFactoryHelper &data_source_factory_;
  ObJavaLogGuard                 java_log_guard_;
};

/**
 * The external table scanner
 * @details get ArrowStreamReader from java and export it to `C`.
 * Arrow doc about export java arrow data to C and C++:
 * https://arrow.apache.org/docs/java/cdata.html
 */
class ObJavaExternalScanner final : public ObExternalScanner
{
public:
  ObJavaExternalScanner(ObJavaGlobalRef &&java_scanner_object,
                        ObJavaDataSourceFactoryHelper &data_source_factory,
                        ObJavaLogGuard &java_log_guard);
  ~ObJavaExternalScanner() override;

  /// @copydoc ObExternalScanner::init
  int init() override;

  /// @copydoc ObExternalScanner::rescan
  int rescan(const ObExternalTableScanParam &param) override;

  /// @copydoc ObExternalScanner::destroy
  void destroy() override;

  /// @copydoc ObExternalScanner::schema
  int schema(std::shared_ptr<arrow::Schema> &schema) override;

  /// @copydoc ObExternalScanner::next
  int next(int64_t capacity, arrow::RecordBatch *&record_batch) override;

protected:
  ObJavaGlobalRef                     java_scanner_object_;
  ArrowArrayStream                    arrow_stream_;
  std::shared_ptr<arrow::Schema>      schema_;
  std::shared_ptr<arrow::RecordBatch> record_batch_;

  ObJavaDataSourceFactoryHelper      &data_source_factory_;
  ObJavaLogGuard                      java_log_guard_;
};

} // namespace plugin
} // namespace oceanbase
