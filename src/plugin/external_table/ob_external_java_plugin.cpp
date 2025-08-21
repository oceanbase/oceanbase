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

#define USING_LOG_PREFIX SHARE

#include "plugin/external_table/ob_external_java_plugin.h"

#include <string>
#include <apache-arrow/arrow/api.h>
#include <apache-arrow/arrow/c/bridge.h>

#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "lib/jni_env/ob_jni_connector.h"
#include "plugin/external_table/ob_external_jni_utils.h"
#include "plugin/external_table/ob_external_filter.h"
#include "plugin/external_table/ob_external_arrow_object.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/access/ob_dml_param.h"

namespace oceanbase {

using namespace common;
using namespace sql;

namespace plugin {

using namespace external;

constexpr const char *PLUGIN_NAME_PROPERTY_NAME = "plugin_name";
constexpr const char *PARAMETERS_PROPERTY_NAME  = "parameters";
constexpr const char *DATA_SOURCE_FACTORY_CLASS_NAME = "com/oceanbase/external/internal/DataSourceFactory";

////////////////////////////////////////////////////////////////////////////////
// class ObJavaLogLabelSetter
ObJavaLogLabelSetter::ObJavaLogLabelSetter()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObJniTool::instance().set_java_log_label())) {
    LOG_WARN("failed to set java log label", K(ret));
  }
}

ObJavaLogLabelSetter::~ObJavaLogLabelSetter()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObJniTool::instance().clear_java_log_label())) {
    LOG_WARN("failed to clear java log label", K(ret));
  }
}


////////////////////////////////////////////////////////////////////////////////
// ObJavaDataSourceFactoryHelper
int ObJavaDataSourceFactoryHelper::init()
{
  int ret = OB_SUCCESS;
  JNIEnv *jni_env = nullptr;
  jclass data_source_factory_class = nullptr;
  LOCAL_REF_GUARD_ENV(data_source_factory_class, jni_env);
  if (OB_NOT_NULL(data_source_factory_class_.handle())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("java data source factory helper init twice");
  } else if (OB_FAIL(ObJniTool::instance().get_jni_env(jni_env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (OB_ISNULL(jni_env)) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("get jni env success but got null", K(ret));
  }
  OBJNI_RUN(data_source_factory_class = jni_env->FindClass(DATA_SOURCE_FACTORY_CLASS_NAME));
  CK (OB_NOT_NULL(data_source_factory_class));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(data_source_factory_class_.from_local_ref(data_source_factory_class, jni_env))) {
    LOG_WARN("failed to create global ref of data source factory class", K(ret));
  }
  OBJNI_RUN(create_data_source_method_ = jni_env->GetStaticMethodID(
      data_source_factory_class, "create", "(Ljava/util/Map;)Lcom/oceanbase/external/api/DataSource;"));
  CK(OB_NOT_NULL(create_data_source_method_));

  return ret;
}

int ObJavaDataSourceFactoryHelper::list_plugins(ObIAllocator &allocator, ObIArray<ObString> &data_source_names)
{
  int ret = OB_SUCCESS;
  JNIEnv *jni_env = nullptr;
  jmethodID data_source_names_method = nullptr;
  jobject jdata_source_names = nullptr;
  LOCAL_REF_GUARD_ENV(jdata_source_names, jni_env);
  if (OB_ISNULL(data_source_factory_class_.handle())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init");
  } else if (OB_FAIL(ObJniTool::instance().get_jni_env(jni_env))) {
    LOG_WARN("failed to get jni env", K(ret));
  }
  OBJNI_RUN(data_source_names_method = jni_env->GetStaticMethodID((jclass)data_source_factory_class_.handle(),
                                                                  "dataSourceNames",
                                                                  "()Ljava/util/List;"));
  OBJNI_RUN(jdata_source_names = jni_env->CallStaticObjectMethod((jclass)data_source_factory_class_.handle(),
                                                                 data_source_names_method));
  CK (OB_NOT_NULL(jdata_source_names));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObJniTool::instance().string_list_from_java(
      jni_env, jdata_source_names, allocator, data_source_names))) {
    LOG_WARN("failed to get data source names from java", K(ret));
  } else {
    LOG_TRACE("list java plugins", K(data_source_names));
  }
  return ret;
}
int ObJavaDataSourceFactoryHelper::create_data_source(const ObString &parameters,
                                                      const ObString &plugin_name,
                                                      ObJavaGlobalRef &data_source_object_ref)
{
  int ret = OB_SUCCESS;
  JNIEnv *jni_env = nullptr;
  jobject data_source_object = nullptr;
  jobject java_properties = nullptr;
  LOCAL_REF_GUARD_ENV(data_source_object, jni_env);
  LOCAL_REF_GUARD_ENV(java_properties, jni_env);

  const ObString map_kvs[] = {
    ObString(PLUGIN_NAME_PROPERTY_NAME), plugin_name,
    ObString(PARAMETERS_PROPERTY_NAME),  parameters
  };
  if (OB_ISNULL(data_source_factory_class_.handle())) {
    ret = OB_NOT_INIT;
    LOG_WARN("cannot create data source. data source factory class is null", K(ret));
  } else if (OB_FAIL(ObJniTool::instance().get_jni_env(jni_env))) {
    LOG_WARN("failed to get jni env", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObJniTool::instance().create_java_map(
      jni_env, map_kvs, sizeof(map_kvs)/sizeof(map_kvs[0]), java_properties))) {
    LOG_WARN("failed to create java properties map", K(ret));
  }

  OBJNI_RUN(data_source_object = jni_env->CallStaticObjectMethod((jclass)data_source_factory_class_.handle(),
                                                                 create_data_source_method_,
                                                                 java_properties));
  CK (OB_NOT_NULL(data_source_object));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(data_source_object_ref.from_local_ref(data_source_object, jni_env))) {
    LOG_WARN("failed to create global ref of data source object", K(ret));
  }

  return ret;
}

int ObJavaDataSourceFactoryHelper::export_arrow_stream(jobject record_batch_reader_object, ArrowArrayStream &arrow_stream)
{
  int ret = OB_SUCCESS;
  JNIEnv *jni_env = nullptr;
  if (OB_ISNULL(record_batch_reader_object)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(record_batch_reader_object));
  } else if (OB_FAIL(ObJniTool::instance().get_jni_env(jni_env))) {
    LOG_WARN("failed to get jni env", K(ret));
  }

  ObJniTool &jni_tool = ObJniTool::instance();
  OBJNI_RUN(jni_env->CallStaticObjectMethod(jni_tool.jni_utils_class(),
                                            jni_tool.jni_utils_export_arrow_stream_method(),
                                            record_batch_reader_object,
                                            (jlong)(&arrow_stream)));

  return ret;
}

int ObJavaDataSourceFactoryHelper::import_record_batch(
    struct ArrowArray *array_c, struct ArrowSchema *schema_c, jobject &vector_schema_root_ret)
{
  int ret = OB_SUCCESS;
  JNIEnv *jni_env = nullptr;
  if (OB_FAIL(ObJniTool::instance().get_jni_env(jni_env))) {
    LOG_WARN("failed to get jni env", K(ret));
  }
  ObJniTool &jni_tool = ObJniTool::instance();
  OBJNI_RUN(vector_schema_root_ret = jni_env->CallStaticObjectMethod(jni_tool.jni_utils_class(),
                                                                     jni_tool.jni_utils_import_record_batch_method(),
                                                                     (jlong)(array_c),
                                                                     (jlong)(schema_c)));
  return ret;
}
////////////////////////////////////////////////////////////////////////////////
// ObJavaExternalPlugin
ObJavaExternalPlugin::~ObJavaExternalPlugin()
{}

int ObJavaExternalPlugin::init(ObPluginParam *param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(data_source_factory_.init())) {
    LOG_WARN("failed to init java data source factory helper", K(ret));
  }
  return ret;
}

int ObJavaExternalPlugin::deinit(ObPluginParam *param)
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObJavaExternalPlugin::validate_properties(ObIAllocator &allocator,
                                              const external::ObTableSchema &table_schema,
                                              const ObString &plugin_name,
                                              ObString &parameters)
{
  int ret = OB_SUCCESS;
  ObJavaExternalDataEngine *engine = nullptr;
  ObJavaGlobalRef create_data_source_object;
  ObArenaAllocator arena_allocator;
  ObString plugin_name_value;
  ObJavaLogGuard java_log_guard;
  if (OB_FAIL(ob_alloc_shared(java_log_guard, allocator))) {
    LOG_WARN("failed to create java log guard", K(ret));
  } else if (OB_FAIL(data_source_factory_.create_data_source(parameters, plugin_name, create_data_source_object))) {
    LOG_WARN("failed to create data source object", K(ret));
  } else if (OB_ISNULL(engine = OB_NEWx(ObJavaExternalDataEngine,
                                        &arena_allocator,
                                        parameters,
                                        std::move(create_data_source_object),
                                        data_source_factory_,
                                        java_log_guard))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObJavaExternalDataEngine", K(sizeof(ObJavaExternalDataEngine)));
  } else if (OB_FAIL(engine->init())) {
    LOG_WARN("failed to init data engine", K(ret));
  }

  if (OB_NOT_NULL(engine)) {
    OB_DELETEx(ObJavaExternalDataEngine, &arena_allocator, engine);
    engine = nullptr;
  }
  LOG_TRACE("validate properties done", K(ret));
  return ret;
}

int ObJavaExternalPlugin::create_data_engine(ObIAllocator &allocator,
                                             const ObString &parameters,
                                             const ObString &plugin_name,
                                             ObExternalDataEngine *&engine)
{
  int ret = OB_SUCCESS;
  engine = nullptr;
  ObJavaGlobalRef create_data_source_object;
  ObJavaLogGuard java_log_guard;
  if (OB_FAIL(ob_alloc_shared(java_log_guard, allocator))) {
    LOG_WARN("failed to create java log guard", K(ret));
  } else if (OB_FAIL(data_source_factory_.create_data_source(parameters, plugin_name, create_data_source_object))) {
    LOG_WARN("failed to create data source object", K(ret));
  } else if (OB_ISNULL(engine = OB_NEWx(ObJavaExternalDataEngine,
                                        &allocator,
                                        parameters,
                                        std::move(create_data_source_object),
                                        data_source_factory_,
                                        java_log_guard))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObJavaExternalDataEngine", K(sizeof(ObJavaExternalDataEngine)));
  } else if (OB_FAIL(engine->init())) {
    LOG_WARN("failed to init data engine", K(ret));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(engine)) {
    OB_DELETEx(ObExternalDataEngine, &allocator, engine);
    engine = nullptr;
  }
  LOG_TRACE("create java external data engine", K(ret));
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObJavaExternalDataEngine
ObJavaExternalDataEngine::ObJavaExternalDataEngine(const ObString &parameters,
                                                   ObJavaGlobalRef &&data_source_object,
                                                   ObJavaDataSourceFactoryHelper &data_source_factory,
                                                   ObJavaLogGuard &java_log_guard)
    : ObExternalDataEngine(parameters),
      data_source_object_(std::move(data_source_object)),
      data_source_factory_(data_source_factory),
      java_log_guard_(java_log_guard)
{}

ObJavaExternalDataEngine::~ObJavaExternalDataEngine()
{
  destroy();
}

int ObJavaExternalDataEngine::init()
{
  int ret = OB_SUCCESS;
  JNIEnv *jni_env = nullptr;
  jclass data_source_class = nullptr;
  LOCAL_REF_GUARD_ENV(data_source_class, jni_env);
  if (OB_ISNULL(data_source_object_.handle())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(data_source_object_.handle()));
  } else if (OB_FAIL(ObJniTool::instance().get_jni_env(jni_env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (OB_ISNULL(data_source_class = jni_env->GetObjectClass(data_source_object_.handle()))) {
    ret = OB_JNI_CLASS_NOT_FOUND_ERROR;
    LOG_WARN("failed to get the class of data source object", K(ret));
  }
  OBJNI_RUN(create_scanner_method_ = jni_env->GetMethodID(data_source_class,
                                                          "createScanner",
                                                          "(Ljava/util/Map;)Lorg/apache/arrow/vector/ipc/ArrowReader;"));
  CK (OB_NOT_NULL(create_scanner_method_));

  //OBJNI_RUN(split_task_method_ = jni_env->GetMethodID(data_source_class,
  //                                                    "splitTask",
  //                                                  "(J)Ljava/util/List;"));
  //CK(OB_NOT_NULL(split_task_method_));

  OBJNI_RUN(pushdown_filters_method_ = jni_env->GetMethodID(data_source_class,
                                                            "pushdownFilters",
                                                            "(Ljava/util/List;)Ljava/util/List;"));
  CK(OB_NOT_NULL(pushdown_filters_method_));

  OBJNI_RUN(display_string_method_ = jni_env->GetMethodID(data_source_class,
                                                          "toDisplayString",
                                                          "()Ljava/lang/String;"));
  CK(OB_NOT_NULL(display_string_method_));
  LOG_TRACE("java external data engine init done", K(ret));
  return ret;
}

void ObJavaExternalDataEngine::destroy()
{
  data_source_object_.clear();
  java_log_guard_.reset();
}

int ObJavaExternalDataEngine::display(ObIAllocator &allocator, ObString &display_string)
{
  int ret = OB_SUCCESS;
  JNIEnv *jni_env = nullptr;
  jobject jdisplay_string = nullptr;
  LOCAL_REF_GUARD_ENV(jdisplay_string, jni_env);
  if (OB_ISNULL(display_string_method_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KP(display_string_method_));
  } else if (OB_FAIL(ObJniTool::instance().get_jni_env(jni_env))) {
    LOG_WARN("failed to get jni env", K(ret));
  }
  OBJNI_RUN(jdisplay_string = jni_env->CallObjectMethod(data_source_object_.handle(), display_string_method_));
  CK (OB_NOT_NULL(jdisplay_string));

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObJniTool::instance().string_from_java(jni_env, (jstring)jdisplay_string, allocator, display_string))) {
    LOG_WARN("failed to convert java string to obstring", K(ret));
  }
  return ret;
}

int ObJavaExternalDataEngine::split_task(ObIAllocator &allocator, int64_t parallelism, ObIArray<ObString> &tasks)
{
  int ret = OB_SUCCESS;
  // create an empty task
  if (OB_FAIL(tasks.push_back(ObString()))) {
    LOG_WARN("failed to push empty string", K(ret));
  }
  LOG_TRACE("java external data engine split task done", K(ret), K(tasks.count()));
  return ret;
}

int ObJavaExternalDataEngine::pushdown_filters(ObIAllocator &allocator,
                                               const ParamStore &param_store,
                                               const ObIArray<ObRawExpr *> &filters,
                                               ObIArray<ObString> &pushdown_filters_ret)
{
  int ret = OB_SUCCESS;
  ObArrowStatus obstatus(ret);
  ObArrowFilterListBuilder filter_list_builder;
  ObArrowFilterBuilder filter_builder(param_store);
  shared_ptr<RecordBatch> arrow_filter;

  /// not all filters can be convert to Java SqlFilter.
  /// so we use a map to indicates the dst filter array elements to the source filter array elements
  ObArray<int64_t> index_map;

  jclass jni_utils_class = ObJniTool::instance().jni_utils_class();
  jmethodID parse_sql_filter_method = ObJniTool::instance().jni_utils_parse_sql_filter_from_arrow_method();
  index_map.set_label(OB_PLUGIN_MEMORY_LABEL);
  index_map.set_tenant_id(MTL_ID());
  if (OB_ISNULL(jni_utils_class) || OB_ISNULL(parse_sql_filter_method)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid state", KP(jni_utils_class), KP(parse_sql_filter_method));
  }

  // tranverse into arrow type
  for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); i++) {
    ObRawExpr *raw_expr = filters.at(i);
    filter_builder.reuse();
    if (OB_ISNULL(raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", KP(raw_expr));
    } else if (OB_FAIL(filter_builder.build_filter(raw_expr, arrow_filter))) {
      LOG_WARN("failed to build arrow filter", K(ret));
    } else if (!arrow_filter) {
      LOG_TRACE("the filter is not supported push down to plugin", K(i));
    } else if (OB_FAIL(filter_list_builder.add_filter(arrow_filter))) {
      LOG_WARN("failed to add filter to filter list builder", K(ret));
    } else if (OB_FAIL(index_map.push_back(i))) {
      LOG_WARN("failed to push back index map", K(ret));
    }
  }

  shared_ptr<RecordBatch> arrow_filter_list;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(filter_list_builder.build(arrow_filter_list))) {
    LOG_WARN("failed to build arrow filter list", K(ret));
  }

  // export C++ arrow into C
  struct ArrowArray array_c;
  struct ArrowSchema schema_c;
  if (OB_FAIL(ret)) {
  } else if (OBARROW_FAIL(ExportRecordBatch(*arrow_filter_list, &array_c, &schema_c))) {
    LOG_WARN("failed to export record batch to C", K(obstatus));
  }

  // export arrow data into Java
  JNIEnv *jni_env = nullptr;
  jobject jvector_schema_root = nullptr;
  LOCAL_REF_GUARD_ENV(jvector_schema_root, jni_env);
  jobject jsql_filter_list = nullptr;
  LOCAL_REF_GUARD_ENV(jsql_filter_list, jni_env);
  jobject accept_filter_list = nullptr;
  LOCAL_REF_GUARD_ENV(accept_filter_list, jni_env);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObJniTool::instance().get_jni_env(jni_env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (OB_FAIL(data_source_factory_.import_record_batch(&array_c, &schema_c, jvector_schema_root))) {
    LOG_WARN("failed to export record batch to java", K(ret));
  }
  // convert arrow::RecordBatch into Java SqlFilter
  OBJNI_RUN(jsql_filter_list = jni_env->CallStaticObjectMethod(
      jni_utils_class, parse_sql_filter_method, jvector_schema_root));
  // call java pushdown_filters
  OBJNI_RUN(accept_filter_list = jni_env->CallObjectMethod(
      data_source_object_.handle(), pushdown_filters_method_, jsql_filter_list));

  ObArray<ObString> java_accept_filter_list;
  java_accept_filter_list.set_label(OB_PLUGIN_MEMORY_LABEL);
  java_accept_filter_list.set_tenant_id(MTL_ID());
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObJniTool::instance().string_list_from_java(
          jni_env, accept_filter_list, allocator, java_accept_filter_list))) {
    LOG_WARN("failed to convert java list to ob list", K(ret));
  } else if (java_accept_filter_list.count() > index_map.count()) {
    ret = OB_PLUGIN_ERROR;
    LOG_WARN("invalid filter number", K(ret), K(java_accept_filter_list.count()), K(index_map.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); i++) {
      if (OB_FAIL(pushdown_filters_ret.push_back(ObString()))) {
        LOG_WARN("failed to push empty string into pushdown_filters_ret", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < java_accept_filter_list.count(); i++) {
      int64_t ob_index = index_map.at(i);
      pushdown_filters_ret.at(ob_index) = java_accept_filter_list.at(i);
    }
  }
  LOG_TRACE("pushdown filters done", K(ret), K(pushdown_filters_ret));
  return ret;
}

int ObJavaExternalDataEngine::open_scanner(ObIAllocator &allocator,
                                           const ObExternalTableScanParam &param,
                                           ObExternalScanner *&scanner)
{
  int ret = OB_SUCCESS;
  scanner = nullptr;
  JNIEnv *jni_env = nullptr;
  jobject java_scanner_object = nullptr;
  LOCAL_REF_GUARD_ENV(java_scanner_object, jni_env);
  jobject extra_parameters = nullptr;
  LOCAL_REF_GUARD_ENV(extra_parameters, jni_env);
  if (OB_ISNULL(data_source_object_.handle()) || OB_ISNULL(create_scanner_method_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("can't open scanner as the engine is not inited", KP(data_source_object_.handle()), KP(create_scanner_method_));
  } else if (OB_FAIL(ObJniTool::instance().get_jni_env(jni_env))) {
    LOG_WARN("failed to get jni env", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(create_scanner_parameters(param, jni_env, extra_parameters))) {
    LOG_WARN("failed to create scanner parameters", K(ret));
  }

  LOG_TRACE("before call 'createScanner'", K(ret));
  OBJNI_RUN(java_scanner_object = jni_env->CallObjectMethod(data_source_object_.handle(),
                                                            create_scanner_method_,
                                                            extra_parameters));
  LOG_TRACE("after call 'createScanner'", K(ret));

  CK (OB_NOT_NULL(java_scanner_object));
  ObJavaGlobalRef scanner_global_ref;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(scanner_global_ref.from_local_ref(java_scanner_object, jni_env))) {
    LOG_WARN("failed to create scanner global ref", K(ret));
  } else if (OB_ISNULL(scanner = OB_NEWx(ObJavaExternalScanner,
                                         &allocator,
                                         std::move(scanner_global_ref),
                                         data_source_factory_,
                                         java_log_guard_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObJavaExternalScanner", K(sizeof(ObJavaExternalScanner)));
  } else if (OB_FAIL(scanner->init())) {
    LOG_WARN("failed to init java external scanner", K(ret));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(scanner)) {
    OB_DELETEx(ObExternalScanner, &allocator, scanner);
    scanner = nullptr;
  }
  LOG_TRACE("java external data engine create scanner done", K(ret));
  return ret;
}

int ObJavaExternalDataEngine::create_param_store_jobject(const ObExternalTableScanParam &param,
                                                         JNIEnv *jni_env,
                                                         jobject &param_store_jobject_ret)
{
  int ret = OB_SUCCESS;
  ObArrowStatus obstatus(ret);

  const ParamStore *param_store = nullptr;
  if (OB_ISNULL(param.storage_param()->op_) ||
      OB_ISNULL(param.storage_param()->op_->get_eval_ctx().exec_ctx_.get_physical_plan_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    param_store = &param.storage_param()->op_->get_eval_ctx().exec_ctx_.get_physical_plan_ctx()->get_param_store();
  }

  OB_TRY_BEGIN;
  // build param store into record batch first
  shared_ptr<Schema> schema;
  SchemaBuilder schema_builder;
  ArrayVector columns;
  columns.reserve(param_store->count());
  shared_ptr<Array> array_value;
  shared_ptr<RecordBatch> record_batch;
  for (int64_t i = 0; OB_SUCC(ret) && i < param_store->count(); i++) {
    const ObObjParam &obj_param = param_store->at(i);
    if (OB_FAIL(ObArrowObject::get_array(obj_param, array_value))) {
      LOG_WARN("failed to get arrow scalar from obobj", K(ret));
    } else if (!array_value) {
      NullBuilder null_builder;
      if (OBARROW_FAIL(null_builder.AppendNull())) {
        LOG_WARN("failed to append null", K(obstatus));
      } else if (OBARROW_FAIL(null_builder.Finish(&array_value))) {
        LOG_WARN("failed to build null array", K(obstatus));
      }
      LOG_TRACE("this object type is not supported yet. ignore");
    }
    if (OB_SUCC(ret)) {
      if (OBARROW_FAIL(schema_builder.AddField(arrow::field(std::to_string(i), array_value->type())))) {
        LOG_WARN("failed to add field to schema builder", K(obstatus));
      } else {
        columns.emplace_back(array_value);
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OBARROW_FAIL(schema_builder.Finish().Value(&schema))) {
    LOG_WARN("failed to build schema", K(obstatus));
  } else if (schema->num_fields() > 0) {
    record_batch = RecordBatch::Make(schema, 1, columns);
    LOG_TRACE("build record batch for param store success", KCSTRING(record_batch->ToString().c_str()));
  }

  // export C++ arrow into C
  struct ArrowArray array_c;
  struct ArrowSchema schema_c;
  jobject jvector_schema_root = nullptr;
  LOCAL_REF_GUARD_ENV(jvector_schema_root, jni_env);
  if (OB_FAIL(ret) || !record_batch) {
  } else if (OBARROW_FAIL(ExportRecordBatch(*record_batch, &array_c, &schema_c))) {
    LOG_WARN("failed to export record batch to C", K(obstatus));
  } else if (OB_FAIL(data_source_factory_.import_record_batch(&array_c, &schema_c, jvector_schema_root))) {
    LOG_WARN("failed to export record batch to java", K(ret));
  }

  jclass jni_utils_class = ObJniTool::instance().jni_utils_class();
  jmethodID parse_question_mark_values_method = ObJniTool::instance().jni_utils_parse_question_mark_values_method();
  if (OB_ISNULL(jni_utils_class) || OB_ISNULL(parse_question_mark_values_method)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid state", KP(jni_utils_class), KP(parse_question_mark_values_method));
  }

  // convert arrow::RecordBatch into Java List<Object>
  jobject java_value_list = nullptr;
  LOCAL_REF_GUARD_ENV(java_value_list, jni_env);
  if (record_batch) {
    OBJNI_RUN(java_value_list = jni_env->CallStaticObjectMethod(
        jni_utils_class, parse_question_mark_values_method, jvector_schema_root));
  }

  if (OB_SUCC(ret) && record_batch) {
    param_store_jobject_ret = java_value_list;
    java_value_list = nullptr;
  }
  OB_TRY_END;
  return ret;
}
int ObJavaExternalDataEngine::create_scanner_parameters(const ObExternalTableScanParam &param,
                                                        JNIEnv *jni_env,
                                                        jobject &map_ret)
{
  int ret = OB_SUCCESS;

  const char *KEY_NAME_COLUMNS = "columns";
  const char *KEY_NAME_FILTERS = "filters";
  const char *KEY_NAME_QUESTION_MARK_VALUES = "question_mark_values";

  map_ret = nullptr;

  jobject extra_parameters = nullptr;
  LOCAL_REF_GUARD_ENV(extra_parameters, jni_env);
  jclass hash_map_class = ObJniTool::instance().hash_map_class();
  jmethodID hash_map_constructor = ObJniTool::instance().hash_map_constructor_method();
  jmethodID hash_map_put_method = ObJniTool::instance().hash_map_put_method();

  if (OB_ISNULL(param.storage_param())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("storage param is null", K(ret));
  }

  OBJNI_RUN(extra_parameters = jni_env->NewObject(hash_map_class, hash_map_constructor));

  jstring columns_name = nullptr;
  LOCAL_REF_GUARD_ENV(columns_name, jni_env);
  jobject java_columns = nullptr;
  LOCAL_REF_GUARD_ENV(java_columns, jni_env);
  jobject put_result = nullptr;
  LOCAL_REF_GUARD_ENV(put_result, jni_env);
  jstring filters_name = nullptr;
  LOCAL_REF_GUARD_ENV(filters_name, jni_env);
  jobject java_filters = nullptr;
  LOCAL_REF_GUARD_ENV(java_filters, jni_env);
  jstring question_mark_values_name = nullptr;
  LOCAL_REF_GUARD_ENV(question_mark_values_name, jni_env);
  jobject java_question_mark_value_list = nullptr;
  LOCAL_REF_GUARD_ENV(java_question_mark_value_list, jni_env);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObJniTool::instance().create_java_string(jni_env, KEY_NAME_COLUMNS, columns_name))) {
    LOG_WARN("failed to create java string", K(ret), KCSTRING(KEY_NAME_COLUMNS));
  } else if (OB_FAIL(ObJniTool::instance().create_java_list_from_array_string(jni_env, param.columns(), java_columns))) {
    LOG_WARN("failed to create java columns", K(ret));
  } else if (OB_NOT_NULL(param.storage_param()->external_pushdown_filters_)) {
    if (OB_FAIL(ObJniTool::instance().create_java_string(jni_env, KEY_NAME_FILTERS, filters_name))) {
      LOG_WARN("failed to create java string", K(ret), KCSTRING(KEY_NAME_FILTERS));
    } else if (OB_FAIL(ObJniTool::instance().create_java_list_from_array_string(
        jni_env, *param.storage_param()->external_pushdown_filters_, java_filters))) {
      LOG_WARN("failed to create java filters", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(create_param_store_jobject(param, jni_env, java_question_mark_value_list))) {
    LOG_WARN("failed to create param store java object", K(ret));
  } else if (OB_ISNULL(java_question_mark_value_list)) {
    LOG_TRACE("no param store");
  } else if (OB_FAIL(ObJniTool::instance().create_java_string(
      jni_env, KEY_NAME_QUESTION_MARK_VALUES, question_mark_values_name))) {
    LOG_WARN("failed to create java question mark values name string", K(ret));
  }

  OBJNI_RUN(put_result = jni_env->CallObjectMethod(extra_parameters, hash_map_put_method, columns_name, java_columns));
  if (OB_SUCC(ret) && OB_NOT_NULL(put_result)) {
    jni_env->DeleteLocalRef(put_result);
    put_result = nullptr;
  }

  OBJNI_RUN(put_result = jni_env->CallObjectMethod(extra_parameters, hash_map_put_method, filters_name, java_filters));
  if (OB_SUCC(ret) && OB_NOT_NULL(put_result)) {
    jni_env->DeleteLocalRef(put_result);
    put_result = nullptr;
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(java_question_mark_value_list) && OB_NOT_NULL(question_mark_values_name)) {
    OBJNI_RUN(put_result = jni_env->CallObjectMethod(
        extra_parameters, hash_map_put_method, question_mark_values_name, java_question_mark_value_list));
    if (OB_SUCC(ret) && OB_NOT_NULL(put_result)) {
      jni_env->DeleteLocalRef(put_result);
      put_result = nullptr;
    }
  }
  if (OB_SUCC(ret)) {
    map_ret = extra_parameters;
    extra_parameters = nullptr;
  }
  LOG_TRACE("create java scan parameters done", K(ret));
  return ret;
}

int ObJavaExternalDataEngine::create_filters(const ObExternalTableScanParam &scan_param,
                                             JNIEnv *jni_env,
                                             jobject &filters_ret)
{
  int ret = OB_SUCCESS;
  jobject filters_pushdown = nullptr;
  LOCAL_REF_GUARD_ENV(filters_pushdown, jni_env);

  const ObTableScanParam *storage_scan_param = scan_param.storage_param();
  ObPushdownFilterExecutor *pushdown_storage_filters = nullptr;
  if (OB_ISNULL(storage_scan_param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no table scan param", KP(storage_scan_param));
  } else if (OB_ISNULL(pushdown_storage_filters = storage_scan_param->pd_storage_filters_)) {
    LOG_TRACE("pushdown filters is null");
  }
  LOG_TRACE("create java filters done", K(ret));
  return ret;
}
////////////////////////////////////////////////////////////////////////////////
// ObJavaExternalScanner
ObJavaExternalScanner::ObJavaExternalScanner(ObJavaGlobalRef &&java_scanner_object,
                                             ObJavaDataSourceFactoryHelper &data_source_factory,
                                             ObJavaLogGuard &java_log_guard)
    : java_scanner_object_(std::move(java_scanner_object)),
      data_source_factory_(data_source_factory),
      java_log_guard_(java_log_guard)
{
  MEMSET(&arrow_stream_, 0, sizeof(arrow_stream_));
}

ObJavaExternalScanner::~ObJavaExternalScanner()
{
  destroy();
}

int ObJavaExternalScanner::init()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(java_scanner_object_.handle())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("invalid argument", KP(java_scanner_object_.handle()));
  } else if (OB_FAIL(data_source_factory_.export_arrow_stream(java_scanner_object_.handle(), arrow_stream_))) {
    LOG_WARN("failed to export arrow stream", K(ret));
  }
  LOG_TRACE("init java external scanner done", K(ret));
  return ret;
}

int ObJavaExternalScanner::rescan(const ObExternalTableScanParam &param)
{
  return OB_NOT_SUPPORTED;
}

void ObJavaExternalScanner::destroy()
{
  if (OB_NOT_NULL(arrow_stream_.release)) {
    arrow_stream_.release(&arrow_stream_);
    if (OB_NOT_NULL(arrow_stream_.release)) {
      arrow_stream_.release = nullptr; // double check
    }
  }
  java_scanner_object_.clear();
  java_log_guard_.reset();
}

int ObJavaExternalScanner::schema(std::shared_ptr<arrow::Schema> &schema)
{
  int ret = OB_SUCCESS;
  int arrow_ret = 0;
  struct ArrowSchema schema_c;
  ObArrowStatus obstatus(ret);
  if (schema_) {
    schema = schema_;
  } else if (OB_ISNULL(arrow_stream_.get_schema) || OB_ISNULL(arrow_stream_.get_last_error)) {
    ret = OB_NOT_INIT;
    LOG_WARN("failed to get schema as scanner is not init",
             K(ret), KP(arrow_stream_.get_schema), KP(arrow_stream_.get_last_error));
  } else if (0 != (arrow_ret = arrow_stream_.get_schema(&arrow_stream_, &schema_c))) {
    ret = OB_JNI_ERROR;
    LOG_WARN("failed to get schema from arrow stream",
             K(ret), K(arrow_ret), KCSTRING(arrow_stream_.get_last_error(&arrow_stream_)));
  } else if (OBARROW_FAIL(arrow::ImportSchema(&schema_c).Value(&schema_))) {
    LOG_WARN("failed to get schema from java", K(ret), K(obstatus));
  } else {
    schema = schema_;
    LOG_TRACE("get schema success", KCSTRING(schema->ToString(true/*metadata*/).c_str()));
  }
  LOG_TRACE("get schema from external java scanner", K(ret));
  return ret;
}

int ObJavaExternalScanner::next(int64_t capacity, arrow::RecordBatch *&record_batch_ret)
{
  int ret = OB_SUCCESS;
  int arrow_ret = 0;
  ObArrowStatus obstatus(ret);
  struct ArrowArray array_c;
  record_batch_ret = nullptr;
  shared_ptr<arrow::Schema> tmp_schema;
  if (OB_UNLIKELY(OB_ISNULL(arrow_stream_.get_next) || OB_ISNULL(arrow_stream_.get_last_error))) {
    ret = OB_NOT_INIT;
    LOG_WARN("failed to get next rows as the 'get_next' or 'get_last_error' is null", K(ret));
  }else if (!schema_ && OB_FAIL(this->schema(tmp_schema))) {
    // init schema if not inited
    LOG_WARN("failed to get schema", K(ret));
  } else if (0 != (arrow_ret = arrow_stream_.get_next(&arrow_stream_, &array_c))) {
    ret = OB_JNI_ERROR;
    LOG_WARN("failed to get next from java", K(ret), KCSTRING(arrow_stream_.get_last_error(&arrow_stream_)));
  } else if (OB_ISNULL(array_c.release)) {
    ret = OB_ITER_END;
    LOG_TRACE("array is released", K(ret));
  } else if (OBARROW_FAIL(arrow::ImportRecordBatch(&array_c, schema_).Value(&record_batch_))) {
    LOG_WARN("failed to import record batch", K(obstatus));
  } else if (!record_batch_ || record_batch_->num_rows() == 0) {
    ret = OB_ITER_END;
  } else {
    record_batch_ret = record_batch_.get();
    LOG_DEBUG("got record batch", KCSTRING(record_batch_ret->ToString().c_str()));
  }
  LOG_TRACE("get next batch rows from java external scanner", K(ret));
  return ret;
}

} // namespace plugin
} // namespace oceanbase
