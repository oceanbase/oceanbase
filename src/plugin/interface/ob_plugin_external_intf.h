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

#include "apache-arrow/arrow/api.h"
#include "plugin/interface/ob_plugin_intf.h"
#include "plugin/external_table/ob_external_struct.h"
#include "plugin/external_table/ob_external_table_schema.h"
#include "common/object/ob_object.h"

namespace oceanbase {

namespace sql {
class ObRawExpr;
}

namespace plugin {

class ObExternalScanner;
class ObExternalDataEngine;

class ObIExternalDescriptor : public ObIPluginDescriptor
{
public:
  ObIExternalDescriptor() = default;
  virtual ~ObIExternalDescriptor() = default;

  // int init(ObPluginParam *) override;
  // int deinit(ObPluginParam *) override;

  virtual const char *name() const = 0;

  /**
   * validate the properties
   * @param[in] table_schema The schema of the table which will be created
   * @param[in] plugin_name  Plugin name.
   * @param[inout] properties The properties given(from SQL request usually) and
   * set the default values if the key not exist.
   * @return return OB_SUCCESS if everything OK, others return an error code.
   */
  virtual int validate_properties(ObIAllocator &allocator,
                                  const external::ObTableSchema &table_schema,
                                  const ObString &plugin_name,
                                  ObString &parameters)
  {
    return OB_SUCCESS;
  }

  virtual int create_data_engine(ObIAllocator &allocator,
                                 const ObString &parameters,
                                 const ObString &plugin_name,
                                 ObExternalDataEngine *&data_engine) = 0;
};

class ObExternalDataEngine
{
public:
  ObExternalDataEngine(const ObString &parameters)
      : parameters_(parameters)
  {}

  virtual ~ObExternalDataEngine() = default;

  virtual int init() = 0;
  virtual void destroy() = 0;

  virtual int display(ObIAllocator &allocator, ObString &display_string)
  {
    return ob_write_string(allocator, parameters_, display_string);
  }

  virtual int split_task(ObIAllocator &allocator, int64_t parallelism, ObIArray<ObString> &tasks) = 0;

  /**
   * detect which filters(predicate) can pushdown
   * @param[in] allocator The allocator to allocate memory for `pushdown_filters`.
   * @param[in] filters The filters may pushdown to storage or external table data source.
   * @param[out] pushdown_filters The filters serialized. Should be empty string if any filter can't be supported.
   * The number of `pushdown_filters` should be equals to `filters`.
   */
  virtual int pushdown_filters(ObIAllocator &allocator,
                               const common::ParamStore &param_store,
                               const ObIArray<sql::ObRawExpr *> &filters,
                               ObIArray<ObString> &pushdown_filters) = 0;
  virtual int open_scanner(ObIAllocator &allocator,
                           const ObExternalTableScanParam &param,
                           ObExternalScanner *&scanner) = 0;

protected:
  const ObString &parameters_;
};

class ObExternalScanner
{
public:
  ObExternalScanner() = default;
  virtual ~ObExternalScanner() = default;

  virtual int init() = 0;
  virtual int rescan(const ObExternalTableScanParam &param) = 0;
  /**
   * get the schema of `record_batch` returned by `next`
   * @detauls A schema describe a list of columns. `table name` is not needed.
   */
  virtual int schema(std::shared_ptr<arrow::Schema> &schema) = 0;

  /**
   * get a batch of records
   * @details You should return array columns in the same order with `shema`.
   */
  virtual int next(int64_t capacity, arrow::RecordBatch *&record_batch) = 0;

  virtual void destroy() = 0;
};

} // namespace plugin
} // namespace oceanbase
