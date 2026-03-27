/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "oceanbase/ob_plugin.h"

namespace oceanbase {

namespace storage {
class ObTableScanParam;
} // namespace storage

namespace plugin {

class ObExternalTableScanParam final
{
public:
  ObExternalTableScanParam() = default;
  ~ObExternalTableScanParam();

  int init();
  void reset();

  int append_column(const ObString &column_name);

  void set_storage_param(const storage::ObTableScanParam *storage_param);
  void set_task(const ObString &task);

  const storage::ObTableScanParam *storage_param() const { return storage_param_; }
  const ObString &task() const { return task_; }
  const ObIArray<ObString> &columns() const { return columns_; }

  TO_STRING_KV(K_(task), KP_(storage_param), K_(columns));

private:
  bool              inited_ = false;
  ObMemAttr         mem_attr_;
  ObString          task_;
  ObArray<ObString> columns_;

  const storage::ObTableScanParam *storage_param_ = nullptr;
};

class ObExternalTaskList final
{
public:
  ObExternalTaskList(ObIAllocator &allocator, ObIArray<ObString> &task_list)
      : allocator_(allocator),
        task_list_(task_list)
  {}

  int append_task(const char buf[], int64_t buf_len);

private:
  ObIAllocator &      allocator_;
  ObIArray<ObString> &task_list_;
};

} // namespace plugin
} // namespace oceanabse
