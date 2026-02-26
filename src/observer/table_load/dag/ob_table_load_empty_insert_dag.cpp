/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/dag/ob_table_load_empty_insert_dag.h"

namespace oceanbase
{
namespace observer
{
using namespace share;
using namespace storage;

int ObTableLoadEmptyInsertDag::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObTableLoadEmptyInsertDagInitParam *init_param =
    static_cast<const ObTableLoadEmptyInsertDagInitParam *>(param);
  if (OB_UNLIKELY(nullptr == init_param || !init_param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(init_param));
  } else if (OB_FAIL(ObDDLIndependentDag::init_by_param(init_param))) {
    LOG_WARN("init ddl independent dag failed", K(ret), KPC(init_param));
  } else {
    ObArray<ObITask *> write_macro_block_tasks;
    if (OB_FAIL(generate_write_macro_block_tasks(write_macro_block_tasks))) {
      LOG_WARN("fail to generate write macro block tasks", KR(ret));
    } else if (OB_FAIL(batch_add_task(write_macro_block_tasks))) {
      LOG_WARN("batch add task failed", K(ret), K(write_macro_block_tasks.count()));
    }
  }
  FLOG_INFO("empty insert dag init", KR(ret), KPC(this));
  return ret;
}

} // namespace observer
} // namespace oceanbase
