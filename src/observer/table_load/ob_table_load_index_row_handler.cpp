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
#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_index_row_handler.h"

namespace oceanbase
{
namespace observer
{
using namespace blocksstable;

ObTableLoadIndexRowHandler::ObTableLoadIndexRowHandler() {}

ObTableLoadIndexRowHandler::~ObTableLoadIndexRowHandler() {}

int ObTableLoadIndexRowHandler::handle_insert_row(const ObTabletID tablet_id,
                                                  const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  //do nothing
  return ret;
}

int ObTableLoadIndexRowHandler::handle_delete_row(const ObTabletID tablet_id,
                                                  const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  // do nothing
  return ret;
}
} // namespace observer
} // namespace oceanbase