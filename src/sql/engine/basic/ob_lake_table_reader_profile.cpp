/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SQL_ENG

#include "ob_lake_table_reader_profile.h"


namespace oceanbase
{
using namespace common;
using namespace lib;
namespace sql {

void ObLakeTableIMetrics::set_label(const common::ObString &label)
{
  label_.assign_ptr(label.ptr(), label.length());
}

int ObLakeTableReaderMetrics::update_profile()
{
  int ret = OB_SUCCESS;
  return ret;
}

void ObLakeTableReaderMetrics::dump_metrics()
{
  LOG_INFO("dump metrics", K_(label), KPC(this));
}

int ObLakeTablePreBufferMetrics::update_profile()
{
  int ret = OB_SUCCESS;
  return ret;
}

void ObLakeTablePreBufferMetrics::dump_metrics()
{
  LOG_INFO("dump metrics", K_(label), KPC(this));
}

int ObLakeTableIOMetrics::update_profile()
{
  int ret = OB_SUCCESS;
  return ret;
}

void ObLakeTableIOMetrics::dump_metrics()
{
  LOG_INFO("dump metrics", K_(label), KPC(this));
}

int ObLakeTableReaderProfile::update_profile()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObLakeTableReaderProfile::register_metrics(ObLakeTableIMetrics *metrics, const ObString &label)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(metrics)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is null", K(ret));
  } else if (FALSE_IT(metrics->set_label(label))) {
  } else if (OB_FAIL(metrics_list_.push_back(metrics))) {
    LOG_WARN("failed to add metrics", K(ret));
  }
  return ret;
}

void ObLakeTableReaderProfile::dump_metrics()
{
  for (int64_t i = 0; i < metrics_list_.count(); ++i) {
    metrics_list_.at(i)->dump_metrics();
  }
}


} // end of oceanbase namespace
} // end of oceanbase namespace