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
 *
 * OBCDC header file
 * This file defines interface of OBCDC
 */


#include "libobcdc.h"
#include <locale.h>

#include "ob_log_instance.h"        // ObLogInstance

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{

ObCDCFactory::ObCDCFactory()
{
  // set max memory limit
  lib::set_memory_limit(get_phy_mem_size() * MAX_MEMORY_USAGE_PERCENT / 100);

  CURLcode curl_code = curl_global_init(CURL_GLOBAL_ALL);

  if (OB_UNLIKELY(CURLE_OK != curl_code)) {
    OBLOG_LOG_RET(ERROR, OB_ERR_SYS, "curl_global_init fail", K(curl_code));
  }

  setlocale(LC_ALL, "");
  setlocale(LC_TIME, "en_US.UTF-8");
}

ObCDCFactory::~ObCDCFactory()
{
  curl_global_cleanup();
}

IObCDCInstance *ObCDCFactory::construct_obcdc()
{
  return ObLogInstance::get_instance();
}

void ObCDCFactory::deconstruct(IObCDCInstance *log)
{
  UNUSED(log);

  ObLogInstance::destroy_instance();
}

CDCTaskStat::CDCTaskStat() { reset(); }
CDCTaskStat::~CDCTaskStat() { reset(); }
void CDCTaskStat::reset()
{
  fetcher_part_trans_count_ = 0;
  ddl_in_process_part_trans_count_ = 0;
  seq_queue_part_trans_count_ = 0;
  seq_ready_trans_count_ = 0;
  seqed_trans_count_ = 0;
  storage_task_count_ = 0;
  reader_task_count_ = 0;
  dml_parser_redo_count_ = 0;
  formatter_br_count_ = 0;
  formatter_redo_count_ = 0;
  formatter_lob_stmt_count_ = 0;
  lob_merger_task_count_ = 0;
  sorter_task_count_ = 0;
  committer_dml_task_count_ = 0;
  committer_ddl_task_count_ = 0;
  br_queue_dml_count_ = 0;
  br_queue_ddl_count_ = 0;
  rc_part_trans_count_ = 0;
  rc_br_count_ = 0;
  out_ddl_br_count_ = 0;
  out_dml_br_count_ = 0;
}
} // namespace libobcdc
} // namespace oceanbase
