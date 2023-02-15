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
#include <curl/curl.h>
#include "lib/allocator/ob_malloc.h"    // ob_set_memory_size_limit
#include "lib/utility/utility.h"        // get_phy_mem_size

#include "ob_log_common.h"          // MAX_MEMORY_USAGE_PERCENT
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

} // namespace libobcdc
} // namespace oceanbase
