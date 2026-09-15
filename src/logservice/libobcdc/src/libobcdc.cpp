/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


#include "libobcdc.h"
#include <locale.h>

#include "lib/signal/ob_signal_struct.h"
#include "ob_log_instance.h"        // ObLogInstance

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{

ObCDCFactory::ObCDCFactory()
{
  // Enable the common signal handler to output CRASH ERROR diagnostics.
  // If observer minicore files are absent, minicoredump() returns directly.
  g_redirect_handler = true;

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
