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
 *
 */

#include <cstring>
#include "lib/utility/ob_macro_utils.h"
#include "ob_log_fetching_mode.h"

namespace oceanbase
{
namespace logfetcher
{
const char *print_fetching_mode(const ClientFetchingMode mode)
{
  const char *mode_str = "INVALID";
  switch (mode) {
    case ClientFetchingMode::FETCHING_MODE_INTEGRATED: {
      mode_str = "Integrated Fetching Mode";
      break;
    }

    case ClientFetchingMode::FETCHING_MODE_DIRECT: {
      mode_str = "Direct Fetching Mode";
      break;
    }

    default: {
      mode_str = "INVALID";
      break;
    }

  }
  return mode_str;
}

ClientFetchingMode get_fetching_mode(const char *fetching_mode_str)
{
  ClientFetchingMode fetching_mode = ClientFetchingMode::FETCHING_MODE_UNKNOWN;
  if (OB_ISNULL(fetching_mode_str)) {

  } else if (0 == strcmp(fetching_mode_str, "integrated")) {
    fetching_mode = ClientFetchingMode::FETCHING_MODE_INTEGRATED;
  } else if (0 == strcmp(fetching_mode_str, "direct")) {
    fetching_mode = ClientFetchingMode::FETCHING_MODE_DIRECT;
  } else {

  }
  return fetching_mode;
}

bool is_fetching_mode_valid(const ClientFetchingMode mode)
{
  return mode > ClientFetchingMode::FETCHING_MODE_UNKNOWN &&
         mode < ClientFetchingMode::FETCHING_MODE_MAX;
}

bool is_integrated_fetching_mode(const ClientFetchingMode mode)
{
  return ClientFetchingMode::FETCHING_MODE_INTEGRATED == mode;
}

bool is_direct_fetching_mode(const ClientFetchingMode mode)
{
  return ClientFetchingMode::FETCHING_MODE_DIRECT == mode;
}

}
}
