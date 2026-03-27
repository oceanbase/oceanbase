/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include "ob_di_tls.h"

namespace oceanbase
{
namespace common
{
thread_local bool is_thread_in_exit = false;
}
}
