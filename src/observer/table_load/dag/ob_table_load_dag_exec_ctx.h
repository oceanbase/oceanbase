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

#pragma once

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadPlan;
class ObTableLoadDag;

struct ObTableLoadDagExecCtx
{
public:
  ObTableLoadDagExecCtx() : plan_(nullptr), dag_(nullptr) {}

  TO_STRING_KV(KP_(plan), KP_(dag));

public:
  ObTableLoadPlan *plan_;
  ObTableLoadDag *dag_;
};

} // namespace observer
} // namespace oceanbase
