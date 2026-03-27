/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
