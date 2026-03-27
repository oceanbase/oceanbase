/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

void fixed_stack_init(fixed_stack_t* stk)
{
  stk->top_ = 0;
  memset(stk->array_, 0, sizeof(stk->array_));
}
extern int fixed_stack_push(fixed_stack_t* stk, void* p);
extern void* fixed_stack_pop(fixed_stack_t* stk);
