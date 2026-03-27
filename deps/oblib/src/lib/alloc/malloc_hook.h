/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef MALLOC_HOOK_H
#define MALLOC_HOOK_H
extern void init_malloc_hook();

inline bool& in_hook()
{
  thread_local bool in_hook = false;
  return in_hook;
}

#endif /* MALLOC_HOOK_H */
