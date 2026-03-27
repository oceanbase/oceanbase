/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifdef RL_DEF
RL_DEF(foo_int, RLInt, "1")
RL_DEF(foo_str, RLStr, "foo")
RL_DEF(foo_cap, RLCap, "1K")
RL_DEF(max_datafile_size, RLCap, "20T")
RL_DEF(max_session_count, RLInt, "100000")
RL_DEF(max_concurrent_query_count, RLInt, "5000")
#endif
