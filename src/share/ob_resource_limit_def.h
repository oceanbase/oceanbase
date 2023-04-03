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
 */

#ifdef RL_DEF
RL_DEF(foo_int, RLInt, "1")
RL_DEF(foo_str, RLStr, "foo")
RL_DEF(foo_cap, RLCap, "1K")
RL_DEF(max_datafile_size, RLCap, "20T")
RL_DEF(max_session_count, RLInt, "100000")
RL_DEF(max_concurrent_query_count, RLInt, "5000")
#endif
