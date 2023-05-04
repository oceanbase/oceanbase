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

#include "ob_trans_define.h"

namespace oceanbase {
namespace transaction {
#define PRE_STATIC_DECODE
#define POST_STATIC_DECODE
#define PRE_DYNAMIC_DECODE \
  auto save_flags = flags_;
#define POST_DYNAMIC_DECODE \
  flags_ = save_flags.update_with(flags_);

#define PRE_EXTRA_DECODE
#define POST_EXTRA_DECODE                                               \

template<class T>
struct SIZE_OF_ { static int64_t get_size(T &x) { return sizeof(x); } };
template<>
int64_t SIZE_OF_<ObTxPartList>::get_size(ObTxPartList &x) { return x.count() * sizeof(ObTxPart); }
#define SIZE_OF(x) SIZE_OF_<typeof(x)>::get_size(x)
#define TXN_UNIS_DECODE(x, idx) OB_UNIS_DECODE(_member_##idx)
#define TXN_STATE_K(x, idx) #x, _member_##idx
#define DEF_MEMBER_(m, idx) decltype(ObTxDesc::m) _member_ ##idx
#define DEF_MEMBER(m, idx) DEF_MEMBER_(m, idx)
#define TXN_FREE_ROUTE_MEMBERS(name, PRE_DECODE_HANDLER, POST_DECODE_HANDLER, ...) \
int ObTxDesc::encode_##name##_state(char *buf, const int64_t buf_len, int64_t &pos) \
{                                                                       \
  int ret = OB_SUCCESS;                                                 \
  LST_DO_CODE(OB_UNIS_ENCODE, ##__VA_ARGS__);                           \
  return ret;                                                           \
}                                                                       \
int ObTxDesc::decode_##name##_state(const char *buf, const int64_t data_len, int64_t &pos) \
{                                                                       \
  int ret = OB_SUCCESS;                                                 \
  PRE_DECODE_HANDLER;                                                   \
  if (OB_SUCC(ret)) {                                                   \
    LST_DO_CODE(OB_UNIS_DECODE, ##__VA_ARGS__);                         \
  }                                                                     \
  POST_DECODE_HANDLER;                                                  \
  return ret;                                                           \
}                                                                       \
int64_t ObTxDesc::name##_state_encoded_length()                       \
{                                                                     \
  int64_t len = 0;                                                    \
  LST_DO_CODE(OB_UNIS_ADD_LEN, ##__VA_ARGS__);                        \
  return len;                                                         \
}                                                                     \
inline int64_t ObTxDesc::est_##name##_size__() { return LST_DO(SIZE_OF, (+), ##__VA_ARGS__); } \
int ObTxDesc::display_##name##_state(const char* buf, const int64_t len, int64_t &pos) \
{                                                                       \
  struct ObTxn##name##StateForDisplay                                   \
  {                                                                     \
    friend class ObTxDesc;                                              \
    int decode(const char* buf, const int64_t data_len, int64_t &pos) { \
      int ret = OB_SUCCESS;                                             \
      LST_DO2(TXN_UNIS_DECODE, (;), ##__VA_ARGS__);                     \
      return ret;                                                       \
    }                                                                   \
    TO_STRING_KV(LST_DO2(TXN_STATE_K, (,), ##__VA_ARGS__))              \
    LST_DO2(DEF_MEMBER, (;), ##__VA_ARGS__);                            \
  };                                                                    \
  int ret = OB_SUCCESS;                                                 \
  ObTxn##name##StateForDisplay state;                                   \
  if (OB_FAIL(state.decode(buf, len, pos))) {                           \
    TRANS_LOG(WARN, "decode fail", K(ret));                             \
  } else {                                                              \
    TRANS_LOG(INFO, "[display state]", "type", #name, "content", state); \
  }                                                                     \
  return ret;                                                           \
}

TXN_FREE_ROUTE_MEMBERS(static, PRE_STATIC_DECODE, POST_STATIC_DECODE,
                       tenant_id_,
                       cluster_id_,
                       cluster_version_,
                       addr_,
                       tx_id_,
                       xid_,
                       xa_tightly_couple_,
                       xa_start_addr_,
                       isolation_,
                       access_mode_,
                       sess_id_,
                       timeout_us_,
                       expire_ts_);
TXN_FREE_ROUTE_MEMBERS(dynamic,PRE_DYNAMIC_DECODE, POST_DYNAMIC_DECODE,
                       op_sn_,
                       state_,
                       flags_.v_,
                       active_ts_,
                       active_scn_,
                       abort_cause_,
                       can_elr_);
TXN_FREE_ROUTE_MEMBERS(parts,,,
                       parts_);
// the fields 'dup with static' are required when preceding of txn is of query like
// savepoint or read only stmt with isolation of SERIALIZABLE / REPEATABLE READ
// because such type of query caused the txn into 'start' in perspective of proxy
TXN_FREE_ROUTE_MEMBERS(extra, PRE_EXTRA_DECODE, POST_EXTRA_DECODE,
                       tx_id_,      // dup with static
                       sess_id_,    // dup with static
                       addr_,       // dup with static
                       isolation_,  // dup with static
                       snapshot_version_,
                       snapshot_scn_);

#undef TXN_FREE_ROUTE_MEMBERS
int64_t ObTxDesc::estimate_state_size()
{
  int64_t len = 0;
#define _EST_SIZE__(x) est_##x##_size__()
#define _EST_SIZE_(x) _EST_SIZE__(x)
  len = LST_DO(_EST_SIZE_, (+), static, dynamic, parts, extra);
  return len;
#undef __EST_SIZE__
#undef __EST_SIZE_
}
}
}
