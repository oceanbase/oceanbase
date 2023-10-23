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
 */

#define DEF_FREE_ROUTE_API_(name)                                       \
    int txn_free_route__update_##name##_state(const uint32_t session_id, ObTxDesc *&tx, ObTxnFreeRouteCtx &ctx, const char* buf, const int64_t len, int64_t &pos); \
    int txn_free_route__serialize_##name##_state(const uint32_t session_id, ObTxDesc *tx, ObTxnFreeRouteCtx &ctx, char* buf, const int64_t len, int64_t &pos); \
    int64_t txn_free_route__get_##name##_state_serialize_size(ObTxDesc *tx, ObTxnFreeRouteCtx &ctx); \
    static int64_t txn_free_route__get_##name##_state_size(ObTxDesc *tx); \
    static int txn_free_route__get_##name##_state(ObTxDesc *tx, const ObTxnFreeRouteCtx &ctx, char *buf, const int64_t len, int64_t &pos); \
    static int txn_free_route__cmp_##name##_state(const char* cur_buf, int64_t cur_len, const char* last_buf, int64_t last_len); \
    static int txn_free_route__display_##name##_state(const char* n, const char* buf, const int64_t len);
#define DEF_FREE_ROUTE_API(name) DEF_FREE_ROUTE_API_(name)
public:
LST_DO(DEF_FREE_ROUTE_API, (;), static, dynamic, parts, extra)
#undef DEF_FREE_ROUTE_API

int calc_txn_free_route(ObTxDesc *tx, ObTxnFreeRouteCtx &ctx);
int tx_free_route_check_alive(ObTxnFreeRouteCtx &ctx, const ObTxDesc &tx, const uint32_t session_id);
int tx_free_route_handle_check_alive(const ObTxFreeRouteCheckAliveMsg &msg, const int retcode);
int tx_free_route_handle_push_state(const ObTxFreeRoutePushState &msg);
private:
int clean_txn_state_(ObTxDesc *&tx, ObTxnFreeRouteCtx &ctx, const ObTransID &tx_id);
static int update_logic_clock_(const int64_t logic_clock, const ObTxDesc *tx, const bool check_fallback);
bool need_fallback_(ObTxDesc &tx, int64_t &state_size);
int push_tx_state_to_remote_(ObTxDesc &tx, const ObAddr &txn_addr);
int txn_free_route__sanity_check_fallback_(ObTxDesc *tx, ObTxnFreeRouteCtx &ctx);
int txn_free_route__handle_tx_exist_(const ObTransID &tx_id, ObTxnFreeRouteAuditRecord &audit_record, ObTxDesc *&tx);
int txn_free_route__kill_session_(const uint32_t session_id);
