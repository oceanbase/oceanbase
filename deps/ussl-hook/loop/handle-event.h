/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef USSL_HOOK_LOOP_HANDLE_EVENT_
#define USSL_HOOK_LOOP_HANDLE_EVENT_

#define IP_STRING_MAX_LEN 64

extern int clientfd_sk_handle_event(clientfd_sk_t *s);
extern int acceptfd_sk_handle_event(acceptfd_sk_t *s);
extern void ussl_get_peer_addr(int fd, char *buf, int len);
extern int is_net_keepalive_connection(ssize_t rbytes, char *buf);
extern int ob_judge_is_tableapi_pcode_from_raw_packet(const char *buf, ssize_t data_len);
extern int ob_is_bypass_pcode(uint32_t pcode);
extern void ussl_reset_rpc_connection_type(int fd);
#endif // USSL_HOOK_LOOP_HANDLE_EVENT_
