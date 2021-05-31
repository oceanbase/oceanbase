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

#ifndef EASY_KFC_HANDLER_H_
#define EASY_KFC_HANDLER_H_

#include "easy_define.h"

EASY_CPP_START

#include "io/easy_io.h"
#include "io/easy_client.h"
#include "io/easy_connection.h"
#include "io/easy_summary.h"

#define EASY_ERR_NO_MEM (-101)
#define EASY_ERR_NO_SERVER (-102)
#define EASY_ERR_ALL_DOWN (-103)
#define EASY_ERR_SERVER_BUSY (-104)
#define EASY_ERR_NO_SEND (-105)
#define EASY_ERR_TIMEOUT (-106)
#define EASY_KFC_HIST_CNT 6
#define EASY_KFC_CHOICE_RR 0
#define EASY_KFC_CHOICE_RT 1

typedef struct easy_kfc_t easy_kfc_t;
typedef struct easy_kfc_group_t easy_kfc_group_t;
typedef struct easy_kfc_node_t easy_kfc_node_t;
typedef struct easy_kfc_agent_t easy_kfc_agent_t;
typedef struct easy_kfc_packet_t easy_kfc_packet_t;
typedef struct easy_kfc_saddr_t easy_kfc_saddr_t;
typedef struct easy_kfc_server_t easy_kfc_server_t;
typedef struct easy_kfc_client_t easy_kfc_client_t;
typedef int(easy_kfc_choice_server_pt)(easy_kfc_agent_t* agent);

struct easy_kfc_t {
  easy_pool_t* pool;
  easy_io_t* eio;
  easy_io_handler_pt chandler;
  easy_atomic32_t gen_chid;
  uint64_t version;
  easy_atomic_t lock;
  easy_array_t* node_array;
  easy_hash_t* node_list;
  easy_hash_t* group_list;
  int iocnt;
  int hist_idx;
  ev_timer hist_watcher;
  easy_summary_t* hist[EASY_KFC_HIST_CNT];
  uint32_t noping : 1;
};

struct easy_kfc_server_t {
  uint64_t group_id;
  easy_thread_pool_t* etp;
  easy_io_process_pt* process;
  easy_request_process_pt* cproc;
  void* args;
  easy_hash_t* client_ip;
  int client_allow;
  pthread_rwlock_t rwlock;
};

struct easy_kfc_client_t {
  uint64_t ip : 63;
  uint64_t allow : 1;
  easy_hash_list_t node;
};

struct easy_kfc_group_t {
  uint64_t group_id;
  easy_addr_t server_addr;
  easy_kfc_server_t* server;
  easy_hash_t* server_list;
  easy_hash_t* client_list;
  easy_hash_list_t node;
  int role;
};

struct easy_kfc_node_t {
  easy_addr_t addr;
  easy_hash_list_t node;
  easy_hash_list_t node_list;
  int16_t status;
  int16_t connected;
  uint32_t rt;
  uint64_t lastrt;
};

struct easy_kfc_saddr_t {
  uint32_t cur, cnt;
  easy_kfc_node_t** addr;
};

struct easy_kfc_agent_t {
  easy_pool_t* pool;
  uint64_t group_id;
  uint64_t version;
  easy_kfc_saddr_t slist;
  easy_session_t* s;
  easy_kfc_t* kfc;
  easy_client_wait_t wobj;
  int offset;
  int status;
  easy_kfc_choice_server_pt* choice_server;
  easy_kfc_node_t* last;
};

struct easy_kfc_packet_t {
  easy_buf_t* b;
  char* data;
  int32_t len;
  uint32_t chid;
  uint64_t group_id;
  char buffer[0];
};

easy_kfc_t* easy_kfc_create(const char* ip_list, int iocnt);
int easy_kfc_start(easy_kfc_t* kfc);
int easy_kfc_wait(easy_kfc_t* kfc);
void easy_kfc_destroy(easy_kfc_t* kfc);

int easy_kfc_set_iplist(easy_kfc_t* kfc, const char* ip_list);
easy_kfc_packet_t* easy_kfc_packet_rnew(easy_request_t* r, int size);
int easy_kfc_join_server(easy_kfc_t* kfc, const char* group_name, easy_io_process_pt* process);
int easy_kfc_join_server_args(easy_kfc_t* kfc, const char* group_name, easy_request_process_pt* process, void* args);
int easy_kfc_join_server_async(
    easy_kfc_t* kfc, const char* group_name, int request_cnt, easy_request_process_pt* request_process);
easy_kfc_agent_t* easy_kfc_join_client(easy_kfc_t* kfc, const char* group_name);
int easy_kfc_leave_client(easy_kfc_agent_t* agent);
int easy_kfc_send_message(easy_kfc_agent_t* agent, char* data, int len, int timeout);
int easy_kfc_recv_message(easy_kfc_agent_t* agent, char* data, int len);

int easy_kfc_recv_buffer(easy_kfc_agent_t* agent, char** data);

void easy_kfc_clear_buffer(easy_kfc_agent_t* agent);

void easy_kfc_allow_client(easy_kfc_t* kfc, char* group_name, char* client, int deny);

void easy_kfc_choice_scheduler(easy_kfc_agent_t* agent, int type);

EASY_CPP_END

#endif
