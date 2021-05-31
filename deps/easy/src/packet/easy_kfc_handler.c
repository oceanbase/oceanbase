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

#include "packet/easy_kfc_handler.h"
#include "packet/http/easy_http_handler.h"
#include <ctype.h>

#define EASY_KFC_PACKET_HEADER_SIZE 16
#define EASY_KFC_DEFAULT_PORT 2903
#define EASY_KFC_ROLE_SERVER 5

#define EASY_KFC_STATUS_UP '\x11'
#define EASY_KFC_STATUS_DOWN '\x22'
#define EASY_KFC_RANGE_MAX 16
typedef struct easy_kfc_range_t {
  int cnt, max, cur;
  int r[EASY_KFC_RANGE_MAX];
  char* s[EASY_KFC_RANGE_MAX];
  easy_pool_t* pool;
} easy_kfc_range_t;

static uint64_t easy_kfc_conv_group_id(char* s);
static int easy_kfc_add_node(
    easy_kfc_t* kfc, easy_pool_t* pool, easy_hash_t* table, char* host, int port, uint64_t group_id, int role);
static void easy_kfc_local_group(easy_kfc_t* kfc, easy_hash_t* group_list);
static easy_kfc_group_t* easy_kfc_get_group(easy_kfc_t* kfc, const char* group_name);
static int easy_kfc_heartbeat(easy_connection_t* c);
static void easy_kfc_do_connect(easy_kfc_agent_t* agent);
static int easy_kfc_connect(easy_connection_t* c);
static int easy_kfc_disconnect(easy_connection_t* c);
static int easy_kfc_choice_round_robin(easy_kfc_agent_t* agent);
static int easy_kfc_choice_response_time(easy_kfc_agent_t* agent);
static int easy_kfc_server_process(easy_request_t* r);
static int easy_kfc_monitor_process(easy_request_t* r);
static int easy_kfc_join_server_ex(easy_kfc_t* kfc, const char* group_name, easy_io_process_pt* process,
    int request_cnt, easy_request_process_pt* request_process, void* args);
static void easy_kfc_hist_watcher_process(struct ev_loop* loop, ev_timer* w, int revents);
static int easy_kfc_ping(easy_kfc_agent_t* agent);
static easy_kfc_packet_t* easy_kfc_packet_new(easy_session_t** sp, int size);
static void easy_kfc_clear_node(easy_hash_t* group_list);
static int easy_kfc_check_ip(easy_kfc_server_t* s, easy_connection_t* c);

static int easy_kfc_range_extra(char* str, char* result);
static void easy_kfc_range_parse(easy_kfc_range_t* range, char* host);
static int easy_kfc_range_get(easy_kfc_range_t* range, char* result);
static void easy_kfc_range_free(easy_kfc_range_t* range);

/**
 * decode
 */
void* easy_kfc_decode(easy_message_t* m)
{
  easy_kfc_packet_t* packet;
  uint32_t len, datalen;

  // length
  if ((len = m->input->last - m->input->pos) < EASY_KFC_PACKET_HEADER_SIZE)
    return NULL;

  // data len
  datalen = *((uint32_t*)m->input->pos);

  if (datalen > 0x4000000) {  // 64M
    easy_error_log("data_len is invalid: %d\n", datalen);
    m->status = EASY_ERROR;
    return NULL;
  }

  len -= EASY_KFC_PACKET_HEADER_SIZE;

  if (len < datalen) {
    m->next_read_len = datalen - len;
    return NULL;
  }

  // alloc packet
  if ((packet = (easy_kfc_packet_t*)easy_pool_calloc(m->pool, sizeof(easy_kfc_packet_t))) == NULL) {
    m->status = EASY_ERROR;
    return NULL;
  }

  memcpy(&packet->len, m->input->pos, EASY_KFC_PACKET_HEADER_SIZE);
  m->input->pos += EASY_KFC_PACKET_HEADER_SIZE;
  packet->data = (char*)m->input->pos;
  m->input->pos += datalen;
  packet->b = NULL;

  return packet;
}

/**
 * encode
 */
int easy_kfc_encode(easy_request_t* r, void* data)
{
  easy_kfc_packet_t* packet;
  int len;

  // set data
  packet = (easy_kfc_packet_t*)data;
  len = packet->len + EASY_KFC_PACKET_HEADER_SIZE;
  easy_buf_set_data(r->ms->pool, packet->b, &packet->len, len);
  easy_request_addbuf(r, packet->b);

  return EASY_OK;
}

uint64_t easy_kfc_packet_id(easy_connection_t* c, void* packet)
{
  return ((easy_kfc_packet_t*)packet)->chid;
}

easy_kfc_packet_t* easy_kfc_packet_rnew(easy_request_t* r, int size)
{
  easy_kfc_packet_t* packet;
  easy_buf_t* b;

  size += sizeof(easy_kfc_packet_t) + sizeof(easy_buf_t);

  if ((b = (easy_buf_t*)easy_pool_alloc(r->ms->pool, size)) == NULL)
    return NULL;

  packet = (easy_kfc_packet_t*)(b + 1);
  memset(packet, 0, sizeof(easy_kfc_packet_t));
  packet->b = b;
  packet->data = &packet->buffer[0];

  return packet;
}

easy_kfc_packet_t* easy_kfc_packet_new(easy_session_t** sp, int size)
{
  easy_session_t* s;
  easy_buf_t* b;
  easy_kfc_packet_t* packet;

  size += sizeof(easy_kfc_packet_t) + sizeof(easy_buf_t);

  if ((s = easy_session_create(size)) == NULL)
    return NULL;

  b = (easy_buf_t*)&s->data[0];
  packet = (easy_kfc_packet_t*)(b + 1);
  memset(packet, 0, sizeof(easy_kfc_packet_t));
  packet->b = b;
  packet->data = &packet->buffer[0];
  s->r.opacket = packet;
  *sp = s;

  return packet;
}

easy_kfc_t* easy_kfc_create(const char* ip_list, int iocnt)
{
  int offset;
  easy_kfc_t* kfc;
  easy_io_t* eio;
  easy_array_t* array = NULL;

  if ((eio = easy_eio_create(NULL, iocnt)) == NULL)
    return NULL;

  kfc = (easy_kfc_t*)easy_pool_calloc(eio->pool, sizeof(easy_kfc_t));
  // client
  kfc->chandler.decode = easy_kfc_decode;
  kfc->chandler.encode = easy_kfc_encode;
  kfc->chandler.get_packet_id = easy_kfc_packet_id;
  kfc->chandler.process = easy_client_wait_process;
  kfc->chandler.batch_process = easy_client_wait_batch_process;
  kfc->chandler.on_idle = easy_kfc_heartbeat;
  kfc->chandler.on_connect = easy_kfc_connect;
  kfc->chandler.on_disconnect = easy_kfc_disconnect;
  kfc->chandler.user_data = kfc;
  // node
  array = easy_array_create(sizeof(easy_kfc_node_t));
  offset = offsetof(easy_kfc_node_t, node_list);
  kfc->node_list = easy_hash_create(array->pool, 1024, offset);
  kfc->node_array = array;
  kfc->eio = eio;
  eio->do_signal = 0;
  eio->send_qlen = 1;

  if (easy_kfc_set_iplist(kfc, ip_list) != EASY_OK)
    goto error_exit;

  return kfc;
error_exit:
  easy_eio_destroy(eio);

  if (array)
    easy_array_destroy(array);

  return NULL;
}

// mmdev1.corp.alimama.com role=server group=tests2 port=2200
int easy_kfc_set_iplist(easy_kfc_t* kfc, const char* iplist)
{
  char host[128], a[3][128];
  int port, role, i, cnt;
  uint64_t group_id;
  easy_pool_t* pool;
  easy_hash_t* group_list;
  char *ip_end, *ip_list, *p, *s, *buffer;

  if (iplist == NULL) {
    easy_error_log("iplist IS NULL.");
    return EASY_ERROR;
  }

  cnt = strlen(iplist);
  buffer = (char*)easy_malloc(cnt + 1);
  ip_end = ip_list = buffer;
  memcpy(ip_list, iplist, cnt);
  ip_list[cnt] = '\0';

  while (*ip_end) {
    if (*ip_end == ';')
      *ip_end = '\n';

    ip_end++;
  }

  pool = easy_pool_create(1024);
  i = offsetof(easy_kfc_group_t, node);
  group_list = easy_hash_create(pool, 64, i);

  while (ip_list && ip_list < ip_end) {
    p = strchr(ip_list, '\n');
    cnt = sscanf(ip_list, "%s%s%s%s", host, a[0], a[1], a[2]);
    ip_list = (p ? (p + 1) : NULL);

    if (cnt <= 1)
      continue;

    // group
    group_id = 0;
    role = EASY_KFC_ROLE_SERVER;
    port = EASY_KFC_DEFAULT_PORT;

    for (i = 0; i < cnt - 1; i++) {
      s = a[i];

      if (memcmp(s, "group=", 6) == 0) {
        group_id = easy_kfc_conv_group_id(s + 6);
      } else if (memcmp(s, "port=", 5) == 0) {
        port = atoi(s + 5);
      } else if (memcmp(s, "role=server", 11) == 0) {
        role = EASY_KFC_ROLE_SERVER;
      } else if (memcmp(s, "role=client", 11) == 0) {
        group_id = 0;
        break;
      }
    }

    if (group_id == 0)
      continue;

    // range
    easy_kfc_range_t range;
    easy_kfc_range_parse(&range, host);

    while (easy_kfc_range_get(&range, a[0])) {
      if (easy_kfc_add_node(kfc, pool, group_list, a[0], port, group_id, role) != EASY_OK) {
        easy_kfc_range_free(&range);
        goto error_exit;
      }
    }

    easy_kfc_range_free(&range);
  }

  easy_kfc_local_group(kfc, group_list);

  if (++kfc->version == 0)
    kfc->version++;

  easy_pool_t* old_pool = kfc->pool;
  easy_hash_t* old_group_list = kfc->group_list;

  easy_spin_lock(&kfc->lock);
  kfc->group_list = group_list;
  kfc->pool = pool;

  if (old_group_list)
    easy_kfc_clear_node(old_group_list);

  if (old_pool)
    easy_pool_destroy(old_pool);

  easy_spin_unlock(&kfc->lock);
  easy_free(buffer);

  return EASY_OK;
error_exit:
  easy_pool_destroy(pool);
  easy_free(buffer);

  return EASY_ERROR;
}

// join
int easy_kfc_join_server(easy_kfc_t* kfc, const char* group_name, easy_io_process_pt* process)
{
  return easy_kfc_join_server_ex(kfc, group_name, process, 0, NULL, NULL);
}

// join server
int easy_kfc_join_server_args(easy_kfc_t* kfc, const char* group_name, easy_request_process_pt* process, void* args)
{
  return easy_kfc_join_server_ex(kfc, group_name, NULL, 0, process, args);
}

int easy_kfc_join_server_async(
    easy_kfc_t* kfc, const char* group_name, int request_cnt, easy_request_process_pt* request_process)
{
  return easy_kfc_join_server_ex(kfc, group_name, NULL, request_cnt, request_process, NULL);
}

easy_kfc_agent_t* easy_kfc_join_client(easy_kfc_t* kfc, const char* group_name)
{
  easy_kfc_group_t* group;
  easy_pool_t* pool;
  easy_kfc_agent_t* agent;
  uint64_t group_id = 0;

  easy_spin_lock(&kfc->lock);

  if ((group = easy_kfc_get_group(kfc, group_name)) != NULL) {
    group_id = group->group_id;
  }

  easy_spin_unlock(&kfc->lock);

  if (group_id == 0) {
    easy_error_log("join client failure.");
    return NULL;
  }

  // new agent
  pool = easy_pool_create(sizeof(easy_kfc_agent_t) * 2);
  agent = (easy_kfc_agent_t*)easy_pool_calloc(pool, sizeof(easy_kfc_agent_t));
  agent->group_id = group_id;
  agent->kfc = kfc;
  agent->pool = pool;
  agent->choice_server = easy_kfc_choice_round_robin;
  easy_client_wait_init(&agent->wobj);

  easy_kfc_do_connect(agent);

  // ping
  if (kfc->noping == 0 && easy_kfc_ping(agent) <= 0) {
    easy_kfc_leave_client(agent);
    agent = NULL;
  }

  return agent;
}

void easy_kfc_choice_scheduler(easy_kfc_agent_t* agent, int type)
{
  if (type == EASY_KFC_CHOICE_RT) {
    agent->choice_server = easy_kfc_choice_response_time;
  } else {
    agent->choice_server = easy_kfc_choice_round_robin;
  }
}

// leave
int easy_kfc_leave_client(easy_kfc_agent_t* agent)
{
  int i;
  easy_kfc_node_t* node;

  if (agent == NULL)
    return EASY_OK;

  // disconnect
  easy_spin_lock(&agent->kfc->lock);

  for (i = 0; i < agent->slist.cnt; i++) {
    node = agent->slist.addr[i];
    node->connected--;

    if (node->connected > 0)
      continue;

    easy_connection_disconnect(agent->kfc->eio, node->addr);
    easy_hash_del_node(&node->node_list);
    easy_hash_del_node(&node->node);
    easy_array_free(agent->kfc->node_array, node);
  }

  easy_spin_unlock(&agent->kfc->lock);

  // destroy session
  pthread_cond_destroy(&agent->wobj.cond);
  pthread_mutex_destroy(&agent->wobj.mutex);
  easy_kfc_clear_buffer(agent);

  // destroy
  if (agent->pool)
    easy_pool_destroy(agent->pool);

  return EASY_OK;
}

int easy_kfc_send_message(easy_kfc_agent_t* agent, char* data, int len, int timeout)
{
  easy_kfc_packet_t* packet;
  easy_session_t* s;
  int ret;

  if (agent->slist.cnt == 0) {
    easy_error_log("easy_kfc_send_message failure.\n");
    return EASY_ERR_NO_SERVER;
  }

  // check version
  easy_kfc_do_connect(agent);

  // choice server
  if ((agent->choice_server)(agent) != EASY_OK) {
    return EASY_ERR_ALL_DOWN;
  }

  if ((packet = easy_kfc_packet_new(&s, len)) == NULL)
    return EASY_ERR_NO_MEM;

  // new
  agent->wobj.done_count = 0;
  easy_list_init(&agent->wobj.next_list);
  easy_list_init(&agent->wobj.session_list);
  easy_session_set_timeout(s, timeout);
  packet->len = len;
  packet->group_id = agent->group_id;
  memcpy(packet->data, data, len);
  packet->chid = easy_atomic32_add_return(&agent->kfc->gen_chid, 1);
  easy_session_set_wobj(s, &agent->wobj);
  s->callback = easy_client_wait_process;

  agent->status = 0;

  if ((ret = easy_client_dispatch(agent->kfc->eio, agent->last->addr, s)) == EASY_ERROR) {
    easy_session_destroy(s);
    return EASY_ERR_SERVER_BUSY;
  }

  easy_kfc_clear_buffer(agent);
  agent->s = s;
  return EASY_OK;
}

int easy_kfc_recv_message(easy_kfc_agent_t* agent, char* data, int len)
{
  easy_kfc_packet_t* resp;
  easy_session_t* s = agent->s;
  int ret = EASY_ERR_TIMEOUT;

  if (!s) {
    easy_error_log("call send_message.\n");
    return EASY_ERR_NO_SEND;
  }

  // wait response
  if (agent->status == 0) {
    easy_client_wait(&agent->wobj, 1);
    agent->status = 1;

    if (s->now && agent->last) {
      int rt = (agent->last->rt + easy_max(1, s->now * 10000)) / 2;
      agent->last->rt = easy_min(rt, 600000);
    }
  }

  // memcpy
  resp = (easy_kfc_packet_t*)s->r.ipacket;

  if (resp) {
    ret = easy_min(len, resp->len);

    if (ret > 0)
      memcpy(data, resp->data, ret);

    resp->data += ret;
    resp->len -= ret;
  }

  if (!resp || resp->len <= 0) {
    easy_kfc_clear_buffer(agent);
  }

  return ret;
}

int easy_kfc_recv_buffer(easy_kfc_agent_t* agent, char** data)
{
  easy_kfc_packet_t* resp;
  easy_session_t* s = agent->s;
  int ret = EASY_ERROR;

  if (!s) {
    easy_error_log("call send_message.\n");
    return EASY_ERROR;
  }

  // wait response
  easy_client_wait(&agent->wobj, 1);

  // response
  resp = (easy_kfc_packet_t*)s->r.ipacket;

  if (resp && (ret = resp->len) >= 0 && data) {
    (*data) = resp->data;
  }

  return ret;
}

void easy_kfc_clear_buffer(easy_kfc_agent_t* agent)
{
  if (agent->s) {
    easy_session_destroy(agent->s);
    agent->s = NULL;
  }
}

static int easy_kfc_ping(easy_kfc_agent_t* agent)
{
  easy_kfc_packet_t* packet;
  easy_session_t *s, *s2;
  int i, cnt;
  easy_addr_t addr;

  if (agent->slist.cnt == 0) {
    return 0;
  }

  // check version
  easy_kfc_do_connect(agent);

  // new
  cnt = 0;
  agent->wobj.done_count = 0;
  easy_list_init(&agent->wobj.next_list);
  easy_list_init(&agent->wobj.session_list);

  for (i = 0; i < agent->slist.cnt; i++) {
    if ((packet = easy_kfc_packet_new(&s, 0)) == NULL)
      continue;

    packet->group_id = agent->group_id;
    packet->chid = easy_atomic32_add_return(&agent->kfc->gen_chid, 1);
    easy_session_set_wobj(s, &agent->wobj);
    s->callback = easy_client_wait_process;

    addr = agent->slist.addr[i]->addr;

    if (easy_client_dispatch(agent->kfc->eio, addr, s) == EASY_ERROR) {
      easy_session_destroy(s);
    } else {
      cnt++;
    }
  }

  if (cnt > 0) {
    // wait response
    easy_client_wait(&agent->wobj, cnt);

    cnt = 0;
    easy_list_for_each_entry_safe(s, s2, &agent->wobj.session_list, session_list_node)
    {
      packet = (easy_kfc_packet_t*)s->r.ipacket;

      if (packet && packet->group_id == agent->group_id)
        cnt++;

      easy_session_destroy(s);
    }
  }

  return cnt;
}

int easy_kfc_start(easy_kfc_t* kfc)
{
  return easy_eio_start(kfc->eio);
}

int easy_kfc_wait(easy_kfc_t* kfc)
{
  return easy_eio_wait(kfc->eio);
}

void easy_kfc_destroy(easy_kfc_t* kfc)
{
  int i;
  easy_array_t* arr = kfc->node_array;

  easy_eio_stop(kfc->eio);
  easy_eio_wait(kfc->eio);

  if (kfc->pool)
    easy_pool_destroy(kfc->pool);

  // destroy summary
  for (i = 0; i < EASY_KFC_HIST_CNT; i++) {
    easy_summary_destroy(kfc->hist[i]);
  }

  easy_eio_destroy(kfc->eio);

  if (arr)
    easy_array_destroy(arr);
}

// add client to server
void easy_kfc_allow_client(easy_kfc_t* kfc, char* group_name, char* client, int allow)
{
  easy_kfc_group_t* group = NULL;
  easy_addr_t addr;

  easy_spin_lock(&kfc->lock);

  if ((group = easy_kfc_get_group(kfc, group_name)) == NULL) {
    goto error_exit;
  }

  // group server
  if (group->server == NULL) {
    goto error_exit;
  }

  easy_kfc_client_t* cl = NULL;
  easy_kfc_server_t* s = group->server;

  // addr
  addr = easy_inet_str_to_addr(client, 0);

  if (addr.family == 0) {
    if (*client == '*') {
      s->client_allow = allow;
    }

    goto error_exit;
  }

  pthread_rwlock_wrlock(&s->rwlock);

  if (s->client_ip == NULL) {
    s->client_ip = easy_hash_create(kfc->eio->pool, 128, offsetof(easy_kfc_client_t, node));
  } else {
    cl = easy_hash_find(s->client_ip, addr.u.addr);
  }

  if (cl == NULL) {
    cl = (easy_kfc_client_t*)easy_pool_alloc(kfc->eio->pool, sizeof(easy_kfc_client_t));
    cl->ip = addr.u.addr;
    cl->allow = allow;
    easy_hash_add(s->client_ip, addr.u.addr, &cl->node);
  } else {
    cl->allow = allow;
  }

  pthread_rwlock_unlock(&s->rwlock);

error_exit:
  easy_spin_unlock(&kfc->lock);
}
///////////////////////////////////////////////////////////////////////////////////////////////////
static uint64_t easy_kfc_conv_group_id(char* s)
{
  uint64_t ret = 0;
  int size = easy_min(strlen(s) + 1, sizeof(uint64_t));
  memcpy((char*)&ret, s, size);
  return ret;
}

static int easy_kfc_add_node(
    easy_kfc_t* kfc, easy_pool_t* pool, easy_hash_t* table, char* host, int port, uint64_t group_id, int role)
{
  easy_addr_t addr;
  easy_kfc_group_t* group = NULL;
  easy_kfc_node_t* node = NULL;
  easy_hash_t* node_table;
  int offset;

  easy_debug_log("host=%s port=%d group=%.*s role=%s",
      host,
      port,
      sizeof(uint64_t),
      &group_id,
      ((role == EASY_KFC_ROLE_SERVER) ? "server" : "client"));

  addr = easy_inet_str_to_addr(host, port);

  if (addr.family == 0) {
    easy_debug_log("ERROR: host=%s port=%d group=%.*s role=%s",
        host,
        port,
        sizeof(uint64_t),
        &group_id,
        ((role == EASY_KFC_ROLE_SERVER) ? "server" : "client"));
    return EASY_ERROR;
  }

  if ((group = (easy_kfc_group_t*)easy_hash_find(table, group_id)) == NULL) {
    group = (easy_kfc_group_t*)easy_pool_calloc(pool, sizeof(easy_kfc_group_t));
    group->group_id = group_id;
    offset = offsetof(easy_kfc_node_t, node);
    group->server_list = easy_hash_create(pool, 64, offset);
    group->client_list = easy_hash_create(pool, 64, offset);
    easy_hash_add(table, group_id, &group->node);
  }

  // find node
  if (role == EASY_KFC_ROLE_SERVER) {
    node_table = group->server_list;
  } else {
    node_table = group->client_list;
  }

  if ((node = (easy_kfc_node_t*)easy_client_list_find(node_table, &addr)) != NULL) {
    easy_debug_log("DUPLICATE ERROR: host=%s port=%d group=%.*s role=%s",
        host,
        port,
        sizeof(uint64_t),
        &group_id,
        ((role == EASY_KFC_ROLE_SERVER) ? "server" : "client"));
    return EASY_ERROR;
  }

  // add node
  addr.cidx = easy_hash_code(&group_id, sizeof(uint64_t), role);

  easy_spin_lock(&kfc->lock);
  node = (easy_kfc_node_t*)easy_client_list_find(kfc->node_list, &addr);

  if (node == NULL) {
    node = (easy_kfc_node_t*)easy_array_alloc(kfc->node_array);
    memset(node, 0, sizeof(easy_kfc_node_t));
    node->addr = addr;
    easy_client_list_add(kfc->node_list, &node->addr, &node->node_list);
  } else {
    easy_hash_del_node(&node->node);
  }

  easy_spin_unlock(&kfc->lock);
  addr.cidx = 0;
  easy_client_list_add(node_table, &node->addr, &node->node);

  return EASY_OK;
}

// TODO
static easy_kfc_node_t* easy_kfc_get_node(easy_hash_t* server_list, uint64_t id)
{
  uint32_t n;
  easy_hash_list_t* hnode;
  easy_kfc_node_t* node;
  easy_hash_for_each(n, hnode, server_list)
  {
    node = (easy_kfc_node_t*)((char*)hnode - server_list->offset);

    if (node->addr.u.addr == (id >> 32))
      return node;
  }
  return NULL;
}

static void easy_kfc_local_group(easy_kfc_t* kfc, easy_hash_t* group_list)
{
  easy_kfc_group_t *hgroup, *ogroup;
  easy_kfc_node_t* hnode;
  easy_hash_list_t* node;
  uint64_t address[32];
  int i, address_size;
  uint32_t n;

  address_size = easy_inet_hostaddr(address, 32, 1);

  for (n = 0; n < group_list->size; n++) {
    node = group_list->buckets[n];

    while (node) {
      ogroup = NULL;
      hgroup = (easy_kfc_group_t*)((char*)node - group_list->offset);

      // old group
      if (kfc->group_list && (ogroup = (easy_kfc_group_t*)easy_hash_find(kfc->group_list, hgroup->group_id)) != NULL) {
        hgroup->role = ogroup->role;
        hgroup->server_addr = ogroup->server_addr;
        hgroup->server = ogroup->server;
      } else {
        hgroup->role = 0;

        for (i = 0; i < address_size; i++) {
          hnode = easy_kfc_get_node(hgroup->server_list, address[i]);

          if (hnode != NULL) {
            hgroup->server_addr = hnode->addr;
            hgroup->role |= EASY_KFC_ROLE_SERVER;
          }
        }
      }

      node = node->next;
    }
  }
}

static easy_kfc_group_t* easy_kfc_get_group(easy_kfc_t* kfc, const char* group_name)
{
  uint64_t group_id;
  easy_kfc_group_t* group;

  if (kfc->group_list == NULL) {
    easy_error_log("group_list is null\n");
    return NULL;
  }

  group_id = easy_kfc_conv_group_id((char*)group_name);

  if ((group = (easy_kfc_group_t*)easy_hash_find(kfc->group_list, group_id)) == NULL) {
    easy_error_log("group not found: %s\n", group_name);
    return NULL;
  }

  return group;
}

static void easy_kfc_do_connect(easy_kfc_agent_t* agent)
{
  easy_kfc_t* kfc = agent->kfc;
  easy_kfc_group_t* group;
  easy_kfc_node_t* node;
  easy_hash_list_t* hnode;
  int i;
  uint32_t n;

  if (kfc->version != agent->version) {
    int oldcnt = agent->slist.cnt;
    easy_kfc_node_t* addr[oldcnt];

    if (oldcnt > 0)
      memcpy(addr, agent->slist.addr, sizeof(easy_kfc_node_t*) * oldcnt);

    easy_pool_clear(agent->pool);
    easy_pool_alloc(agent->pool, sizeof(easy_kfc_agent_t));

    // lock
    easy_spin_lock(&kfc->lock);
    group = (easy_kfc_group_t*)easy_hash_find(kfc->group_list, agent->group_id);
    if (group == NULL) {
      easy_error_log("group is null\n");
      return;
    }
    agent->slist.cnt = group->server_list->count;
    agent->slist.addr = (easy_kfc_node_t**)easy_pool_alloc(agent->pool, sizeof(easy_kfc_node_t*) * agent->slist.cnt);

    // for each
    i = 0;
    easy_hash_for_each(n, hnode, group->server_list)
    {
      node = (easy_kfc_node_t*)((char*)hnode - group->server_list->offset);
      agent->slist.addr[i++] = node;
    }
    agent->version = kfc->version;

    for (i = 0; i < agent->slist.cnt; i++) {
      node = agent->slist.addr[i];

      if (node->connected == 0) {
        easy_connection_connect(kfc->eio, node->addr, &kfc->chandler, 5000, node, 1);
      }

      node->connected++;
    }

    for (i = 0; i < oldcnt; i++) {
      node = addr[i];
      node->connected--;

      if (node->connected > 0)
        continue;

      easy_connection_disconnect(kfc->eio, node->addr);
      easy_hash_del_node(&node->node_list);
      easy_hash_del_node(&node->node);
      easy_array_free(kfc->node_array, node);
    }

    // unlock
    easy_spin_unlock(&agent->kfc->lock);
  }
}

static int easy_kfc_heartbeat(easy_connection_t* c)
{
  return EASY_OK;
}

// on_connect
static int easy_kfc_connect(easy_connection_t* c)
{
  if (c->user_data) {
    easy_kfc_node_t* node = (easy_kfc_node_t*)c->user_data;
    node->status = EASY_KFC_STATUS_UP;
  }

  return EASY_OK;
}

// on_disconnect
static int easy_kfc_disconnect(easy_connection_t* c)
{
  if (c->user_data) {
    easy_kfc_node_t* node = (easy_kfc_node_t*)c->user_data;
    node->status = EASY_KFC_STATUS_DOWN;
  }

  return EASY_OK;
}

static int easy_kfc_join_server_ex(easy_kfc_t* kfc, const char* group_name, easy_io_process_pt* process,
    int request_cnt, easy_request_process_pt* request_process, void* args)
{
  easy_kfc_group_t* group;
  easy_io_handler_pt* handler;
  easy_kfc_server_t* server;
  easy_io_thread_t* ioth;
  int i;

  easy_spin_lock(&kfc->lock);

  if ((group = easy_kfc_get_group(kfc, group_name)) == NULL || group->server) {
    goto error_exit;
  }

  // kfc role server
  if ((group->role & EASY_KFC_ROLE_SERVER) == 0) {
    easy_error_log("join failure.");
    goto error_exit;
  }

  // calloc
  handler = (easy_io_handler_pt*)easy_pool_calloc(kfc->eio->pool, sizeof(easy_io_handler_pt));
  server = (easy_kfc_server_t*)easy_pool_calloc(kfc->eio->pool, sizeof(easy_kfc_server_t));
  server->group_id = group->group_id;
  server->client_allow = 1;
  pthread_rwlock_init(&server->rwlock, NULL);

  if (request_process) {
    if (request_cnt <= 0) {
      server->cproc = request_process;
      server->args = args;
    } else {
      server->etp = easy_thread_pool_create(kfc->eio, request_cnt, request_process, server);
    }
  } else {
    server->process = process;
  }

  // server
  handler->decode = easy_kfc_decode;
  handler->encode = easy_kfc_encode;
  handler->get_packet_id = easy_kfc_packet_id;
  handler->process = easy_kfc_server_process;
  handler->user_data = (void*)server;

  if (easy_connection_listen_addr(kfc->eio, group->server_addr, handler) == NULL) {
    goto error_exit;
  }

  // monitor
  easy_io_handler_pt* mh = (easy_io_handler_pt*)easy_pool_calloc(kfc->eio->pool, sizeof(easy_io_handler_pt));
  mh->decode = easy_http_server_on_decode;
  mh->encode = easy_http_server_on_encode;
  mh->process = easy_kfc_monitor_process;
  mh->user_data = kfc;

  // history stat
  for (i = 0; i < EASY_KFC_HIST_CNT; i++) {
    kfc->hist[i] = easy_summary_create();
    kfc->hist[i]->time = ev_time();
  }

  kfc->hist_idx = (EASY_KFC_HIST_CNT - 2);

  // start monitor listen
  easy_listen_t* lt;
  easy_addr_t addr = easy_inet_add_port(&group->server_addr, 1);

  if ((lt = easy_connection_listen_addr(kfc->eio, addr, mh)) == NULL) {
    goto error_exit;
  }

  lt->hidden_sum = 1;

  // hist watcher
  ioth = (easy_io_thread_t*)easy_thread_pool_index(kfc->eio->io_thread_pool, 0);
  ev_timer_init(&kfc->hist_watcher, easy_kfc_hist_watcher_process, 0., 20.0);
  kfc->hist_watcher.data = kfc;
  ev_timer_start(ioth->loop, &kfc->hist_watcher);
  group->server = server;
  easy_spin_unlock(&kfc->lock);

  return EASY_OK;
error_exit:
  easy_spin_unlock(&kfc->lock);

  return EASY_ERROR;
}

static int easy_kfc_server_process(easy_request_t* r)
{
  easy_kfc_server_t* s = (easy_kfc_server_t*)r->ms->c->handler->user_data;
  easy_kfc_packet_t* p = (easy_kfc_packet_t*)r->ipacket;

  if (p->group_id != s->group_id || easy_kfc_check_ip(s, r->ms->c) == 0)
    return EASY_ERROR;

  // ping
  if (p->len == 0) {
    easy_kfc_packet_t* o = easy_kfc_packet_rnew(r, 0);
    o->chid = p->chid;
    o->group_id = s->group_id;
    r->opacket = o;
    return EASY_OK;
  }

  // process
  if (s->process)
    return (s->process)(r);
  else if (s->cproc)
    return (s->cproc)(r, s->args);

  // push work thread
  easy_thread_pool_push(s->etp, r, p->chid);
  return EASY_AGAIN;
}

static int easy_kfc_monitor_process(easy_request_t* r)
{
  easy_kfc_t* kfc;
  easy_http_request_t* p;
  char* ptr;
  easy_summary_t* diff;
  easy_summary_t* sum;
  int minute;

  // process uri
  kfc = (easy_kfc_t*)r->ms->c->handler->user_data;
  p = (easy_http_request_t*)r->ipacket;

  if (p->str_path.len == 0 || strcmp(p->str_path.data, "/"))
    return EASY_ERROR;

  // args
  ptr = easy_http_get_args(p, "type");

  if (ptr && memcmp(ptr, "5min", 4) == 0)
    minute = kfc->hist_idx;
  else
    minute = kfc->hist_idx + EASY_KFC_HIST_CNT - 2;

  // hist summary
  minute %= EASY_KFC_HIST_CNT;
  sum = kfc->hist[minute];
  diff = easy_summary_diff(kfc->eio->eio_summary, sum);
  diff->time = ev_time() - sum->time;

  // output
  easy_summary_html_output(r->ms->pool, &p->output, diff, kfc->eio->eio_summary);

  // print debug info
  ptr = easy_http_get_args(p, "debug");

  if (ptr && memcmp(ptr, "yes", 3) == 0) {
    char buffer[128];
    lnprintf(buffer, 64, "showminute=%d, eio_summary\ntime=%.f", minute, ev_time());
    easy_summary_raw_output(r->ms->pool, &p->output, kfc->eio->eio_summary, buffer);

    for (minute = 0; minute < EASY_KFC_HIST_CNT; minute++) {
      if (minute == kfc->hist_idx % EASY_KFC_HIST_CNT) {
        lnprintf(buffer, 64, ">> minute%d, cur=%d", minute, kfc->hist_idx);
      } else {
        lnprintf(buffer, 64, "minute%d, cur=%d", minute, kfc->hist_idx);
      }

      easy_summary_raw_output(r->ms->pool, &p->output, kfc->hist[minute], buffer);
    }

    easy_summary_raw_output(r->ms->pool, &p->output, diff, "diff");
  }

  r->opacket = p;
  p->content_length = -1;

  // destroy
  easy_summary_destroy(diff);

  return EASY_OK;
}

static void easy_kfc_hist_watcher_process(struct ev_loop* loop, ev_timer* w, int revents)
{
  easy_kfc_t* kfc;
  easy_summary_t* sum;
  int idx;

  kfc = (easy_kfc_t*)w->data;
  idx = kfc->hist_idx;
  idx %= EASY_KFC_HIST_CNT;
  sum = kfc->hist[idx];

  easy_summary_copy(kfc->eio->eio_summary, sum);
  sum->time = ev_time();

  kfc->hist_idx++;
}

static void easy_kfc_clear_node(easy_hash_t* group_list)
{
  uint32_t n;
  easy_hash_list_t* hnode;
  easy_kfc_group_t* group;
  easy_hash_for_each(n, hnode, group_list)
  {
    group = (easy_kfc_group_t*)((char*)hnode - group_list->offset);

    if (group->server_list)
      easy_hash_clear(group->server_list);

    if (group->client_list)
      easy_hash_clear(group->client_list);
  }
}

static int easy_kfc_check_ip(easy_kfc_server_t* s, easy_connection_t* c)
{
  easy_kfc_client_t* cl = NULL;
  int ret = s->client_allow;

  if (s->client_ip) {
    pthread_rwlock_rdlock(&s->rwlock);

    if ((cl = easy_hash_find(s->client_ip, c->addr.u.addr)) != NULL)
      ret = cl->allow;

    pthread_rwlock_unlock(&s->rwlock);
  }

  easy_debug_log("check_ip: %s => %d, cl: %p", easy_connection_str(c), ret, cl);
  return ret;
}

// easy_kfc_range
///////////////////////////////////////////////////////////////////////////////////////////////////
static int easy_kfc_range_extra(char* str, char* result)
{
  char d, c = '\0';
  char* p = str;
  int i = 0, j;

  while (*p) {
    if (*p == '-') {
      d = *(p + 1);

      if (!isalnum(c) || !isalnum(d) || c > d)
        return 0;

      for (j = c; j <= *(p + 1); j++)
        result[i++] = (char)j;

      c = '\0';
      p++;
    } else if (isalnum(*p)) {
      if (c)
        result[i++] = c;

      c = *p;
    } else if (c) {
      result[i++] = c;
      c = '\0';
    }

    p++;
  }

  if (c)
    result[i++] = c;

  result[i] = '\0';
  return i;
}
static void easy_kfc_range_parse(easy_kfc_range_t* range, char* host)
{
  char *p, *q;
  int i = 0, len, max = 1;
  char result[256];

  memset(range, 0, sizeof(easy_kfc_range_t));
  range->pool = easy_pool_create(0);
  p = q = easy_pool_strdup(range->pool, host);

  while (*p) {
    if (*p == '[') {
      if ((len = p - q) > 0) {
        *p = '\0';
        range->s[i++] = q;
      }

      q = p + 1;
    } else if (*p == ']') {
      if ((len = p - q) > 0) {
        *p = '\0';
        range->r[i] = easy_kfc_range_extra(q, result);
        max *= range->r[i];
        range->s[i++] = easy_pool_strdup(range->pool, result);
      }

      q = p + 1;
    }

    p++;
  }

  if ((len = p - q) > 0) {
    *p = '\0';
    range->s[i++] = q;
  }

  range->cnt = i;
  range->max = max;
}
static int easy_kfc_range_get(easy_kfc_range_t* range, char* result)
{
  if (range->cur >= range->max)
    return 0;

  int i, index = range->cur++;
  char* p = result;

  for (i = 0; i < range->cnt; i++) {
    if (range->r[i]) {
      *p++ = range->s[i][index % range->r[i]];
      index /= range->r[i];
    } else {
      p += lnprintf(p, 128, "%s", range->s[i]);
    }
  }

  return 1;
}
static void easy_kfc_range_free(easy_kfc_range_t* range)
{
  easy_pool_destroy(range->pool);
}

/**
 * round-robin
 */
static int easy_kfc_choice_round_robin(easy_kfc_agent_t* agent)
{
  int i, idx;
  easy_kfc_saddr_t* saddr = &agent->slist;

  // current
  agent->last = NULL;

  for (i = 0; i < saddr->cnt; i++) {
    idx = (saddr->cur++) % saddr->cnt;

    if (saddr->addr[idx]->status == EASY_KFC_STATUS_DOWN)
      continue;

    agent->last = saddr->addr[idx];
    return EASY_OK;
  }

  return EASY_ERROR;
}

/**
 * response_time
 */
static int easy_kfc_choice_response_time(easy_kfc_agent_t* agent)
{
  int i;
  uint64_t lastrt = 0;
  easy_kfc_saddr_t* saddr = &agent->slist;
  easy_kfc_node_t* node = NULL;

  // current
  agent->last = NULL;

  for (i = 0; i < saddr->cnt; i++) {
    if (saddr->addr[i]->status == EASY_KFC_STATUS_DOWN)
      continue;

    if (saddr->addr[i]->lastrt + saddr->addr[i]->rt < lastrt || lastrt == 0) {
      lastrt = saddr->addr[i]->lastrt + saddr->addr[i]->rt;
      node = saddr->addr[i];
    }
  }

  if (node) {
    agent->last = node;
    node->lastrt = lastrt;
    return EASY_OK;
  }

  return EASY_ERROR;
}
