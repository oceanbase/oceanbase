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

#include "io/easy_io.h"
#include "io/easy_message.h"
#include "io/easy_connection.h"
#include "io/easy_request.h"
#include "io/easy_baseth_pool.h"
#include "util/easy_util.h"

#ifdef EASY_DEBUG_DOING
easy_atomic_t easy_debug_uuid = 0;
#endif

easy_message_t* easy_message_create_nlist(easy_connection_t* c)
{
  easy_pool_t* pool;
  easy_message_t* m;
  easy_buf_t* input;
  int size;

  if ((pool = easy_pool_create(c->default_msglen)) == NULL) {
    return NULL;
  }

  pool->ref = 1;
  m = (easy_message_t*)easy_pool_calloc(pool, sizeof(easy_message_t));
  size = c->first_msglen;
  input = easy_buf_create(pool, size);

  if (m == NULL || input == NULL) {
    easy_error_log("Failed to alloc easy buffer due to OOM. System will crash.");
    easy_pool_destroy(pool);
    return NULL;
  }

#ifdef EASY_DEBUG_MAGIC
  m->magic = EASY_DEBUG_MAGIC_MESSAGE;
#endif

  m->pool = pool;
  m->c = c;
  m->next_read_len = (c->sc != NULL) ? EASY_IO_BUFFER_SIZE : size;
  m->input = input;
  m->type = EASY_TYPE_MESSAGE;
  m->status = EASY_OK;
  m->request_list_count = 0;
  easy_list_init(&m->request_list);
  easy_list_init(&m->all_list);

  return m;
}

easy_message_t* easy_message_create(easy_connection_t* c)
{
  easy_message_t* m = easy_message_create_nlist(c);

  if (m == NULL) {
    return NULL;
  } else {
    easy_list_add_tail(&m->message_list_node, &c->message_list);
  }

  return m;
}

int easy_message_destroy(easy_message_t* m, int del)
{
  easy_request_t *r, *n;

  // delete from message_list
  if (del) {
    if (m->status == EASY_MESG_DESTROY)
      return EASY_OK;

    m->status = EASY_MESG_DESTROY;
    easy_list_del(&m->message_list_node);
  }

  if (easy_atomic_add_return(&m->pool->ref, -1) == 0) {
    // server done
    easy_list_for_each_entry_safe(r, n, &m->all_list, all_node)
    {
      easy_list_del(&r->all_node);
      easy_list_del(&r->request_list_node);
      easy_request_server_done(r);
    }

    easy_list_del(&m->message_list_node);

    if (m->input)
      easy_buf_destroy(m->input);

#ifdef EASY_DEBUG_MAGIC
    m->magic++;
#endif
    if (m->enable_trace) {
      easy_debug_log("easy_message_destroy, m(%p), lbt(%s).", m, easy_lbt());
    }
    easy_pool_destroy(m->pool);
    return EASY_BREAK;
  }

  return EASY_OK;
}

easy_session_t* easy_session_create(int64_t asize)
{
  easy_pool_t* pool;
  easy_session_t* s;
  int hint = (int)(asize >> 32);
  int size = (int)asize;

  size += sizeof(easy_session_t);

  if ((pool = easy_pool_create((hint < size ? size : hint))) == NULL) {
    return NULL;
  }

  if ((s = (easy_session_t*)easy_pool_alloc(pool, size)) == NULL) {
    easy_pool_destroy(pool);
    return NULL;
  }

  memset(s, 0, sizeof(easy_session_t));
#ifdef EASY_DEBUG_MAGIC
  s->magic = EASY_DEBUG_MAGIC_SESSION;
  s->r.magic = EASY_DEBUG_MAGIC_REQUEST;
#endif
#ifdef EASY_DEBUG_DOING
  s->r.uuid = easy_atomic_add_return(&easy_debug_uuid, 1);
#endif
  s->pool = pool;
  s->r.ms = (easy_message_session_t*)s;
  s->type = EASY_TYPE_SESSION;
  easy_list_init(&s->session_list_node);

  return s;
}

void easy_session_destroy(void* data)
{
  easy_message_t* m;
  easy_session_t* s;

  s = (easy_session_t*)data;

  if (s->cleanup) {
    (s->cleanup)(&s->r, NULL);
  }

  if (s->async && (m = (easy_message_t*)s->r.request_list_node.next)) {
    s->r.request_list_node.next = NULL;
    easy_message_destroy(m, 0);
  }

#ifdef EASY_DEBUG_MAGIC
  s->magic++;
#endif
  easy_pool_destroy(s->pool);
}

int discard_residual_data_of_timeout_session = 0;

static easy_buf_t* easy_copy_and_replace_residual_data_node(easy_buf_t* b)
{
  easy_buf_t* nb = discard_residual_data_of_timeout_session ? NULL : easy_buf_clone_with_private_pool(b);
  if (nb) {
    easy_list_replace(&b->node, &nb->node);
    easy_debug_log("easy keep residual data for timeout session: size=%ld", easy_buf_len(nb));
  }
  return nb;
}

int easy_session_process_low_level(easy_session_t* s, int stop, int need_copy_residual_data)
{
  int ret = EASY_ERROR;
  int discard_cnt = 0;
  int keep_cnt = 0;
  int do_replace = 0;
  easy_list_t *cur = NULL, *prev = NULL;
  easy_buf_t* ebuf = NULL;
  easy_connection_t* conn;

  EASY_STAT_TIME_GUARD((ev_client_cb_count++, ev_client_cb_time += cost), "session_process");
  if (stop) {
    ev_timer_stop(s->c->loop, &s->timeout_watcher);
    easy_list_del(&s->session_list_node);
    easy_request_client_done(&s->r);
    easy_atomic_dec(&s->c->pool->ref);
  }

  conn = s->c;
  if (s->nextb && easy_list_empty(s->nextb) == 0) {
    /*
     * The only purpose to copy the buffers armed on connection output queue is
     * to keep data integrity. When timeout happens, there are 3 cases for the
     * session status: (1) the message bound to the session is completely sent
     * to kernel; (2) the message is patially sent to kernel; (3) no data of the
     * message is sent to kernel. Only for case(2), we have to copy the rest of
     * data of the message which has not been sent to kernel. For other cases,
     * they will not destroy data integrity, so we will not copy it and we just
     * remove it from connection output queue.
     */
    if (need_copy_residual_data) {
      if (s->sent_buf_count > 0) {
        do_replace = 1;
      } else {
        cur = s->nextb;
        while (cur != &(conn->output)) {
          ebuf = easy_list_entry(cur, easy_buf_t, node);
          if (ebuf->args != s->pool) {
            break;
          }

          if (ebuf->pos != ebuf->data) {
            do_replace = 1;
            break;
          }
          cur = cur->prev;
        }
      }
    }

    cur = s->nextb;
    while (cur != &(conn->output)) {
      ebuf = easy_list_entry(cur, easy_buf_t, node);
      if (ebuf->args != s->pool) {
        break;
      }

      prev = cur->prev;
      if (do_replace) {
        if (NULL == easy_copy_and_replace_residual_data_node(ebuf)) {
          easy_list_del(cur);
          discard_cnt++;
        } else {
          keep_cnt++;
        }
      } else {
        easy_list_del(cur);
      }
      cur = prev;
    }

    s->nextb = NULL;
  }

  if (discard_cnt > 0 || keep_cnt > 0) {
    conn->copied_buf_num += keep_cnt;
    conn->discarded_buf_num += discard_cnt;
    easy_debug_log("Totally %d easy_buf coppied, and %d discarded for timeout sessions "
                   "on connection(%p).\n",
        conn->copied_buf_num,
        conn->discarded_buf_num,
        conn);
  }

  if (s->callback) {
    if (s->now) {
      s->now = ev_now(s->c->loop) - s->now;
    }

    ret = (s->callback)(&s->r);
  } else {
    easy_error_log("session callback is null, s = %p\n", s);
    easy_session_destroy(s);
  }

  return (discard_cnt > 0 ? EASY_ERROR : ret);
}

int easy_session_process(easy_session_t* s, int stop, int err)
{
  s->error = err;
  return easy_session_process_low_level(s, stop, 0);
}

int easy_session_process_keep_connection_resilient(easy_session_t* s, int stop, int err)
{
  s->error = err;
  return easy_session_process_low_level(s, stop, 1);
}
