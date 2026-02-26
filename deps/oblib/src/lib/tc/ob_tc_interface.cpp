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

const int QID_CAPACITY = 1<<15;
const int MAX_QDISC_COUNT = QID_CAPACITY * (MAX_N_CHAN + 1);
void** qdtable = nullptr;

int __attribute__((constructor)) init_qdtable()
{
  int ret = 0;
  qdtable = ALLOCATE_QDTABLE(MAX_QDISC_COUNT * sizeof(void*), "qdtable");
  if (qdtable == nullptr) {
    ret = -ENOMEM;
  } else {
    std::memset(qdtable, 0, MAX_QDISC_COUNT * sizeof(void*));
  }
  return ret;
}

static void* imap_fetch(int id)
{
  return id >= 0? qdtable[id % MAX_QDISC_COUNT]: NULL;
}

static int imap_lock()
{
  for(int i = 0; i < QID_CAPACITY; i++) {
    if (!qdtable[i]) {
      return i;
    }
  }
  return -1;
}

static int imap_set(int id, void* p)
{
  qdtable[id] = p;
  return id;
}

static IQDisc* qdisc_new_from_tpl(QDesc* tpl, int chan_id)
{
  switch(tpl->get_type()) {
    case QDISC_ROOT:
      return new QDiscRoot(tpl, chan_id);
    case QDISC_BUFFER_QUEUE:
      return new BufferQueue(tpl, chan_id);
      break;
    case QDISC_WEIGHTED_QUEUE:
      return new WeightedQueue(tpl, chan_id);
      break;
    default:
      abort();
  }
}

static IQDisc* fetch_qdisc(int qid, int chan_id)
{
  int64_t target_id = (chan_id + 1) * QID_CAPACITY + qid;
  return (IQDisc*)qdtable[target_id];
}

static QDiscRoot* fetch_root(int qid, int chan_id)
{
  return (QDiscRoot*)fetch_qdisc(qid, chan_id);
}

static QDiscRoot* create_and_fetch_root(int qid, int chan_id)
{
  int64_t target_id = (chan_id + 1) * QID_CAPACITY + qid;
  QDesc* tpl = (typeof(tpl))qdtable[qid];
  IQDisc** slot = (typeof(slot))(qdtable + target_id);
  if (NULL == *slot) {
    *slot = qdisc_new_from_tpl(tpl, chan_id);
  }
  return (QDiscRoot*)*slot;
}

static IQDisc* create_path_and_fetch_leaf(int qid, int chan_id)
{
  if (qid < 0 || NULL == qdtable[qid]) return NULL;
  int64_t target_id = (chan_id + 1) * QID_CAPACITY + qid;
  IQDisc** slot = (typeof(slot))(qdtable + target_id);
  if (*slot) return *slot;
  QDesc* tpl = (typeof(tpl))qdtable[qid];
  int parent_id = tpl->get_parent();
  assert(parent_id >= 0);
  IQDisc* parent = create_path_and_fetch_leaf(parent_id, chan_id);
  if (NULL == parent) return NULL;
  return (*slot = qdisc_new_from_tpl(tpl, chan_id));
}

static QDesc* qdesc_new(int id, int type, int parent_id, int root, const char* name)
{
  switch(type) {
    case QDISC_ROOT:
      return new QDescRoot(id, name);
    default:
      return new QDesc(id, type, parent_id, root, name);
  }
}

int qdisc_create(int type, int parent_id, const char* name)
{
  QWGuard("qdisc_create");
  int id = imap_lock();
  int root = -1;
  if (id >= 0) {
    if (type != QDISC_ROOT) {
      QDesc* parent = (typeof(parent))imap_fetch(parent_id);
      root = parent->get_root();
      if (root < 0) {
        root = parent_id;
      }
    }
    QDesc* q = qdesc_new(id, type, parent_id, root, name);
    imap_set(id, q);
    if (NULL == q) {
      id = -1;
    }
    TC_INFO("qdisc create: type: %d, parent_id: %d, name: %s", type, parent_id, name);
  }
  return id;
}

void qdisc_destroy(int qid)
{}

int qdisc_set_weight(int qid, int64_t weight)
{
  int err = 0;
  QWGuard("set_weight");
  QDesc* q = (typeof(q))imap_fetch(qid);
  if (NULL == q) {
    err = -ENOENT;
  } else {
    q->set_weight(weight);
  }
  return err;
}

int qdisc_set_limit(int qid, int64_t limit)
{
  int err = 0;
  QWGuard("set_limit");
  limit = (limit <= 0) ? INT64_MAX : limit;
  QDesc* q = (typeof(q))imap_fetch(qid);
  if (NULL == q) {
    err = -ENOENT;
  } else {
    q->set_limit(limit);
  }
  return err;
}

int qdisc_set_reserve(int qid, int64_t limit)
{
  int err = 0;
  QWGuard("set_reserve");
  limit = limit < 0 ? INT64_MAX : limit;
  QDesc* q = (typeof(q))imap_fetch(qid);
  if (NULL == q) {
    err = -ENOENT;
  } else {
    q->set_reserve(limit);
  }
  return err;
}

int qdisc_add_limit(int qid, int limiter_id)
{
  int err = 0;
  QWGuard("add_limit");
  QDesc* q = (typeof(q))imap_fetch(qid);
  ITCLimiter* L = (typeof(L))imap_fetch(limiter_id);
  if (NULL == q) {
    err = -ENOENT;
    TC_INFO("qdisc add limit fail, get qdesc fail: qid: %d, limiter_id: %d, q: %p, L: %p", qid, limiter_id, q, L);
  } else if (NULL == L) {
    err = -ENOENT;
    TC_INFO("qdisc add limit fail, get limiter fail: qid: %d, limiter_id: %d, q: %p, L: %p", qid, limiter_id, q, L);
  } else {
    q->add_limit(L);
  }
  return err;
}

int qdisc_del_limit(int qid, int limiter_id)
{
  int err = 0;
  QWGuard("del_limit");
  QDesc* q = (typeof(q))imap_fetch(qid);
  ITCLimiter* L = (typeof(L))imap_fetch(limiter_id);
  if (NULL == q) {
    err = -ENOENT;
    TC_INFO("qdisc del limit fail, get qdesc fail: qid: %d, limiter_id: %d, q: %p, L: %p", qid, limiter_id, q, L);
  } else if (NULL == L) {
    err = -ENOENT;
    TC_INFO("qdisc del limit fail, get limiter fail: qid: %d, limiter_id: %d, q: %p, L: %p", qid, limiter_id, q, L);
  } else {
    q->del_limit(L);
  }
  return err;
}

int qdisc_add_reserve(int qid, int limiter_id)
{
  int err = 0;
  QWGuard("add_reserve");
  QDesc* q = (typeof(q))imap_fetch(qid);
  ITCLimiter* L = (typeof(L))imap_fetch(limiter_id);
  if (NULL == q) {
    err = -ENOENT;
    TC_INFO("qdisc add reserve fail, get qdesc fail: qid: %d, limiter_id: %d, q: %p, L: %p", qid, limiter_id, q, L);
  } else if (NULL == L) {
    err = -ENOENT;
    TC_INFO("qdisc add reserve fail, get limiter fail: qid: %d, limiter_id: %d, q: %p, L: %p", qid, limiter_id, q, L);
  } else {
    q->add_reserve(L);
  }
  return err;
}

int qdisc_del_reserve(int qid, int limiter_id)
{
  int err = 0;
  QWGuard("del_reserve");
  QDesc* q = (typeof(q))imap_fetch(qid);
  ITCLimiter* L = (typeof(L))imap_fetch(limiter_id);
  if (NULL == q) {
    err = -ENOENT;
    TC_INFO("qdisc del reserve fail, get qdesc fail: qid: %d, limiter_id: %d, q: %p, L: %p", qid, limiter_id, q, L);
  } else if (NULL == L) {
    err = -ENOENT;
    TC_INFO("qdisc del reserve fail, get limiter fail: qid: %d, limiter_id: %d, q: %p, L: %p", qid, limiter_id, q, L);
  } else {
    q->del_reserve(L);
  }
  return err;
}

int qsched_set_handler(int qid, ITCHandler* handler)
{
  int err = 0;
  QWGuard("set_handler");
  QDescRoot* root = (typeof(root))imap_fetch(qid);
  if (NULL == root) {
    err = -ENOENT;
    TC_INFO("qdisc set handler fail: qid: %d, handler: %p, root: %p", qid, handler, root);
  } else {
    root->set_handler(handler);
  }
  return err;
}

int qsched_start(int qid, int n_thread)
{
  int err = 0;
  QWGuard("start");
  if (n_thread >= MAX_N_CHAN) {
    n_thread = MAX_N_CHAN;
  }
  QDescRoot* root = (typeof(root))imap_fetch(qid);
  if (NULL == root) {
    err = -ENOENT;
    TC_INFO("qdisc start fail: qid: %d, n_thread: %d, root: %p", qid, n_thread, root);
  } else {
    root->qsched_start(n_thread);
  }
  return err;
}

int qsched_stop(int qid)
{
  int err = 0;
  QWGuard("stop");
  QDescRoot* root = (typeof(root))imap_fetch(qid);
  if (NULL == root) {
    err = -ENOENT;
    TC_INFO("qdisc stop fail: qid: %d, root: %p", qid, root);
  } else {
    root->qsched_stop();
  }
  return err;
}

int qsched_wait(int qid)
{
  int err = 0;
  QWGuard("wait");
  QDescRoot* root = (typeof(root))imap_fetch(qid);
  if (NULL == root) {
    err = -ENOENT;
    TC_INFO("qdisc wait fail: qid: %d, root: %p", qid, root);
  } else {
    root->qsched_wait();
  }
  return err;
}

int qsched_submit(int qid, TCRequest* req, uint32_t chan_id)
{
  int err = 0;
  QRGuard("submit");
  QDescRoot* root = (typeof(root))imap_fetch(qid);
  if (NULL == root) {
    err = -ENOENT;
    TC_INFO("qdisc submit fail: qid: %d, req: %p, chan_id: %d", qid, req, chan_id);
  } else {
    err = root->qsched_submit(req, chan_id);
  }
  return err;
}

ITCLimiter* tclimit_new(int id, int type, const char* name, uint64_t storage_key = 0)
{
  switch(type) {
    case TCLIMIT_BYTES:
      return new BytesLimiter(id, name, storage_key);
    case TCLIMIT_COUNT:
      return new CountLimiter(id, name, storage_key);
      break;
    default:
      abort();
  }
}

int tclimit_create(int type, const char* name, uint64_t storage_key)
{
  QWGuard("tclimit_create");
  int id = imap_lock();
  if (id >= 0) {
    ITCLimiter* limiter = tclimit_new(id, type, name, storage_key);
    imap_set(id, limiter);
    if (NULL == limiter) {
      id = -1;
    }
  }
  TC_INFO("tclimit create: type: %d, name: %s, storage_key: %lu, id: %d", type, name, storage_key, id);
  return id;
}

void tclimit_destroy(int id)
{
}

int tclimit_set_limit(int limiter_id, int64_t limit)
{
  int err = 0;
  QWGuard("set_limit");
  limit = (limit <= 0) ? INT64_MAX : limit;
  ITCLimiter* L = (typeof(L))imap_fetch(limiter_id);
  if (NULL == L) {
    err = -ENOENT;
  } else {
    L->set_limit_per_sec(limit);
  }
  return err;
}

int tclimit_get_limit(int limiter_id, int64_t &limit)
{
  int err = 0;
  QRGuard("get_limit");
  ITCLimiter* L = (typeof(L))imap_fetch(limiter_id);
  if (NULL == L) {
    err = -ENOENT;
  } else {
    limit = L->get_limit_per_sec();
  }
  return err;
}