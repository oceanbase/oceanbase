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

#include "ob_kvcache_hazard_version.h"
#include "ob_kv_storecache.h"


namespace oceanbase{
namespace common{

/*
 * -----------------------------------------------------------KVCacheHazardNode-----------------------------------------------------------
 */
KVCacheHazardNode::KVCacheHazardNode()
    : tenant_id_(OB_INVALID_TENANT_ID),
      hazard_next_(nullptr),
      version_(UINT64_MAX)
{
}

KVCacheHazardNode::~KVCacheHazardNode()
{
}

void KVCacheHazardNode::set_next(KVCacheHazardNode * const next)
{
  if (this != next) {
    hazard_next_ = next;
  }
}

/*
 * -----------------------------------------------------------KVCacheHazardThreadStore-----------------------------------------------------------
 */
KVCacheHazardThreadStore::KVCacheHazardThreadStore()
    : acquired_version_(UINT64_MAX),
      delete_list_(nullptr),
      waiting_nodes_count_(0),
      last_retire_version_(0),
      next_(nullptr),
      thread_id_(0),
      is_retiring_(false),
      inited_(false)
{
}

KVCacheHazardThreadStore::~KVCacheHazardThreadStore()
{
}

int KVCacheHazardThreadStore::init(const int64_t thread_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(thread_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid arguments", K(ret), K(thread_id));
  } else if (!ATOMIC_BCAS(&inited_, false, true)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "This KVCacheHazardThreadStore has been inited", K(ret), K(inited_));
  } else {
    thread_id_ = thread_id;
  }

  return ret;
}

void KVCacheHazardThreadStore::set_exit()
{
  thread_id_ = 0;
  acquired_version_ = UINT64_MAX;
  ATOMIC_SET(&inited_, false);
}

int KVCacheHazardThreadStore::delete_node(KVCacheHazardNode &node)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "This KVCacheHazardThreadStore is not inited", K(ret), K(inited_));
  } else {
    add_nodes(node);
    ATOMIC_AAF(&waiting_nodes_count_, 1);
  }

  return ret;
}

void KVCacheHazardThreadStore::retire(const uint64_t version, const uint64_t tenant_id)
{
  if (version > ATOMIC_LOAD(&last_retire_version_) || tenant_id != OB_INVALID_TENANT_ID) {
    while(!ATOMIC_BCAS(&is_retiring_, false, true)) {
      // wait until get retiring
      PAUSE();
    }
    KVCacheHazardNode *head = ATOMIC_LOAD(&delete_list_);
    if (nullptr != head) {
      if (version > last_retire_version_) {
        (void) ATOMIC_SET(&last_retire_version_, version);
      }
      KVCacheHazardNode *temp_node = head;
      while (temp_node != (head = ATOMIC_VCAS(&delete_list_, temp_node, nullptr))) {
        temp_node = head;
      }

      int64_t retire_count = 0;
      KVCacheHazardNode *remain_list = nullptr;
      while (head != nullptr) {
        temp_node = head;
        head = head->get_next();
        if (temp_node->get_version() < version || tenant_id == temp_node->tenant_id_) {
          temp_node->retire();
          temp_node = nullptr;
          ++retire_count;
        } else {
          temp_node->set_next(remain_list);
          remain_list = temp_node;
        }
      }
      if (remain_list != nullptr) {
        add_nodes(*remain_list);
      }
      if (retire_count > 0) {
        ATOMIC_SAF(&waiting_nodes_count_, retire_count);
      }
    }
    ATOMIC_SET(&is_retiring_, false);  // return retiring
  }
}

void KVCacheHazardThreadStore::add_nodes(KVCacheHazardNode &list)
{
  // Remember to udapte waiting_nodes_count_ outside

  KVCacheHazardNode *tail = &list;
  while (nullptr != tail->get_next()) {
    tail = tail->get_next();
  }

  KVCacheHazardNode *curr = ATOMIC_LOAD(&delete_list_);
  KVCacheHazardNode *old = curr;
  tail->set_next(curr);
  while (old != (curr = ATOMIC_VCAS(&delete_list_, old, &list))) {
    old = curr;
    tail->set_next(old);
  }
}

/*
 * -----------------------------------------------------------GlobalHazardVersion-----------------------------------------------------------
 */

GlobalHazardVersion::GlobalHazardVersion()
    : version_(0),
      thread_waiting_node_threshold_(0),
      thread_store_lock_(common::ObLatchIds::THREAD_STORE_LOCK),
      thread_stores_(nullptr),
      thread_store_allocator_(),
      ts_key_(OB_INVALID_PTHREAD_KEY),
      inited_(false)
{
}

GlobalHazardVersion::~GlobalHazardVersion()
{
  destroy();
}

int GlobalHazardVersion::init(const int64_t thread_waiting_node_threshold)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "This HazardVersion has been inited", K(ret), K(inited_));
  } else if (OB_FAIL(thread_store_allocator_.init(OB_MALLOC_MIDDLE_BLOCK_SIZE, "KVCACHE_HAZARD", OB_SERVER_TENANT_ID,
                      INT64_MAX))) {
    COMMON_LOG(WARN, "Fail to init thread store allocator", K(ret));
  } else {
    lib::ObMemAttr attr(OB_SERVER_TENANT_ID, "KVCACHE_HAZARD");
    SET_USE_500(attr);
    thread_store_allocator_.set_attr(attr);
    int syserr = pthread_key_create(&ts_key_, deregister_thread);
    if (OB_UNLIKELY(0 != syserr)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "Fail to create pthread key", K(ret), K(syserr));
    }
  }

  if (OB_FAIL(ret)) {
    destroy();
  } else {
    version_ = 0;
    thread_waiting_node_threshold_ = thread_waiting_node_threshold;
    inited_ = true;
  }

  return ret;
}

void GlobalHazardVersion::destroy()
{
  COMMON_LOG(INFO, "Hazard version begin to destroy");

  inited_ = false;
  thread_stores_ = nullptr;
  thread_store_allocator_.reset();
}

int GlobalHazardVersion::delete_node(KVCacheHazardNode *node)
{
  int ret = OB_SUCCESS;

  KVCacheHazardThreadStore *ts = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "This HazardVersion is not inited", K(ret), K(inited_));
  } else if (OB_UNLIKELY(nullptr == node || OB_INVALID_TENANT_ID == node->tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(ret), KPC(node));
  } else if (OB_FAIL(get_thread_store(ts))) {
    COMMON_LOG(WARN, "Fail to get thread store", K(ret));
  } else {
    node->set_version(ATOMIC_FAA(&version_, 1));
    if (OB_UNLIKELY(nullptr != node->get_next())) {
      COMMON_LOG(ERROR, "Unexpected hazard next", KPC(node));
      ob_abort();
    } else if (OB_FAIL(ts->delete_node(*node))) {
      COMMON_LOG(WARN, "Fail to add node to threadstore", K(ret), K(*ts));
    }
  }

  return ret;
}

int GlobalHazardVersion::acquire()
{
  int ret = OB_SUCCESS;

  KVCacheHazardThreadStore *ts = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "This HazardVersion is not inited", K(ret), K(inited_));
  } else if (OB_FAIL(get_thread_store(ts))) {
    COMMON_LOG(WARN, "Fail to get thread store", K(ret));
  } else {
    ts->set_acquired_version(version_);
    while (ts->get_acquired_version() != ATOMIC_LOAD(&version_)) {
      ts->set_acquired_version(version_);
    }
  }

  return ret;
}

void GlobalHazardVersion::release()
{
  int ret = OB_SUCCESS;

  KVCacheHazardThreadStore *ts = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret  = OB_NOT_INIT;
    COMMON_LOG(WARN, "This HazardVersion is not inited", K(ret));
  } else if (OB_FAIL(get_thread_store(ts))) {
    COMMON_LOG(WARN, "Fail to get thread store", K(ret));
  } else {
    ts->set_acquired_version(UINT64_MAX);
    if (ts->get_waiting_count() >= thread_waiting_node_threshold_) {
      uint64_t min_version = UINT64_MAX;
      if (OB_FAIL(get_min_version(min_version))) {
        COMMON_LOG(WARN, "Fail to get min version", K(ret));
      } else {
        ts->retire(min_version, OB_INVALID_TENANT_ID);
      }
    }
  }
  if (OB_FAIL(ret)) {
    COMMON_LOG(ERROR, "Fail to release version", K(ret));
  }
}

int GlobalHazardVersion::retire(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  uint64_t min_version = UINT64_MAX;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "This HazardVersion is not inited", K(ret), K(inited_));
  } else if (OB_FAIL(get_min_version(min_version))) {
    COMMON_LOG(WARN, "Fail to get current min version", K(ret));
  } else {
    KVCacheHazardThreadStore *ts = thread_stores_;
    while (ts != nullptr) {
      ts->retire(min_version, tenant_id);
      ts = ts->get_next();
    }
  }

  if (tenant_id != OB_INVALID_TENANT_ID) {
    COMMON_LOG(INFO, "erase tenant hazard map node details", K(ret), K(tenant_id));
  }

  return ret;
}

int GlobalHazardVersion::get_thread_store(KVCacheHazardThreadStore *&ts)
{
  int ret = OB_SUCCESS;

  ts = static_cast<KVCacheHazardThreadStore *>(pthread_getspecific(ts_key_));
  if (OB_UNLIKELY(nullptr == ts)) {
    int64_t thread_id = GETTID();
    int syserr = 0;

    // find free thread store to reuse
    {
      lib::ObMutexGuard guard(thread_store_lock_);
      KVCacheHazardThreadStore *free_store = thread_stores_;
      while (OB_SUCC(ret) && nullptr != free_store) {
        if (!free_store->is_inited()) {
          if (OB_FAIL(free_store->init(thread_id))) {
            if (OB_INIT_TWICE == ret) {
              ret = OB_SUCCESS;
            } else {
              COMMON_LOG(WARN, "Falil to init thread store", K(ret), K(thread_id));
            }
          } else if (OB_UNLIKELY(0 != (syserr = pthread_setspecific(ts_key_, free_store)))) {
            ret = OB_ERR_UNEXPECTED;
            COMMON_LOG(WARN, "Fail to set thread local pointer when reuse", K(ret), K(syserr));
            free_store->set_exit();
          } else {
            ts = free_store;
            break;
          }
        }
        free_store = free_store->get_next();
      }

    }  // thread_store_lock_ guard

    // create new thread store if no free thread store can be reuse
    if (OB_SUCC(ret) && nullptr == ts) {
      void *buf = thread_store_allocator_.alloc(sizeof(KVCacheHazardThreadStore));
      if (OB_UNLIKELY(nullptr == buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "Fail to alloc memory for KVCacheHazardThreadStore", K(ret), K(thread_id));
      } else {
        ts = new (buf) KVCacheHazardThreadStore();
        if (OB_FAIL(ts->init(thread_id))) {
          COMMON_LOG(WARN, "Fail to init KVCacheHazardThreadStore", K(ret));
        } else if (OB_UNLIKELY(0 != (syserr = pthread_setspecific(ts_key_, ts)))) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "Fail to set thread local pointer when creaet", K(ret), K(syserr));
        } else {
          lib::ObMutexGuard guard(thread_store_lock_);
          ts->set_next(thread_stores_);
          thread_stores_ = ts;
        }  // thread_store_lock_ guard
        if (OB_FAIL(ret)) {
          ts->~KVCacheHazardThreadStore();
          thread_store_allocator_.free(ts);
          ts = nullptr;
        }
      }
    }
  }

  return ret;
}

int GlobalHazardVersion::print_current_status() const
{
  int ret = OB_SUCCESS;

  static const int64_t BUFLEN = 1 << 18;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "This HazardVersion is not inited", K(ret), K(inited_));
  } else {
    lib::ContextParam param;
    param.set_mem_attr(common::OB_SERVER_TENANT_ID, ObModIds::OB_TEMP_VARIABLES);
    CREATE_WITH_TEMP_CONTEXT(param) {
      int64_t ctxpos = 0;
      KVCacheHazardThreadStore *ts = thread_stores_;
      int64_t total_nodes_num = 0;
      uint64_t min_version = UINT64_MAX;
      int64_t ts_count = 0;
      char *buf = nullptr;
      if (OB_ISNULL(buf = (char *)lib::ctxalp(BUFLEN))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(ERROR, "[KVCACHE-HAZARD] no memory", K(ret));
      } else {
        while (OB_SUCC(ret) && nullptr != ts) {
          int64_t waiting_nodes_count = ts->get_waiting_count();
          uint64_t acquired_version = ts->get_acquired_version();
          total_nodes_num += waiting_nodes_count;
          if (waiting_nodes_count == 0 && acquired_version == UINT64_MAX) {
          } else if (OB_FAIL(databuff_printf(buf, BUFLEN, ctxpos,
              "[KVCACHE-HAZARD] i=%8ld | thread_id=%8ld | inited=%8d | waiting_nodes_count=%8ld | last_retire_version=%8lu | acquired_version=%12lu |\n",
              ts_count,
              ts->get_thread_id(),
              ts->is_inited(),
              waiting_nodes_count,
              ts->get_last_retire_version(),
              acquired_version))) {
            COMMON_LOG(WARN, "Fail to write data buf", K(ret), K(ctxpos), K(BUFLEN));
          }
          ++ts_count;
          ts = ts->get_next();
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(get_min_version(min_version))) {
            COMMON_LOG(WARN, "Fail to get min version of hazard version", K(ret));
          }
          _OB_LOG(INFO, "[KVCACHE-HAZARD] hazard version status info: current version: %8ld | min_version=%8lu | total thread store count: %8ld | total nodes count: %8ld |\n%s",
              ATOMIC_LOAD(&version_),
              min_version,
              ts_count,
              total_nodes_num,
              buf);
        }
      }
    }
  }

  return ret;
}

int GlobalHazardVersion::get_min_version(uint64_t &min_version) const
{
  int ret = OB_SUCCESS;

  min_version = INT64_MAX;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "This HazardVersion is not inited", K(ret), K(inited_));
  } else {
    min_version = ATOMIC_LOAD(&version_);
    KVCacheHazardThreadStore *ts = thread_stores_;
    uint64_t thread_version = UINT64_MAX;

    while (nullptr != ts) {
      thread_version = ts->get_acquired_version();
      if (thread_version < min_version) {
        min_version = thread_version;
      }
      ts = ts->get_next();
    }
  }

  return ret;
}


void GlobalHazardVersion::deregister_thread(void *d_ts)
{
  COMMON_LOG(INFO, "Deregister from hazard_version", KPC(static_cast<KVCacheHazardThreadStore *>(d_ts)));
  static_cast<KVCacheHazardThreadStore *>(d_ts)->set_exit();
}

/*
 * -----------------------------------------------------------GlobalHazardVersionGuard-----------------------------------------------------------
 */

GlobalHazardVersionGuard::GlobalHazardVersionGuard(GlobalHazardVersion &g_version)
    : global_hazard_version_(g_version),
      ret_(OB_SUCCESS)
{
  if (OB_UNLIKELY( OB_SUCCESS != (ret_ = global_hazard_version_.acquire()) )) {
    COMMON_LOG_RET(WARN, ret_, "Fail to acquire hazard version", K(ret_));
  }
}

GlobalHazardVersionGuard::~GlobalHazardVersionGuard()
{
  global_hazard_version_.release();
}


}  // end namespace common
}  // end namespace oceanbase
