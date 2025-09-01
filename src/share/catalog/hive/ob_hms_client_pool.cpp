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

#define USING_LOG_PREFIX SHARE

#include "lib/alloc/alloc_assist.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/rc/ob_rc.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/utility.h"
#include "ob_hive_metastore.h"
#include "ob_hms_client_pool.h"
#include "observer/virtual_table/ob_all_virtual_hms_client_pool_stat.h"

namespace oceanbase
{
namespace share
{
/* --------------------- start of ObHMSClientPoolKey ---------------------*/

uint64_t ObHMSClientPoolKey::do_hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_ret);
  hash_ret = murmurhash(&catalog_id_, sizeof(catalog_id_), hash_ret);
  return hash_ret;
}

int ObHMSClientPoolKey::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = do_hash();
  return ret;
}

bool ObHMSClientPoolKey::operator==(const ObHMSClientPoolKey &other) const
{
  return tenant_id_ == other.tenant_id_ && catalog_id_ == other.catalog_id_;
}

int ObHMSClientPoolKey::assign(const ObHMSClientPoolKey &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    catalog_id_ = other.catalog_id_;
  }
  return ret;
}

/* --------------------- end of ObHMSClientPoolKey ---------------------*/

/* --------------------- start of ObHMSClientPool ---------------------*/

ObHMSClientPool::ObHMSClientPool()
    : allocator_(nullptr), tenant_id_(OB_INVALID_TENANT_ID), catalog_id_(OB_INVALID_ID), uri_(),
      properties_(), client_queue_(nullptr), pool_size_(DEFAULT_HMS_CLIENT_POOL_SIZE),
      total_clients_(0), in_use_clients_(0), pool_lock_(ObLatchIds::OBJECT_DEVICE_LOCK),
      pool_cond_(), is_inited_(false), last_access_ts_(0)
{
}

ObHMSClientPool::~ObHMSClientPool()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(pool_lock_);

  // Clean up all clients in the queue
  // Pool doesn't hold references to clients, so we just need to destroy them
  ObHiveMetastoreClient *client = nullptr;

  if (OB_NOT_NULL(client_queue_) && OB_NOT_NULL(allocator_)) {
    while (OB_SUCC(client_queue_->pop_front(client))) {
      if (OB_NOT_NULL(client)) {
        LOG_TRACE("destroy client in pool destructor",
                  K(ret),
                  KP(client),
                  K(client->get_client_id()));
        // Directly destroy the client object
        client->~ObHiveMetastoreClient();
        allocator_->free(client);
        client = nullptr;
      }
    }

    // Clean up the queue itself
    client_queue_->~HMSClientList();
    allocator_->free(client_queue_);
    client_queue_ = nullptr;
  }

  // Reset state
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  catalog_id_ = OB_INVALID_ID;
  total_clients_ = 0;
  in_use_clients_ = 0;
  uri_.reset();
  properties_.reset();
  allocator_ = nullptr;
}

int ObHMSClientPool::init(const uint64_t &tenant_id,
                          const uint64_t &catalog_id,
                          const ObString &uri,
                          const ObString &properties,
                          ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(pool_lock_);

  ObHMSCatalogProperties hms_properties;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObHMSClientPool init twice", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(OB_INVALID_ID == catalog_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid catalog id", K(ret), K(catalog_id));
  } else if (OB_ISNULL(uri) || OB_LIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid metastore uri", K(ret), K(uri));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid allocator", K(ret), KP(allocator));
  } else if (OB_FAIL(hms_properties.load_from_string(properties, *allocator))) {
    LOG_WARN("fail to init hms catalog properties", K(ret), K_(properties));
  } else if (OB_FAIL(hms_properties.decrypt(*allocator))) {
    LOG_WARN("failed to decrypt", K(ret));
  } else {
    pool_size_ = hms_properties.max_client_pool_size_;
    socket_timeout_ = hms_properties.socket_timeout_;

    LOG_TRACE("init hms client pool", K(ret), K(pool_size_), K(socket_timeout_));
    allocator_ = allocator;
    tenant_id_ = tenant_id;
    catalog_id_ = catalog_id;

    // Create client_queue_ with the provided allocator
    void *queue_buf = allocator_->alloc(sizeof(HMSClientList));
    if (OB_ISNULL(queue_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc buf for client queue", K(ret));
    } else {
      client_queue_ = new (queue_buf) HMSClientList(*allocator_);
      if (OB_FAIL(ob_write_string(*allocator_, uri, uri_, true))) {
        LOG_WARN("failed to write metastore uri", K(ret), K(uri));
      } else if (OB_FAIL(ob_write_string(*allocator_, properties, properties_, true))) {
        LOG_WARN("failed to write properties", K(ret), K(properties));
      } else {
        is_inited_ = true;
        last_access_ts_ = ObTimeUtility::current_time();
      }

      if (OB_FAIL(ret)) {
        if (OB_NOT_NULL(client_queue_)) {
          client_queue_->~HMSClientList();
          allocator_->free(client_queue_);
          client_queue_ = nullptr;
        }
      }
    }
  }

  return ret;
}

int ObHMSClientPool::get_client(ObHiveMetastoreClient *&client)
{
  int ret = OB_SUCCESS;
  client = nullptr;
  const int64_t remaining_time_us = THIS_WORKER.get_timeout_remain();
  // Using the smaller one of socket timeout and remaining time as timeout.
  const int64_t timeout_us = std::min(socket_timeout_, remaining_time_us);
  LOG_TRACE("get client by timeout",
            K(ret),
            K(timeout_us),
            K(socket_timeout_),
            K(remaining_time_us));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHMSClientPool not inited", K(ret));
  } else if (OB_UNLIKELY(0 >= timeout_us)) {
    ret = OB_TIMEOUT;
    LOG_WARN("query timeout is reached", K(timeout_us));
  } else {
    const int64_t start_time = ObTimeUtility::current_time();
    bool got_client = false;

    // Retry until get client or timeout
    do {
      // 使用 SpinWLockGuard 保护共享资源访问
      {
        SpinWLockGuard guard(pool_lock_);
        last_access_ts_ = ObTimeUtility::current_time();

        // Try to get client from existing queue first
        int tmp_ret = try_get_client_from_queue_unlocked_(client);
        if (OB_SUCCESS == tmp_ret) {
          got_client = true;
        } else if (can_create_client_unlocked_()) {
          LOG_TRACE("try to create and get client",
                    K(ret),
                    K(tmp_ret),
                    K(total_clients_),
                    K(in_use_clients_),
                    K(pool_size_));
          tmp_ret = try_create_and_get_client_unlocked_(client);
          if (OB_SUCCESS == tmp_ret) {
            got_client = true;
            LOG_TRACE("got client",
                      K(ret),
                      K(tmp_ret),
                      K(total_clients_),
                      K(in_use_clients_),
                      K(pool_size_));
          } else if (OB_INVALID_HMS_METASTORE == tmp_ret) {
            ret = OB_INVALID_HMS_METASTORE;
            LOG_WARN("invalid metastore to skip try again", K(ret));
          } else if (OB_KERBEROS_ERROR == tmp_ret) {
            ret = OB_KERBEROS_ERROR;
            LOG_WARN("failed to get or create client due to kerberos error", K(ret));
          } else {
            LOG_WARN("failed to get or create client", K(ret), K(tmp_ret));
          }
        } else {
          // All clients are in use and pool is full, need to wait
          LOG_TRACE("all clients are in use and pool is full, need to wait",
                    K(total_clients_),
                    K(in_use_clients_),
                    K(pool_size_));
        }
      }

      // Check timeout and wait if we haven't got client yet
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to get client", K(ret));
      } else if (!got_client) {
        const int64_t elapsed_time = ObTimeUtility::current_time() - start_time;
        if (elapsed_time >= timeout_us) {
          ret = OB_TIMEOUT;
          LOG_WARN("timeout waiting for client",
                   K(ret),
                   K(timeout_us),
                   K(elapsed_time),
                   K_(tenant_id),
                   K_(catalog_id));
        } else {
          const int64_t remaining_time = timeout_us - elapsed_time;
          LOG_TRACE("waiting for available client",
                    K(total_clients_),
                    K(in_use_clients_),
                    K(pool_size_),
                    K(elapsed_time),
                    K(remaining_time));

          if (OB_FAIL(pool_cond_.timedwait(remaining_time))) {
            if (OB_TIMEOUT == ret) {
              LOG_WARN("timeout waiting for client condition",
                       K(ret),
                       K(timeout_us),
                       K(elapsed_time),
                       K_(tenant_id),
                       K_(catalog_id));
            } else {
              LOG_WARN("failed to wait for client condition", K(ret));
            }
            break;
          }
          LOG_TRACE("wait for available client done",
                    K(ret),
                    K(total_clients_),
                    K(in_use_clients_),
                    K(pool_size_),
                    K(elapsed_time),
                    K(remaining_time));
        }
      }
    } while (OB_SUCC(ret) && !got_client);
  }
  return ret;
}

int ObHMSClientPool::return_client(ObHiveMetastoreClient *&client)
{
  int ret = OB_SUCCESS;

  LOG_TRACE("return client to pool", K(ret), KP(client), K_(tenant_id), K_(catalog_id));
  if (OB_ISNULL(client)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("client is null", K(ret), KP(client));
  } else if (OB_UNLIKELY(!client->is_valid())) {
    // Client is invalid, destroy it to prevent memory leak
    client->~ObHiveMetastoreClient();
    allocator_->free(client);
    client = nullptr;

    // Need to update counters when destroying invalid client
    {
      SpinWLockGuard guard(pool_lock_);
      if (total_clients_ > 0) {
        total_clients_--; // Adjust counter for destroyed invalid client
      }
      if (in_use_clients_ > 0) {
        in_use_clients_--; // This client was in use, so decrease the counter
      }
    }

    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("client is invalid, destroying it", K(ret), KP(client), K_(tenant_id), K_(catalog_id));
  } else if (OB_UNLIKELY(!is_inited_)) {
    // Pool is shutdown, cannot return to queue, must destroy the client
    client->~ObHiveMetastoreClient();
    allocator_->free(client);
    client = nullptr;
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool shutdown, destroyed client since cannot return to queue",
             K(ret),
             K_(tenant_id),
             K_(catalog_id));
  } else {
    SpinWLockGuard guard(pool_lock_);

    last_access_ts_ = ObTimeUtility::current_time();

    // Check if pool size has been reduced and we need to shrink the pool.
    if (OB_LIKELY(can_destroy_client_unlocked_())) {
      // Pool is full (or over capacity due to configuration change), destroy the client
      LOG_TRACE("pool is full, destroying returned client to shrink pool size",
                K(ret),
                KP(client),
                K(total_clients_),
                K(pool_size_),
                K(client->get_client_id()));
      client->~ObHiveMetastoreClient();
      allocator_->free(client);
      client = nullptr;
      total_clients_--;
      in_use_clients_--;
    } else if (OB_NOT_NULL(client_queue_)) {
      // Pool still has capacity, try to return the client to the queue
      if (OB_FAIL(client_queue_->push_back(client))) {
        // Failed to return to queue, must destroy the client to avoid memory leak
        LOG_WARN("failed to push client back to queue, destroying client", K(ret), KP(client));
        client->~ObHiveMetastoreClient();
        allocator_->free(client);
        client = nullptr;
        // Still decrease in_use_clients_ count since the client was in use.
        in_use_clients_--;
        total_clients_--;
      } else {
        in_use_clients_--;
        // Client has been successfully returned to pool and becomes idle.
        LOG_TRACE("client successfully returned to pool queue",
                  K(ret),
                  KP(client),
                  K(in_use_clients_),
                  K(total_clients_),
                  K(client->get_client_id()));
      }
    } else {
      // client_queue_ is null, cannot return to queue, must destroy the client
      client->~ObHiveMetastoreClient();
      allocator_->free(client);
      client = nullptr;
      total_clients_--;
      in_use_clients_--;
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("client queue is null, destroying client", K(ret), KP(client));
    }
  }

  // 在释放 pool_lock_ 后再发送 signal
  pool_cond_.signal();

  return ret;
}

void ObHMSClientPool::get_pool_stats(int64_t &total_clients,
                                     int64_t &in_use_clients,
                                     int64_t &idle_clients) const
{
  SpinRLockGuard guard(pool_lock_);
  total_clients = total_clients_;
  in_use_clients = in_use_clients_;
  idle_clients = (OB_NOT_NULL(client_queue_)) ? client_queue_->size() : 0;
}

int ObHMSClientPool::cleanup()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(pool_lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    // Pool is shutdown, skip cleanup.
    ret = OB_SUCCESS;
    LOG_INFO("skip cleanup on shutdown pool", K(ret), K(catalog_id_));
  } else {
    const int64_t current_time = ObTimeUtility::current_time();
    const int64_t idle_time_us = current_time - last_access_ts_;
    if (OB_LIKELY(idle_time_us >= POOL_IDLE_THRESHOLD_US) && OB_NOT_NULL(client_queue_)) {
      ObHiveMetastoreClient *client = nullptr;
      int64_t cleaned_count = 0;
      int64_t current_available = client_queue_->size();

      if (OB_LIKELY(current_available > 0)) {
        LOG_TRACE("start idle client cleanup", K(ret), K(current_available), K(idle_time_us));

        // For cleanup, we only clean clients that are idle and only held by pool
        // We use a simple approach: only clean clients that can be safely removed
        while (OB_SUCC(client_queue_->pop_front(client)) && cleaned_count < current_available) {
          if (OB_NOT_NULL(client)) {
            LOG_TRACE("cleanup idle client",
                      K(ret),
                      K(current_available),
                      K_(tenant_id),
                      K_(catalog_id),
                      KP(client),
                      K(client->get_client_id()));

            // Since we don't use reference counting anymore, directly clean idle clients
            // All clients in the queue are considered idle and can be cleaned
            client->~ObHiveMetastoreClient();
            allocator_->free(client);
            client = nullptr;
            cleaned_count++;
          } else {
            // Found null client in queue, this should not happen but handle it gracefully
            LOG_INFO("found null client in queue during cleanup",
                     K(ret),
                     K_(tenant_id),
                     K_(catalog_id));
            // Note: We don't increment cleaned_count for null clients because
            // they were never counted in total_clients_ in the first place
            // This prevents total_clients_ counter from becoming negative.
          }
        }

        // Reset ret to OB_SUCCESS if the client is not exist.
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        }

        if (OB_FAIL(ret)) {
        } else if (OB_LIKELY(cleaned_count > 0)) {
          total_clients_ -= cleaned_count;
          LOG_TRACE("cleaned up all idle clients",
                    K(ret),
                    K_(tenant_id),
                    K_(catalog_id),
                    K(cleaned_count),
                    K(total_clients_),
                    K(client_queue_->size()));
        }
      } else {
        LOG_TRACE("no idle clients to cleanup",
                  K(ret),
                  K_(tenant_id),
                  K_(catalog_id),
                  K(current_available));
      }
    } else {
      LOG_TRACE("skip cleanup, pool recently used",
                K(ret),
                K_(tenant_id),
                K_(catalog_id),
                K(idle_time_us));
    }
  }

  LOG_TRACE("cleanup done idle hms client pools",
            K(ret),
            K(tenant_id_),
            K(catalog_id_),
            K(pool_size_));
  return ret;
}

bool ObHMSClientPool::is_pool_full_unlocked_()
{
  return total_clients_ > pool_size_ && pool_size_ > 0;
}

bool ObHMSClientPool::idle_clients_available_unlocked_()
{
  return total_clients_ > in_use_clients_;
}

bool ObHMSClientPool::can_create_client_unlocked_()
{
  return idle_clients_available_unlocked_() || !is_pool_full_unlocked_() || pool_size_ <= 0;
}

bool ObHMSClientPool::can_destroy_client_unlocked_()
{
  return is_pool_full_unlocked_() || pool_size_ <= 0;
}

int ObHMSClientPool::create_new_client_unlocked_()
{
  int ret = OB_SUCCESS;

  ObHiveMetastoreClient *client = nullptr;
  // Create client directly instead of using ObHiveMetastoreClientFactory singleton.
  // This avoids the limitation of the factory's map size (16).
  void *hms_client_buf = nullptr;
  if (OB_ISNULL(hms_client_buf = allocator_->alloc(sizeof(ObHiveMetastoreClient)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf for ob hive metastore client", K(ret));
  } else {
    LOG_TRACE("create new client by socket timeout", K(ret), K(socket_timeout_));
    client = new (hms_client_buf) ObHiveMetastoreClient();
    if (OB_FAIL(client->init(socket_timeout_, allocator_))) {
      LOG_WARN("failed to init hive metastore client", K(ret));
    } else if (OB_FAIL(client->setup_hive_metastore_client(uri_, properties_))) {
      LOG_WARN("failed to create hive thrift client", K(ret));
    } else {
      // Set pool pointer at creation time - this establishes the ownership relationship
      client->set_client_pool(this);
      // Pool doesn't hold reference count, just add to queue.
      if (OB_ISNULL(client_queue_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("client queue is null", K(ret));
      } else if (OB_FAIL(client_queue_->push_back(client))) {
        LOG_WARN("failed to push client to queue", K(ret));
      } else {
        total_clients_++;
        LOG_TRACE("created new client and added to pool",
                  K(ret),
                  KP(client),
                  K(total_clients_),
                  K(client->get_client_id()));
      }
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(client)) {
        client->~ObHiveMetastoreClient();
        client = nullptr;
      }
      if (OB_NOT_NULL(hms_client_buf)) {
        allocator_->free(hms_client_buf);
        hms_client_buf = nullptr;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!client->is_valid())) {
    // Client was created and added to queue but is invalid, remove it.
    if (OB_NOT_NULL(client_queue_) && OB_SUCC(client_queue_->pop_back())) {
      total_clients_--;
      LOG_WARN("created client is invalid, removing from queue", K(ret), KP(client));
      // Destroy the invalid client to prevent memory leak
      client->~ObHiveMetastoreClient();
      allocator_->free(client);
      client = nullptr;
    } else {
      // Failed to remove invalid client from queue, this is unexpected
      LOG_WARN("failed to remove invalid client from queue", K(ret), KP(client));
    }
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hive client is not valid", K(ret), KP(client));
  }
  return ret;
}

int ObHMSClientPool::try_get_client_from_queue_unlocked_(ObHiveMetastoreClient *&client)
{
  int ret = OB_SUCCESS;
  client = nullptr;

  if (OB_ISNULL(client_queue_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("client queue is null", K(ret));
  } else if (OB_LIKELY(!client_queue_->empty())) {
    if (OB_FAIL(client_queue_->pop_front(client))) {
      LOG_WARN("failed to pop client from queue", K(ret));
    } else if (OB_ISNULL(client) || OB_UNLIKELY(!client->is_valid())) {
      // Found invalid client, destroy it and adjust counter
      destroy_invalid_client_unlocked_(client);
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get the client from pool queue is null or invalid",
               K(ret),
               K_(tenant_id),
               K_(catalog_id));
    } else {
      in_use_clients_++;
      client->set_in_use_state(true);
      LOG_TRACE("get client from pool queue",
                K(ret),
                KP(client),
                K(in_use_clients_),
                K(total_clients_),
                K(client->get_client_id()));
    }
  } else {
    ret = OB_ENTRY_NOT_EXIST; // No client available in queue
    LOG_WARN("no client available in queue", K(ret));
  }

  return ret;
}

int ObHMSClientPool::try_create_and_get_client_unlocked_(ObHiveMetastoreClient *&client)
{
  int ret = OB_SUCCESS;
  client = nullptr;

  if (is_pool_full_unlocked_()) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("pool is full, cannot create more clients", K(ret));
  } else if (OB_FAIL(create_new_client_unlocked_())) {
    LOG_WARN("failed to create new client", K(ret));
  } else if (OB_ISNULL(client_queue_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("client queue is null", K(ret));
  } else if (OB_FAIL(client_queue_->pop_front(client))) {
    LOG_WARN("failed to pop newly created client from queue", K(ret));
  } else if (OB_ISNULL(client) || OB_UNLIKELY(!client->is_valid())) {
    // Found invalid newly created client, destroy it
    destroy_invalid_client_unlocked_(client);
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("newly created client is null or invalid", K(ret), K_(tenant_id), K_(catalog_id));
  } else {
    in_use_clients_++;
    client->set_in_use_state(true);
    LOG_TRACE("created and got new client",
              K(ret),
              KP(client),
              K(in_use_clients_),
              K(total_clients_),
              K(client->get_client_id()));
  }

  return ret;
}

void ObHMSClientPool::destroy_invalid_client_unlocked_(ObHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(client)) {
    LOG_TRACE("destroying invalid client", K(ret), KP(client), K_(tenant_id), K_(catalog_id));
    client->~ObHiveMetastoreClient();
    allocator_->free(client);
    client = nullptr;
    // Adjust counter for destroyed invalid client
    if (total_clients_ > 0) {
      total_clients_--;
    }
  }
}

/* --------------------- end of ObHMSClientPool ---------------------*/

/* --------------------- start of ObHMSClientPoolIdleClearTask ---------------------*/
void ObHMSClientPoolIdleClearTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(pool_mgr_) && pool_mgr_->is_inited()) {
    LOG_INFO("run timer task to cleanup idle pools", K(ret), K(pool_mgr_->get_tenant_id()));
    if (OB_FAIL(pool_mgr_->cleanup_idle_pools())) {
      LOG_WARN("failed to cleanup idle pools", K(ret), K(pool_mgr_->get_tenant_id()));
    }
  }
}
/* --------------------- end of ObHMSClientPoolIdleClearTask ---------------------*/

/* --------------------- start of ObHMSClientPoolMgr ---------------------*/

ObHMSClientPoolMgr::ObHMSClientPoolMgr()
    : inner_allocator_(nullptr), is_inited_(false), destroyed_(false),
      tenant_id_(OB_INVALID_TENANT_ID), tg_id_(-1), last_access_ts_(0), pool_map_(),
      pool_mgr_lock_(ObLatchIds::OBJECT_DEVICE_LOCK)
{
}

ObHMSClientPoolMgr::~ObHMSClientPoolMgr()
{
  if (is_inited_) {
    destroy();
  }
}

int ObHMSClientPoolMgr::mtl_init(ObHMSClientPoolMgr *&node_list)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = lib::current_resource_owner_id();
  if (OB_FAIL(node_list->init(tenant_id))) {
    LOG_WARN("failed to init hms client pool mgr", K(ret), K(tenant_id));
  }
  return ret;
}

void ObHMSClientPoolMgr::mtl_stop(ObHMSClientPoolMgr *&node_list)
{
  if (OB_LIKELY(nullptr != node_list)) {
    TG_STOP(node_list->tg_id_);
  }
}

void ObHMSClientPoolMgr::mtl_wait(ObHMSClientPoolMgr *&node_list)
{
  if (OB_LIKELY(nullptr != node_list)) {
    TG_WAIT(node_list->tg_id_);
  }
}

int ObHMSClientPoolMgr::get_client(const uint64_t &tenant_id,
                                   const uint64_t &catalog_id,
                                   const ObString &uri,
                                   const ObString &properties,
                                   ObHiveMetastoreClient *&client)
{
  int ret = OB_SUCCESS;
  client = nullptr;
  ObHMSClientPool *pool = nullptr;

  // Get or create pool by tenant_id and catalog_id
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHMSClientPoolMgr not inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || tenant_id != tenant_id_)) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid tenant id or not the same as current tenant", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(OB_INVALID_ID == catalog_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid catalog id", K(ret), K(catalog_id));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid metastore uri", K(ret), K(uri));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_or_create_pool(tenant_id, catalog_id, uri, properties, pool))) {
    LOG_WARN("failed to get or create pool", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_ISNULL(pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to get valid pool", K(ret));
  } else if (OB_FAIL(pool->get_client(client))) {
    LOG_WARN("failed to get client from pool", K(ret));
  } else {
    LOG_TRACE("get client from pool",
              K(ret),
              K(tenant_id),
              K(catalog_id),
              K(client->get_client_id()));
  }

  return ret;
}

int ObHMSClientPoolMgr::cleanup_idle_pools()
{
  int ret = OB_SUCCESS;
  ObArray<ObHMSClientPoolKey> candidate_keys;

  // Step 1: collect all keys (read lock protected)
  {
    SpinRLockGuard guard(pool_mgr_lock_);
    hash::ObHashMap<ObHMSClientPoolKey, ObHMSClientPool *>::iterator iter = pool_map_.begin();
    for (; OB_SUCC(ret) && iter != pool_map_.end(); ++iter) {
      if (OB_FAIL(candidate_keys.push_back(iter->first))) {
        LOG_WARN("push back key failed", K(ret));
      }
    }
  }

  // Step 2: process candidate pools
  ObArray<ObHMSClientPoolKey> pools_to_remove;
  for (int i = 0; OB_SUCC(ret) && i < candidate_keys.count(); ++i) {
    const ObHMSClientPoolKey &key = candidate_keys[i];
    bool can_remove = false;
    int tmp_ret = OB_SUCCESS;

    {
      SpinRLockGuard guard(pool_mgr_lock_);
      ObHMSClientPool *pool = nullptr;
      tmp_ret = pool_map_.get_refactored(key, pool);

      if (OB_SUCC(tmp_ret) && OB_NOT_NULL(pool)) {
        // Execute cleanup (pool has its own lock)
        if (OB_SUCCESS != (tmp_ret = pool->cleanup())) {
          LOG_WARN("cleanup pool failed", K(tmp_ret), K(key));
        }

        int64_t total_clients, in_use_clients, idle_clients;
        pool->get_pool_stats(total_clients, in_use_clients, idle_clients);
        const int64_t idle_time = ObTimeUtility::current_time() - pool->get_last_access_ts();

        if (0 == total_clients && 0 == in_use_clients && 0 == idle_clients
            && idle_time >= POOL_REMOVAL_THRESHOLD_US) {
          can_remove = true;
        }
      } else if (OB_HASH_NOT_EXIST == tmp_ret) {
        // Pool has been removed, but the key is still in the map.
        tmp_ret = OB_SUCCESS;
        LOG_TRACE("pool has been removed", K(tmp_ret), K(key));
      } else {
        // Unexpected error, reset tmp_ret to OB_SUCCESS.
        LOG_WARN("get pool failed, unexpected", K(tmp_ret), K(key));
        tmp_ret = OB_SUCCESS;
      }
    }

    if (OB_SUCC(tmp_ret) && can_remove) {
      if (OB_FAIL(pools_to_remove.push_back(key))) {
        LOG_WARN("push back key failed", K(ret), K(key));
      }
    }
  }

  // Step 3: actually remove pools
  for (int i = 0; OB_SUCC(ret) && i < pools_to_remove.count(); ++i) {
    const ObHMSClientPoolKey &key = pools_to_remove[i];
    int tmp_ret = OB_SUCCESS;

    SpinWLockGuard guard(pool_mgr_lock_);
    ObHMSClientPool *pool = nullptr;
    tmp_ret = pool_map_.get_refactored(key, pool);

    if (OB_SUCC(tmp_ret) && OB_NOT_NULL(pool)) {
      // Final status verification
      int64_t total_clients, in_use_clients, idle_clients;
      pool->get_pool_stats(total_clients, in_use_clients, idle_clients);
      const int64_t idle_time = ObTimeUtility::current_time() - pool->get_last_access_ts();

      if (0 == total_clients && 0 == in_use_clients && 0 == idle_clients
          && idle_time >= POOL_REMOVAL_THRESHOLD_US) {
        if (OB_SUCCESS != (tmp_ret = pool_map_.erase_refactored(key))) {
          LOG_WARN("erase failed", K(tmp_ret), K(key));
        } else {
          pool->~ObHMSClientPool();
          inner_allocator_->free(pool);
          pool = nullptr;
          LOG_INFO("removed idle pool", K(key));
        }
      }
    } else if (OB_HASH_NOT_EXIST == tmp_ret) {
      LOG_INFO("pool has been removed", K(tmp_ret), K(key));
    } else {
      LOG_WARN("get pool failed, unexpected", K(tmp_ret), K(key));
    }
  }

  return ret;
}

int ObHMSClientPoolMgr::generate_pool_stat_monitoring_info_rows(
    observer::ObHMSClientPoolGetter &getter)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(pool_mgr_lock_);
  if (OB_FAIL(pool_map_.foreach_refactored(getter))) {
    LOG_WARN("fail to generate opt stat monitoring info rows", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObHMSClientPoolMgr::get_or_create_pool(const uint64_t &tenant_id,
                                           const uint64_t &catalog_id,
                                           const ObString &uri,
                                           const ObString &properties,
                                           ObHMSClientPool *&pool)
{
  int ret = OB_SUCCESS;
  pool = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHMSClientPoolMgr not inited", K(ret));
  } else if (OB_ISNULL(inner_allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner allocator is not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || tenant_id != tenant_id_)) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid tenant id or not the same as current tenant", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(OB_INVALID_ID == catalog_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid catalog id", K(ret), K(catalog_id));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid metastore uri", K(ret), K(uri));
  } else {
    ObHMSClientPoolKey key(tenant_id, catalog_id);

    // First check: read lock
    {
      SpinRLockGuard r_guard(pool_mgr_lock_);
      ret = pool_map_.get_refactored(key, pool);
      if (OB_SUCC(ret) && OB_LIKELY(pool->is_inited())) {
        LOG_TRACE("found valid pool", K(ret), K(key));
      } else {
        pool = nullptr;
        ret = OB_HASH_NOT_EXIST;
      }
    }

    // If pool does not exist or is invalid, create new one.
    if (OB_HASH_NOT_EXIST == ret) {
      void *pool_buf = inner_allocator_->alloc(sizeof(ObHMSClientPool));
      if (OB_ISNULL(pool_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc buf for ob hive client pool", K(ret));
      } else {
        ObHMSClientPool *new_pool = new (pool_buf) ObHMSClientPool();
        if (OB_FAIL(new_pool->init(tenant_id, catalog_id, uri, properties, inner_allocator_))) {
          LOG_WARN("failed to init hive client pool", K(ret));
        }

        // If new pool is valid, insert it into map.
        if (OB_SUCC(ret)) {
          // Second check: write lock
          SpinWLockGuard w_guard(pool_mgr_lock_);
          bool need_insert_new_pool = false;
          ret = pool_map_.get_refactored(key, pool);

          if (OB_SUCC(ret)) {
            need_insert_new_pool = !pool->is_inited();
            if (need_insert_new_pool) {
              ret = pool_map_.erase_refactored(key);
              if (OB_SUCC(ret)) {
                pool->~ObHMSClientPool();
                inner_allocator_->free(pool);
                pool = nullptr;
              } else {
                LOG_WARN("failed to erase invalid pool", K(ret), K(key));
              }
            } else {
              LOG_TRACE("use existing valid pool", K(ret), K(key));
            }
          } else if (OB_HASH_NOT_EXIST == ret) {
            need_insert_new_pool = true;
            ret = OB_SUCCESS;
          }

          if (OB_SUCC(ret) && need_insert_new_pool) {
            ret = pool_map_.set_refactored(key, new_pool);
            if (OB_SUCC(ret)) {
              pool = new_pool;
              LOG_TRACE("succeed create new hive client pool", K(ret), K(key));
            } else {
              LOG_WARN("failed to insert new pool into map", K(ret), K(key));
            }
          }

          // If new pool is not used, free it.
          if (!need_insert_new_pool || OB_FAIL(ret)) {
            new_pool->~ObHMSClientPool();
            inner_allocator_->free(pool_buf);
          }
        } else {
          new_pool->~ObHMSClientPool();
          inner_allocator_->free(pool_buf);
        }
      }
    }
  }

  return ret;
}

int ObHMSClientPoolMgr::init(const uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::ReqMemEvict, tg_id_))) {
    LOG_WARN("failed to create tg", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("failed to start tg", K(ret));
  } else if (OB_FAIL(init_mem_context(tenant_id))) {
    LOG_WARN("failed to init mem context", K(ret));
  } else {
    clear_task_.pool_mgr_ = this;
    if (OB_FAIL(TG_SCHEDULE(tg_id_, clear_task_, POOL_CLEANUP_INTERVAL_US, true))) {
      LOG_WARN("failed to schedule clear task", K(ret));
    } else if (OB_FAIL(create_pool_map(tenant_id))) {
      LOG_WARN("failed to create pool map", K(ret));
    } else {
      tenant_id_ = tenant_id;
      last_access_ts_ = ObTimeUtility::current_time();
      is_inited_ = true;
      destroyed_ = false;
    }
  }

  if (OB_FAIL(ret) && !is_inited_) {
    destroy();
  }
  return ret;
}

int ObHMSClientPoolMgr::init_mem_context(const uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_context_)) {
    lib::ContextParam param;
    param.set_mem_attr(tenant_id, HMS_CLIENT_POOL_MGR_ALLOCATOR, ObCtxIds::DEFAULT_CTX_ID)
        .set_properties(lib::ALLOC_THREAD_SAFE)
        .set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE);
    if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("failed to create memory entity", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create memory entity", K(ret));
    } else {
      inner_allocator_ = &mem_context_->get_malloc_allocator();
    }
  }
  return ret;
}

void ObHMSClientPoolMgr::destroy()
{
  if (OB_LIKELY(!destroyed_)) {
    is_inited_ = false;
    destroyed_ = true;
    TG_DESTROY(tg_id_);
    SpinWLockGuard guard(pool_mgr_lock_);
    if (pool_map_.created()) {
      destroy_pool_map();
    }
    if (OB_NOT_NULL(inner_allocator_)) {
      inner_allocator_->reset();
      inner_allocator_ = nullptr;
    }
    if (OB_NOT_NULL(mem_context_)) {
      DESTROY_CONTEXT(mem_context_);
      mem_context_ = nullptr;
    }
  }
}

int ObHMSClientPoolMgr::create_pool_map(const uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(inner_allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner allocator is not init", K(ret));
  } else {
    SpinWLockGuard guard(pool_mgr_lock_);
    if (OB_LIKELY(!pool_map_.created())) {
      const ObMemAttr attr(tenant_id, HMS_CLIENT_POOL_ALLOCATOR);
      if (OB_FAIL(pool_map_.create(MAX_HMS_CATALOG_CONNECTIONS, attr))) {
        LOG_WARN("failed to create pool map", K(ret));
      } else {
        LOG_TRACE("succeed to create pool map", K(ret), K(tenant_id), K(pool_map_.size()));
      }
    }
  }
  return ret;
}

void ObHMSClientPoolMgr::destroy_pool_map()
{
  if (pool_map_.created() && OB_NOT_NULL(inner_allocator_)) {
    ObHMSClientPool *pool = nullptr;
    hash::ObHashMap<ObHMSClientPoolKey, ObHMSClientPool *>::iterator iter = pool_map_.begin();
    while (iter != pool_map_.end()) {
      if (OB_NOT_NULL(iter->second)) {
        pool = iter->second;
        pool->~ObHMSClientPool();
        inner_allocator_->free(pool);
        iter->second = nullptr;
      }
      iter++;
    }
    pool_map_.destroy();
  }
}

/* --------------------- end of ObHMSClientPoolMgr ---------------------*/

} // namespace share
} // namespace oceanbase