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

#define USING_LOG_PREFIX SERVER

#include "ob_dl_queue.h"

namespace oceanbase {
namespace common {

int ObDlQueue::init(const char *label, uint64_t tenant_id,
                    int64_t root_queue_size,
                    int64_t leaf_queue_size,
                    int64_t idle_leaf_queue_num) {
  int ret = OB_SUCCESS;
  root_queue_size_ = root_queue_size;
  leaf_queue_size_ = leaf_queue_size;
  idle_leaf_queue_num_ = idle_leaf_queue_num;
  label_ = label;
  if (OB_FAIL(rq_.init(label, root_queue_size_, tenant_id))) {
    LOG_WARN("fail to init root queue", K(tenant_id));
  } else {
    rq_cur_idx_ = 0;
    start_idx_ = 0;
    end_idx_ = 0;
    tenant_id_ = tenant_id;
  }

  // construct default leaf queue
  if (OB_FAIL(ret)) {
  } else {
    for (int64_t i = 0; i < idle_leaf_queue_num_ && OB_SUCC(ret); i++) {
      if (OB_FAIL(construct_leaf_queue())) {
        LOG_WARN("fail to construct leaf queue", K(i), K(tenant_id_));
      } else {
        LOG_INFO("construct leaf queue idx succ", K(rq_.get_push_idx()), K(tenant_id));
      }
    }
  }

  return ret;
}

int ObDlQueue::prepare_alloc_queue() {
  int ret = OB_SUCCESS;
  int64_t construct_num = idle_leaf_queue_num_ -
                          (get_push_idx() - get_cur_idx());
  LOG_INFO("Construct Queue Num", K(construct_num),
      K(get_push_idx()), K(get_cur_idx()), K(get_pop_idx()));
  for (int64_t i = 0; i < construct_num && OB_SUCC(ret); i++) {
    if (OB_FAIL(construct_leaf_queue())) {
      LOG_WARN("fail to construct leaf queue", K(i), K(tenant_id_));
    }
  }
  return ret;
}

int ObDlQueue::construct_leaf_queue() {
  int ret = OB_SUCCESS;
  void** lf_queue = NULL;
  ObLeafQueue *cur_lf_queue = NULL;
  ObMemAttr mem_attr;
  mem_attr.label_ = label_;
  mem_attr.tenant_id_ = tenant_id_;
  int64_t root_push_idx = 0;
  void *buf = NULL;
  if (OB_ISNULL(buf = ob_malloc(sizeof(ObLeafQueue), mem_attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else if (OB_ISNULL(cur_lf_queue = new(buf) ObLeafQueue())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else if (OB_FAIL(cur_lf_queue->init(label_, leaf_queue_size_, tenant_id_))) {
    cur_lf_queue->destroy();
    ob_free(cur_lf_queue);
    cur_lf_queue = NULL;
    LOG_WARN("fail to init leaf queue", K(ret), K(tenant_id_));
  } else if (OB_FAIL(rq_.push_with_imme_seq(cur_lf_queue, root_push_idx))) {
    SERVER_LOG(WARN, "Failed to push leaf queue", K(ret));
    cur_lf_queue->destroy();
    ob_free(cur_lf_queue);
    cur_lf_queue = NULL;
  }

  return ret;
}

int ObDlQueue::push(void* p, int64_t& seq) {
  int ret = OB_SUCCESS;
  bool need_retry = false;
  uint64_t retry_times = 0;
  do {
    need_retry = false;
    Root_Ref tmp_ref;
    int64_t root_queue_cur_idx = ATOMIC_LOAD(&rq_cur_idx_);
    ObLeafQueue *leaf_rec = NULL;
    int64_t leaf_queue_cur_idx = INT_MIN64;
    if (OB_FAIL(get_leaf_queue(root_queue_cur_idx, leaf_rec, &tmp_ref))) {
      if (ret == OB_SIZE_OVERFLOW) {
        SERVER_LOG(WARN, "queue is full",
        K(root_queue_cur_idx), K(ret));
      } else {
        ret = OB_SUCCESS;
        retry_times++; // retry 3 times
        if (retry_times > RETRY_TIMES) {
          need_retry = false;
          ret = OB_SIZE_OVERFLOW;
        } else {
          need_retry = true;
        }
      }
    } else if (leaf_rec == NULL) {
    } else if (OB_FAIL(leaf_rec->push(p, leaf_queue_cur_idx,
                      root_queue_cur_idx, seq, leaf_queue_size_))) {
      //When the sql audit secondary slot is full
      // the push will fail and needs to be retried.
      if (root_queue_cur_idx < (rq_.get_push_idx() - 1)) {
        ret = OB_SUCCESS;
        retry_times++; // retry 3 times
        if (retry_times > RETRY_TIMES) {
          need_retry = false;
          ret = OB_SIZE_OVERFLOW;
        } else {
          need_retry = true;
        }
      }
    } else {

      // set_end_idx(seq); Performance optimization

      // Push is successfully used to control the first-level offset
      if (leaf_queue_cur_idx == leaf_queue_size_ - 1) {
        (void)ATOMIC_AAF(&rq_cur_idx_, 1);
      }
    }
    revert_leaf_queue(&tmp_ref, leaf_rec);
  } while (need_retry);

  return ret;
}

int ObDlQueue::get_leaf_queue(const int64_t idx, ObLeafQueue *&leaf_queue, Root_Ref* ref)
{
  int ret = OB_SUCCESS;
  // defense construct & get scene.
  if (OB_FAIL(rq_.is_null_leaf(idx))) {
    ret = OB_SIZE_OVERFLOW;
    SERVER_LOG(WARN, "is null leaf",
        K(idx), K(ret), K(rq_cur_idx_), K(get_pop_idx()), K(get_push_idx()));
  } else if (NULL == (leaf_queue = reinterpret_cast<ObLeafQueue*>(rq_.get(idx, ref)))) {
    ret = OB_ENTRY_NOT_EXIST;
        SERVER_LOG(WARN, "get_leaf_queue",
        K(idx), K(ret), K(rq_cur_idx_), K(get_pop_idx()), K(get_push_idx()));
  }
  return ret;
}

void ObDlQueue::revert_leaf_queue(Root_Ref* ref, ObLeafQueue *&leaf_queue) {
  if (leaf_queue != NULL) {
    rq_.revert(ref);
    leaf_queue = NULL;
  }
}


ObLeafQueue* ObDlQueue::pop() {
  ObLeafQueue* p = NULL;
  p = reinterpret_cast<ObLeafQueue*>(rq_.pop());
  return p;
}

void* ObDlQueue::get(uint64_t seq, DlRef* ref) {
  int ret = OB_SUCCESS;
  void* record = NULL;
  int64_t root_queue_cur_idx = (seq / leaf_queue_size_) % root_queue_size_;
  int64_t leaf_queue_cur_idx =  seq % leaf_queue_size_;
  ObLeafQueue *leaf_rec = NULL;
  if (OB_FAIL(get_leaf_queue(root_queue_cur_idx, leaf_rec, &ref->root_ref_))) {
    SERVER_LOG(WARN, "fail to get leaf queue pointer",
    K(root_queue_cur_idx), K(ret));
  } else if (NULL == (record = leaf_rec->get(leaf_queue_cur_idx, &ref->leaf_ref_))) {
    ret = OB_ENTRY_NOT_EXIST;
    SERVER_LOG(WARN, "fail to get record", K(root_queue_cur_idx), K(leaf_queue_cur_idx));
    revert_leaf_queue(&ref->root_ref_, leaf_rec);
  }

  return record;
}


int ObLeafQueue::push(void* p, int64_t& leaf_idx, int64_t root_idx, int64_t& seq, int64_t leaf_queue_size) {
  int ret = OB_SUCCESS;
  if (NULL == array_) {
    ret = OB_NOT_INIT;
  } else if (NULL == p) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    uint64_t push_limit = capacity_;
    uint64_t push_idx = faa_bounded(&push_, &push_limit, push_limit);
    if (push_idx < push_limit) {
      void** addr = get_addr(push_idx);
      leaf_idx = push_idx;
      // request_id
      seq = root_idx * leaf_queue_size + leaf_idx;
      while(!ATOMIC_BCAS(addr, NULL, p))
        ;
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

void ObDlQueue::revert(DlRef* ref) {
  if (NULL != ref) {
    int64_t root_queue_cur_idx = ref->root_ref_.idx_;
    ObLeafQueue* ret = NULL;
    if (NULL != rq_.get_root_array()) {
      ObLeafQueue** addr = reinterpret_cast<ObLeafQueue**>(rq_.get_addr(root_queue_cur_idx));
      ATOMIC_STORE(&ret, *addr);
      ret->revert(&ref->leaf_ref_);
      revert_leaf_queue(&ref->root_ref_, ret);
    }
  }
}


void ObDlQueue::destroy() {
  while (rq_.get_pop_idx() < rq_.get_push_idx()) {
    ObLeafQueue** addr = reinterpret_cast<ObLeafQueue**>(rq_.get_addr(rq_.get_pop_idx()));
    ObLeafQueue* second_queue = *addr;
    if (NULL != second_queue) {
      second_queue->destroy();
      ob_free(second_queue);
      second_queue = NULL;
    }
    rq_.pop();
  }
  rq_.destroy();
  rq_cur_idx_ = 0;
  start_idx_ = 0;
  end_idx_ = 0;
  tenant_id_ = 0;
  release_wait_times_ = 0;
}

int64_t ObDlQueue::get_end_idx() {
  int ret = OB_SUCCESS;
  int64_t idx = INT64_MAX;
  ObLeafQueue *leaf_rec = NULL;
  Root_Ref tmp_ref;
  int64_t leaf_idx = -1;
  if (OB_FAIL(get_leaf_queue(rq_cur_idx_, leaf_rec, &tmp_ref))) {
    // rq_cur_idx_ == push_idx_ scene
    idx = (int64_t)rq_cur_idx_ * leaf_queue_size_;
  } else {
    leaf_idx = leaf_rec->get_push_idx();
    revert_leaf_queue(&tmp_ref, leaf_rec);
    idx = (int64_t)rq_cur_idx_ * leaf_queue_size_ + leaf_idx;
  }
  return idx;
}

int64_t ObDlQueue::get_capacity() {
  return (rq_.get_push_idx() - rq_.get_pop_idx()) * leaf_queue_size_;
}

int64_t ObDlQueue::get_size_used() {
  return (rq_cur_idx_ - rq_.get_pop_idx()) * leaf_queue_size_;
}

int ObDlQueue::get_leaf_size_used(int64_t idx, int64_t &size) {
  int ret = OB_SUCCESS;
  ObLeafQueue *leaf_rec = NULL;
  Root_Ref tmp_ref;
  if (OB_FAIL(get_leaf_queue(idx, leaf_rec, &tmp_ref))) {
    LOG_WARN("fail to get second level queue pointer",K(idx));
  } else {
    size = leaf_rec->get_pop_idx();
    revert_leaf_queue(&tmp_ref, leaf_rec);
  }
  return ret;
}

int ObDlQueue::need_clean_leaf_queue(int64_t idx, bool &need_clean) {
  int ret = OB_SUCCESS;
  ObLeafQueue *leaf_rec = NULL;
  Root_Ref tmp_ref;
  if (OB_FAIL(get_leaf_queue(idx, leaf_rec, &tmp_ref))) {
    LOG_WARN("fail to get second level queue pointer",K(idx));
  } else {
    need_clean = leaf_rec->get_pop_idx() == leaf_rec->get_push_idx();
    revert_leaf_queue(&tmp_ref, leaf_rec);
  }
  return ret;
}

int ObDlQueue::release_record(int64_t release_cnt,
                              const std::function<void(void*)> &free_callback,
                              bool is_destroyed) {
  int ret = OB_SUCCESS;
  int64_t rq_release_idx = get_pop_idx();
  int64_t rq_push_idx = get_push_idx();
  if (rq_release_idx == rq_push_idx) {
    SERVER_LOG(INFO, "no need release record", K(rq_release_idx), K(rq_push_idx));
  } else {
    int64_t size = 0;
    bool need_clean = false;
    if (OB_FAIL(clear_leaf_queue(rq_release_idx, release_cnt,
        free_callback))) {
      SERVER_LOG(WARN, "fail to clear leaf queue",
          K(rq_release_idx), K(ret));
    } else if (OB_FAIL(get_leaf_size_used(rq_release_idx, size))) {
      SERVER_LOG(WARN, "fail to get second level size used",
          K(rq_release_idx), K(ret));
    } else if (is_destroyed && OB_FAIL(need_clean_leaf_queue(rq_release_idx, need_clean))) {
      SERVER_LOG(WARN, "fail to get second level size used",
          K(rq_release_idx), K(ret));
    } else {
      if (size == leaf_queue_size_ || need_clean) {
        release_wait_times_++;
        ObLeafQueue *leaf_queue = NULL;
        // delay pop for performance.
        if (release_wait_times_ >= RELEASE_WAIT_TIMES) {
          leaf_queue = pop();
          if (leaf_queue != NULL) {
            leaf_queue->destroy();
            ob_free(leaf_queue);
          }
          SERVER_LOG(INFO, "release ref is 0",
              K(rq_release_idx), K(need_clean),K(ret));
          release_wait_times_ = 0;
        } else {
          SERVER_LOG(INFO, "release ref is not 0 or release_wait_times < 3",
              K(rq_release_idx), K(ret), K(release_wait_times_), K(need_clean));
        }
      }
    }
  }
  return ret;
}

}; // end namespace common
}; // end namespace oceanbase