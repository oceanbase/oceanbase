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
#define USING_LOG_PREFIX STORAGE

#include "ob_tenant_seq_generator.h"
#include "storage/meta_store/ob_tenant_storage_meta_persister.h"
#include "observer/omt/ob_tenant.h"
namespace oceanbase
{
namespace storage
{

OB_SERIALIZE_MEMBER(ObTenantMonotonicIncSeqs, object_seq_, tmp_file_seq_, write_seq_);


int ObTenantSeqGenerator::init(const bool is_shared_storage, ObTenantStorageMetaPersister &persister)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", K(ret));
  } else {
    is_shared_storage_ = is_shared_storage;
    persister_ = &persister;
    if (is_shared_storage_) {
      omt::ObTenant *tenant = static_cast<omt::ObTenant*>(MTL_CTX());
      curr_seqs_ = tenant->get_super_block().preallocated_seqs_;
      preallocated_seqs_.set(
          curr_seqs_.object_seq_ + BATCH_PREALLOCATE_NUM,
          curr_seqs_.tmp_file_seq_ + BATCH_PREALLOCATE_NUM,
          curr_seqs_.write_seq_ + BATCH_PREALLOCATE_NUM);
      if (OB_FAIL(persister.update_tenant_preallocated_seqs(preallocated_seqs_))) {
        LOG_WARN("fail to update tenant prealloacated seqs", K(ret), K_(preallocated_seqs));
      } else {
        is_inited_ = true;
        LOG_INFO("succeed to init ObTenantSeqGenerator", K_(curr_seqs), K_(preallocated_seqs));
      }
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTenantSeqGenerator::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_) {
    // do nothing
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::WriteCkpt, tg_id_))) {
    LOG_WARN("fail to tg create tenant", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("TG_START failed", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, *this, TRY_PREALLOACTE_INTERVAL, true))) {
    LOG_WARN("TG_SCHEDULE failed", K(ret));
  }
  return ret;
}

void ObTenantSeqGenerator::stop()
{
  if (tg_id_ != -1) {
    TG_STOP(tg_id_);
  }
}

void ObTenantSeqGenerator::wait()
{
  if (tg_id_ != -1) {
    TG_WAIT(tg_id_);
  }
}

void ObTenantSeqGenerator::destroy()
{
  if (tg_id_ != -1) {
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
  persister_ = nullptr;
  curr_seqs_.reset();
  preallocated_seqs_.reset();
  is_inited_ = false;
}

int ObTenantSeqGenerator::get_private_object_seq(uint64_t &seq)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_shared_storage_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support for shared-nothing", K(ret));
  }
  int retry_times = 0;
  while (OB_SUCC(ret) &&
      (seq = ATOMIC_AAF(&curr_seqs_.object_seq_, 1)) > ATOMIC_LOAD(&preallocated_seqs_.object_seq_)) {
    if (retry_times < MAX_RETRY_TIME) {
      sleep(1);
      retry_times++;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to get private object seq", K(ret), K_(curr_seqs), K_(preallocated_seqs), K(retry_times));
    }
  }
  return ret;
}

int ObTenantSeqGenerator::get_tmp_file_seq(uint64_t &seq)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_shared_storage_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support for shared-nothing", K(ret));
  }
  int retry_times = 0;
  while (OB_SUCC(ret) &&
      (seq = ATOMIC_AAF(&curr_seqs_.tmp_file_seq_, 1)) > ATOMIC_LOAD(&preallocated_seqs_.tmp_file_seq_)) {
    if (retry_times < MAX_RETRY_TIME) {
      usleep(TRY_PREALLOACTE_INTERVAL);
      retry_times++;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to get tmp file seq", K(ret), K_(curr_seqs), K_(preallocated_seqs), K(retry_times));
    }
  }
  return ret;
}

int ObTenantSeqGenerator::get_write_seq(uint64_t &seq)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_shared_storage_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support for shared-nothing", K(ret));
  }
  int retry_times = 0;
  while (OB_SUCC(ret) &&
      (seq = ATOMIC_AAF(&curr_seqs_.write_seq_, 1)) > ATOMIC_LOAD(&preallocated_seqs_.write_seq_)) {
    if (retry_times < MAX_RETRY_TIME) {
      sleep(1);
      retry_times++;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to get write seq", K(ret), K_(curr_seqs), K_(preallocated_seqs), K(retry_times));
    }
  }
  return ret;
}

void ObTenantSeqGenerator::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  if (OB_FAIL(try_preallocate_())) {
    LOG_WARN("fail to try preallocate", K(ret));
  }
}

int ObTenantSeqGenerator::try_preallocate_()
{
  int ret = OB_SUCCESS;
  bool need_update = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObTenantMonotonicIncSeqs seqs;
    if (ATOMIC_LOAD(&curr_seqs_.object_seq_) >
        (ATOMIC_LOAD(&preallocated_seqs_.object_seq_) - PREALLOCATION_TRIGGER_MARGIN)) {
      need_update = true;
      seqs.object_seq_ = ATOMIC_LOAD(&preallocated_seqs_.object_seq_) + BATCH_PREALLOCATE_NUM;
    } else {
      seqs.object_seq_ = ATOMIC_LOAD(&preallocated_seqs_.object_seq_);
    }
    if (ATOMIC_LOAD(&curr_seqs_.tmp_file_seq_) >
        (ATOMIC_LOAD(&preallocated_seqs_.tmp_file_seq_) - PREALLOCATION_TRIGGER_MARGIN)) {
      need_update = true;
      seqs.tmp_file_seq_ = ATOMIC_LOAD(&preallocated_seqs_.tmp_file_seq_) + BATCH_PREALLOCATE_NUM;
    } else {
      seqs.tmp_file_seq_ = ATOMIC_LOAD(&preallocated_seqs_.tmp_file_seq_);
    }
    if (ATOMIC_LOAD(&curr_seqs_.write_seq_) >
        (ATOMIC_LOAD(&preallocated_seqs_.write_seq_) - PREALLOCATION_TRIGGER_MARGIN)) {
      need_update = true;
      seqs.write_seq_ = ATOMIC_LOAD(&preallocated_seqs_.write_seq_) + BATCH_PREALLOCATE_NUM;
    } else {
      seqs.write_seq_ = ATOMIC_LOAD(&preallocated_seqs_.write_seq_);
    }

    if (need_update) {
      if (OB_FAIL(persister_->update_tenant_preallocated_seqs(seqs))) {
        LOG_WARN("fail to update_tenant_preallocated_seqs", K(ret), K(seqs));
      } else {
        ATOMIC_STORE(&preallocated_seqs_.object_seq_, seqs.object_seq_);
        ATOMIC_STORE(&preallocated_seqs_.tmp_file_seq_, seqs.tmp_file_seq_);
        ATOMIC_STORE(&preallocated_seqs_.write_seq_, seqs.write_seq_);
        LOG_INFO("succeed to update tenant preallocated seqs", K(preallocated_seqs_), K(seqs));
      }
    }
  }
  return ret;
}


} // namespace storage
} // namespace oceanbase