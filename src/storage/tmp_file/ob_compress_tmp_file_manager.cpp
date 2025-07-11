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

#define USING_LOG_PREFIX STORAGE
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "storage/tmp_file/ob_compress_tmp_file_manager.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace tmp_file
{

ObTenantCompTmpFileManagerWithMTLSwitch &ObTenantCompTmpFileManagerWithMTLSwitch::get_instance()
{
  static ObTenantCompTmpFileManagerWithMTLSwitch mgr;

  return mgr;
}

int ObTenantCompTmpFileManagerWithMTLSwitch::alloc_dir(const uint64_t tenant_id, int64_t &dir_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    if (tenant_id != MTL_ID()) {
      if (OB_FAIL(guard.switch_to(tenant_id))) {
        LOG_WARN("fail to switch tenant", KR(ret), K(tenant_id));
      }
    }
    ObTenantCompressTmpFileManager* comp_tmp_file_mgr = MTL(ObTenantCompressTmpFileManager*);
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(comp_tmp_file_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("compress tmp file manager is null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(comp_tmp_file_mgr->alloc_dir(tenant_id, dir_id))) {
      LOG_WARN("fail to alloc dir", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObTenantCompTmpFileManagerWithMTLSwitch::open(const uint64_t tenant_id,
                                                  int64_t &fd,
                                                  const int64_t dir_id,
                                                  const char* const label,
                                                  const ObCompressorType comptype,
                                                  const int64_t comp_unit_size,
                                                  const int64_t dop)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    if (tenant_id != MTL_ID()) {
      if (OB_FAIL(guard.switch_to(tenant_id))) {
        LOG_WARN("fail to switch tenant", KR(ret), K(tenant_id));
      }
    }
    ObTenantCompressTmpFileManager* comp_tmp_file_mgr = MTL(ObTenantCompressTmpFileManager*);
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(comp_tmp_file_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("compress tmp file manager is null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(comp_tmp_file_mgr->open(tenant_id, fd, dir_id, label, comptype, comp_unit_size, dop))) {
      LOG_WARN("fail to open", KR(ret), K(tenant_id), K(fd), K(dir_id), KP(label), K(comptype), K(comp_unit_size), K(dop));
    }
  }
  return ret;
}

int ObTenantCompTmpFileManagerWithMTLSwitch::remove(const uint64_t tenant_id, const int64_t fd)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    if (tenant_id != MTL_ID()) {
      if (OB_FAIL(guard.switch_to(tenant_id))) {
        LOG_WARN("fail to switch tenant", KR(ret), K(tenant_id));
      }
    }
    ObTenantCompressTmpFileManager* comp_tmp_file_mgr = MTL(ObTenantCompressTmpFileManager*);
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(comp_tmp_file_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("compress tmp file manager is null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(comp_tmp_file_mgr->remove(tenant_id, fd))) {
      LOG_WARN("fail to remove", KR(ret), K(tenant_id), K(fd));
    }
  }
  return ret;
}

// int ObTenantCompTmpFileManagerWithMTLSwitch::aio_read(
//     const uint64_t tenant_id,
//     const ObTmpFileIOInfo &io_info,
//     ObCompTmpFileIOHandle &io_handle)
// {
//   int ret = OB_SUCCESS;
//   if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
//     ret = OB_INVALID_ARGUMENT;
//     LOG_WARN("invalid argument", KR(ret), K(tenant_id));
//   } else {
//     MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
//     if (tenant_id != MTL_ID()) {
//       if (OB_FAIL(guard.switch_to(tenant_id))) {
//         LOG_WARN("fail to switch tenant", KR(ret), K(tenant_id));
//       }
//     }
//     ObTenantCompressTmpFileManager* comp_tmp_file_mgr = MTL(ObTenantCompressTmpFileManager*);
//     if (OB_FAIL(ret)) {
//     } else if (OB_ISNULL(comp_tmp_file_mgr)) {
//       ret = OB_ERR_UNEXPECTED;
//       LOG_ERROR("compress tmp file manager is null", KR(ret), K(tenant_id));
//     } else if (OB_FAIL(comp_tmp_file_mgr->aio_read(tenant_id, io_info, io_handle))) {
//       LOG_WARN("fail to aio read", KR(ret), K(tenant_id), K(io_info));
//     }
//   }
//   return ret;
// }

int ObTenantCompTmpFileManagerWithMTLSwitch::read(
    const uint64_t tenant_id,
    const ObTmpFileIOInfo &io_info,
    ObCompTmpFileIOHandle &io_handle,
    ObCompTmpFileBunchHandle* file_bunch_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    if (tenant_id != MTL_ID()) {
      if (OB_FAIL(guard.switch_to(tenant_id))) {
        LOG_WARN("fail to switch tenant", KR(ret), K(tenant_id));
      }
    }
    ObTenantCompressTmpFileManager* comp_tmp_file_mgr = MTL(ObTenantCompressTmpFileManager*);
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(comp_tmp_file_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("compress tmp file manager is null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(comp_tmp_file_mgr->read(tenant_id, io_info, io_handle, file_bunch_handle))) {
      LOG_WARN("fail to read", KR(ret), K(tenant_id), K(io_info));
    }
  }
  return ret;
}

int ObTenantCompTmpFileManagerWithMTLSwitch::write(
    const uint64_t tenant_id,
    const ObTmpFileIOInfo &io_info,
    ObCompTmpFileBunchHandle* file_bunch_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    if (tenant_id != MTL_ID()) {
      if (OB_FAIL(guard.switch_to(tenant_id))) {
        LOG_WARN("fail to switch tenant", KR(ret), K(tenant_id));
      }
    }
    ObTenantCompressTmpFileManager* comp_tmp_file_mgr = MTL(ObTenantCompressTmpFileManager*);
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(comp_tmp_file_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tmp file manager is null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(comp_tmp_file_mgr->write(tenant_id, io_info, file_bunch_handle))) {
      LOG_WARN("fail to write", KR(ret), K(tenant_id), K(io_info));
    }
  }
  return ret;
}

int ObTenantCompTmpFileManagerWithMTLSwitch::seal(
    const uint64_t tenant_id,
    const int64_t fd,
    ObCompTmpFileBunchHandle* file_bunch_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    if (tenant_id != MTL_ID()) {
      if (OB_FAIL(guard.switch_to(tenant_id))) {
        LOG_WARN("fail to switch tenant", KR(ret), K(tenant_id));
      }
    }
    ObTenantCompressTmpFileManager* comp_tmp_file_mgr = MTL(ObTenantCompressTmpFileManager*);
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(comp_tmp_file_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("compress tmp file manager is null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(comp_tmp_file_mgr->seal(tenant_id, fd, file_bunch_handle))) {
      LOG_WARN("fail to seal", KR(ret), K(tenant_id), K(fd));
    }
  }
  return ret;
}

int ObTenantCompTmpFileManagerWithMTLSwitch::get_tmp_file_size(
    const uint64_t tenant_id,
    const int64_t fd,
    int64_t &file_size,
    ObCompTmpFileBunchHandle* file_bunch_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    if (tenant_id != MTL_ID()) {
      if (OB_FAIL(guard.switch_to(tenant_id))) {
        LOG_WARN("fail to switch tenant", KR(ret), K(tenant_id));
      }
    }
    ObTenantCompressTmpFileManager* comp_tmp_file_mgr = MTL(ObTenantCompressTmpFileManager*);
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(comp_tmp_file_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("compress tmp file manager is null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(comp_tmp_file_mgr->get_tmp_file_size(tenant_id, fd, file_size, file_bunch_handle))) {
      LOG_WARN("fail to get tmp file size", KR(ret), K(tenant_id), K(fd));
    }
  }
  return ret;
}

int ObTenantCompTmpFileManagerWithMTLSwitch::get_tmp_file_bunch(const uint64_t tenant_id,
                                                                const int64_t fd,
                                                                ObCompTmpFileBunchHandle &comp_file_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    if (tenant_id != MTL_ID()) {
      if (OB_FAIL(guard.switch_to(tenant_id))) {
        LOG_WARN("fail to switch tenant", KR(ret), K(tenant_id));
      }
    }
    ObTenantCompressTmpFileManager* comp_tmp_file_mgr = MTL(ObTenantCompressTmpFileManager*);
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(comp_tmp_file_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("compress tmp file manager is null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(comp_tmp_file_mgr->get_tmp_file_bunch(fd, comp_file_handle))) {
      LOG_WARN("fail to get tmp file size", KR(ret), K(tenant_id), K(fd));
    }
  }
  return ret;
}

int ObTenantCompressTmpFileManager::mtl_init(ObTenantCompressTmpFileManager *&manager)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(manager)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to mtl init compress tmp file manager, null pointer argument", KR(ret), KP(manager));
  } else if (OB_FAIL(manager->init())) {
    LOG_WARN("fail to init ObTenantCompressTmpFileManager", KR(ret));
  }
  return ret;
}

int ObTenantCompressTmpFileManager::init()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();

  const int64_t comp_file_bunches_hash_dir_size = (1ll<<17); // 128KB
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantCompressTmpFileManager init twice", KR(ret), K(is_inited_));
  } else if (OB_FAIL(comp_tmp_file_allocator_.init(common::OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                   ObModIds::OB_TMP_FILE_MANAGER, tenant_id,
                                                   INT64_MAX))) {
    LOG_WARN("fail to init compress tmp file allocator", KR(ret), K(tenant_id));
  } else if (OB_FAIL(compress_buf_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                                  OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                  ObMemAttr(tenant_id, "TmpFileCompress", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init compress or decompress buf allocator", KR(ret), K(tenant_id));
  } else if (OB_FAIL(comp_file_bunch_allocator_.init(common::OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                     ObModIds::OB_TMP_FILE_MANAGER, tenant_id,
                                                     INT64_MAX))) {
    LOG_WARN("fail to init compress tmp file bunch allocator", KR(ret), K(tenant_id));
  } else if (OB_FAIL(compressible_files_.init("CompTmpFileMap", tenant_id))) {
    LOG_WARN("fail to init compress tmp files map", KR(ret), K(tenant_id));
  } else if (OB_FAIL(comp_file_bunches_.init(OB_MALLOC_NORMAL_BLOCK_SIZE, /* Small segment. */
             OB_MALLOC_BIG_BLOCK_SIZE, /* Large segment. */
             comp_file_bunches_hash_dir_size, /* Dir size, small when init, expand * 2. */
             ObMemAttr(tenant_id, "CompBunchMap", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init compress tmp file bunches map", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObTenantCompressTmpFileManager::destroy()
{
  uint64_t tenant_id = MTL_ID();
  seal_controller_.destroy();
  if (is_inited_) {
    int64_t curr_file_cnt = compressible_files_.count();
    int64_t curr_file_bunch_cnt = comp_file_bunches_.count();
    if (OB_UNLIKELY(curr_file_cnt > 0)) {
      int ret = OB_SUCCESS;
      ObCompressTmpFileHandle handle;
      CompTmpFileMap::BlurredIterator iter(compressible_files_);
      while (OB_SUCC(ret)) {
        handle.reset();
        ObTmpFileKey unused_key(ObTmpFileGlobal::INVALID_TMP_FILE_FD);
        if (OB_FAIL(iter.next(unused_key, handle))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next compress tmp file", KR(ret), K(tenant_id));
          }
        } else {
          LOG_ERROR("the compress tmp file has not been removed when compress tmp file mgr is destroying", KPC(handle.get()));
        }
      } // end while

      int64_t new_file_cnt = compressible_files_.count();
      if (OB_UNLIKELY(new_file_cnt != curr_file_cnt)) {
        LOG_ERROR("there are some operation for tmp files when compress tmp file mgr is destroying", K(tenant_id), K(curr_file_cnt));
      }
    }
    if (OB_UNLIKELY(curr_file_bunch_cnt > 0)) {
      int ret = OB_SUCCESS;
      ObCompTmpFileBunchHandle file_bunch_handle;
      CompTmpFileBunchMap::BlurredIterator iter(comp_file_bunches_);
      while (OB_SUCC(ret)) {
        file_bunch_handle.reset();
        ObTmpFileKey unused_key(ObTmpFileGlobal::INVALID_TMP_FILE_FD);
        if (OB_FAIL(iter.next(unused_key, file_bunch_handle))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next compress tmp file bunch", KR(ret), K(tenant_id));
          }
        } else {
          LOG_ERROR("the compress tmp file bunch has not been removed when compress tmp file mgr is destroying", KPC(file_bunch_handle.get()));
        }
      } // end while

      int64_t new_bunch_cnt = comp_file_bunches_.count();
      if (OB_UNLIKELY(new_bunch_cnt != curr_file_bunch_cnt)) {
        LOG_ERROR("there are some operation for tmp file bunches when compress tmp file mgr is destroying", K(tenant_id), K(curr_file_cnt));
      }
    }
    compressible_files_.destroy();
    comp_file_bunches_.destroy();
    comp_tmp_file_allocator_.reset();
    compress_buf_allocator_.reset();
    comp_file_bunch_allocator_.reset();
  }
  LOG_INFO("ObTenantCompressTmpFileManager destroy", K(tenant_id), KP(this));
}

int ObTenantCompressTmpFileManager::alloc_dir(
    const uint64_t tenant_id,
    int64_t &dir_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(tenant_id, dir_id))) {
    LOG_WARN("fail to alloc dir", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTenantCompressTmpFileManager::cal_tenant_tmp_file_bunch_parallel_num_(
    const uint64_t tenant_id,
    const int64_t dop,
    ObCompressTmpFileBunch *comp_tmp_file_bunch,
    int64_t& parallel_num)
{
  int ret = OB_SUCCESS;
  int64_t suggested_max_tmp_file_num = 0;

  if (OB_UNLIKELY(dop <= 0 || NULL == comp_tmp_file_bunch)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input argument", KR(ret), K(dop), KP(comp_tmp_file_bunch));
  } else {
    const int64_t write_cache_size_expected_reside_in_memory_per_comp_file =
       (64 * 1024) * 2 /* buffer tmp file */ + 16 * 1024 /* compressed tmp file */;

    if (OB_FAIL(MTL(ObTenantTmpFileManager*)->get_suggested_max_tmp_file_num(suggested_max_tmp_file_num,
            write_cache_size_expected_reside_in_memory_per_comp_file))) {
      LOG_WARN("fail to get_suggested_max_tmp_file_num", KR(ret));
    } else if (OB_FAIL(seal_controller_.add_bunch_to_list(tenant_id, dop, suggested_max_tmp_file_num, comp_tmp_file_bunch, parallel_num))) {
      LOG_WARN("fail to add_bunch_to_list", KR(ret), K(tenant_id), K(dop), K(suggested_max_tmp_file_num), KP(comp_tmp_file_bunch));
    }
  }
  return ret;
}

int ObTenantCompressTmpFileManager::open(
    const uint64_t tenant_id,
    int64_t &fd,
    const int64_t dir_id,
    const char* const label,
    const ObCompressorType comptype,
    const int64_t comp_unit_size,
    const int64_t dop)
{
  int ret = OB_SUCCESS;
  fd = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  void *buf = nullptr;
  ObCompressTmpFileBunch *comp_tmp_file_bunch = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCompressTmpFileManager has not been inited", KR(ret), K(tenant_id));
  } else {
    int64_t compressible_tmp_file_cnt = 1;
    bool cal_success = false;
    if (OB_ISNULL(buf = comp_file_bunch_allocator_.alloc(sizeof(ObCompressTmpFileBunch),
                                                         lib::ObMemAttr(tenant_id, "CompFileBunch")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for compressible tmp file bunch",
              KR(ret), K(tenant_id), K(sizeof(ObCompressTmpFileBunch)));
    } else if (FALSE_IT(comp_tmp_file_bunch = new (buf) ObCompressTmpFileBunch())) {
    } else if (OB_FAIL(cal_tenant_tmp_file_bunch_parallel_num_(tenant_id, dop, comp_tmp_file_bunch, compressible_tmp_file_cnt))) {
      LOG_WARN("fail to cal_tenant_tmp_file_bunch_parallel_num_", KR(ret), K(tenant_id), K(dop), KP(comp_tmp_file_bunch));
    } else {
      cal_success = true;
    }
    if (FAILEDx(comp_tmp_file_bunch->init(tenant_id, fd, dir_id, label, comptype, comp_unit_size, compressible_tmp_file_cnt,
                                      &comp_tmp_file_allocator_, &compress_buf_allocator_, &compressible_files_))) {
      LOG_WARN("fail to init compress tmp file bunch", KR(ret), K(tenant_id), K(dir_id), KP(label), K(comptype), K(comp_unit_size),
                  K(compressible_tmp_file_cnt), KP(&comp_tmp_file_allocator_), KP(&compress_buf_allocator_), KP(&compressible_files_));
    } else if (OB_FAIL(comp_file_bunches_.insert(ObTmpFileKey(fd), comp_tmp_file_bunch))) {
      LOG_WARN("fail to set refactored to compress tmp file bunch map", KR(ret), K(fd), KP(comp_tmp_file_bunch));
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(comp_tmp_file_bunch)) {
      if (cal_success) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(seal_controller_.remove_bunch_from_list(comp_tmp_file_bunch, compressible_tmp_file_cnt))) {
          LOG_WARN("fail to remove bunch from list", KR(ret), KP(comp_tmp_file_bunch), K(compressible_tmp_file_cnt));
        }
      }
      comp_tmp_file_bunch->~ObCompressTmpFileBunch();
      comp_file_bunch_allocator_.free(comp_tmp_file_bunch);
      comp_tmp_file_bunch = nullptr;
    }
  }

  LOG_INFO("open a compress tmp file bunch over", KR(ret), K(fd), K(dir_id), KP(comp_tmp_file_bunch), K(lbt()));
  return ret;
}

int ObTenantCompressTmpFileManager::remove(
    const uint64_t tenant_id,
    const int64_t fd)
{
  int ret = OB_SUCCESS;
  ObCompTmpFileBunchHandle file_bunch_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id));
  } else if (OB_FAIL(comp_file_bunches_.erase(ObTmpFileKey(fd), file_bunch_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("erase non-exist compress tmp file bunch", K(fd), K(lbt()));
    } else {
      LOG_WARN("fail to erase compress tmp file bunch", KR(ret), K(fd), K(lbt()));
    }
  } else if (OB_ISNULL(file_bunch_handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid compressible tmp file bunch pointer", KR(ret), K(fd), KP(file_bunch_handle.get()));
  } else {
    int64_t unsealed_file_cnt = 0;
    file_bunch_handle.get()->set_deleting(unsealed_file_cnt);
    if (OB_FAIL(seal_controller_.remove_bunch_from_list(file_bunch_handle.get(), unsealed_file_cnt))) {
      LOG_WARN("fail to remove bunch from list", KR(ret), KP(file_bunch_handle.get()), K(unsealed_file_cnt));
    }
  }
  if (OB_SUCC(ret)) {
    ObSEArray<int64_t, 128> &comp_fds = file_bunch_handle.get()->get_fds();
    ObCompressTmpFileHandle comp_tmp_file_handle;
    int64_t comp_fd = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
    ARRAY_FOREACH_N(comp_fds, i, cnt) {
      comp_tmp_file_handle.reset();
      comp_fd = comp_fds.at(i);
      if (OB_FAIL(compressible_files_.get(ObTmpFileKey(comp_fd), comp_tmp_file_handle))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("erase non-exist compress tmp file", K(comp_fd), K(lbt()));
        } else {
          LOG_WARN("fail to erase compress tmp file", KR(ret), K(comp_fd), K(lbt()));
        }
      } else if (OB_ISNULL(comp_tmp_file_handle.get())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", KR(ret), KP(comp_tmp_file_handle.get()), K(comp_fd));
      } else if (OB_FAIL(comp_tmp_file_handle.get()->set_deleting())) {
        LOG_WARN("fail to set deleting", KR(ret), KP(comp_tmp_file_handle.get()), K(tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t LOG_WARN_TIMEOUT = 10 * 1000 * 1000;
      const int64_t LOG_ERROR_TIMEOUT = 60 * 1000 * 1000;
      ObCompressTmpFileBunch *comp_tmp_file_bunch = file_bunch_handle.get();
      int64_t start_remove_ts = ObTimeUtility::current_time();
      while(!comp_tmp_file_bunch->can_remove())
      {
        int64_t current_ts = ObTimeUtility::current_time();
        if (start_remove_ts + LOG_ERROR_TIMEOUT < current_ts) {
          LOG_ERROR("wait thread release compress file bunch reference too long",
              K(start_remove_ts), K(current_ts), KP(comp_tmp_file_bunch), KPC(comp_tmp_file_bunch), K(lbt()));
          sleep(10); // 10s
        } else if (start_remove_ts + LOG_WARN_TIMEOUT < current_ts) {
          LOG_WARN("wait thread release compress file bunch reference too long",
              K(start_remove_ts), K(current_ts), KP(comp_tmp_file_bunch), KPC(comp_tmp_file_bunch), K(lbt()));
          sleep(2); // 2s
        } else {
          ob_usleep(100 * 1000); // 100ms
        }
      }
      ARRAY_FOREACH_N(comp_fds, i, cnt) {
        comp_tmp_file_handle.reset();
        comp_fd = comp_fds.at(i);
        if (OB_FAIL(compressible_files_.erase(ObTmpFileKey(comp_fd), comp_tmp_file_handle))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            LOG_INFO("erase non-exist compress tmp file", K(comp_fd), K(lbt()));
          } else {
            LOG_WARN("fail to erase compress tmp file", KR(ret), K(comp_fd), K(lbt()));
          }
        } else if (OB_ISNULL(comp_tmp_file_handle.get())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", KR(ret), KP(comp_tmp_file_handle.get()), K(comp_fd));
        } else if (OB_FAIL(comp_tmp_file_handle.get()->remove_files(tenant_id))) {
          LOG_WARN("fail to remove files", KR(ret), KP(comp_tmp_file_handle.get()), K(tenant_id));
        } else {
          ObCompressTmpFile *comp_tmp_file = comp_tmp_file_handle.get();
          int64_t start_remove_ts = ObTimeUtility::current_time();
          while(!comp_tmp_file->can_remove())
          {
            int64_t current_ts = ObTimeUtility::current_time();
            if (start_remove_ts + LOG_ERROR_TIMEOUT < current_ts) {
              LOG_ERROR("wait thread release compress file reference too long",
                  K(start_remove_ts), K(current_ts), KP(comp_tmp_file), KPC(comp_tmp_file), K(lbt()));
              sleep(10); // 10s
            } else if (start_remove_ts + LOG_WARN_TIMEOUT < current_ts) {
              LOG_WARN("wait thread release compress file reference too long",
                  K(start_remove_ts), K(current_ts), KP(comp_tmp_file), KPC(comp_tmp_file), K(lbt()));
              sleep(2); // 2s
            } else {
              ob_usleep(100 * 1000); // 100ms
            }
          }
          comp_tmp_file_handle.reset();
          comp_tmp_file_allocator_.free(comp_tmp_file);
        }
      }
      if (OB_SUCC(ret)) {
        file_bunch_handle.reset();
        comp_file_bunch_allocator_.free(comp_tmp_file_bunch);
      }
    }
  }
  return ret;
}

int ObTenantCompressTmpFileManager::read(
    const uint64_t tenant_id,
    const ObTmpFileIOInfo &io_info,
    ObCompTmpFileIOHandle &io_handle,
    ObCompTmpFileBunchHandle *file_bunch_handle_hint)
{
  int ret = OB_SUCCESS;
  ObCompTmpFileBunchHandle file_bunch_handle;
  ObCompTmpFileBunchHandle* file_bunch_handle_ptr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCompressTmpFileManager has not been inited", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_tmp_file_bunch_(
          io_info.fd_, file_bunch_handle_hint, file_bunch_handle_ptr, file_bunch_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("compressible tmp file bunch does not exist", KR(ret), K(io_info));
    } else {
      LOG_WARN("fail to get compressible tmp file bunch", KR(ret), K(io_info));
    }
  } else if (OB_FAIL(file_bunch_handle_ptr->get()->read(tenant_id, io_info, io_handle))) {
    LOG_WARN("fail to read", KR(ret), K(tenant_id), K(io_info));
  }
  return ret;
}

int ObTenantCompressTmpFileManager::write(
    const uint64_t tenant_id,
    const ObTmpFileIOInfo &io_info,
    ObCompTmpFileBunchHandle *file_bunch_handle_hint)
{
  int ret = OB_SUCCESS;
  ObCompTmpFileBunchHandle file_bunch_handle;
  ObCompTmpFileBunchHandle* file_bunch_handle_ptr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCompressTmpFileManager has not been inited", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_tmp_file_bunch_(
          io_info.fd_, file_bunch_handle_hint, file_bunch_handle_ptr, file_bunch_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("compressible tmp file bunch does not exist", KR(ret), K(io_info));
    } else {
      LOG_WARN("fail to get compressible tmp file bunch", KR(ret), K(io_info));
    }
  } else if (OB_FAIL(file_bunch_handle_ptr->get()->write(tenant_id, io_info))) {
    LOG_WARN("fail to write", KR(ret), K(tenant_id), K(io_info));
  }
  return ret;
}

int ObTenantCompressTmpFileManager::seal(
    const uint64_t tenant_id,
    const int64_t fd,
    ObCompTmpFileBunchHandle *file_bunch_handle_hint)
{
  int ret = OB_SUCCESS;
  ObCompTmpFileBunchHandle file_bunch_handle;
  ObCompTmpFileBunchHandle* file_bunch_handle_ptr = nullptr;
  int64_t sealed_comp_file_num = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCompressTmpFileManager has not been inited", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_tmp_file_bunch_(
          fd, file_bunch_handle_hint, file_bunch_handle_ptr, file_bunch_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("compressible tmp file bunch does not exist", KR(ret), K(fd));
    } else {
      LOG_WARN("fail to get compressible tmp file bunch", KR(ret), K(fd));
    }
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(file_bunch_handle_ptr->get()->seal(tenant_id, sealed_comp_file_num))) {
      LOG_WARN("fail to seal", KR(ret), K(fd), K(tenant_id));
    } else if (sealed_comp_file_num < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fatal error! unexpected sealed_comp_file_num", KR(ret), K(fd), K(sealed_comp_file_num));
    }
    if (OB_TMP_FAIL(seal_controller_.remove_bunch_from_list(file_bunch_handle_ptr->get(), sealed_comp_file_num))) {
      LOG_WARN("fail to remove bunch from list", KR(ret), KR(tmp_ret), K(fd), KP(file_bunch_handle_ptr->get()), K(sealed_comp_file_num));
    }
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }
  return ret;
}

void ObTenantCompressTmpFileManager::FileSealController::destroy()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(unsealed_bunch_list_.get_size() > 0 || total_unsealed_comp_file_cnt_ > 0)) {
    LOG_ERROR("there are some unsealed tmp file bunches when compress tmp file mgr is destroying",
                  K(total_unsealed_comp_file_cnt_), KP(cur_file_bunch_ptr_), K(unsealed_bunch_list_));
  } else {
    unsealed_bunch_list_.reset();
  }
}

int ObTenantCompressTmpFileManager::FileSealController::add_bunch_to_list(
    const uint64_t tenant_id,
    const int64_t dop,
    const int64_t suggested_max_tmp_file_num,
    ObCompressTmpFileBunch *file_bunch_ptr,
    int64_t &result_file_num)
{
  int ret = OB_SUCCESS;
  int64_t max_unsealed_file_num_each_bunch = 0;
  int64_t total_need_seal_file_num = 0;
  int64_t actual_seal_file_num = 0;
  ObSpinLockGuard lock_guard(lock_);
  if (total_unsealed_comp_file_cnt_ + dop > suggested_max_tmp_file_num) {
    max_unsealed_file_num_each_bunch = MAX(1, suggested_max_tmp_file_num / (unsealed_bunch_list_.get_size() + 1));
    const int64_t new_bunch_file_num = MIN(max_unsealed_file_num_each_bunch, dop);
    total_need_seal_file_num = total_unsealed_comp_file_cnt_ + new_bunch_file_num - suggested_max_tmp_file_num;
    if (total_need_seal_file_num > 0) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(seal_partial_comp_tmp_files_(tenant_id, max_unsealed_file_num_each_bunch, total_need_seal_file_num, actual_seal_file_num))) {
        LOG_WARN("fail to seal selected comp tmp files without lock", KR(tmp_ret), K(tenant_id), K(max_unsealed_file_num_each_bunch), K(total_need_seal_file_num));
      }
      if (0 < actual_seal_file_num) {
        //If it fails, we also do this to avoid problems with statistics.
        //  In the future, we will recalculate statistics if err occured.
        total_unsealed_comp_file_cnt_ -= actual_seal_file_num;
      }
    }
    if (OB_SUCC(ret)) {
      result_file_num = new_bunch_file_num;
    }
  } else {
    result_file_num = dop;
  }
  if (OB_SUCC(ret)) {
    if (!unsealed_bunch_list_.add_last(file_bunch_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to add last to unsealed_bunch_list_", KR(ret), KP(file_bunch_ptr));
    } else {
      file_bunch_ptr->inc_ref_cnt();
      total_unsealed_comp_file_cnt_ += result_file_num;
    }
  }
  LOG_INFO("add comp file bunch to list", KR(ret), K(total_unsealed_comp_file_cnt_), KP(cur_file_bunch_ptr_), K(unsealed_bunch_list_.get_size()),
    K(suggested_max_tmp_file_num), K(max_unsealed_file_num_each_bunch), K(dop), K(result_file_num), K(total_need_seal_file_num), K(actual_seal_file_num));
  return ret;
}


int ObTenantCompressTmpFileManager::FileSealController::remove_bunch_from_list(
    ObCompressTmpFileBunch *file_bunch_ptr,
    const int64_t file_num)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file_bunch_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("file_bunch_ptr is nullptr", KR(ret), KP(file_bunch_ptr));
  } else {
    ObSpinLockGuard lock_guard(lock_);
    if (OB_ISNULL(file_bunch_ptr->get_prev()) || OB_ISNULL(file_bunch_ptr->get_next())) {
      LOG_INFO("file bunch ptr has already been removed", KR(ret), KP(file_bunch_ptr), K(file_num));
    } else if (OB_ISNULL(unsealed_bunch_list_.remove(file_bunch_ptr))) {
      //should not happen
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Fail to remove file_bunch_ptr from unsealed_bunch_list_", KR(ret), KP(file_bunch_ptr), K(unsealed_bunch_list_));
    } else {
      const ObCompressTmpFileBunch *const_file_bunch_ptr = file_bunch_ptr;
      if (const_file_bunch_ptr == cur_file_bunch_ptr_) {
        cur_file_bunch_ptr_ = nullptr;
      }
      file_bunch_ptr->dec_ref_cnt();
    }
    if (0 < file_num) {
      total_unsealed_comp_file_cnt_ -= file_num;
    }
  }
  LOG_INFO("remove comp file bunch from list", KR(ret), K(file_num), KP(file_bunch_ptr), KP(cur_file_bunch_ptr_), K(total_unsealed_comp_file_cnt_), K(unsealed_bunch_list_.get_size()));
  return ret;
}

int ObTenantCompressTmpFileManager::FileSealController::seal_partial_comp_tmp_files_(
    const uint64_t tenant_id,
    const int64_t max_unsealed_file_num_each_bunch,
    const int64_t total_need_seal_file_num,
    int64_t &actual_seal_file_num)
{
  int ret = OB_SUCCESS;
  actual_seal_file_num = 0;
  int64_t sealed_file_num = 0;
  ObCompressTmpFileBunch *it = cur_file_bunch_ptr_;
  if (NULL != it) {
    bool is_normal_bunch = false;
    int64_t unsealed_file_num = 0;
    it->get_unsealed_file_cnt(is_normal_bunch, unsealed_file_num);
    if (!is_normal_bunch || unsealed_file_num > max_unsealed_file_num_each_bunch) {
      it = unsealed_bunch_list_.get_first();
    } else {
      //we can sure that none of the previous bunches need a seal operation.
      it = it->get_next();
    }
  } else {
    it = unsealed_bunch_list_.get_first();
  }
  while (OB_SUCC(ret)
         && it != unsealed_bunch_list_.get_header()
         && actual_seal_file_num < total_need_seal_file_num) {
    sealed_file_num = 0;
    bool has_set = false;
    if (OB_ISNULL(it)) {
      //should not happen
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null ptr in unsealed_bunch_list_", KR(ret), KP(it), K(unsealed_bunch_list_));
    } else if (OB_FAIL(it->seal_partial_comp_tmp_files(tenant_id, max_unsealed_file_num_each_bunch, sealed_file_num))) {
      LOG_WARN("Fail to seal partial comp tmp files", KR(ret), K(tenant_id), K(max_unsealed_file_num_each_bunch));
    } else if (OB_UNLIKELY(sealed_file_num < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected sealed_file_num", KR(ret), KP(it), K(sealed_file_num));
    } else {
      cur_file_bunch_ptr_ = it;
      it = it->get_next();
    }
    if (sealed_file_num > 0) {
      actual_seal_file_num += sealed_file_num;
    }
  }
  return ret;
}

int ObTenantCompressTmpFileManager::get_tmp_file_size(
    const uint64_t tenant_id,
    const int64_t fd,
    int64_t &file_size,
    ObCompTmpFileBunchHandle* file_bunch_handle_hint)
{
  int ret = OB_SUCCESS;
  ObCompTmpFileBunchHandle file_bunch_handle;
  ObCompTmpFileBunchHandle* file_bunch_handle_ptr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCompressTmpFileManager has not been inited", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_tmp_file_bunch_(fd, file_bunch_handle_hint, file_bunch_handle_ptr, file_bunch_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("compressible tmp file bunch does not exist", KR(ret), K(fd));
    } else {
      LOG_WARN("fail to get compressible tmp file bunch", KR(ret), K(fd));
    }
  } else if (OB_FAIL(file_bunch_handle_ptr->get()->get_total_file_size(file_size))) {
    LOG_WARN("fail to get total file size", KR(ret), K(fd));
  }
  return ret;
}

int ObTenantCompressTmpFileManager::get_tmp_file(
    const int64_t fd,
    ObCompressTmpFileHandle &comp_file_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantCompTmpFileManager has not been inited", KR(ret));
  } else if (OB_FAIL(compressible_files_.get(ObTmpFileKey(fd), comp_file_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("compressible tmp file does not exist", KR(ret), K(fd));
    } else {
      LOG_WARN("fail to get compressible tmp file", KR(ret), K(fd));
    }
  } else if (OB_ISNULL(comp_file_handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid compressible tmp file pointer", KR(ret), K(fd), KP(comp_file_handle.get()));
  }
  return ret;
}

int ObTenantCompressTmpFileManager::get_tmp_file_bunch(
    const int64_t fd,
    ObCompTmpFileBunchHandle &comp_file_bunch_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantCompTmpFileManager has not been inited", KR(ret));
  } else if (OB_FAIL(comp_file_bunches_.get(ObTmpFileKey(fd), comp_file_bunch_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("compressible tmp file bunch does not exist", KR(ret), K(fd));
    } else {
      LOG_WARN("fail to get compressible tmp file bunch", KR(ret), K(fd));
    }
  } else if (OB_ISNULL(comp_file_bunch_handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid compressible tmp file bunch pointer", KR(ret), K(fd), KP(comp_file_bunch_handle.get()));
  }
  return ret;
}

int ObTenantCompressTmpFileManager::get_tmp_file_bunch_(
    const int64_t fd,
    ObCompTmpFileBunchHandle* file_bunch_handle_hint,
    ObCompTmpFileBunchHandle*& file_bunch_handle_ptr,
    ObCompTmpFileBunchHandle &file_bunch_handle)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(fd == ObTmpFileGlobal::INVALID_TMP_FILE_FD)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd));
  }

  if (OB_SUCC(ret)) {
    if (file_bunch_handle_hint != nullptr) {
      if (!file_bunch_handle_hint->is_inited() || OB_ISNULL(file_bunch_handle_hint->get())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(fd), KPC(file_bunch_handle_hint));
      } else if (file_bunch_handle_hint->get()->get_fd() != fd) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument",
            KR(ret), K(fd), KPC(file_bunch_handle_hint), KPC(file_bunch_handle_hint->get()));
      } else {
        file_bunch_handle_ptr = file_bunch_handle_hint;
      }
    } else {
      if (OB_FAIL(comp_file_bunches_.get(ObTmpFileKey(fd), file_bunch_handle))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_WARN("tmp file bunch does not exist", KR(ret), K(fd));
        } else {
          LOG_WARN("fail to get tmp file bunch", KR(ret), K(fd));
        }
      } else {
        file_bunch_handle_ptr = &file_bunch_handle;
      }
    }
  }

  if (OB_SUCC(ret) && (OB_ISNULL(file_bunch_handle_ptr) || OB_ISNULL(file_bunch_handle_ptr->get()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid file bunch pointer",
        KR(ret), K(fd), KP(file_bunch_handle_ptr), KP(file_bunch_handle_hint));
  }

  return ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
