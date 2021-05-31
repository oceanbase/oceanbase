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

#define USING_LOG_PREFIX SQL_EXE

#include "ob_interm_result_pool.h"
#include "ob_interm_result.h"

namespace oceanbase {
namespace sql {
using namespace common;

ObIntermResultPool* ObIntermResultPool::instance_ = NULL;

ObIntermResultPool::ObIntermResultPool() : inited_(false), allocator_(), scanner_allocator_()
{}

ObIntermResultPool::~ObIntermResultPool()
{}

void ObIntermResultPool::reset()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(allocator_.destroy())) {
    LOG_ERROR("destroy allocator failed", K(ret));
  }

  if (OB_FAIL(scanner_allocator_.destroy())) {
    LOG_ERROR("destroy allocator failed", K(ret));
  }
  inited_ = false;
}

int ObIntermResultPool::build_instance()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL != instance_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("instance is not NULL, build twice", K(ret));
  } else if (OB_ISNULL(instance_ = OB_NEW(ObIntermResultPool, ObModIds::OB_SQL_EXECUTOR))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("instance is NULL, unexpected", K(ret));
  } else if (OB_FAIL(instance_->init())) {
    OB_DELETE(ObIntermResultPool, ObModIds::OB_SQL_EXECUTOR, instance_);
    instance_ = NULL;
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to init interm result pool", K(ret));
  } else {
  }
  return ret;
}

ObIntermResultPool* ObIntermResultPool::get_instance()
{
  ObIntermResultPool* instance = NULL;
  if (OB_ISNULL(instance_) || OB_UNLIKELY(!instance_->inited_)) {
    LOG_ERROR("instance is NULL or not inited", K(instance_));
  } else {
    instance = instance_;
  }
  return instance;
}

int ObIntermResultPool::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("interm result pool init twice", K(ret));
  } else if (OB_FAIL(allocator_.init(sizeof(ObIntermResult),
                 ObModIds::OB_SQL_EXECUTOR,
                 OB_SERVER_TENANT_ID,
                 OB_MALLOC_NORMAL_BLOCK_SIZE,
                 1,
                 INTERM_RESULT_CAPACITY))) {
    LOG_WARN(
        "inter interm result allocator failed", K(ret), K(sizeof(ObIntermResult)), LITERAL_K(INTERM_RESULT_CAPACITY));
  } else if (OB_FAIL(scanner_allocator_.init(sizeof(ObScanner),
                 ObModIds::OB_SQL_EXECUTOR,
                 OB_SERVER_TENANT_ID,
                 OB_MALLOC_MIDDLE_BLOCK_SIZE,
                 1,
                 SCANNER_CAPACITY))) {
    LOG_WARN("init scanner allocator failed", K(ret), K(sizeof(ObScanner)), LITERAL_K(SCANNER_CAPACITY));
  } else {
    inited_ = true;
    LOG_INFO("initialize interm result pool", LITERAL_K(INTERM_RESULT_CAPACITY), LITERAL_K(SCANNER_CAPACITY));
  }
  return ret;
}

int ObIntermResultPool::alloc_interm_result(ObIntermResult*& interm_result)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc obj", K(ret));
  } else {
    interm_result = new (buf) ObIntermResult();
  }
  return ret;
}

int ObIntermResultPool::free_interm_result(ObIntermResult* interm_result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(interm_result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the free interm result is NULL", K(ret));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (ObIntermResult::STATE_NORMAL != interm_result->get_state()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("the state of the free interm result is not STATE_NORMAL, can not free it", K(ret));
    } else {
      interm_result->~ObIntermResult();
      allocator_.free(interm_result);
    }
  }
  return ret;
}

int ObIntermResultPool::alloc_scanner(common::ObScanner*& scanner)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(buf = scanner_allocator_.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc scanner failed", K(ret));
  } else {
    scanner = new (buf) ObScanner(ObModIds::OB_SQL_EXECUTOR_INTERM_RESULT_SCANNER);
    STATIC_ASSERT(
        SCANNER_MEM_LIMIT <= common::ObScanner::DEFAULT_MAX_SERIALIZE_SIZE, "scanner exceed max serialize size");
    scanner->set_mem_size_limit(SCANNER_MEM_LIMIT);
  }
  return ret;
}

void ObIntermResultPool::free_scanner(common::ObScanner* scanner)
{
  if (NULL != scanner && inited_) {
    scanner->~ObScanner();
    scanner_allocator_.free(scanner);
  }
}

} /* namespace sql */
} /* namespace oceanbase */
