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

template <typename T>
int ObDecodeResourcePool::alloc(T *&item)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "decode resource pool is not inited", K(ret));
  } else {
    ObSmallObjPool<T> &obj_pool = get_pool<T>();
    if(OB_FAIL(obj_pool.alloc(item))) {
      STORAGE_LOG(WARN, "decode resource pool failed to alloc", K(ret));
    }
  }
  return ret;
}

template <typename T>
int ObDecodeResourcePool::free(T *item)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(ERROR, "decode resource pool is not inited", K(ret));
  } else if (NULL != item) {
    ObSmallObjPool<T> &obj_pool = get_pool<T>();
    if(OB_FAIL(obj_pool.free(item))) {
      STORAGE_LOG(ERROR, "decode resource pool failed to free", K(ret), KP(item));
    } else {
      item = NULL;
    }
  }
  return ret;
}
////////////////////////// decoder pool ///////////////////////////////////////

template <typename T>
inline int ObDecoderPool::free_decoders(ObDecodeResourcePool &decode_res_pool,
                                              const ObColumnHeader::Type &type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int16_t &free_cnt_ = free_cnts_[type];
  while (free_cnt_ > 0) {
    T* d = static_cast<T *>(pools_[type][free_cnt_ - 1]);
    if (OB_TMP_FAIL(decode_res_pool.free<T>(d))) {
      ret = tmp_ret;
      STORAGE_LOG(ERROR, "decode resource pool failed to free", K(tmp_ret), K(type), KP(d));
    }
    pools_[type][free_cnt_ - 1] = NULL;
    --free_cnt_;
  }
  return ret;
}

template<typename T>
inline int ObDecoderPool::alloc(T *&item)
{
  int ret = common::OB_SUCCESS;
  const ObColumnHeader::Type type = T::type_;
  item = NULL;
  if (has_decoder(type)) {
    pop_decoder<T>(type, item);
  } else if (OB_FAIL(alloc_miss_cache(item))) {
    STORAGE_LOG(WARN, "allocate miss cache failed", K(ret), K(type));
  }
  return ret;
}

template<typename T>
inline int ObDecoderPool::alloc_miss_cache(T *&item)
{
  int ret = common::OB_SUCCESS;
  ObDecodeResourcePool *decode_res_pool = MTL(ObDecodeResourcePool*);
  if (OB_ISNULL(decode_res_pool)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,"NULL tenant decode resource pool", K(ret));
  } else if (OB_FAIL(decode_res_pool->alloc(item))) {
    STORAGE_LOG(WARN, "allocate decoder failed", K(ret));
  }
  return ret;
}


template<typename T>
inline int ObDecoderPool::free(T *item)
{
  int ret = OB_SUCCESS;
  if (NULL != item) {
    const ObColumnHeader::Type &type = T::type_;
    if (not_reach_limit(type)) {
      push_decoder(type, item);
      item->reuse();
      item = NULL;
    } else {
      ObDecodeResourcePool *decode_res_pool = MTL(ObDecodeResourcePool*);
      if (OB_ISNULL(decode_res_pool)) {
        int ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "NULL tenant decode resource pool", K(ret));
      } else if (FALSE_IT(item->reuse())) {
      } else if (OB_FAIL(decode_res_pool->free(item))) {
        STORAGE_LOG(ERROR, "decode resource pool failed to free", K(ret), K(type), KP(item));
      } else {
        item = NULL;
      }
    }
  }
  return ret;
}

inline bool ObDecoderPool::has_decoder(const ObColumnHeader::Type &type) const
{
  return free_cnts_[type] > 0;
}

inline bool ObDecoderPool::not_reach_limit(const ObColumnHeader::Type &type) const
{
  return free_cnts_[type] < ObDecoderPool::MAX_CNTS[type];
}

template <typename T>
inline void ObDecoderPool::pop_decoder(const ObColumnHeader::Type &type, T *&item)
{
  item = static_cast<T *>(pools_[type][--free_cnts_[type]]);
}

inline void ObDecoderPool::push_decoder(const ObColumnHeader::Type &type,
                                          ObIColumnDecoder *item)
{
  pools_[type][free_cnts_[type]++] = item;
}
////////////////////////// cs decoder pool ///////////////////////////////////////

template <typename T>
inline int ObCSDecoderPool::free_decoders(ObDecodeResourcePool &decode_res_pool,
                                              const ObCSColumnHeader::Type &type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int16_t &free_cnt_ = free_cnts_[type];
  while (free_cnt_ > 0) {
    T* d = static_cast<T *>(pools_[type][free_cnt_ - 1]);
    if (OB_TMP_FAIL(decode_res_pool.free<T>(d))) {
      ret = tmp_ret;
      STORAGE_LOG(ERROR, "decode resource pool failed to free", K(ret), K(type), KP(d));
    }
    pools_[type][free_cnt_ - 1] = NULL;
    --free_cnt_;
  }
  return ret;
}

template<typename T>
inline int ObCSDecoderPool::alloc(T *&item)
{
  int ret = common::OB_SUCCESS;
  const ObCSColumnHeader::Type type = T::type_;
  item = NULL;
  if (has_decoder(type)) {
    pop_decoder<T>(type, item);
  } else if (OB_FAIL(alloc_miss_cache(item))) {
    STORAGE_LOG(WARN, "allocate miss cache failed", K(ret), K(type));
  }
  return ret;
}


template<typename T>
int ObCSDecoderPool::alloc_miss_cache(T *&item)
{
  int ret = common::OB_SUCCESS;
  ObDecodeResourcePool *decode_res_pool = MTL(ObDecodeResourcePool*);
  if (OB_ISNULL(decode_res_pool)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,"NULL tenant decode resource pool", K(ret));
  } else if (OB_FAIL(decode_res_pool->alloc(item))) {
    STORAGE_LOG(WARN, "allocate decoder failed", K(ret));
  }
  return ret;
}


template<typename T>
inline int ObCSDecoderPool::free(T *item)
{
  int ret = OB_SUCCESS;
  if (NULL != item) {
    const ObCSColumnHeader::Type type = T::type_;
    if (not_reach_limit(type)) {
      push_decoder(type, item);
      item->reuse();
      item = NULL;
    } else {
      ObDecodeResourcePool *decode_res_pool = MTL(ObDecodeResourcePool*);
      if (OB_ISNULL(decode_res_pool)) {
        int ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "NULL tenant decode resource pool", K(ret));
      } else if (FALSE_IT(item->reuse())) {
      } else if (OB_FAIL(decode_res_pool->free(item))) {
        STORAGE_LOG(ERROR, "decode resource pool failed to free", K(ret), K(type), KP(item));
      } else {
        item = NULL;
      }
    }
  }
  return ret;
}

inline bool ObCSDecoderPool::has_decoder(const ObCSColumnHeader::Type &type) const
{
  return free_cnts_[type] > 0;
}
inline bool ObCSDecoderPool::not_reach_limit(const ObCSColumnHeader::Type &type) const
{
  return free_cnts_[type] < ObCSDecoderPool::MAX_CS_CNTS[type];
}
template <typename T>
inline void ObCSDecoderPool::pop_decoder(const ObCSColumnHeader::Type &type, T *&item)
{
  item = static_cast<T *>(pools_[type][--free_cnts_[type]]);
}
inline void ObCSDecoderPool::push_decoder(const ObCSColumnHeader::Type &type,
                                          ObIColumnCSDecoder *item)
{
  pools_[type][free_cnts_[type]++] = item;
}