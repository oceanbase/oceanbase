typedef struct easy_head_t
{
  uint32_t magic_;
  uint32_t len_;
  uint32_t pkt_id_;
  uint32_t reserved_;
} easy_head_t;

#define rk_bswap32  __builtin_bswap32
#if PNIO_ENABLE_CRC
static uint32_t calc_crc(const void* b, size_t len)
{
  return fasthash64(b, len, 0);
}
#endif

inline void eh_set(easy_head_t* h, uint32_t len, uint32_t pkt_id)
{
  h->magic_ = 0xcedbdb01;
  h->len_ = rk_bswap32(len);
  h->pkt_id_ = rk_bswap32(pkt_id);
  h->reserved_ = 0;
}

inline uint64_t eh_packet_id(const char* b)
{
  return rk_bswap32(((easy_head_t*)b)->pkt_id_);
}

static int64_t eh_decode(char* b, int64_t s)
{
  int64_t bytes = sizeof(easy_head_t);
  if (s >= bytes) {
    easy_head_t* h = (typeof(h))b;
    bytes += rk_bswap32(h->len_);
    PNIO_CRC(assert(s < bytes || h->reserved_ == calc_crc(b + sizeof(easy_head_t), bytes - sizeof(easy_head_t))));
  }
  return bytes;
}

static void eh_copy_msg(str_t* m, uint32_t pkt_id, const char* b, int64_t s)
{
  easy_head_t* h = (typeof(h))m->b;
  m->s = s + sizeof(*h);
  eh_set(h, s, pkt_id);
  if ((uint64_t)(h+1) != (uint64_t)b) {
    memcpy((void*)(h + 1), b, s);
  }
  PNIO_CRC(h->reserved_ = calc_crc(b, s));
}
