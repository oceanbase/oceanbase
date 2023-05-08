#ifndef USSL_HOOK_MESSAGE_H
#define USSL_HOOK_MESSAGE_H

#include <stdint.h>

#define NEGOTIATION_MAGIC 0xbeef0312
#define MAX_EXTRA_INFO_LEN 20
#define USSL_BUF_LEN 256

typedef struct negotiation_head_t
{
  uint32_t magic;
  uint32_t version;
  uint32_t len;
} negotiation_head_t;

typedef struct negotiation_message_t
{
  int type;
  uint64_t client_gid;
} negotiation_message_t;

int send_negotiation_message(int fd, const char *b, int sz);

#endif // USSL_HOOK_MESSAGE_H