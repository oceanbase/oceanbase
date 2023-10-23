#ifndef __EASY_MACCEPT_H__
#define __EASY_MACCEPT_H__

#include "easy_define.h"
EASY_CPP_START
extern void easy_ma_init(int port);
extern int easy_ma_start();
extern void easy_ma_stop();
extern int easy_ma_regist(int gid, int idx);
extern int easy_ma_handshake(int fd, int id);

EASY_CPP_END
#endif /* __EASY_MACCEPT_H__ */
