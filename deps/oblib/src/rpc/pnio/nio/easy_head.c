extern  void eh_set(easy_head_t* h, uint32_t len, uint32_t pkt_id);
extern uint64_t eh_packet_id(const char* b);
extern int64_t eh_decode(char* b, int64_t s);
extern void eh_copy_msg(str_t* m, uint32_t pkt_id, const char* b, int64_t s);
