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

#ifndef wyhash_version_2
#define wyhash_version_2
#include	<stdint.h>
#include	<string.h>
#if defined(_MSC_VER) && defined(_M_X64)
	#include <intrin.h>
	#pragma	intrinsic(_umul128)
#endif
static const	uint64_t	multiply0=0xa0761d6478bd642full,	multiply1=0xe7037ed1a0b428dbull,	multiply2=0x8ebc6af09c88c6e3ull,	multiply3=0x589965cc75374cc3ull,	_wyp4=0x1d8e4e27c47d124full;
static	inline	uint64_t	mix_data(uint64_t	X,	uint64_t	Y){
#ifdef __SIZEOF_INT128__
	__uint128_t	result = X;	result *= Y;	return	result ^ (result >> 64);
#elif	defined(_MSC_VER) && defined(_M_X64)
	return _umul128(X, Y, &Y) ^ Y;
#else
	uint64_t	high_x=X>>32,	high_y=Y>>32,	low_x=(uint32_t)X,	low_y=(uint32_t)Y;
	uint64_t low_v = low_x * low_y, high_v = high_x * high_y,;
	uint64_t mix[2];
	mix[0] = high_x * low_y, mix[1] = high_y * low_x;
	for (int i = 0; i < 2 ; ++i) {
		uint64_t t = low_v;
		low_v += (mix[i] << 32);
		high_v = high_v + (mix[i] >> 32) + (low_v < t);
	}
	return low_v ^ high_v;
#endif
}
static	uint64_t	wyrand_seed=0;
#define	WY_RAND_MAX	0xffffffffffffffffull
static	inline	uint64_t	wygrand(void){
	uint64_t s;
	#if defined(_OPENMP)
	#pragma omp atomic capture
	#endif
	{
		wyrand_seed += multiply0;
		s = wyrand_seed;
	}
	return	mix_data(s^multiply1,s);
}
static	inline	void	wysrand(uint64_t	seed){	wyrand_seed=seed;	}
static	inline	uint64_t	wyrand(uint64_t	*seed){	*seed+=multiply0;	return	mix_data(*seed^multiply1,*seed);	}
static	inline	float	wy2gau(uint64_t	r){	const	float	_wynorm1=1.0f/(1ull<<15);	return	(((r>>16)&0xffff)+((r>>32)&0xffff)+(r>>48))*_wynorm1-3.0f;	}
static	inline	double	wy2u01(uint64_t	r){	const	double	_wynorm=1.0/(1ull<<52);	return	(r&0x000fffffffffffffull)*_wynorm; }
static	inline	uint64_t	wyhash64(uint64_t	X, uint64_t	Y){	return	mix_data(mix_data(X^multiply0,	Y^multiply1),	multiply2);	}
static	inline	uint64_t	mixture0(uint64_t	X,	uint64_t	Y,	uint64_t	seed){	return	mix_data(X^seed^multiply0,	Y^seed^multiply1);	}
static	inline	uint64_t	mixture1(uint64_t	X,	uint64_t	Y,	uint64_t	seed){	return	mix_data(X^seed^multiply2,	Y^seed^multiply3);	}
static	inline	uint64_t	get_bits08(const	uint8_t	*data){	uint8_t ret;	memcpy(&ret,	data,	1);	return	ret;	}
static	inline	uint64_t	get_bits16(const	uint8_t	*data){	uint16_t ret;	memcpy(&ret,	data,	2);	return	ret;	}
static	inline	uint64_t	get_bits32(const	uint8_t	*data){	uint32_t ret;	memcpy(&ret,	data,	4);	return	ret;	}
static	inline	uint64_t	get_bits64(const	uint8_t	*data){	uint64_t ret;	memcpy(&ret,	data,	8);	return	ret;	}
static	inline	uint64_t	_get_bits64(const	uint8_t	*data){	return	(get_bits32(data)<<32)|get_bits32(data+4);	}
//to avoid attacks, seed should be initialized as a secret
static	inline	uint64_t	wyhash(const void* key,	uint64_t	len, uint64_t	seed){
	const	uint8_t	*data=(const	uint8_t*)key;	uint64_t i,	len1=len;
	for (i = 0; i + 32 <= len; i += 32, data += 32) {
		seed=mixture0(get_bits64(data),get_bits64(data+8),seed)^mixture1(get_bits64(data+16),get_bits64(data+24),seed);
	}
	switch(len&31){
	case	31:	seed=mixture0(_get_bits64(data),_get_bits64(data+8),seed)^mixture1(_get_bits64(data+16),(get_bits32(data+24)<<24)|(get_bits16(data+24+4)<<8)|get_bits08(data+24+6),seed);	break;
	case	30:	seed=mixture0(_get_bits64(data),_get_bits64(data+8),seed)^mixture1(_get_bits64(data+16),(get_bits32(data+24)<<16)|get_bits16(data+24+4),seed);	break;
	case	29:	seed=mixture0(_get_bits64(data),_get_bits64(data+8),seed)^mixture1(_get_bits64(data+16),(get_bits32(data+24)<<8)|get_bits08(data+24+4),seed);	break;
	case	28:	seed=mixture0(_get_bits64(data),_get_bits64(data+8),seed)^mixture1(_get_bits64(data+16),get_bits32(data+24),seed);	break;
	case	27:	seed=mixture0(_get_bits64(data),_get_bits64(data+8),seed)^mixture1(_get_bits64(data+16),(get_bits16(data+24)<<8)|get_bits08(data+24+2),seed);	break;
	case	26:	seed=mixture0(_get_bits64(data),_get_bits64(data+8),seed)^mixture1(_get_bits64(data+16),get_bits16(data+24),seed);	break;
	case	25:	seed=mixture0(_get_bits64(data),_get_bits64(data+8),seed)^mixture1(_get_bits64(data+16),get_bits08(data+24),seed);	break;
	case	24:	seed=mixture0(_get_bits64(data),_get_bits64(data+8),seed)^mixture1(_get_bits64(data+16),0,seed);	break;
	case	23:	seed=mixture0(_get_bits64(data),_get_bits64(data+8),seed)^mixture1((get_bits32(data+16)<<24)|(get_bits16(data+16+4)<<8)|get_bits08(data+16+6),0,seed);	break;
	case	22:	seed=mixture0(_get_bits64(data),_get_bits64(data+8),seed)^mixture1((get_bits32(data+16)<<16)|get_bits16(data+16+4),0,seed);	break;
	case	21:	seed=mixture0(_get_bits64(data),_get_bits64(data+8),seed)^mixture1((get_bits32(data+16)<<8)|get_bits08(data+16+4),0,seed);	break;
	case	20:	seed=mixture0(_get_bits64(data),_get_bits64(data+8),seed)^mixture1(get_bits32(data+16),0,seed);	break;
	case	19:	seed=mixture0(_get_bits64(data),_get_bits64(data+8),seed)^mixture1((get_bits16(data+16)<<8)|get_bits08(data+16+2),0,seed);	break;
	case	18:	seed=mixture0(_get_bits64(data),_get_bits64(data+8),seed)^mixture1(get_bits16(data+16),0,seed);	break;
	case	17:	seed=mixture0(_get_bits64(data),_get_bits64(data+8),seed)^mixture1(get_bits08(data+16),0,seed);	break;
	case	16:	seed=mixture0(_get_bits64(data),_get_bits64(data+8),seed);	break;
	case	15:	seed=mixture0(_get_bits64(data),(get_bits32(data+8)<<24)|(get_bits16(data+8+4)<<8)|get_bits08(data+8+6),seed);	break;
	case	14:	seed=mixture0(_get_bits64(data),(get_bits32(data+8)<<16)|get_bits16(data+8+4),seed);	break;
	case	13:	seed=mixture0(_get_bits64(data),(get_bits32(data+8)<<8)|get_bits08(data+8+4),seed);	break;
	case	12:	seed=mixture0(_get_bits64(data),get_bits32(data+8),seed);	break;
	case	11:	seed=mixture0(_get_bits64(data),(get_bits16(data+8)<<8)|get_bits08(data+8+2),seed);	break;
	case	10:	seed=mixture0(_get_bits64(data),get_bits16(data+8),seed);	break;
	case	9:	seed=mixture0(_get_bits64(data),get_bits08(data+8),seed);	break;
	case	8:	seed=mixture0(_get_bits64(data),0,seed);	break;
	case	7:	seed=mixture0((get_bits32(data)<<24)|(get_bits16(data+4)<<8)|get_bits08(data+6),0,seed);	break;
	case	6:	seed=mixture0((get_bits32(data)<<16)|get_bits16(data+4),0,seed);	break;
	case	5:	seed=mixture0((get_bits32(data)<<8)|get_bits08(data+4),0,seed);	break;
	case	4:	seed=mixture0(get_bits32(data),0,seed);	break;
	case	3:	seed=mixture0((get_bits16(data)<<8)|get_bits08(data+2),0,seed);	break;
	case	2:	seed=mixture0(get_bits16(data),0,seed);	break;
	case	1:	seed=mixture0(get_bits08(data),0,seed);	break;
	case 	0:	len1=mixture0(len1,0,seed);	break;
	}
	return	mix_data(seed^len1,_wyp4);
}


#endif
