
template <typename vtype1,
          typename vtype2,
          typename zmm_t = typename vtype1::zmm_t,
          typename index_type = typename vtype2::zmm_t>
X86_SIMD_SORT_INLINE zmm_t sort_zmm_64bit(zmm_t key_zmm, index_type &index_zmm)
{
    const typename vtype1::zmmi_t rev_index1 = vtype1::seti(NETWORK_64BIT_2);
    const typename vtype2::zmmi_t rev_index2 = vtype2::seti(NETWORK_64BIT_2);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            vtype1::template shuffle<SHUFFLE_MASK(1, 1, 1, 1)>(key_zmm),
            index_zmm,
            vtype2::template shuffle<SHUFFLE_MASK(1, 1, 1, 1)>(index_zmm),
            0xAA);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            vtype1::permutexvar(vtype1::seti(NETWORK_64BIT_1), key_zmm),
            index_zmm,
            vtype2::permutexvar(vtype2::seti(NETWORK_64BIT_1), index_zmm),
            0xCC);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            vtype1::template shuffle<SHUFFLE_MASK(1, 1, 1, 1)>(key_zmm),
            index_zmm,
            vtype2::template shuffle<SHUFFLE_MASK(1, 1, 1, 1)>(index_zmm),
            0xAA);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            vtype1::permutexvar(rev_index1, key_zmm),
            index_zmm,
            vtype2::permutexvar(rev_index2, index_zmm),
            0xF0);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            vtype1::permutexvar(vtype1::seti(NETWORK_64BIT_3), key_zmm),
            index_zmm,
            vtype2::permutexvar(vtype2::seti(NETWORK_64BIT_3), index_zmm),
            0xCC);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            vtype1::template shuffle<SHUFFLE_MASK(1, 1, 1, 1)>(key_zmm),
            index_zmm,
            vtype2::template shuffle<SHUFFLE_MASK(1, 1, 1, 1)>(index_zmm),
            0xAA);
    return key_zmm;
}
// Assumes zmm is bitonic and performs a recursive half cleaner
template <typename vtype1,
          typename vtype2,
          typename zmm_t = typename vtype1::zmm_t,
          typename index_type = typename vtype2::zmm_t>
X86_SIMD_SORT_INLINE zmm_t bitonic_merge_zmm_64bit(zmm_t key_zmm,
                                                   index_type &index_zmm)
{

    // 1) half_cleaner[8]: compare 0-4, 1-5, 2-6, 3-7
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            vtype1::permutexvar(vtype1::seti(NETWORK_64BIT_4), key_zmm),
            index_zmm,
            vtype2::permutexvar(vtype2::seti(NETWORK_64BIT_4), index_zmm),
            0xF0);
    // 2) half_cleaner[4]
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            vtype1::permutexvar(vtype1::seti(NETWORK_64BIT_3), key_zmm),
            index_zmm,
            vtype2::permutexvar(vtype2::seti(NETWORK_64BIT_3), index_zmm),
            0xCC);
    // 3) half_cleaner[1]
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            vtype1::template shuffle<SHUFFLE_MASK(1, 1, 1, 1)>(key_zmm),
            index_zmm,
            vtype2::template shuffle<SHUFFLE_MASK(1, 1, 1, 1)>(index_zmm),
            0xAA);
    return key_zmm;
}
// Assumes zmm1 and zmm2 are sorted and performs a recursive half cleaner
template <typename vtype1,
          typename vtype2,
          typename zmm_t = typename vtype1::zmm_t,
          typename index_type = typename vtype2::zmm_t>
X86_SIMD_SORT_INLINE void bitonic_merge_two_zmm_64bit(zmm_t &key_zmm1,
                                                      zmm_t &key_zmm2,
                                                      index_type &index_zmm1,
                                                      index_type &index_zmm2)
{
    const typename vtype1::zmmi_t rev_index1 = vtype1::seti(NETWORK_64BIT_2);
    const typename vtype2::zmmi_t rev_index2 = vtype2::seti(NETWORK_64BIT_2);
    // 1) First step of a merging network: coex of zmm1 and zmm2 reversed
    key_zmm2 = vtype1::permutexvar(rev_index1, key_zmm2);
    index_zmm2 = vtype2::permutexvar(rev_index2, index_zmm2);

    zmm_t key_zmm3 = vtype1::min(key_zmm1, key_zmm2);
    zmm_t key_zmm4 = vtype1::max(key_zmm1, key_zmm2);

    typename vtype1::opmask_t movmask = vtype1::eq(key_zmm3, key_zmm1);

    index_type index_zmm3 = vtype2::mask_mov(index_zmm2, movmask, index_zmm1);
    index_type index_zmm4 = vtype2::mask_mov(index_zmm1, movmask, index_zmm2);

    /* need to reverse the lower registers to keep the correct order */
    key_zmm4 = vtype1::permutexvar(rev_index1, key_zmm4);
    index_zmm4 = vtype2::permutexvar(rev_index2, index_zmm4);

    // 2) Recursive half cleaner for each
    key_zmm1 = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm3, index_zmm3);
    key_zmm2 = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm4, index_zmm4);
    index_zmm1 = index_zmm3;
    index_zmm2 = index_zmm4;
}
// Assumes [zmm0, zmm1] and [zmm2, zmm3] are sorted and performs a recursive
// half cleaner
template <typename vtype1,
          typename vtype2,
          typename zmm_t = typename vtype1::zmm_t,
          typename index_type = typename vtype2::zmm_t>
X86_SIMD_SORT_INLINE void bitonic_merge_four_zmm_64bit(zmm_t *key_zmm,
                                                       index_type *index_zmm)
{
    const typename vtype1::zmmi_t rev_index1 = vtype1::seti(NETWORK_64BIT_2);
    const typename vtype2::zmmi_t rev_index2 = vtype2::seti(NETWORK_64BIT_2);
    // 1) First step of a merging network
    zmm_t key_zmm2r = vtype1::permutexvar(rev_index1, key_zmm[2]);
    zmm_t key_zmm3r = vtype1::permutexvar(rev_index1, key_zmm[3]);
    index_type index_zmm2r = vtype2::permutexvar(rev_index2, index_zmm[2]);
    index_type index_zmm3r = vtype2::permutexvar(rev_index2, index_zmm[3]);

    zmm_t key_zmm_t1 = vtype1::min(key_zmm[0], key_zmm3r);
    zmm_t key_zmm_t2 = vtype1::min(key_zmm[1], key_zmm2r);
    zmm_t key_zmm_m1 = vtype1::max(key_zmm[0], key_zmm3r);
    zmm_t key_zmm_m2 = vtype1::max(key_zmm[1], key_zmm2r);

    typename vtype1::opmask_t movmask1 = vtype1::eq(key_zmm_t1, key_zmm[0]);
    typename vtype1::opmask_t movmask2 = vtype1::eq(key_zmm_t2, key_zmm[1]);

    index_type index_zmm_t1 = vtype2::mask_mov(
            index_zmm3r, movmask1, index_zmm[0]);
    index_type index_zmm_m1 = vtype2::mask_mov(
            index_zmm[0], movmask1, index_zmm3r);
    index_type index_zmm_t2 = vtype2::mask_mov(
            index_zmm2r, movmask2, index_zmm[1]);
    index_type index_zmm_m2 = vtype2::mask_mov(
            index_zmm[1], movmask2, index_zmm2r);

    // 2) Recursive half clearer: 16
    zmm_t key_zmm_t3 = vtype1::permutexvar(rev_index1, key_zmm_m2);
    zmm_t key_zmm_t4 = vtype1::permutexvar(rev_index1, key_zmm_m1);
    index_type index_zmm_t3 = vtype2::permutexvar(rev_index2, index_zmm_m2);
    index_type index_zmm_t4 = vtype2::permutexvar(rev_index2, index_zmm_m1);

    zmm_t key_zmm0 = vtype1::min(key_zmm_t1, key_zmm_t2);
    zmm_t key_zmm1 = vtype1::max(key_zmm_t1, key_zmm_t2);
    zmm_t key_zmm2 = vtype1::min(key_zmm_t3, key_zmm_t4);
    zmm_t key_zmm3 = vtype1::max(key_zmm_t3, key_zmm_t4);

    movmask1 = vtype1::eq(key_zmm0, key_zmm_t1);
    movmask2 = vtype1::eq(key_zmm2, key_zmm_t3);

    index_type index_zmm0 = vtype2::mask_mov(
            index_zmm_t2, movmask1, index_zmm_t1);
    index_type index_zmm1 = vtype2::mask_mov(
            index_zmm_t1, movmask1, index_zmm_t2);
    index_type index_zmm2 = vtype2::mask_mov(
            index_zmm_t4, movmask2, index_zmm_t3);
    index_type index_zmm3 = vtype2::mask_mov(
            index_zmm_t3, movmask2, index_zmm_t4);

    key_zmm[0] = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm0, index_zmm0);
    key_zmm[1] = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm1, index_zmm1);
    key_zmm[2] = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm2, index_zmm2);
    key_zmm[3] = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm3, index_zmm3);

    index_zmm[0] = index_zmm0;
    index_zmm[1] = index_zmm1;
    index_zmm[2] = index_zmm2;
    index_zmm[3] = index_zmm3;
}

template <typename vtype1,
          typename vtype2,
          typename zmm_t = typename vtype1::zmm_t,
          typename index_type = typename vtype2::zmm_t>
X86_SIMD_SORT_INLINE void bitonic_merge_eight_zmm_64bit(zmm_t *key_zmm,
                                                        index_type *index_zmm)
{
    const typename vtype1::zmmi_t rev_index1 = vtype1::seti(NETWORK_64BIT_2);
    const typename vtype2::zmmi_t rev_index2 = vtype2::seti(NETWORK_64BIT_2);
    zmm_t key_zmm4r = vtype1::permutexvar(rev_index1, key_zmm[4]);
    zmm_t key_zmm5r = vtype1::permutexvar(rev_index1, key_zmm[5]);
    zmm_t key_zmm6r = vtype1::permutexvar(rev_index1, key_zmm[6]);
    zmm_t key_zmm7r = vtype1::permutexvar(rev_index1, key_zmm[7]);
    index_type index_zmm4r = vtype2::permutexvar(rev_index2, index_zmm[4]);
    index_type index_zmm5r = vtype2::permutexvar(rev_index2, index_zmm[5]);
    index_type index_zmm6r = vtype2::permutexvar(rev_index2, index_zmm[6]);
    index_type index_zmm7r = vtype2::permutexvar(rev_index2, index_zmm[7]);

    zmm_t key_zmm_t1 = vtype1::min(key_zmm[0], key_zmm7r);
    zmm_t key_zmm_t2 = vtype1::min(key_zmm[1], key_zmm6r);
    zmm_t key_zmm_t3 = vtype1::min(key_zmm[2], key_zmm5r);
    zmm_t key_zmm_t4 = vtype1::min(key_zmm[3], key_zmm4r);

    zmm_t key_zmm_m1 = vtype1::max(key_zmm[0], key_zmm7r);
    zmm_t key_zmm_m2 = vtype1::max(key_zmm[1], key_zmm6r);
    zmm_t key_zmm_m3 = vtype1::max(key_zmm[2], key_zmm5r);
    zmm_t key_zmm_m4 = vtype1::max(key_zmm[3], key_zmm4r);

    typename vtype1::opmask_t movmask1 = vtype1::eq(key_zmm_t1, key_zmm[0]);
    typename vtype1::opmask_t movmask2 = vtype1::eq(key_zmm_t2, key_zmm[1]);
    typename vtype1::opmask_t movmask3 = vtype1::eq(key_zmm_t3, key_zmm[2]);
    typename vtype1::opmask_t movmask4 = vtype1::eq(key_zmm_t4, key_zmm[3]);

    index_type index_zmm_t1 = vtype2::mask_mov(
            index_zmm7r, movmask1, index_zmm[0]);
    index_type index_zmm_m1 = vtype2::mask_mov(
            index_zmm[0], movmask1, index_zmm7r);
    index_type index_zmm_t2 = vtype2::mask_mov(
            index_zmm6r, movmask2, index_zmm[1]);
    index_type index_zmm_m2 = vtype2::mask_mov(
            index_zmm[1], movmask2, index_zmm6r);
    index_type index_zmm_t3 = vtype2::mask_mov(
            index_zmm5r, movmask3, index_zmm[2]);
    index_type index_zmm_m3 = vtype2::mask_mov(
            index_zmm[2], movmask3, index_zmm5r);
    index_type index_zmm_t4 = vtype2::mask_mov(
            index_zmm4r, movmask4, index_zmm[3]);
    index_type index_zmm_m4 = vtype2::mask_mov(
            index_zmm[3], movmask4, index_zmm4r);

    zmm_t key_zmm_t5 = vtype1::permutexvar(rev_index1, key_zmm_m4);
    zmm_t key_zmm_t6 = vtype1::permutexvar(rev_index1, key_zmm_m3);
    zmm_t key_zmm_t7 = vtype1::permutexvar(rev_index1, key_zmm_m2);
    zmm_t key_zmm_t8 = vtype1::permutexvar(rev_index1, key_zmm_m1);
    index_type index_zmm_t5 = vtype2::permutexvar(rev_index2, index_zmm_m4);
    index_type index_zmm_t6 = vtype2::permutexvar(rev_index2, index_zmm_m3);
    index_type index_zmm_t7 = vtype2::permutexvar(rev_index2, index_zmm_m2);
    index_type index_zmm_t8 = vtype2::permutexvar(rev_index2, index_zmm_m1);

    COEX<vtype1, vtype2>(key_zmm_t1, key_zmm_t3, index_zmm_t1, index_zmm_t3);
    COEX<vtype1, vtype2>(key_zmm_t2, key_zmm_t4, index_zmm_t2, index_zmm_t4);
    COEX<vtype1, vtype2>(key_zmm_t5, key_zmm_t7, index_zmm_t5, index_zmm_t7);
    COEX<vtype1, vtype2>(key_zmm_t6, key_zmm_t8, index_zmm_t6, index_zmm_t8);
    COEX<vtype1, vtype2>(key_zmm_t1, key_zmm_t2, index_zmm_t1, index_zmm_t2);
    COEX<vtype1, vtype2>(key_zmm_t3, key_zmm_t4, index_zmm_t3, index_zmm_t4);
    COEX<vtype1, vtype2>(key_zmm_t5, key_zmm_t6, index_zmm_t5, index_zmm_t6);
    COEX<vtype1, vtype2>(key_zmm_t7, key_zmm_t8, index_zmm_t7, index_zmm_t8);
    key_zmm[0]
            = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t1, index_zmm_t1);
    key_zmm[1]
            = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t2, index_zmm_t2);
    key_zmm[2]
            = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t3, index_zmm_t3);
    key_zmm[3]
            = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t4, index_zmm_t4);
    key_zmm[4]
            = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t5, index_zmm_t5);
    key_zmm[5]
            = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t6, index_zmm_t6);
    key_zmm[6]
            = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t7, index_zmm_t7);
    key_zmm[7]
            = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t8, index_zmm_t8);

    index_zmm[0] = index_zmm_t1;
    index_zmm[1] = index_zmm_t2;
    index_zmm[2] = index_zmm_t3;
    index_zmm[3] = index_zmm_t4;
    index_zmm[4] = index_zmm_t5;
    index_zmm[5] = index_zmm_t6;
    index_zmm[6] = index_zmm_t7;
    index_zmm[7] = index_zmm_t8;
}

template <typename vtype1,
          typename vtype2,
          typename zmm_t = typename vtype1::zmm_t,
          typename index_type = typename vtype2::zmm_t>
X86_SIMD_SORT_INLINE void bitonic_merge_sixteen_zmm_64bit(zmm_t *key_zmm,
                                                          index_type *index_zmm)
{
    const typename vtype1::zmmi_t rev_index1 = vtype1::seti(NETWORK_64BIT_2);
    const typename vtype2::zmmi_t rev_index2 = vtype2::seti(NETWORK_64BIT_2);
    zmm_t key_zmm8r = vtype1::permutexvar(rev_index1, key_zmm[8]);
    zmm_t key_zmm9r = vtype1::permutexvar(rev_index1, key_zmm[9]);
    zmm_t key_zmm10r = vtype1::permutexvar(rev_index1, key_zmm[10]);
    zmm_t key_zmm11r = vtype1::permutexvar(rev_index1, key_zmm[11]);
    zmm_t key_zmm12r = vtype1::permutexvar(rev_index1, key_zmm[12]);
    zmm_t key_zmm13r = vtype1::permutexvar(rev_index1, key_zmm[13]);
    zmm_t key_zmm14r = vtype1::permutexvar(rev_index1, key_zmm[14]);
    zmm_t key_zmm15r = vtype1::permutexvar(rev_index1, key_zmm[15]);

    index_type index_zmm8r = vtype2::permutexvar(rev_index2, index_zmm[8]);
    index_type index_zmm9r = vtype2::permutexvar(rev_index2, index_zmm[9]);
    index_type index_zmm10r = vtype2::permutexvar(rev_index2, index_zmm[10]);
    index_type index_zmm11r = vtype2::permutexvar(rev_index2, index_zmm[11]);
    index_type index_zmm12r = vtype2::permutexvar(rev_index2, index_zmm[12]);
    index_type index_zmm13r = vtype2::permutexvar(rev_index2, index_zmm[13]);
    index_type index_zmm14r = vtype2::permutexvar(rev_index2, index_zmm[14]);
    index_type index_zmm15r = vtype2::permutexvar(rev_index2, index_zmm[15]);

    zmm_t key_zmm_t1 = vtype1::min(key_zmm[0], key_zmm15r);
    zmm_t key_zmm_t2 = vtype1::min(key_zmm[1], key_zmm14r);
    zmm_t key_zmm_t3 = vtype1::min(key_zmm[2], key_zmm13r);
    zmm_t key_zmm_t4 = vtype1::min(key_zmm[3], key_zmm12r);
    zmm_t key_zmm_t5 = vtype1::min(key_zmm[4], key_zmm11r);
    zmm_t key_zmm_t6 = vtype1::min(key_zmm[5], key_zmm10r);
    zmm_t key_zmm_t7 = vtype1::min(key_zmm[6], key_zmm9r);
    zmm_t key_zmm_t8 = vtype1::min(key_zmm[7], key_zmm8r);

    zmm_t key_zmm_m1 = vtype1::max(key_zmm[0], key_zmm15r);
    zmm_t key_zmm_m2 = vtype1::max(key_zmm[1], key_zmm14r);
    zmm_t key_zmm_m3 = vtype1::max(key_zmm[2], key_zmm13r);
    zmm_t key_zmm_m4 = vtype1::max(key_zmm[3], key_zmm12r);
    zmm_t key_zmm_m5 = vtype1::max(key_zmm[4], key_zmm11r);
    zmm_t key_zmm_m6 = vtype1::max(key_zmm[5], key_zmm10r);
    zmm_t key_zmm_m7 = vtype1::max(key_zmm[6], key_zmm9r);
    zmm_t key_zmm_m8 = vtype1::max(key_zmm[7], key_zmm8r);

    index_type index_zmm_t1 = vtype2::mask_mov(
            index_zmm15r, vtype1::eq(key_zmm_t1, key_zmm[0]), index_zmm[0]);
    index_type index_zmm_m1 = vtype2::mask_mov(
            index_zmm[0], vtype1::eq(key_zmm_t1, key_zmm[0]), index_zmm15r);
    index_type index_zmm_t2 = vtype2::mask_mov(
            index_zmm14r, vtype1::eq(key_zmm_t2, key_zmm[1]), index_zmm[1]);
    index_type index_zmm_m2 = vtype2::mask_mov(
            index_zmm[1], vtype1::eq(key_zmm_t2, key_zmm[1]), index_zmm14r);
    index_type index_zmm_t3 = vtype2::mask_mov(
            index_zmm13r, vtype1::eq(key_zmm_t3, key_zmm[2]), index_zmm[2]);
    index_type index_zmm_m3 = vtype2::mask_mov(
            index_zmm[2], vtype1::eq(key_zmm_t3, key_zmm[2]), index_zmm13r);
    index_type index_zmm_t4 = vtype2::mask_mov(
            index_zmm12r, vtype1::eq(key_zmm_t4, key_zmm[3]), index_zmm[3]);
    index_type index_zmm_m4 = vtype2::mask_mov(
            index_zmm[3], vtype1::eq(key_zmm_t4, key_zmm[3]), index_zmm12r);

    index_type index_zmm_t5 = vtype2::mask_mov(
            index_zmm11r, vtype1::eq(key_zmm_t5, key_zmm[4]), index_zmm[4]);
    index_type index_zmm_m5 = vtype2::mask_mov(
            index_zmm[4], vtype1::eq(key_zmm_t5, key_zmm[4]), index_zmm11r);
    index_type index_zmm_t6 = vtype2::mask_mov(
            index_zmm10r, vtype1::eq(key_zmm_t6, key_zmm[5]), index_zmm[5]);
    index_type index_zmm_m6 = vtype2::mask_mov(
            index_zmm[5], vtype1::eq(key_zmm_t6, key_zmm[5]), index_zmm10r);
    index_type index_zmm_t7 = vtype2::mask_mov(
            index_zmm9r, vtype1::eq(key_zmm_t7, key_zmm[6]), index_zmm[6]);
    index_type index_zmm_m7 = vtype2::mask_mov(
            index_zmm[6], vtype1::eq(key_zmm_t7, key_zmm[6]), index_zmm9r);
    index_type index_zmm_t8 = vtype2::mask_mov(
            index_zmm8r, vtype1::eq(key_zmm_t8, key_zmm[7]), index_zmm[7]);
    index_type index_zmm_m8 = vtype2::mask_mov(
            index_zmm[7], vtype1::eq(key_zmm_t8, key_zmm[7]), index_zmm8r);

    zmm_t key_zmm_t9 = vtype1::permutexvar(rev_index1, key_zmm_m8);
    zmm_t key_zmm_t10 = vtype1::permutexvar(rev_index1, key_zmm_m7);
    zmm_t key_zmm_t11 = vtype1::permutexvar(rev_index1, key_zmm_m6);
    zmm_t key_zmm_t12 = vtype1::permutexvar(rev_index1, key_zmm_m5);
    zmm_t key_zmm_t13 = vtype1::permutexvar(rev_index1, key_zmm_m4);
    zmm_t key_zmm_t14 = vtype1::permutexvar(rev_index1, key_zmm_m3);
    zmm_t key_zmm_t15 = vtype1::permutexvar(rev_index1, key_zmm_m2);
    zmm_t key_zmm_t16 = vtype1::permutexvar(rev_index1, key_zmm_m1);
    index_type index_zmm_t9 = vtype2::permutexvar(rev_index2, index_zmm_m8);
    index_type index_zmm_t10 = vtype2::permutexvar(rev_index2, index_zmm_m7);
    index_type index_zmm_t11 = vtype2::permutexvar(rev_index2, index_zmm_m6);
    index_type index_zmm_t12 = vtype2::permutexvar(rev_index2, index_zmm_m5);
    index_type index_zmm_t13 = vtype2::permutexvar(rev_index2, index_zmm_m4);
    index_type index_zmm_t14 = vtype2::permutexvar(rev_index2, index_zmm_m3);
    index_type index_zmm_t15 = vtype2::permutexvar(rev_index2, index_zmm_m2);
    index_type index_zmm_t16 = vtype2::permutexvar(rev_index2, index_zmm_m1);

    COEX<vtype1, vtype2>(key_zmm_t1, key_zmm_t5, index_zmm_t1, index_zmm_t5);
    COEX<vtype1, vtype2>(key_zmm_t2, key_zmm_t6, index_zmm_t2, index_zmm_t6);
    COEX<vtype1, vtype2>(key_zmm_t3, key_zmm_t7, index_zmm_t3, index_zmm_t7);
    COEX<vtype1, vtype2>(key_zmm_t4, key_zmm_t8, index_zmm_t4, index_zmm_t8);
    COEX<vtype1, vtype2>(key_zmm_t9, key_zmm_t13, index_zmm_t9, index_zmm_t13);
    COEX<vtype1, vtype2>(
            key_zmm_t10, key_zmm_t14, index_zmm_t10, index_zmm_t14);
    COEX<vtype1, vtype2>(
            key_zmm_t11, key_zmm_t15, index_zmm_t11, index_zmm_t15);
    COEX<vtype1, vtype2>(
            key_zmm_t12, key_zmm_t16, index_zmm_t12, index_zmm_t16);

    COEX<vtype1, vtype2>(key_zmm_t1, key_zmm_t3, index_zmm_t1, index_zmm_t3);
    COEX<vtype1, vtype2>(key_zmm_t2, key_zmm_t4, index_zmm_t2, index_zmm_t4);
    COEX<vtype1, vtype2>(key_zmm_t5, key_zmm_t7, index_zmm_t5, index_zmm_t7);
    COEX<vtype1, vtype2>(key_zmm_t6, key_zmm_t8, index_zmm_t6, index_zmm_t8);
    COEX<vtype1, vtype2>(key_zmm_t9, key_zmm_t11, index_zmm_t9, index_zmm_t11);
    COEX<vtype1, vtype2>(
            key_zmm_t10, key_zmm_t12, index_zmm_t10, index_zmm_t12);
    COEX<vtype1, vtype2>(
            key_zmm_t13, key_zmm_t15, index_zmm_t13, index_zmm_t15);
    COEX<vtype1, vtype2>(
            key_zmm_t14, key_zmm_t16, index_zmm_t14, index_zmm_t16);

    COEX<vtype1, vtype2>(key_zmm_t1, key_zmm_t2, index_zmm_t1, index_zmm_t2);
    COEX<vtype1, vtype2>(key_zmm_t3, key_zmm_t4, index_zmm_t3, index_zmm_t4);
    COEX<vtype1, vtype2>(key_zmm_t5, key_zmm_t6, index_zmm_t5, index_zmm_t6);
    COEX<vtype1, vtype2>(key_zmm_t7, key_zmm_t8, index_zmm_t7, index_zmm_t8);
    COEX<vtype1, vtype2>(key_zmm_t9, key_zmm_t10, index_zmm_t9, index_zmm_t10);
    COEX<vtype1, vtype2>(
            key_zmm_t11, key_zmm_t12, index_zmm_t11, index_zmm_t12);
    COEX<vtype1, vtype2>(
            key_zmm_t13, key_zmm_t14, index_zmm_t13, index_zmm_t14);
    COEX<vtype1, vtype2>(
            key_zmm_t15, key_zmm_t16, index_zmm_t15, index_zmm_t16);
    //
    key_zmm[0]
            = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t1, index_zmm_t1);
    key_zmm[1]
            = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t2, index_zmm_t2);
    key_zmm[2]
            = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t3, index_zmm_t3);
    key_zmm[3]
            = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t4, index_zmm_t4);
    key_zmm[4]
            = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t5, index_zmm_t5);
    key_zmm[5]
            = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t6, index_zmm_t6);
    key_zmm[6]
            = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t7, index_zmm_t7);
    key_zmm[7]
            = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t8, index_zmm_t8);
    key_zmm[8]
            = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t9, index_zmm_t9);
    key_zmm[9] = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t10,
                                                         index_zmm_t10);
    key_zmm[10] = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t11,
                                                          index_zmm_t11);
    key_zmm[11] = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t12,
                                                          index_zmm_t12);
    key_zmm[12] = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t13,
                                                          index_zmm_t13);
    key_zmm[13] = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t14,
                                                          index_zmm_t14);
    key_zmm[14] = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t15,
                                                          index_zmm_t15);
    key_zmm[15] = bitonic_merge_zmm_64bit<vtype1, vtype2>(key_zmm_t16,
                                                          index_zmm_t16);

    index_zmm[0] = index_zmm_t1;
    index_zmm[1] = index_zmm_t2;
    index_zmm[2] = index_zmm_t3;
    index_zmm[3] = index_zmm_t4;
    index_zmm[4] = index_zmm_t5;
    index_zmm[5] = index_zmm_t6;
    index_zmm[6] = index_zmm_t7;
    index_zmm[7] = index_zmm_t8;
    index_zmm[8] = index_zmm_t9;
    index_zmm[9] = index_zmm_t10;
    index_zmm[10] = index_zmm_t11;
    index_zmm[11] = index_zmm_t12;
    index_zmm[12] = index_zmm_t13;
    index_zmm[13] = index_zmm_t14;
    index_zmm[14] = index_zmm_t15;
    index_zmm[15] = index_zmm_t16;
}
