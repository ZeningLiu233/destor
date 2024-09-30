//
// Created by borelset on 2019/3/20.
//
#include "destor.h"
#include <stdint.h>
#include <stdlib.h>
#include <memory.h>
#include <assert.h>
#include <stdio.h>
#include <math.h>

#include "isa-l_crypto/md5_mb.h"

#define SymbolCount 256
#define DigistLength 16
#define SeedLength 64
#define MaxChunkSizeOffset 3
#define MinChunkSizeOffset 2

uint64_t g_gear_matrix[SymbolCount];
uint32_t g_min_fastcdc_chunk_size;
uint32_t g_max_fastcdc_chunk_size;
uint32_t g_expect_fastcdc_chunk_size;

enum
{
    Mask_64B,
    Mask_128B,
    Mask_256B,
    Mask_512B,
    Mask_1KB,
    Mask_2KB,
    Mask_4KB,
    Mask_8KB,
    Mask_16KB,
    Mask_32KB,
    Mask_64KB,
    Mask_128KB
};

uint64_t g_condition_mask[] = {
    0x00001803110,      // 64B
    0x000018035100,     // 128B
    0x00001800035300,   // 256B
    0x000019000353000,  // 512B
    0x0000590003530000, // 1KB
    0x0000d90003530000, // 2KB
    0x0000d90103530000, // 4KB
    0x0000d90303530000, // 8KB
    0x0000d90313530000, // 16KB
    0x0000d90f03530000, // 32KB
    0x0000d90303537000, // 64KB
    0x0000d90703537000  // 128KB
};

uint64_t using_mask0;
uint64_t using_mask1;

void fastcdc_init(uint32_t expectCS)
{
    MD5_HASH_CTX_MGR *mgr;
    int ret = posix_memalign((void *)&mgr, 16, sizeof(MD5_HASH_CTX_MGR));
    if ((ret != 0) || (mgr == NULL))
    {
        printf("posix_memalign failed test aborted\n");
        return;
    }
    md5_ctx_mgr_init(mgr);
    char seed[SeedLength];
    for (int i = 0; i < SymbolCount; i++)
    {
        for (int j = 0; j < SeedLength; j++)
        {
            seed[j] = i;
        }
        g_gear_matrix[i] = 0;
        MD5_HASH_CTX ctxpool;
        hash_ctx_init(&ctxpool);
        md5_ctx_mgr_submit(mgr, &ctxpool, seed, SeedLength, HASH_ENTIRE);
        md5_ctx_mgr_flush(mgr);
        memcpy(&g_gear_matrix[i], ctxpool.job.result_digest, sizeof(uint64_t));
    }

    g_min_fastcdc_chunk_size = destor.chunk_min_size;
    g_max_fastcdc_chunk_size = destor.chunk_max_size;
    g_expect_fastcdc_chunk_size = destor.chunk_avg_size;
    
    int bit = round(log2(g_expect_fastcdc_chunk_size)) - 6;  // 2^6 = 64
    assert(bit >= 0);

    // normalized level is set to 2
    int normalized_level = 2;
    int mask0_b = bit + normalized_level;      // stricter mask
    int mask1_b = bit - normalized_level;
    const int mask_b = sizeof(g_condition_mask) / sizeof(uint64_t);
    if (mask0_b > mask_b - 1) {
        mask0_b = mask_b - 1;
    }
    if (mask1_b < 0) {
        mask1_b = 0;
    }

    using_mask0 = g_condition_mask[mask0_b];
    using_mask1 = g_condition_mask[mask1_b];
}

int fastcdc_chunk_data(unsigned char *p, int n)
{

    uint64_t fingerprint = 0, digest;
    int i = g_min_fastcdc_chunk_size, Mid = g_expect_fastcdc_chunk_size;
    // return n;

    if (n <= g_min_fastcdc_chunk_size) // the minimal  subChunk Size.
        return n;
    // windows_reset();
    if (n > g_max_fastcdc_chunk_size)
        n = g_max_fastcdc_chunk_size;
    else if (n < Mid)
        Mid = n;
    while (i < Mid)
    {
        fingerprint = (fingerprint << 1) + (g_gear_matrix[p[i]]);
        if ((!(fingerprint & using_mask0)))
        { // AVERAGE*2, *4, *8
            return i;
        }
        i++;
    }
    while (i < n)
    {
        fingerprint = (fingerprint << 1) + (g_gear_matrix[p[i]]);
        if ((!(fingerprint & using_mask1)))
        { // Average/2, /4, /8
            return i;
        }
        i++;
    }
    return i;
}