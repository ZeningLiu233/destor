//
// Created by borelset on 2019/3/20.
//

#include <stdint.h>
#include <stdlib.h>
#include <memory.h>
#include <assert.h>
#include <stdio.h>

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

void fastcdc_init(uint32_t expectCS)
{
    char seed[SeedLength];
    for (int i = 0; i < SymbolCount; i++)
    {
        for (int j = 0; j < SeedLength; j++)
        {
            seed[j] = i;
        }

        g_gear_matrix[i] = 0;

        MD5_HASH_CTX_MGR *mgr;
        int ret;

        ret = posix_memalign((void *)&mgr, 16, sizeof(MD5_HASH_CTX_MGR));
        if ((ret != 0) || (mgr == NULL))
        {
            printf("posix_memalign failed test aborted\n");
            return;
        }

        MD5_HASH_CTX ctxpool, *ctx = NULL;

        hash_ctx_init(&ctxpool);
        ctxpool.user_data = (void *)&seed;

        ctx = &ctxpool;
        ctx = md5_ctx_mgr_submit(mgr, ctx, seed, SeedLength, HASH_ENTIRE);
        ctx = md5_ctx_mgr_flush(mgr);

        memcpy(&g_gear_matrix[i], ctxpool.job.result_digest, sizeof(uint64_t));
    }

    g_min_fastcdc_chunk_size = 2048;
    g_max_fastcdc_chunk_size = 65536;
    g_expect_fastcdc_chunk_size = expectCS;
}

int fastcdc_chunk_data(unsigned char *p, int n)
{

    uint64_t fingerprint = 0, digest;
    int i = g_min_fastcdc_chunk_size, Mid = g_min_fastcdc_chunk_size + 8 * 1024;
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
        if ((!(fingerprint & 0x0000d90f03530000)))
        { // AVERAGE*2, *4, *8
            return i;
        }
        i++;
    }
    while (i < n)
    {
        fingerprint = (fingerprint << 1) + (g_gear_matrix[p[i]]);
        if ((!(fingerprint & 0x0000d90003530000)))
        { // Average/2, /4, /8
            return i;
        }
        i++;
    }
    return i;
}