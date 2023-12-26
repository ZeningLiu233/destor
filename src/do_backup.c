#include "destor.h"
#include "jcr.h"
#include "utils/sync_queue.h"
#include "index/index.h"
#include "backup.h"
#include "storage/containerstore.h"

/* Output of read phase. */
SyncQueue *read_queue;
/* Output of chunk phase. */
SyncQueue *chunk_queue;
/* Output of hash phase. */
SyncQueue *hash_queue;
/* Output of trace phase. */
SyncQueue *trace_queue;
/* Output of dedup phase */
SyncQueue *dedup_queue;
/* Output of rewrite phase. */
SyncQueue *rewrite_queue;

/* defined in index.c */
extern struct
{
	/* Requests to the key-value store */
	int lookup_requests;
	int update_requests;
	int lookup_requests_for_unique;
	/* Overheads of prefetching module */
	int read_prefetching_units;
} index_overhead;

void do_backup(char *path)
{

	init_recipe_store();
	init_container_store();
	init_index();

	init_backup_jcr(path);

	puts("==== backup begin ====");

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);

	time_t start = time(NULL);
	if (destor.simulation_level == SIMULATION_ALL)
	{
		start_read_trace_phase();
	}
	else
	{
		start_read_phase();
		start_chunk_phase();
		start_hash_phase();
	}
	start_dedup_phase();
	start_rewrite_phase();
	start_filter_phase();

	do
	{
		usleep(100);
		/*time_t now = time(NULL);*/
		fprintf(stderr, "job %" PRId32 ", %" PRId64 " bytes, %" PRId32 " chunks, %d files processed\r",
				jcr.id, jcr.data_size, jcr.chunk_num, jcr.file_num);
	} while (jcr.status == JCR_STATUS_RUNNING || jcr.status != JCR_STATUS_DONE);
	fprintf(stderr, "job %" PRId32 ", %" PRId64 " bytes, %" PRId32 " chunks, %d files processed\n",
			jcr.id, jcr.data_size, jcr.chunk_num, jcr.file_num);

	if (destor.simulation_level == SIMULATION_ALL)
	{
		stop_read_trace_phase();
	}
	else
	{
		stop_read_phase();
		stop_chunk_phase();
		stop_hash_phase();
	}
	stop_dedup_phase();
	stop_rewrite_phase();
	stop_filter_phase();

	TIMER_END(1, jcr.total_time);

	close_index();
	close_container_store();
	close_recipe_store();

	update_backup_version(jcr.bv);

	free_backup_version(jcr.bv);

	puts("==== backup end ====");

	VERBOSE("job id: %" PRId32 "", jcr.id);
	NOTICE("backup path: %s", jcr.path);
	NOTICE("number of files: %d", jcr.file_num);
	NOTICE("number of chunks: %" PRId32 " (%" PRId64 " bytes on average)", jcr.chunk_num,
		   jcr.data_size / jcr.chunk_num);
	NOTICE("number of unique chunks: %" PRId32 "", jcr.unique_chunk_num);
	NOTICE("total size(B): %" PRId64 "", jcr.data_size);
	NOTICE("stored data size(B): %" PRId64 "",
		   jcr.unique_data_size + jcr.rewritten_chunk_size);
	NOTICE("deduplication ratio: %.4f",
		   jcr.data_size != 0 ? (jcr.data_size - jcr.unique_data_size - jcr.rewritten_chunk_size) / (double)(jcr.data_size) : 0);
	NOTICE("total time(s): %.3f", jcr.total_time / 1000000);
	NOTICE("throughput(MB/s): %.2f",
		   (double)jcr.data_size * 1000000 / (1024 * 1024 * jcr.total_time));
	VERBOSE("number of zero chunks: %" PRId32 "", jcr.zero_chunk_num);
	VERBOSE("size of zero chunks: %" PRId64 "", jcr.zero_chunk_size);
	VERBOSE("number of rewritten chunks: %" PRId32 "", jcr.rewritten_chunk_num);
	VERBOSE("size of rewritten chunks: %" PRId64 "", jcr.rewritten_chunk_size);
	VERBOSE("rewritten rate in size: %.3f",
			jcr.rewritten_chunk_size / (double)jcr.data_size);

	destor.data_size += jcr.data_size;
	destor.stored_data_size += jcr.unique_data_size + jcr.rewritten_chunk_size;

	destor.chunk_num += jcr.chunk_num;
	destor.stored_chunk_num += jcr.unique_chunk_num + jcr.rewritten_chunk_num;
	destor.zero_chunk_num += jcr.zero_chunk_num;
	destor.zero_chunk_size += jcr.zero_chunk_size;
	destor.rewritten_chunk_num += jcr.rewritten_chunk_num;
	destor.rewritten_chunk_size += jcr.rewritten_chunk_size;

	NOTICE("read_time: %.3fs, %.2fMB/s", jcr.read_time / 1000000,
		   jcr.data_size * 1000000 / jcr.read_time / 1024 / 1024);
	NOTICE("chunk_time: %.3fs, %.2fMB/s", jcr.chunk_time / 1000000,
		   jcr.data_size * 1000000 / jcr.chunk_time / 1024 / 1024);
	NOTICE("hash_time: %.3fs, %.2fMB/s", jcr.hash_time / 1000000,
		   jcr.data_size * 1000000 / jcr.hash_time / 1024 / 1024);

	NOTICE("dedup_time: %.3fs, %.2fMB/s",
		   jcr.dedup_time / 1000000,
		   jcr.data_size * 1000000 / jcr.dedup_time / 1024 / 1024);

	VERBOSE("rewrite_time: %.3fs, %.2fMB/s", jcr.rewrite_time / 1000000,
			jcr.data_size * 1000000 / jcr.rewrite_time / 1024 / 1024);

	VERBOSE("filter_time: %.3fs, %.2fMB/s",
			jcr.filter_time / 1000000,
			jcr.data_size * 1000000 / jcr.filter_time / 1024 / 1024);

	NOTICE("write_time: %.3fs, %.2fMB/s", jcr.write_time / 1000000,
		   jcr.data_size * 1000000 / jcr.write_time / 1024 / 1024);

	char logfile[] = "backup.log";
	FILE *fp = fopen(logfile, "a");

	// Check if the file is empty
	fseek(fp, 0, SEEK_END);
	if (ftell(fp) == 0)
	{
		// Write column information
		fprintf(fp, "job id, total size(B), deduplication rate, throughput(MB/s)\n");
	}

	fprintf(fp, "%" PRId32 " %" PRId64 " %.4f"
				" %.2f\n",
			jcr.id,
			jcr.data_size,
			jcr.data_size != 0 ? (jcr.data_size - jcr.rewritten_chunk_size - jcr.unique_data_size) / (double)(jcr.data_size)
							   : 0,
			(double)jcr.data_size * 1000000 / (1024 * 1024 * jcr.total_time));

	fclose(fp);
}