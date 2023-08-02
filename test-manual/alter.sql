alter system set shared_buffers = '512MB';
alter system set max_wal_size = '5GB';
alter system set work_mem = '128MB';
alter system set max_worker_processes = 15;