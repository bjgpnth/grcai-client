# ui/components/metrics_viewer.py
"""
Metrics formatting and display utilities.
"""

# Mapping of Postgres metric names to their SQL commands
POSTGRES_METRIC_COMMANDS = {
    "active_connections": "SELECT count(*) FROM pg_stat_activity;",
    "connections_active": """SELECT COALESCE(NULLIF(state, ''), 'idle') as state, count(*) 
FROM pg_stat_activity 
GROUP BY COALESCE(NULLIF(state, ''), 'idle')
HAVING COALESCE(NULLIF(state, ''), 'idle') = 'active';""",
    "connections_idle": """SELECT COALESCE(NULLIF(state, ''), 'idle') as state, count(*) 
FROM pg_stat_activity 
GROUP BY COALESCE(NULLIF(state, ''), 'idle')
HAVING COALESCE(NULLIF(state, ''), 'idle') = 'idle';""",
    "slow_queries_count": """SELECT query, calls, total_exec_time
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 5;""",
    "autovacuum_processes": """SELECT count(*) as autovacuum_count
FROM pg_stat_activity
WHERE query LIKE 'autovacuum%';""",
    "database_count": "SELECT datname, pg_database_size(datname) FROM pg_database;",
    "total_locks": "SELECT mode, count(*) FROM pg_locks GROUP BY mode;",
    "replication_streams": "SELECT * FROM pg_stat_replication;",
    "replication_lag_seconds": """SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())) as lag_seconds
FROM pg_stat_replication;""",
}

# Mapping of Nginx metric names to their commands
NGINX_METRIC_COMMANDS = {
    "status": "systemctl is-active nginx || ps aux | grep '[n]ginx'",
    "liveness_check": "curl -s -o /dev/null -w '%{http_code}' http://localhost/ || systemctl is-active nginx",
    "version": "nginx -v",
    "process": "ps aux | grep '[n]ginx' || ps -ef | grep '[n]ginx'",
    "listen_ports": "ss -tlnp | grep nginx || netstat -tlnp | grep nginx",
    "access_log_analysis": "tail -n 200 /var/log/nginx/access.log | awk '{print $9}' | sort | uniq -c | sort -rn",
    "worker_processes": "grep -E '^worker_processes' /etc/nginx/nginx.conf",
    "worker_connections": "grep -E 'worker_connections' /etc/nginx/nginx.conf",
    "active_connections": "curl -s http://localhost/nginx_status | grep 'Active connections'",
    "accepted_requests": "curl -s http://localhost/nginx_status | grep 'server accepts handled requests'",
    "handled_requests": "curl -s http://localhost/nginx_status | grep 'server accepts handled requests'",
    "total_requests": "curl -s http://localhost/nginx_status | grep 'server accepts handled requests'",
    "reading": "curl -s http://localhost/nginx_status | grep 'Reading:'",
    "writing": "curl -s http://localhost/nginx_status | grep 'Reading:'",
    "waiting": "curl -s http://localhost/nginx_status | grep 'Reading:'",
    "status_2xx": "tail -n 200 /var/log/nginx/access.log | grep -c '\" 2[0-9][0-9] '",
    "status_3xx": "tail -n 200 /var/log/nginx/access.log | grep -c '\" 3[0-9][0-9] '",
    "status_4xx": "tail -n 200 /var/log/nginx/access.log | grep -c '\" 4[0-9][0-9] '",
    "status_5xx": "tail -n 200 /var/log/nginx/access.log | grep -c '\" 5[0-9][0-9] '",
}

# Mapping of Tomcat metric names to their commands
TOMCAT_METRIC_COMMANDS = {
    "thread_pool_blocked": "jstack <PID> | grep -c 'BLOCKED'",
    "thread_pool_waiting": "jstack <PID> | grep -c 'WAITING'",
    "thread_pool_deadlock": "jstack <PID> | grep -i deadlock",
    "total_threads": "jstack <PID> | grep -c 'java.lang.Thread.State'",
    "heap_used_mb": "jmap -heap <PID> | grep 'used'",
    "heap_capacity_mb": "jmap -heap <PID> | grep 'capacity'",
    "heap_max_mb": "jmap -heap <PID> | grep 'MaxHeapSize'",
    "access_log_requests": "tail -n 200 <CATALINA_HOME>/logs/localhost_access_log.*.txt | wc -l",
    "requests_per_second": "tail -n 200 <CATALINA_HOME>/logs/localhost_access_log.*.txt | awk '{print $4}' | uniq -c",
    "access_log_analysis": "tail -n 200 <CATALINA_HOME>/logs/localhost_access_log.*.txt | awk '{print $9}' | sort | uniq -c | sort -rn",
}

# Mapping of Kafka metric names to their commands
KAFKA_METRIC_COMMANDS = {
    "version": "kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | grep -i version || grep 'Kafka version' <KAFKA_HOME>/logs/server.log",
    "broker_id": "grep '^broker.id=' <KAFKA_HOME>/config/server.properties || grep 'nodeId=' <KAFKA_HOME>/logs/server.log | head -1",
    "mode": "grep -i 'KafkaRaftServer\\|KRaft' <KAFKA_HOME>/logs/server.log | head -1 || echo 'Zookeeper'",
    "log_dirs": "grep '^log.dirs=' <KAFKA_HOME>/config/server.properties",
    "network_threads": "grep '^num.network.threads=' <KAFKA_HOME>/config/server.properties",
    "io_threads": "grep '^num.io.threads=' <KAFKA_HOME>/config/server.properties",
    "total_topics": "kafka-topics.sh --bootstrap-server localhost:9092 --list | wc -l",
    "total_partitions": "kafka-topics.sh --bootstrap-server localhost:9092 --describe | grep -c 'Partition:'",
    "avg_replication_factor": "kafka-topics.sh --bootstrap-server localhost:9092 --describe | grep 'ReplicationFactor' | awk '{sum+=$NF; count++} END {print sum/count}'",
    "total_consumer_groups": "kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list | wc -l",
    "total_consumer_lag": "kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups | grep -E 'LAG' | awk '{sum+=$NF} END {print sum}'",
    "avg_consumer_lag": "kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups | grep -E 'LAG' | awk '{sum+=$NF; count++} END {print sum/count}'",
}

# Mapping of Node.js metric names to their commands
NODEJS_METRIC_COMMANDS = {
    "process_count": "ps aux | grep -E '[n]ode|[n]pm' | grep -v grep | wc -l",
    "memory_usage_percent": "ps aux | grep -E '[n]ode' | awk '{sum+=$4} END {print sum}'",
    "cpu_usage_percent": "ps aux | grep -E '[n]ode' | awk '{sum+=$3} END {print sum}'",
    "listening_ports_count": "ss -tlnp | grep -E 'node|pid=' | grep LISTEN | wc -l || netstat -tlnp | grep -E 'node|pid=' | grep LISTEN | wc -l",
    "dependencies_count": "cat package.json 2>/dev/null | grep -E '\"dependencies\"|\"devDependencies\"' | wc -l || echo '0'",
    "version": "node --version",
    "npm_version": "npm --version",
}

# Mapping of MSSQL metric names to their SQL commands
MSSQL_METRIC_COMMANDS = {
    "active_connections": "SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE is_user_process = 1;",
    "connections_running": "SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE status = 'running' AND is_user_process = 1;",
    "connections_sleeping": "SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE status = 'sleeping' AND is_user_process = 1;",
    "database_count": "SELECT COUNT(*) FROM sys.databases;",
    "table_count": """SELECT COUNT(*) 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'BASE TABLE';""",
    "total_wait_time_ms": """SELECT SUM(wait_time_ms) 
FROM sys.dm_os_wait_stats 
WHERE wait_type NOT IN ('CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE', 'SLEEP_TASK', 'SLEEP_SYSTEMTASK');""",
    "top_wait_type": """SELECT TOP 1 wait_type 
FROM sys.dm_os_wait_stats 
WHERE wait_type NOT IN ('CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE', 'SLEEP_TASK', 'SLEEP_SYSTEMTASK')
ORDER BY wait_time_ms DESC;""",
    "top_wait_time_ms": """SELECT TOP 1 wait_time_ms 
FROM sys.dm_os_wait_stats 
WHERE wait_type NOT IN ('CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE', 'SLEEP_TASK', 'SLEEP_SYSTEMTASK')
ORDER BY wait_time_ms DESC;""",
    "page_life_expectancy": """SELECT TOP 1 cntr_value 
FROM sys.dm_os_performance_counters 
WHERE counter_name = 'Page life expectancy' AND instance_name = '';""",
    "buffer_cache_hit_ratio_percent": """SELECT 
    (SELECT TOP 1 cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Buffer cache hit ratio' AND instance_name = '') * 100.0 / 
    (SELECT TOP 1 cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Buffer cache hit ratio base' AND instance_name = '') as hit_ratio_percent;""",
    "total_database_size_mb": """SELECT SUM(size) * 8 / 1024 as total_size_mb 
FROM sys.master_files;""",
}

# Mapping of Redis metric names to their redis-cli commands
REDIS_METRIC_COMMANDS = {
    "used_memory_mb": "redis-cli -p 6379 INFO memory | grep '^used_memory:'",
    "used_memory_human": "redis-cli -p 6379 INFO memory | grep '^used_memory_human:'",
    "max_memory_mb": "redis-cli -p 6379 INFO memory | grep '^maxmemory:'",
    "memory_usage_percent": "redis-cli -p 6379 INFO memory | grep -E '^used_memory:|^maxmemory:'",
    "evicted_keys": "redis-cli -p 6379 INFO stats | grep '^evicted_keys:'",
    "hit_ratio": "redis-cli -p 6379 INFO stats | grep -E '^keyspace_hits:|^keyspace_misses:'",
    "miss_ratio": "redis-cli -p 6379 INFO stats | grep -E '^keyspace_hits:|^keyspace_misses:'",
    "keyspace_hits": "redis-cli -p 6379 INFO stats | grep '^keyspace_hits:'",
    "keyspace_misses": "redis-cli -p 6379 INFO stats | grep '^keyspace_misses:'",
    "total_commands_processed": "redis-cli -p 6379 INFO stats | grep '^total_commands_processed:'",
    "connected_clients": "redis-cli -p 6379 INFO clients | grep '^connected_clients:'",
    "total_keys": "redis-cli -p 6379 INFO keyspace | grep -o 'keys=[0-9]*' | awk -F= '{sum+=$2} END {print sum}'",
    "blocked_clients": "redis-cli -p 6379 INFO clients | grep '^blocked_clients:'",
    "instantaneous_ops_per_sec": "redis-cli -p 6379 INFO stats | grep '^instantaneous_ops_per_sec:'",
    "total_connections_received": "redis-cli -p 6379 INFO stats | grep '^total_connections_received:'",
    "rejected_connections": "redis-cli -p 6379 INFO stats | grep '^rejected_connections:'",
    "expired_keys": "redis-cli -p 6379 INFO stats | grep '^expired_keys:'",
    "used_cpu_sys": "redis-cli -p 6379 INFO cpu | grep '^used_cpu_sys:'",
    "used_cpu_user": "redis-cli -p 6379 INFO cpu | grep '^used_cpu_user:'",
    "role": "redis-cli -p 6379 INFO replication | grep '^role:'",
    "connected_slaves": "redis-cli -p 6379 INFO replication | grep '^connected_slaves:'",
    "rdb_last_save_time": "redis-cli -p 6379 INFO persistence | grep '^rdb_last_save_time:'",
    "aof_enabled": "redis-cli -p 6379 INFO persistence | grep '^aof_enabled:'",
}


def format_metrics_table(metrics_data, component_type=None):
    """
    Format metrics data into displayable tables.
    
    Args:
        metrics_data: dict with keys like "cpu", "memory", "disk", "kernel", or any metric keys
        component_type: Optional component type (e.g., "nginx", "tomcat", "postgres") to use component-specific command mappings
        
    Returns:
        dict: Formatted metrics ready for display
    """
    formatted = {}
    
    # Component-specific command mapping priority based on component type
    component_priority = {
        "mssql": [MSSQL_METRIC_COMMANDS, POSTGRES_METRIC_COMMANDS, NGINX_METRIC_COMMANDS, KAFKA_METRIC_COMMANDS, TOMCAT_METRIC_COMMANDS, NODEJS_METRIC_COMMANDS, REDIS_METRIC_COMMANDS],
        "postgres": [POSTGRES_METRIC_COMMANDS, MSSQL_METRIC_COMMANDS, NGINX_METRIC_COMMANDS, KAFKA_METRIC_COMMANDS, TOMCAT_METRIC_COMMANDS, NODEJS_METRIC_COMMANDS, REDIS_METRIC_COMMANDS],
        "nginx": [NGINX_METRIC_COMMANDS, KAFKA_METRIC_COMMANDS, TOMCAT_METRIC_COMMANDS, POSTGRES_METRIC_COMMANDS, MSSQL_METRIC_COMMANDS, NODEJS_METRIC_COMMANDS, REDIS_METRIC_COMMANDS],
        "tomcat": [TOMCAT_METRIC_COMMANDS, NGINX_METRIC_COMMANDS, KAFKA_METRIC_COMMANDS, POSTGRES_METRIC_COMMANDS, MSSQL_METRIC_COMMANDS, NODEJS_METRIC_COMMANDS, REDIS_METRIC_COMMANDS],
        "kafka": [KAFKA_METRIC_COMMANDS, NGINX_METRIC_COMMANDS, TOMCAT_METRIC_COMMANDS, POSTGRES_METRIC_COMMANDS, MSSQL_METRIC_COMMANDS, NODEJS_METRIC_COMMANDS, REDIS_METRIC_COMMANDS],
        "nodejs": [NODEJS_METRIC_COMMANDS, REDIS_METRIC_COMMANDS, POSTGRES_METRIC_COMMANDS, MSSQL_METRIC_COMMANDS, NGINX_METRIC_COMMANDS, KAFKA_METRIC_COMMANDS, TOMCAT_METRIC_COMMANDS],
        "redis": [REDIS_METRIC_COMMANDS, NODEJS_METRIC_COMMANDS, POSTGRES_METRIC_COMMANDS, MSSQL_METRIC_COMMANDS, NGINX_METRIC_COMMANDS, KAFKA_METRIC_COMMANDS, TOMCAT_METRIC_COMMANDS],
    }
    
    # Default priority (when component_type is not specified or not in priority map)
    default_priority = [MSSQL_METRIC_COMMANDS, POSTGRES_METRIC_COMMANDS, NGINX_METRIC_COMMANDS, KAFKA_METRIC_COMMANDS, TOMCAT_METRIC_COMMANDS, NODEJS_METRIC_COMMANDS, REDIS_METRIC_COMMANDS]
    
    # Select command mapping priority based on component type
    command_priority = component_priority.get(component_type, default_priority) if component_type else default_priority
    
    # Process each metric in the data
    for metric_name, metric_value in metrics_data.items():
        if metric_value is None:
            continue
            
        # Check if this is a metric that we have a command mapping for
        # Use component-specific priority if available
        metric_command = None
        for cmd_dict in command_priority:
            if metric_name in cmd_dict:
                metric_command = cmd_dict[metric_name]
                break
            
        if isinstance(metric_value, str):
            # Metric is just a string - display as-is
            formatted[metric_name] = {
                "command": metric_command if metric_command else None,
                "output": metric_value,
                "error": None
            }
        elif isinstance(metric_value, dict):
            # Metric is a dict - extract command, output, error
            # Handle different possible field names
            cmd = metric_value.get("cmd") or metric_value.get("command")
            stdout = metric_value.get("stdout") or metric_value.get("output") or metric_value.get("content")
            stderr = metric_value.get("stderr")
            error = metric_value.get("error")
            
            # Check if this is a nested metrics dict (like access_log_analysis) without cmd/stdout fields
            # If so, format it nicely for display
            if not cmd and not stdout and not stderr and not error:
                # This is likely a nested metrics structure - format it nicely
                import json
                # Try to format as a readable string representation
                if isinstance(metric_value, dict):
                    # Format nested dict as key-value pairs for better readability
                    formatted_items = []
                    for k, v in metric_value.items():
                        if isinstance(v, (dict, list)):
                            formatted_items.append(f"{k}: {json.dumps(v, indent=2)}")
                        else:
                            formatted_items.append(f"{k}: {v}")
                    stdout = "\n".join(formatted_items)
                else:
                    stdout = json.dumps(metric_value, indent=2)
            
            # Use command from dict if available, otherwise try metric mapping
            final_command = cmd if cmd else (metric_command if metric_command else "N/A")
            
            formatted[metric_name] = {
                "command": final_command if final_command != "N/A" else None,
                "output": stdout if stdout else "",
                "error": stderr or error
            }
        elif isinstance(metric_value, (list, tuple)):
            # Metric is a list - convert to string
            formatted[metric_name] = {
                "command": metric_command if metric_command else None,
                "output": "\n".join(str(item) for item in metric_value),
                "error": None
            }
        else:
            # Other types - convert to string
            formatted[metric_name] = {
                "command": metric_command if metric_command else None,
                "output": str(metric_value),
                "error": None
            }
    
    return formatted

