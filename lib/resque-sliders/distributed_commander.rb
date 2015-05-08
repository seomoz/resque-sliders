require 'priority_queue'

module Resque
  module Plugins
    module ResqueSliders
      class DistributedCommander < Commander
        include Helpers

        # use a min-key priority queue, based on the number of current workers per host, to 
        # determine allocation of new workers
        def distributed_change(queue, count)
          host_job_mappings = {}
          host_current_workers = host_current_worker_map
          pq = PriorityQueue.new
          host_current_workers.each do |host, current_count|
            pq[current_count] << host
          end

          count.times do |numb|
            current_host = pq.shift
            host_current_workers[current_host] += 1
            pq[host_current_workers[current_host]] << current_host
            host_job_mappings[current_host] ||= 0
            host_job_mappings[current_host] += 1
          end
          
          distributed_delete(queue)
          host_job_mappings.each do |host, job_count|
            change(host, queue, job_count, true)
          end
        end

        def all_queue_values
          all_hosts.map{|host| queue_values(host)}.inject({}) do |acc, host|
            acc.merge(host){|key, v1,v2| v1.to_i + v2.to_i}
          end
        end

        def clear_queues!
          # If this is performed without re-adding hosts that are currently stale they will be lost!
          all_hosts.each do |host|
            Resque.redis.del "plugins:resque-sliders:#{host}"
          end
        end

        def distributed_delete(queue)
          all_hosts.each do |host|
            delete(host, queue)
          end
        end

        def stop_all_hosts!
          all_hosts.each do |host|
            redis_set_hash(host_config_key, "#{host}:stop", 1)
          end
        end

        def restart_all_hosts!
          all_hosts.each do |host|
            redis_set_hash(host_config_key, "#{host}:reload", 1)
          end
        end

        def start_all_hosts!
          all_hosts.each do |host|
            redis_del_hash(host_config_key, "#{host}:stop")
          end
        end

        def force_host(host_name)
          @stale_hosts << host_name
          @stale_hosts.sort!.uniq!
        end
      end
    end
  end
end
