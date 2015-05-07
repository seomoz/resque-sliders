module Resque
  module Plugins
    module ResqueSliders
      class DistributedCommander < Commander
        include Helpers

        def distributed_change(queue, count)
          distributed_delete(queue)

          host_current_workers = hosts_with_current_worker_count.sort_by{|host, current| current}

          average_current = host_current_workers.map{|a| a.last}.reduce(&:+) / host_current_workers.count

          remaining = count

          host_job_mappings = {}
          host_current_workers.each do |host, current|
            host_job_mappings[host] ||= 0
            if current < average_current && remaining > 0
              num_to_add = average_current.to_i - current
              host_job_mappings[host] += num_to_add
              remaining -= num_to_add
            end
          end

          if remaining > 0
            remaining.times do |numb|
              # this might need to be a min-priority queue where the key of an object is the number of
              # current workers
              current_host = host_current_workers.rotate!.first.first
              host_job_mappings[current_host] ||= 0
              host_job_mappings[current_host] += 1
            end
          end

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
