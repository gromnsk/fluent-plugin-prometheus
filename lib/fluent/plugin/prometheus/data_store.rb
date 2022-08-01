# The default Prometheus client data store has no means of removing values.
# For the "retention" feature we need to be able to remove metrics with specific labels after some time of inactivity.
# By patching the Metric class and using our own DataStore we implement that missing feature.
module Prometheus
  module Client
    class Metric
      def remove(labels)
        label_set = label_set_for(labels)
        @store.remove(labels: label_set)
      end

      def reset_values
        @store.reset_values
      end
    end
  end
end

module Fluent
  module Plugin
    module Prometheus
      # Stores all the data in simple hashes, one per metric. Each of these metrics
      # synchronizes access to their hash, but multiple metrics can run observations
      # concurrently.
      class DataStore
        class InvalidStoreSettingsError < StandardError; end
        DEFAULT_METRIC_SETTINGS = { topk: 0 }

        def for_metric(metric_name, metric_type:, metric_settings: {})
          settings = DEFAULT_METRIC_SETTINGS.merge(metric_settings)
          validate_metric_settings(metric_settings: settings)
          MetricStore.new(metric_settings: settings)
        end

        private

        def validate_metric_settings(metric_settings:)
          unless metric_settings.has_key?(:topk) &&
            (metric_settings[:topk].is_a? Integer) &&
            metric_settings[:topk] >= 0
            raise InvalidStoreSettingsError,
                  "Metrics need a valid :topk key"
          end
        end

        class MetricStore
          def initialize(metric_settings:)
            @internal_store = Hash.new { |hash, key| hash[key] = 0.0 }
            @topk = metric_settings[:topk]
            @lock = Monitor.new
          end

          def synchronize
            @lock.synchronize { yield }
          end

          def set(labels:, val:)
            synchronize do
              @internal_store[labels] = val.to_f
            end
          end

          def increment(labels:, by: 1)
            synchronize do
              @internal_store[labels] += by
            end
          end

          def get(labels:)
            synchronize do
              @internal_store[labels]
            end
          end

          def remove(labels:)
            synchronize do
              @internal_store.delete(labels)
            end
          end

          def all_values
            synchronize do
              store = @internal_store.dup
              if @topk > 0
                store.sort_by { |_, value| -value }.first(@topk).to_h
              else
                store
              end
            end
          end

          def reset_values
            synchronize do
              @internal_store.transform_values! { |_| 0.0 }
            end
          end
        end

        private_constant :MetricStore
      end
    end
  end
end
