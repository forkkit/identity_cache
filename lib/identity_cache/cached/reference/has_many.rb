# frozen_string_literal: true
module IdentityCache
  module Cached
    module Reference
      class HasMany < Association # :nodoc:
        def initialize(name, inverse_name:, reflection:)
          super
          @cached_ids_name = "fetch_#{ids_name}"
          @ids_variable_name = :"@#{ids_cached_reader_name}"
        end

        attr_reader :cached_ids_name, :ids_variable_name

        def build
          reflection.active_record.class_eval(<<-RUBY, __FILE__, __LINE__ + 1)
            attr_reader :#{ids_cached_reader_name}

            def #{cached_ids_name}
              #{ids_variable_name} ||= #{ids_name}
            end

            def #{cached_accessor_name}
              association_klass = association(:#{name}).klass
              if association_klass.should_use_cache? && !#{name}.loaded?
                #{records_variable_name} ||= #{reflection.class_name}.fetch_multi(#{cached_ids_name})
              else
                #{name}.to_a
              end
            end
          RUBY

          ParentModelExpiration.add_parent_expiry_hook(self)
        end

        def read(record)
          record.public_send(cached_ids_name)
        end

        def write(record, ids)
          record.instance_variable_set(ids_variable_name, ids)
        end

        def clear(record)
          [ids_variable_name, records_variable_name].each do |ivar|
            if record.instance_variable_defined?(ivar)
              record.remove_instance_variable(ivar)
            end
          end
        end

        def fetch(records)
          fetch_embedded(records)

          ids_to_parent_record = records.each_with_object({}) do |record, hash|
            child_ids = record.send(cached_ids_name)
            child_ids.each do |child_id|
              hash[child_id] = record
            end
          end

          parent_record_to_child_records = Hash.new { |h, k| h[k] = [] }

          child_records = reflection.klass.fetch_multi(*ids_to_parent_record.keys)
          child_records.each do |child_record|
            parent_record = ids_to_parent_record[child_record.id]
            parent_record_to_child_records[parent_record] << child_record
          end

          parent_record_to_child_records.each do |parent, children|
            parent.instance_variable_set(records_variable_name, children)
          end

          child_records
        end

        private

        def embedded_fetched?(records)
          record = records.first
          super || record.instance_variable_defined?(ids_variable_name)
        end

        def singular_name
          name.to_s.singularize
        end

        def ids_name
          "#{singular_name}_ids"
        end

        def ids_cached_reader_name
          "cached_#{ids_name}"
        end
      end
    end
  end
end
