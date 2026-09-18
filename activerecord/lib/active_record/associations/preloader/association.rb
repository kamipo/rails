# frozen_string_literal: true

# :enddoc:

module ActiveRecord
  module Associations
    class Preloader
      class Association # :nodoc:
        class LoaderQuery
          attr_reader :scope, :destination_key_name

          def initialize(scope, destination_key_name)
            @scope = scope
            @destination_key_name = destination_key_name
          end

          def eql?(other)
            destination_key_name == other.destination_key_name &&
              scope.table_name == other.scope.table_name &&
              scope.model.connection_specification_name == other.scope.model.connection_specification_name &&
              scope.values_for_queries == other.scope.values_for_queries
          end

          def hash
            [destination_key_name, scope.model.table_name, scope.model.connection_specification_name, scope.values_for_queries].hash
          end

          def records_for(loaders)
            LoaderRecords.new(loaders, self).records
          end

          def load_records_in_batch(loaders)
            raw_records = records_for(loaders)

            loaders.each do |loader|
              loader.load_records(raw_records)
              loader.run
            end
          end

          def load_records_for_keys(keys, &block)
            return [] if keys.empty?

            filtered = apply_key_filter(keys)

            if ActiveRecord.respect_association_scope_limits &&
                (filtered.limit_value || filtered.offset_value)
              filtered = partition_by_owner(filtered, keys)
            end

            filtered.load(&block)
          end

          private
            def apply_key_filter(keys)
              if destination_key_name.is_a?(Array)
                query_constraints = Hash.new { |hsh, key| hsh[key] = Set.new }
                keys.each_with_object(query_constraints) do |values_set, constraints|
                  destination_key_name.each_with_index do |key_name, index|
                    constraints[key_name] << values_set[index]
                  end
                end

                scope.where(query_constraints)
              else
                scope.where(destination_key_name => keys)
              end
            end

            def partition_by_owner(relation, keys)
              relation.model.with_connection do |connection|
                if connection.supports_lateral_joins? && connection.supports_values_table?
                  partition_by_lateral(relation, keys, connection)
                else
                  relation
                end
              end
            end

            def partition_by_lateral(relation, keys, connection)
              model = relation.model
              table = model.arel_table
              key_names = Array(destination_key_name)
              owners = Arel::Table.new(name: "__preload_owners")

              # A row per owner key to run the scope against. The keys travel as
              # bind parameters, so batches of the same size can share a
              # prepared statement.
              types = key_names.map { |name| model.type_for_attribute(name) }
              rows = keys.map do |values|
                Array(values).each_with_index.map do |value, i|
                  Arel::Nodes::BindParam.new(Relation::QueryAttribute.new(key_names[i], value, types[i]))
                end
              end
              owner_keys = connection.values_table(
                owners.name, key_names, rows,
                sql_types: key_names.map { |name| model.columns_hash[name]&.sql_type }
              )

              # Correlate on `scope`, not on the filtered relation: `unscope`ing
              # its `IN` filter would take the scope's own conditions on the key
              # column with it.
              inner = scope.where(
                key_names.map { |name| table[name].eq(owners[name]) }.inject(:and)
              )

              join = Arel::Nodes::CrossJoin.new(
                Arel::Nodes::TableAlias.new(Arel::Nodes::Lateral.new(inner.arel.ast), table.name)
              )

              lateral = model.unscoped.from(owner_keys)
                             .joins(join)
                             .select(table[Arel.star])

              orders = outer_orders(relation)
              orders.empty? ? lateral : lateral.order(*orders)
            end

            # The join is unordered, so the scope's order is repeated outside --
            # but only what the subquery projects can be named there. An order
            # left behind still picks the rows; only their order falls to the
            # database.
            def outer_orders(relation)
              orders = relation.arel.orders
              return [] unless orders.all? { |order| projected?(order, relation) }

              orders
            end

            def projected?(order, relation)
              order = order.expr if order.is_a?(Arel::Nodes::Ordering)
              return false unless order.is_a?(Arel::Attributes::Attribute)
              return false unless order.relation.name == relation.model.arel_table.name

              # Without a `select` every column comes back; with one, only a
              # column named plainly in it.
              relation.select_values.empty? || relation.select_values.any? do |value|
                (value.is_a?(Symbol) || value.is_a?(String)) && value.to_s == order.name.to_s
              end
            end
        end

        class LoaderRecords
          def initialize(loaders, loader_query)
            @loader_query = loader_query
            @loaders = loaders
            @keys_to_load = Set.new
            @already_loaded_records_by_key = {}

            populate_keys_to_load_and_already_loaded_records
          end

          def records
            load_records + already_loaded_records
          end

          private
            attr_reader :loader_query, :loaders, :keys_to_load, :already_loaded_records_by_key

            def populate_keys_to_load_and_already_loaded_records
              loaders.each do |loader|
                loader.owners_by_key.each do |key, owners|
                  if loaded_owner = owners.find { |owner| loader.loaded?(owner) }
                    already_loaded_records_by_key[key] = loader.target_for(loaded_owner)
                  else
                    keys_to_load << key
                  end
                end
              end

              @keys_to_load.subtract(already_loaded_records_by_key.keys)
            end

            def load_records
              loader_query.load_records_for_keys(keys_to_load) do |record|
                loaders.each { |l|  l.set_inverse(record) }
              end
            end

            def already_loaded_records
              already_loaded_records_by_key.values.flatten
            end
        end

        def self.association_route_for(reflection, owner_class, klass)
          reflection.association_route(origin_class: owner_class, destination_class: klass)
        end

        attr_reader :klass

        def initialize(klass, owners, reflection, preload_scope, reflection_scope, associate_by_default)
          @klass         = klass
          @owners        = owners.uniq(&:__id__)
          @reflection    = reflection
          @preload_scope = preload_scope
          @reflection_scope = reflection_scope
          @associate     = associate_by_default || !preload_scope || preload_scope.empty_scope?
          @model         = owners.first && owners.first.class
          @reuse_loaded_target = true
          @run = false
        end

        def table_name
          @klass.table_name
        end

        def future_classes
          if run?
            []
          else
            [@klass]
          end
        end

        def runnable_loaders
          [self]
        end

        def run?
          @run
        end

        def run
          return self if run?
          @run = true

          records = records_by_owner

          owners.each do |owner|
            associate_records_to_owner(owner, records[owner] || [])
          end if @associate

          self
        end

        def records_by_owner
          load_records unless defined?(@records_by_owner)

          @records_by_owner
        end

        def preloaded_records
          load_records unless defined?(@preloaded_records)

          @preloaded_records
        end

        # The name of the key on the associated records
        def destination_key_name
          association_route.destination_key.name
        end

        def loader_query
          LoaderQuery.new(scope, destination_key_name)
        end

        def owners_by_key
          @owners_by_key ||= begin
            route = association_route
            owners.each_with_object({}) do |owner, result|
              next unless route.origin_reference_complete?(owner)

              (result[origin_key_value(owner)] ||= []) << owner
            end
          end
        end

        # Owners' already loaded targets don't reflect the preload scope, so
        # they must not be reused when the scope filters the records.
        def ignore_loaded_target!
          @reuse_loaded_target = false
        end

        def loaded?(owner)
          @reuse_loaded_target && owner.association(reflection.name).loaded?
        end

        def target_for(owner)
          Array.wrap(owner.association(reflection.name).target)
        end

        def scope
          @scope ||= build_scope
        end

        def set_inverse(record)
          if owners = owners_by_key[destination_key_value(record)]
            # Processing only the first owner
            # because the record is modified but not an owner
            association = owners.first.association(reflection.name)
            association.set_inverse_instance(record)
          end
        end

        def load_records(raw_records = nil)
          # owners can be duplicated when a relation has a collection association join
          # #compare_by_identity makes such owners different hash keys
          @records_by_owner = {}.compare_by_identity
          raw_records ||= loader_query.records_for([self])
          @preloaded_records = raw_records.select do |record|
            assignments = false

            owners_by_key[destination_key_value(record)]&.each do |owner|
              entries = (@records_by_owner[owner] ||= [])

              if reflection.collection? || entries.empty?
                entries << record
                assignments = true
              end
            end

            assignments
          end
        end

        def associate_records_from_unscoped(unscoped_records)
          return if unscoped_records.nil? || unscoped_records.empty?
          return if !reflection_scope.empty_scope?
          return if preload_scope && !preload_scope.empty_scope?
          return if reflection.collection?

          unscoped_records.each do |record|
            owners = owners_by_key[destination_key_value(record)]
            owners&.each_with_index do |owner, i|
              association = owner.association(reflection.name)
              association.target = record

              if i == 0 # Set inverse on first owner
                association.set_inverse_instance(record)
              end
            end
          end
        end

        private
          attr_reader :owners, :reflection, :preload_scope, :model

          def association_route
            @association_route ||= self.class.association_route_for(reflection, model, klass)
          end

          def associate_records_to_owner(owner, records)
            return if loaded?(owner)

            association = owner.association(reflection.name)

            if reflection.collection?
              not_persisted_records = association.target.reject(&:persisted?)
              association.target = records + not_persisted_records
            else
              association.target = records.first
            end
          end

          # Owners are grouped so that each loader's owners share its route.
          def origin_key_value(record)
            association_route._origin_key._value_of(record, match_normalizers.first)
          end

          # Loaded records can be subclasses of klass with their own aliases.
          def destination_key_value(record)
            if record.class.equal?(klass)
              association_route._destination_key._value_of(record, match_normalizers.last)
            else
              association_route.destination_key.value_of(record, match_normalizers.last)
            end
          end

          def match_normalizers
            @match_normalizers ||= association_route.match_normalizers(origin_class: model, destination_class: klass)
          end

          def reflection_scope
            @reflection_scope ||= reflection.join_scopes(klass.arel_table, klass.predicate_builder, klass).inject(klass.unscoped, &:merge!)
          end

          def destination_fixed_values
            association_route.destination_fixed_values
          end

          def build_scope
            scope = klass.scope_for_association
            fixed_values = destination_fixed_values
            scope._where!(fixed_values) unless fixed_values.empty?

            scope.merge!(reflection_scope) unless reflection_scope.empty_scope?
            scope.default_order!(reflection.options[:default_order]) if reflection.options[:default_order].present?

            if preload_scope && !preload_scope.empty_scope?
              scope.merge!(preload_scope)
            end

            cascade_strict_loading(scope)
          end

          def cascade_strict_loading(scope)
            preload_scope&.strict_loading_value ? scope.strict_loading : scope
          end
      end
    end
  end
end
