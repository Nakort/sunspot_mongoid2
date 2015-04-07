require 'sunspot'
require 'mongoid'
require 'sunspot/rails'

module Sunspot
  module Mongoid2
    def self.included(base)
      base.class_eval do
        extend Sunspot::Rails::Searchable::ActsAsMethods
        extend Sunspot::Mongoid2::ActsAsMethods
        Sunspot::Adapters::DataAccessor.register(DataAccessor, base)
        Sunspot::Adapters::InstanceAdapter.register(InstanceAdapter, base)
      end
    end

    module ActsAsMethods
      # ClassMethods isn't loaded until searchable is called so we need
      # call it, then extend our own ClassMethods.
      def searchable(opt = {}, &block)
        super
        extend ClassMethods
      end
    end

    module ClassMethods
      # The sunspot solr_index method is very dependent on ActiveRecord, so
      # we'll change it to work more efficiently with Mongoid.
      def solr_index(opt={})
        Sunspot.index!(all)
      end

      def solr_index_orphans(opts={})
        batch_size = opts[:batch_size] || Sunspot.config.indexing.default_batch_size          

        solr_page = 0
        solr_ids = []
        while (solr_page = solr_page.next)
          ids = solr_search_ids { paginate(:page => solr_page, :per_page => 1000) }.to_a
          break if ids.empty?
          solr_ids.concat ids
        end

        return solr_ids - self.pluck(:id).map(&:to_s)
      end

      # 
      # Find IDs of records of this class that are indexed in Solr but do not
      # exist in the database, and remove them from Solr. Under normal
      # circumstances, this should not be necessary; this method is provided
      # in case something goes wrong.
      #
      # ==== Options (passed as a hash)
      #
      # batch_size<Integer>:: Batch size with which to load records
      #                       Default is 50
      # 
      def solr_clean_index_orphans(opts={})
        solr_index_orphans(opts).each do |id|
          new do |fake_instance|
            fake_instance._id = id
          end.solr_remove_from_index
        end
      end
    end


    class InstanceAdapter < Sunspot::Adapters::InstanceAdapter
      def id
        @instance.id
      end
    end

    class DataAccessor < Sunspot::Adapters::DataAccessor

      attr_accessor :include

      def initialize(clazz)
        super(clazz)
        @inherited_attributes = [:include]
      end

      def scope_for_load
        scope = @clazz
        scope = scope.includes(@include) if @include.present?
        scope 
      end

      def load(id)
        scope_for_load.find(bson_id(id)) rescue nil
      end

      def load_all(ids)
        scope_for_load.where(:_id.in => ids.map { |id| bson_id(id) })
      end

      def bson_id(id)
        if Gem::Version.new(Mongoid::VERSION) >= Gem::Version.new('4')
          ::BSON::ObjectId.from_string(id)
        elsif Gem::Version.new(Mongoid::VERSION) >= Gem::Version.new('3')
          ::Moped::BSON::ObjectId.from_string(id)
        else
          ::BSON::ObjectId.from_string(id)
        end
      end
    end
  end
end
