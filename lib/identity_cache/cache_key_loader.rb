# frozen_string_literal: true

module IdentityCache
  # A generic cache key loader that supports different types of
  # cache fetchers, each of which can use their own cache key
  # format and have their own cache miss resolvers.
  #
  # Here is the interface of a cache fetcher in the
  # [ruby-signature](https://github.com/ruby/ruby-signature)'s
  # format.
  #
  # ```
  # interface _CacheFetcher[DbKey, DbValue, CacheableValue]
  #   def cache_key: (DbKey) -> String
  #   def cache_encode: (DbValue) -> CacheableValue
  #   def cache_decode: (CacheableValue) -> DbValue
  #   def load_one_from_db: (DbKey) -> DbValue
  #   def load_multi_from_db: (Array[DbKey]) -> Hash[DbKey, DbValue]
  # end
  # ```
  module CacheKeyLoader
    class << self
      # Load a single key for a cache fetcher.
      #
      # @param cache_fetcher [_CacheFetcher]
      # @param db_key Reference to what to load from the database.
      # @return The database value corresponding to the database key.
      def load(cache_fetcher, db_key)
        cache_key = cache_fetcher.cache_key(db_key)

        db_value = nil

        cache_value = IdentityCache.fetch(cache_key) do
          db_value = cache_fetcher.load_one_from_db(db_key)
          cache_fetcher.cache_encode(db_value)
        end

        db_value || cache_fetcher.cache_decode(cache_value)
      end

      # Load multiple keys for a cache fetcher.
      #
      # @param cache_fetcher [_CacheFetcher]
      # @param db_key [Array] Reference to what to load from the database.
      # @return [Hash] A hash mapping each database key to its corresponding value
      def load_multi(cache_fetcher, db_keys)
        cache_key_to_db_key_hash = {}

        db_keys.each do |db_key|
          cache_key = cache_fetcher.cache_key(db_key)
          cache_key_to_db_key_hash[cache_key] = db_key
        end

        load_result = nil

        cache_keys = cache_key_to_db_key_hash.keys
        cache_result = cache_fetch_multi(cache_keys) do |unresolved_cache_keys|
          resolve_miss_result = {}
          load_result = resolve_multi_on_miss(cache_fetcher, unresolved_cache_keys,
            cache_key_to_db_key_hash, resolve_miss_result)
          resolve_miss_result
        end

        load_result ||= {}
        cache_result.each do |cache_key, cache_value|
          db_key = cache_key_to_db_key_hash.fetch(cache_key)
          load_result[db_key] ||= cache_fetcher.cache_decode(cache_value)
        end

        load_result
      end

      private

      def cache_fetch_multi(cache_keys)
        IdentityCache.fetch_multi(cache_keys) do |unresolved_cache_keys|
          cache_key_to_cache_value_hash = yield unresolved_cache_keys
          cache_key_to_cache_value_hash.fetch_values(*unresolved_cache_keys)
        end
      end

      def resolve_multi_on_miss(cache_fetcher, unresolved_cache_keys, cache_key_to_db_key_hash, resolve_miss_result)
        db_keys = unresolved_cache_keys.map do |cache_key|
          cache_key_to_db_key_hash.fetch(cache_key)
        end

        load_result = cache_fetcher.load_multi_from_db(db_keys)

        unresolved_cache_keys.each do |cache_key|
          db_key = cache_key_to_db_key_hash.fetch(cache_key)
          db_value = load_result[db_key]
          resolve_miss_result[cache_key] = cache_fetcher.cache_encode(db_value)
        end

        load_result
      end
    end
  end
  private_constant :CacheKeyLoader
end
