# frozen_string_literal: true

module Dilock
  # Encodes and validates the shared namespace metadata contract.
  # @private Used by backend providers during connection setup.
  module NamespaceMetadata
    # Shared metadata protocol major version.
    # @return [Integer]
    PROTOCOL_MAJOR = 1
    # Shared resource identity encoding identifier.
    # @return [String]
    IDENTITY_SCHEME = 'sha256-framed-utf8-v1'

    module_function

    def encode(profile, epoch)
      raise ArgumentError, 'metadata epoch is required' unless epoch.is_a?(String) && !epoch.empty?

      JSON.generate(
        protocol_major: PROTOCOL_MAJOR,
        identity_scheme: IDENTITY_SCHEME,
        profile: profile,
        coordination_epoch: epoch,
        fixed_lease_ms: nil
      )
    end

    # Validates all protocol fields before a provider can use the namespace.
    # @param raw [String] encoded metadata value
    # @param profile [String] selected backend profile identifier
    # @param epoch [String] expected coordination epoch
    # @return [true]
    # @raise [ProtocolMismatch] when metadata is invalid or differs from expectations
    def validate!(raw, profile, epoch)
      validate_size!(raw)
      value = parse(raw)
      validate_object!(value)
      validate_fields!(value, profile, epoch)
      true
    rescue JSON::ParserError, TypeError, ArgumentError => e
      raise ProtocolMismatch.new("invalid namespace metadata: #{e.message}", category: :protocol_mismatch)
    end

    def validate_size!(raw)
      return if raw.is_a?(String) && raw.bytesize <= 4_096

      raise ProtocolMismatch.new('namespace metadata exceeds 4096 bytes', category: :protocol_mismatch)
    end
    private_class_method :validate_size!

    def parse(raw)
      JSON.parse(raw, object_class: UniqueHash, max_nesting: 8)
    end
    private_class_method :parse

    def validate_object!(value)
      return if value.is_a?(Hash)

      raise ProtocolMismatch.new('namespace metadata must be a JSON object', category: :protocol_mismatch)
    end
    private_class_method :validate_object!

    def validate_fields!(value, profile, epoch)
      expected_metadata(profile, epoch).each do |field, expected_value|
        next if value.key?(field) && value[field] == expected_value

        raise ProtocolMismatch.new("namespace metadata mismatch for #{field}",
                                   category: :protocol_mismatch)
      end
      return if value['fixed_lease_ms'].nil?

      raise ProtocolMismatch.new('namespace metadata has unsupported fixed lease',
                                 category: :protocol_mismatch)
    end
    private_class_method :validate_fields!

    def expected_metadata(profile, epoch)
      { 'protocol_major' => PROTOCOL_MAJOR, 'identity_scheme' => IDENTITY_SCHEME,
        'profile' => profile, 'coordination_epoch' => epoch, 'fixed_lease_ms' => nil }
    end
    private_class_method :expected_metadata

    # Rejects repeated JSON keys instead of accepting an ambiguous contract.
    class UniqueHash < Hash
      def []=(key, value)
        raise ArgumentError, "duplicate JSON field #{key.inspect}" if key?(key)

        super
      end
    end
    private_constant :UniqueHash
  end
end
