# frozen_string_literal: true

require 'minitest/autorun'
require 'rubygems'

class DilockGemspecTest < Minitest::Test
  def test_gemspec_includes_every_ruby_runtime_source
    sdk_root = File.expand_path('..', __dir__)
    specification = Dir.chdir(sdk_root) { Gem::Specification.load('dilock.gemspec') }
    runtime_sources = Dir.glob(File.join(sdk_root, 'lib/**/*.rb')).map do |path|
      path.delete_prefix("#{sdk_root}#{File::SEPARATOR}").tr('\\', '/')
    end

    assert specification, 'dilock.gemspec must load'
    missing = runtime_sources - specification.files
    assert_empty missing, "Ruby runtime source omitted from the gem: #{missing.join(', ')}"
  end
end
