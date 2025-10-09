# frozen_string_literal: true

require 'aws-sdk-rds'
require 'uri'

# Module to generate AWS IAM authentication tokens for RDS
module IamAuth
  class << self
    attr_accessor :token_cache, :cache_mutex
  end

  self.token_cache = {}
  self.cache_mutex = Mutex.new

  def self.generate_token(database_url, logger: nil)
    uri = URI.parse(database_url)

    # Extract host, port, and username from the connection URL
    host = uri.host
    port = uri.port || 5432
    username = uri.user

    raise 'Username must be present in database URL for IAM authentication' unless username
    raise 'Host must be present in database URL for IAM authentication' unless host

    # Check cache first
    cache_key = "#{username}@#{host}:#{port}"
    cache_mutex.synchronize do
      cached = token_cache[cache_key]
      if cached && cached[:expires_at] > Time.now
        return cached[:token]
      end
    end

    logger&.debug("Generating IAM authentication token for #{username}@#{host}:#{port}")

    # Create RDS auth token generator with default credentials
    # This will automatically use IAM instance profile, AWS profiles, or environment variables
    client = Aws::RDS::AuthTokenGenerator.new(
      credentials: Aws::CredentialProviderChain.new.resolve
    )

    # Generate authentication token (valid for 15 minutes)
    token = client.auth_token(
      region: extract_region_from_host(host),
      endpoint: "#{host}:#{port}",
      user_name: username
    )

    # Cache the token for 90% of its lifetime (15 minutes * 0.9 = 13.5 minutes)
    cache_mutex.synchronize do
      token_cache[cache_key] = {
        token: token,
        expires_at: Time.now + (15 * 60 * 0.9)
      }
    end

    token
  end

  def self.extract_region_from_host(host)
    # RDS endpoint format: instance-name.random-id.region.rds.amazonaws.com
    # Aurora endpoint format: instance-name.cluster-random-id.region.rds.amazonaws.com
    parts = host.split('.')

    # Find the region part (typically 3rd or 4th segment from the end)
    if parts.length >= 4
      # Check if it's a valid AWS region format
      parts.each do |part|
        return part if part.match?(/^[a-z]{2}-[a-z]+-\d+$/)
      end
    end

    # Fallback: try to get region from AWS SDK default configuration
    # This will use AWS_REGION env var, AWS config file, or EC2 metadata
    begin
      Aws.config[:region] || ENV['AWS_REGION'] || ENV['AWS_DEFAULT_REGION'] || raise('Region not found')
    rescue StandardError
      raise "Unable to extract AWS region from host '#{host}'. " \
            'Please set AWS_REGION environment variable or use standard RDS endpoint format.'
    end
  end
end
