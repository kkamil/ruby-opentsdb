require 'socket'
require 'opentsdb/logging'
require 'net/http'

module OpenTSDB
  class Client
    include Logging

    attr_reader :connection

    def initialize(options = {})
      begin
        @hostname = options[:hostname] || 'localhost'
        @port = options[:port] || 4242
        @connection = TCPSocket.new(@hostname, @port)
      rescue
        raise "Unable to connect or invalid connection data"
      end
    end

    def put(options = {})
      timestamp = options[:timestamp].to_i
      metric_name = options[:metric]
      value = options[:value].to_f
      tags = options[:tags].map{|k,v| "#{k}=#{v}"}.join(" ")
      @connection.puts("put #{metric_name} #{timestamp} #{value} #{tags}")
    end

    def query(options = {})
      start_date = options[:start_date].strftime("%Y/%m/%d-%H:%M:%S")
      end_date = options[:end_date].strftime("%Y/%m/%d-%H:%M:%S")
      aggr = options[:aggr]
      metric = options[:metric]
      tags = options[:tags] && options[:tags].is_a?(Hash) ? "{#{options[:tags].to_a.map{|el| el.join("=")}.join(',')}}" : ''
      query = "/q?start=#{start_date}&end=#{end_date}&m=#{aggr}:#{metric}#{tags}&ascii"
      Net::HTTP.get(@hostname, query, @port).scan(/\w+ ([0-9]+) ([-+]?[0-9]+\.?[0-9]*) *(.*)/).map{|el| OpenStruct.new(:created_at => el[0].to_i, :value => el[1].to_f, :tags => Hash[el[2].scan(/(\w+)=(\w+)/)])}
    end
  end
end
