# 
# Copyright 2013 Cloudera Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# ensure the lib directory is in the search path
require 'pathname'
lib_path = Pathname.new(__FILE__).expand_path.parent
$LOAD_PATH << lib_path.to_s unless $LOAD_PATH.include? lib_path.to_s

module Crunch
  java_import 'org.apache.crunch.types.avro.Avros'
end

module Kite
  java_import 'org.kitesdk.lang.Carrier'
  java_import 'org.kitesdk.lang.Script'
  java_import 'org.kitesdk.lang.generics.CustomData'

  CARRIER_TYPES = {
    :parallel => Carrier::Type::PARALLEL,
    :combine => Carrier::Type::COMBINE,
    :reduce => Carrier::Type::REDUCE
  }

  GENERIC_TYPE = Crunch::Avros.generics(CustomData::GENERIC_SCHEMA);

  module_function

  def parallel( name, options={}, &block )
    if options[:from]
      read options[:from]
    end
    return add_carrier( :parallel, name, &block )
  end
  alias_method :map, :parallel
  alias_method :extract, :parallel

  def reduce( name, options={}, &block )
    return add_carrier( :reduce, name, &block )
  end
  alias_method :summarize, :reduce

  def combine( name, options={}, &block )
    return add_carrier( :combine, name, &block )
  end

  def read( source )
    script.read( source )
  end

  def write( sink )
    script.write( sink )
    nil
  end

  def group!
    script.group
  end

  def verbose!
    script.enable_debug
  end

  def get_carrier( name )
    return script.get_carrier( name )
  end
  alias_method :carrier, :get_carrier

  private

  def script
    $SCRIPT
  end

  def add_carrier( type_sym, name, &block )
    raise RuntimeError, 'A block is required' unless block_given?

    carrier_type = CARRIER_TYPES[ type_sym ]

    if block.arity == 2
      base = Carrier::Arity2
    else
      base = Carrier::Arity1
    end

    # TODO: add before/after blocks here
    carrier_class = Class.new( base ) do

      attr_reader :name, :type, :keyType, :valueType

      def initialize( name, type, key, value )
        super() # superclass has no-arg constructor
        @name = name
        @type = type
        @keyType = key
        @valueType = value
      end

      define_method( :call, &block )

    end

    # it is possible to name the class by assigning it to a constant here

    script.add_carrier( carrier_class.new(
        name, carrier_type, GENERIC_TYPE, GENERIC_TYPE
      ) )
  end

  # where do before/after blocks go?
  # before/after blocks will be registered to a name, which can come collect
  # them from the analytic when it is time to run them
end


def bananalytic( name, options={}, &block )
  puts " _"
  puts "//\\"
  puts "V  \\"
  puts " \\  \\_"
  puts "  \\,'.`-.          BANANALYTICS!"
  puts "   |\\ `. `."
  puts "   ( \\  `. `-.                        _,.-:\\"
  puts "    \\ \\   `.  `-._             __..--' ,-';/"
  puts "     \\ `.   `-.   `-..___..---'   _.--' ,'/"
  puts "      `. `.    `-._        __..--'    ,' /"
  puts "        `. `-_     ``--..''       _.-' ,'"
  puts "          `-_ `-.___        __,--'   ,'"
  puts '             `-.__  `----"""    __.-\''
  puts "                  `--..____..--'"
  instance_exec( &block ) if block_given?
end
