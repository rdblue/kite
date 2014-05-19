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
  java_import 'org.kitesdk.lang.Stage'
  java_import 'org.kitesdk.lang.Script'
  java_import 'org.kitesdk.lang.generics.CustomData'

  STAGE_TYPES = {
    :parallel => Script::StageType::PARALLEL,
    :combine => Script::StageType::COMBINE,
    :reduce => Script::StageType::REDUCE
  }

  GENERIC_TYPE = Crunch::Avros.generics(CustomData::GENERIC_SCHEMA);

  class Analytic
    # for now, source and sink should be String filenames because the
    # implementation is using read_text_file and write_text_file
    def initialize( name, options={}, &block )
      @name = name
      instance_exec( &block ) if block_given?
    end

    def parallel( name, options={}, &block )
      if options[:from]
        read options[:from]
      end
      return add_stage( :parallel, name, &block )
    end
    alias_method :map, :parallel
    alias_method :extract, :parallel

    def reduce( name, options={}, &block )
      return add_stage( :reduce, name, &block )
    end
    alias_method :summarize, :reduce

    def combine( name, options={}, &block )
      return add_stage( :combine, name, &block )
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

    def get_stage( name )
      return script.get_stage( name )
    end
    alias_method :stage, :get_stage

    private

    def script
      $SCRIPT
    end

    def add_stage( stage, name, &block )
      raise RuntimeError, 'A block is required' unless block_given?

      stage_enum = STAGE_TYPES[stage]

      if block.arity == 2
        stage_base = Stage::Arity2
      else
        stage_base = Stage::Arity1
      end

      # TODO: add before/after blocks here
      stage_class = Class.new( stage_base ) do
        define_method( :call, &block )
      end

      # it is possible to name the stage class by assigning it to a constant here

      script.add_stage( name, stage_enum, stage_class.new, GENERIC_TYPE )
    end

    # where do before/after blocks go?
    # before/after blocks will be registered to a name, which can come collect
    # them from the analytic when it is time to run them
  end
end

def analytic( name, options={}, &block )
  return Kite::Analytic.new( name, options, &block )
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
  return analytic( name, options, &block )
end
