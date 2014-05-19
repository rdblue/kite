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

if not _script:
    raise RuntimeError('[BUG] _script is not set!')

import types
import inspect

# crunch
import org.apache.crunch.types.avro.Avros as Avros

# kite java classes
import org.kitesdk.lang.generics.CustomData.GENERIC_SCHEMA as SCHEMA
import org.kitesdk.lang.Stage as Stage
import org.kitesdk.lang.Script as Script

# set up avro types
_generic_type = Avros.generics(SCHEMA)


# decorators that use the last analytic

def parallel( *args, **kwargs ):
    """ Decorates a function to be used as a parallel do.

    Decorating a function with this method will add that function to the
    existing pipeline to process the collection produced by the last DoFn.  If
    arguments are provided, they are used for configuration.

    Currently, a single String argument is passed to read and other arguments
    are ignored. The collection produced by read is used as the input to the
    function.

    Example:
      @parallel
      def func( context, key[, value] ):
          pass
    """
    return _decorator( 'parallel', *args, **kwargs )

map = parallel

def combine( *args, **kwargs ):
    """ Decorates a function to be used as a combiner.

    Decorating a function with this method will add that function to the
    existing pipeline to process the (grouped) collection produced by the last
    DoFn. If the last collection is a PTable, it will be grouped. If the last
    collection is a PCollection, it will be grouped with null values.

    The collection produced by this function is a PTable.

    Example:
      @combine
      def combine_func( context, key, value ):
          pass
    """
    return _decorator( 'combine', *args, **kwargs )

def reduce( *args, **kwargs ):
    """ Decorates a function to be used as a reducer.

    Decorating a function with this method will add that function to the
    existing pipeline to process the (grouped) collection produced by the last
    DoFn. If the last collection is a PTable, it will be grouped. If the last
    collection is a PCollection, it will be grouped with null values.

    Example:
      @reduce
      def combine_func( context, key, value ):
          pass
    """
    return _decorator( 'reduce', *args, **kwargs )

summarize = reduce

def read( *args, **kwargs ):
    """ Reads source_file text into a PCollection of lines.
    """
    return _script.read( *args, **kwargs )

def write( *args, **kwargs ):
    """ Writes the last collection to a text file
    """
    return _script.write( *args, **kwargs )

def group():
    """ Adds a group-by operation to the last collection.

    If the last collection is a PTable, it will be grouped. If the last
    collection is a PCollection, it will be grouped by key with null values if
    the collection does not contain Pairs.  If the last collection is a
    PGroupedTable, this will do nothing.
    """
    _script.group()

def verbose():
    """ Enables debugging on the underlying pipeline.
    """
    _script.enableDebug()

def _make_stage( func ):
    """ Returns an instance of the java class with func as the call method
    """
    if _arity(func) == 3: # 1 added for self / context
        stage_type = Stage.Arity2
    else:
        stage_type = Stage.Arity1

    inst_class = type( func.func_name, (stage_type,), {
            # this is where setup/teardown methods go, too
            'call': func,
            'name': func.func_name,
            '__call__': func.__call__
        } )
    return inst_class() # return an instance

def _decorator( stage_name, *args, **kwargs ):
    """ Creates a decorator that adds the function as the given stage

    Because this method may be used as a decorator itself, it will check
    the args and run the decorator function if necessary.
    """
    def decorator( func ):
        stage = _make_stage( func )
        # add it to the script
        _script.addStage( func.func_name, stage_name, stage, _generic_type )
        # return a Function; the decorated object should still be callable
        return stage

    func, others = _split_args( types.FunctionType, args )
    if func:
        # used as a decorator
        return decorator( func )
    else:
        # used as a decorator factory
        if len(others) > 0 and stage_name == 'parallel':
            _script.read( others[0] )
        return decorator

def _split_args( some_type, args ):
    if len( args ) >= 1 and isinstance( args[0], some_type ):
        return args[0], args[1:]
    else:
        return None, args

def _arity( func ):
    return len( inspect.getargspec( func ).args )

# in an older version, the infected class was wrapped in a Function object like
# this. I forget why, so I'm keeping this around until the same functionality
# is reimplemented, in case it is needed.
#class Function(object):
#    def __init__( self, func, infected ):
#        self.raw_func = func
#        self.infected = infected
#
#    def name( self ):
#        return self.raw_func.func_name
#
#    # this should still be usable as a function
#    def __call__( self, *args, **kwargs ):
#        self.raw_func( *args, **kwargs )

# helpers

# it's possible to set __getattr__ on some java classes to make configuration
# easier by aliasing lower_case_names to camelCase equivalents
#
#def getattr_camelcase():
#    pass
#
#def to_camel_case(str):
#    pass
