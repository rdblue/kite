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
import re
import string
from kite import *

ws = re.compile(r'\s+')

@map("../LICENSE.txt")
def each_line( context, line ):
    for word in ws.split( line ):
        context.emit( word.strip(string.punctuation).lower(), 1 )

@combine
def sum( context, word, counts ):
    total = 0
    for count in counts:
        total += count
    context.emit( word, total )

@parallel
def rebase( context, word, count ):
    print( repr( {'word': word, 'count': count} ) )
    context.emit( 'word', 1 )

@reduce
def final_output( context, word, counts ):
    total = 0
    for count in counts:
        total += count
    context.emit( "unique words => %d" % (total,) )

write( "target/output.text" )
