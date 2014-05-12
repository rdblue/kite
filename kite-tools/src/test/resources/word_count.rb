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
require 'kite'

analytic 'Word count' do

  map 'map phase', :from => '../LICENSE.txt' do |line|
    puts( line.inspect )
    line.split(/\s+/).each do |word|
      word.gsub! /[[:punct:]]/, ''  # remove punctuation
      word.downcase!                # convert to lower case
      emit word, 1 unless word.nil? or word.empty?
    end
  end

  combine 'combine phase' do |word, counts|
    total = 0
    counts.each do |count|
      total += count
    end
    emit word, total
  end

  parallel 'final output' do |word, count|
    puts( {:word => word, :count => count}.inspect )
    emit "#{word} => #{count}"
  end

  write 'target/output.text'

end
