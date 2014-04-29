/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.cli.commands;

import com.beust.jcommander.internal.Lists;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.ValidationException;
import org.slf4j.Logger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TestCreatePartitionStrategyCommand {
  private Logger console;
  private CreatePartitionStrategyCommand command;

  @Before
  public void setupCommand() {
    this.console = mock(Logger.class);
    this.command = new CreatePartitionStrategyCommand(console);
    command.setConf(new Configuration());
    command.avroSchemaFile = "user.avsc";
  }

  @Test
  public void testBasic() throws Exception {
    command.partitions = Lists.newArrayList(
        "username:hash[16]", "username:copy");
    command.run();

    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .hash("username", 16)
        .identity("username", "username_copy", Object.class, -1)
        .build();
    verify(console).info(strategy.toString(true));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testTime() throws Exception {
    command.partitions = Lists.newArrayList(
        "created_at:year", "created_at:month", "created_at:day",
        "created_at:hour", "created_at:minute"
    );
    command.run();

    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .year("created_at")
        .month("created_at")
        .day("created_at")
        .hour("created_at")
        .minute("created_at")
        .build();
    verify(console).info(strategy.toString(true));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testMissingWidth() throws Exception {
    command.partitions = Lists.newArrayList("username:hash");
    TestHelpers.assertThrows("Should reject missing hash width",
        ValidationException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            command.run();
            return null;
          }
        });
  }

  @Test
  public void testUnknownSourceField() {
    command.partitions = Lists.newArrayList("id:copy");
    TestHelpers.assertThrows("Should reject missing field \"id\"",
        IllegalStateException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            command.run();
            return null;
          }
        });
  }

  @Test
  public void testUnknownType() {
    command.partitions = Lists.newArrayList("username:id");
    TestHelpers.assertThrows("Should unknown partition function \"id\"",
        ValidationException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            command.run();
            return null;
          }
        });
  }

  private long encodeDouble(double d) {
    long j = Double.doubleToLongBits(d);
    j ^= (j >> 63) | Double.doubleToLongBits(Double.MIN_VALUE);
    return j;
  }

  @Test
  public void testEncoding() {
    long posInf = encodeDouble(Double.POSITIVE_INFINITY);
    long negInf = encodeDouble(Double.NEGATIVE_INFINITY);
    long nan = encodeDouble(Double.NaN);

    long one = encodeDouble(-341234.12);
    long zero = (encodeDouble(0.0) | Double.doubleToLongBits(Double.MIN_VALUE));
    long two = encodeDouble(5128.10239871);

    Assert.assertTrue("+inf > -inf", posInf > negInf);
    Assert.assertTrue("+inf > one", posInf > one);
    Assert.assertTrue("+inf > two", posInf > two);
    Assert.assertTrue("two > one", two > one);
    Assert.assertTrue("two > 0.0", two > zero);
    Assert.assertTrue("0.0 > one", zero > one);
    Assert.assertTrue("one > -inf", one > negInf);
    Assert.assertTrue("two > -inf", two > negInf);
    Assert.assertTrue("nan > +inf", nan > posInf);
    Assert.assertTrue("+inf > dmax", posInf > encodeDouble(Double.MAX_VALUE));
    Assert.assertTrue("nan > dmax", nan > encodeDouble(Double.MAX_VALUE));
    //Assert.assertTrue("dmin > -inf", encodeDouble(Double.MIN_VALUE) > negInf);
  }
}
