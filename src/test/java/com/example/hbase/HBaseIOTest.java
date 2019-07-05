/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.hbase;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test HBaseIO. */
@RunWith(JUnit4.class)
public class HBaseIOTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public TemporaryHBaseTable tmpTable = new TemporaryHBaseTable();

  private static HBaseTestingUtility htu;
  private static Admin admin;

  private static final Configuration conf = HBaseConfiguration.create();
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("info");
  private static final byte[] COLUMN_NAME = Bytes.toBytes("name");
  private static final byte[] COLUMN_EMAIL = Bytes.toBytes("email");

  @BeforeClass
  public static void beforeClass() throws Exception {
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    // Try to bind the hostname to localhost to solve an issue when it is not configured or
    // no DNS resolution available.
    conf.setStrings("hbase.master.hostname", "localhost");
    conf.setStrings("hbase.regionserver.hostname", "localhost");
    htu = new HBaseTestingUtility(conf);

    // We don't use the full htu.startMiniCluster() to avoid starting unneeded HDFS/MR daemons
    MiniHBaseCluster hbm = htu.startMiniCluster(4);
    hbm.waitForActiveAndReadyMaster();
    admin = htu.getConnection().getAdmin();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (admin != null) {
      admin.close();
      admin = null;
    }
    if (htu != null) {
      htu.shutdownMiniHBaseCluster();
      htu.cleanupDataTestDirOnTestFS();
      htu = null;
    }
  }

  @Test
  public void testRead() throws Exception {
    final String table = tmpTable.getName();
    createAndWriteData(table, 10);
    List<Result> results = readTable(table, new Scan());
    for (Result result : results) {
      System.out.println(result);
    }
  }

  // HBase helper methods
  private static void createTable(String tableId) throws Exception {
    byte[][] splitKeys = {
      "4".getBytes(StandardCharsets.UTF_8),
      "8".getBytes(StandardCharsets.UTF_8),
      "C".getBytes(StandardCharsets.UTF_8)
    };
    createTable(tableId, COLUMN_FAMILY, splitKeys);
  }

  private static void createTable(String tableId, byte[] columnFamily, byte[][] splitKeys)
      throws Exception {
    TableName tableName = TableName.valueOf(tableId);
    ColumnFamilyDescriptor colDef = ColumnFamilyDescriptorBuilder.of(columnFamily);
    TableDescriptor desc =
        TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(colDef).build();
    admin.createTable(desc, splitKeys);
  }

  /** Helper function to create a table and return the rows that it created. */
  private static void writeData(String tableId, int numRows) throws Exception {
    Connection connection = admin.getConnection();
    TableName tableName = TableName.valueOf(tableId);
    BufferedMutator mutator = connection.getBufferedMutator(tableName);
    List<Mutation> mutations = makeTableData(numRows);
    mutator.mutate(mutations);
    mutator.flush();
    mutator.close();
  }

  private static void createAndWriteData(final String tableId, final int numRows) throws Exception {
    createTable(tableId);
    writeData(tableId, numRows);
  }

  private static List<Mutation> makeTableData(int numRows) {
    List<Mutation> mutations = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; ++i) {
      // We pad values in hex order 0,1, ... ,F,0, ...
      String prefix = String.format("%X", i % 16);
      // This 21 is to have a key longer than an input
      byte[] rowKey = Bytes.toBytes(StringUtils.leftPad("_" + String.valueOf(i), 21, prefix));
      byte[] value = Bytes.toBytes(String.valueOf(i));
      byte[] valueEmail = Bytes.toBytes(String.valueOf(i) + "@email.com");
      mutations.add(new Put(rowKey).addColumn(COLUMN_FAMILY, COLUMN_NAME, value));
      mutations.add(new Put(rowKey).addColumn(COLUMN_FAMILY, COLUMN_EMAIL, valueEmail));
    }
    return mutations;
  }

  private static ResultScanner scanTable(String tableId, Scan scan) throws Exception {
    Connection connection = ConnectionFactory.createConnection(conf);
    TableName tableName = TableName.valueOf(tableId);
    Table table = connection.getTable(tableName);
    return table.getScanner(scan);
  }

  private static List<Result> readTable(String tableId, Scan scan) throws Exception {
    ResultScanner scanner = scanTable(tableId, scan);
    List<Result> results = new ArrayList<>();
    for (Result result : scanner) {
      results.add(result);
    }
    scanner.close();
    return results;
  }

  // Beam helper methods
  /** Helper function to make a single row mutation to be written. */
  private static Iterable<Mutation> makeMutations(String key, String value, int numMutations) {
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < numMutations; i++) {
      mutations.add(makeMutation(key + i, value));
    }
    return mutations;
  }

  private static Mutation makeMutation(String key, String value) {
    return new Put(key.getBytes(StandardCharsets.UTF_8))
        .addColumn(COLUMN_FAMILY, COLUMN_NAME, Bytes.toBytes(value))
        .addColumn(COLUMN_FAMILY, COLUMN_EMAIL, Bytes.toBytes(value + "@email.com"));
  }

  private static Mutation makeBadMutation(String key) {
    return new Put(key.getBytes(StandardCharsets.UTF_8));
  }

  private static class TemporaryHBaseTable extends ExternalResource {
    private String name;

    @Override
    protected void before() {
      name = "table_" + UUID.randomUUID();
    }

    String getName() {
      return name;
    }
  }
}
