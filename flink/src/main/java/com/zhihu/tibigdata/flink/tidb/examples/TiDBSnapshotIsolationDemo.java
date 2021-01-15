/*
 * Copyright 2020 Zhihu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zhihu.tibigdata.flink.tidb.examples;

import com.zhihu.tibigdata.flink.tidb.TiDBCatalog;
import com.zhihu.tibigdata.tidb.ClientConfig;
import com.zhihu.tibigdata.tidb.ClientSession;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.tikv.common.meta.TiTimestamp;
import java.util.HashMap;
import java.util.Map;

public class TiDBSnapshotIsolationDemo {

  // --tidb.database.url jdbc:mysql://address=(protocol=tcp)(host=1.2.3.4)(port=4000) --tidb.username root --tidb.database.name test --tidb.table.name t
  public static void main(String[] args) throws Exception {
    // properties
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    final Map<String, String> properties = new HashMap<>(parameterTool.toMap());
    final String databaseName = parameterTool.getRequired("tidb.database.name");
    final String tableName = parameterTool.getRequired("tidb.table.name");
    // env
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inBatchMode().build();
    TableEnvironment tableEnvironment = TableEnvironment.create(settings);

    // get timestamp from pd
    ClientSession clientSession = ClientSession.create(new ClientConfig(properties));
    TiTimestamp snapshotTimeStamp = clientSession.getTimestampFromPD();
    clientSession.close();
    System.out.println(snapshotTimeStamp);
    properties.put("tidb.timestamp", String.valueOf(snapshotTimeStamp.getVersion()));

    // register TiDBCatalog
    TiDBCatalog catalog = new TiDBCatalog(properties);
    catalog.open();
    tableEnvironment.registerCatalog("tidb", catalog);

    // query at snapshotTimeStamp
    String sql = String.format("SELECT * FROM `tidb`.`%s`.`%s` LIMIT 100", databaseName, tableName);
    System.out.println("Flink SQL: " + sql);
    TableResult tableResult = tableEnvironment.executeSql(sql);
    tableResult.print();
    catalog.close();
  }
}
