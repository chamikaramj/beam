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
package org.apache.beam.examples.multilanguage;

import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"rawtypes", "nullness"})
public class TestExpansionServiceClient {

  private static final Logger LOG = LoggerFactory.getLogger(TestExpansionServiceClient.class);
  private static final String DATA_ROOT = "gs://apache-beam-testing-chamikara/managed-transforms/prototype/";
  // private static final String DATA_ROOT_LOCAL = "/Users/chamikara/testing/managed_transforms/prototype";

  private static void writeRecords(Table table, String location, Configuration hadoopConf)
      throws IOException {
    GenericRecord record = GenericRecord.create(table.schema());
    ImmutableList<Record> records =
        ImmutableList.of(
            record.copy(ImmutableMap.of("id", 0L, "name", "Person-0")),
            record.copy(ImmutableMap.of("id", 1L, "name", "Person-1")),
            record.copy(ImmutableMap.of("id", 2L, "name", "Person-2")));

    Path path = new Path(location, "file1.parquet");

    FileAppender<Record> appender =
        Parquet.write(HadoopOutputFile.fromPath(path, hadoopConf))
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .schema(table.schema())
            .overwrite()
            .build();
    appender.addAll(records);
    appender.close();

    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withInputFile(HadoopInputFile.fromPath(path, hadoopConf))
            .withMetrics(appender.metrics())
            .build();

    table.newFastAppend().appendFile(dataFile).commit();
  }

  public static void main(String[] args) throws Exception {
    LOG.info("******* Running the test expansion service client.");


    String location;
    Configuration hadoopConf = new Configuration();
    Catalog catalog;
    TableIdentifier tableId = TableIdentifier.of("db", "table1");
    Table table;


    // For local files uncommment below.
    // location = "file:" + DATA_ROOT_LOCAL + "/iceberg";
    // catalog =
    //     CatalogUtil.loadCatalog(
    //         CatalogUtil.ICEBERG_CATALOG_HADOOP,
    //         "local",
    //         ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, location),
    //         hadoopConf);


    location = DATA_ROOT + "/iceberg";

    Configuration catalogHadoopConf = new Configuration();
    catalogHadoopConf.set("fs.gs.project.id", "apache-beam-testing-chamikara");
    catalogHadoopConf.set("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE");
    catalogHadoopConf.set(
        "fs.gs.auth.service.account.json.keyfile", System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));

    catalog = new HadoopCatalog(catalogHadoopConf, location);

    Schema schema =
        new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(2, "name", Types.StringType.get()));

    table = catalog.createTable(tableId, schema);

    writeRecords(table, location, hadoopConf);

    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    // Configure the Iceberg source I/O
    Map catalogConfig =
        ImmutableMap.<String, Object>builder()
            .put("catalog_name", "local")
            .put("warehouse_location", location)
            .put("catalog_type", "hadoop")
            .build();

    ImmutableMap<String, Object> config =
        ImmutableMap.<String, Object>builder()
            .put("table", "db.table1")
            .put("catalog_config", catalogConfig)
            .build();

    // Build the pipeline
    PCollectionRowTuple.empty(pipeline)
        .apply(Managed.read(Managed.ICEBERG).withConfig(config))
        .get("output")
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (row -> {
                      return String.format("%d:%s", row.getInt64("id"), row.getString("name"));
                    })))
        // Write to a text file.
        .apply(TextIO.write().to("output").withNumShards(1).withSuffix(".txt"));
    pipeline.run().waitUntilFinish();








    // LOG.info("******* ExpansionServiceConfig file: " + options.getExpansionServiceConfigFile());

    // Yaml yaml = new Yaml();
    // InputStream inputStream = new FileInputStream(options.getExpansionServiceConfigFile());
    // Map<Object, Object> config = yaml.load(inputStream);
    //
    //
    // if (config != null) {
    //   List<String> allowList = null;
    //   if (config.get("allowlist") != null) {
    //     allowList = (List<String>) config.get("allowlist");
    //     LOG.info("allowlist: " + allowList);
    //   }
    //
    //   Map<String, List<Dependency>> dependencies = new HashMap<>();
    //   if (config.get("dependencies") != null) {
    //     Map<String, List<Object>> dependenciesFromConfig = (Map<String, List<Object>>)
    // config.get("dependencies");
    //     LOG.info("dependenciesFromConfig: " + dependenciesFromConfig);
    //     dependenciesFromConfig.forEach( (k, v) -> {
    //       dependencies.put(k, getDependencies(v));
    //     });
    //     LOG.info("Final dependencies: " + dependencies);
    //   }
    // }

    // ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    // File configFileObj = new File(options.getExpansionServiceConfigFile());
    // Object obj = mapper.readValue(configFileObj, ExpansionServiceConfig.class);

    // LoaderOptions loaderoptions = new LoaderOptions();
    // TagInspector taginspector =
    //     tag -> tag.getClassName().equals(User.class.getName());
    // loaderoptions.setTagInspector(taginspector);

    //
    // // Map<Object, Object> config = yaml.load(inputStream);
    //
    // TestExpansionServiceConfig config = yaml.loadAs(inputStream,
    // TestExpansionServiceConfig.class);
    // if (yamlConfig == null) {
    //   throw new IllegalArgumentException("should not be null");
    // }

    // List<String> allowlist = new ArrayList<>();
    // Map<String, List<Dependency>>  dependencies = new HashMap<>();
    //
    //
    //
    // for (Object key : yamlConfig.keySet()) {
    //   if ("allowlist".equals(key)) {
    //
    //
    //   } else if ("")
    // }
    //
    // ExpansionServiceConfig config = ExpansionServiceConfig.create(allowlist, dependencies);

    // for (String allowListEntry : config.getAllowList()) {
    //   LOG.info("Allowlist entry: " + allowListEntry);
    // }
    // for (String key : config.getDependencies().keySet()) {
    //   List<TestDependency> deps = config.getDependencies().get(key);
    //   LOG.info("Dependencies for transform " + key + ":");
    //   for (TestDependency dep : deps) {
    //     LOG.info("    " + dep.getPath());
    //   }
    // }

    // PAssert.that(output.get("output")).containsInAnyOrder(expectedRows);

    // ExpansionApi.ExpansionRequest.Builder requestBuilder =
    //     ExpansionApi.ExpansionRequest.newBuilder();
    //
    // RunnerApi.PTransform.Builder ptransformBuilder =
    //     RunnerApi.PTransform.newBuilder()
    //         .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn("my_dummy_urn").build());
    //
    // ExpansionApi.ExpansionRequest request =
    //     requestBuilder.setTransform(ptransformBuilder.build()).build();
    //
    // LOG.info("******* Expansion request: " + request);
    //
    // ExpansionServiceClientFactory clientFactory =
    //     DefaultExpansionServiceClientFactory.create(
    //         endPoint ->
    // ManagedChannelBuilder.forTarget(endPoint.getUrl()).usePlaintext().build());
    //
    // Endpoints.ApiServiceDescriptor expansionServiceEndpoint =
    //     Endpoints.ApiServiceDescriptor.newBuilder().setUrl("localhost:12345").build();
    // ExpansionApi.ExpansionResponse response =
    //     clientFactory.getExpansionServiceClient(expansionServiceEndpoint).expand(request);
    //
    // LOG.info("******* Expansion response: " + response);

    LOG.info("******* DONE running the test expansion service client.");
  }
}
