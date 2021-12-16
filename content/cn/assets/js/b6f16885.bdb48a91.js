"use strict";(self.webpackChunkhudi=self.webpackChunkhudi||[]).push([[3537],{3905:function(e,t,a){a.d(t,{Zo:function(){return u},kt:function(){return m}});var r=a(67294);function n(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function o(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,r)}return a}function i(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?o(Object(a),!0).forEach((function(t){n(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):o(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function s(e,t){if(null==e)return{};var a,r,n=function(e,t){if(null==e)return{};var a,r,n={},o=Object.keys(e);for(r=0;r<o.length;r++)a=o[r],t.indexOf(a)>=0||(n[a]=e[a]);return n}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)a=o[r],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(n[a]=e[a])}return n}var l=r.createContext({}),c=function(e){var t=r.useContext(l),a=t;return e&&(a="function"==typeof e?e(t):i(i({},t),e)),a},u=function(e){var t=c(e.components);return r.createElement(l.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},p=r.forwardRef((function(e,t){var a=e.components,n=e.mdxType,o=e.originalType,l=e.parentName,u=s(e,["components","mdxType","originalType","parentName"]),p=c(a),m=n,h=p["".concat(l,".").concat(m)]||p[m]||d[m]||o;return a?r.createElement(h,i(i({ref:t},u),{},{components:a})):r.createElement(h,i({ref:t},u))}));function m(e,t){var a=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var o=a.length,i=new Array(o);i[0]=p;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:n,i[1]=s;for(var c=2;c<o;c++)i[c]=a[c];return r.createElement.apply(null,i)}return r.createElement.apply(null,a)}p.displayName="MDXCreateElement"},67685:function(e,t,a){a.r(t),a.d(t,{frontMatter:function(){return s},contentTitle:function(){return l},metadata:function(){return c},toc:function(){return u},default:function(){return p}});var r=a(87462),n=a(63366),o=(a(67294),a(3905)),i=["components"],s={title:"Streaming Ingestion",keywords:["hudi","deltastreamer","hoodiedeltastreamer"]},l=void 0,c={unversionedId:"hoodie_deltastreamer",id:"version-0.9.0/hoodie_deltastreamer",isDocsHomePage:!1,title:"Streaming Ingestion",description:"DeltaStreamer",source:"@site/versioned_docs/version-0.9.0/hoodie_deltastreamer.md",sourceDirName:".",slug:"/hoodie_deltastreamer",permalink:"/cn/docs/0.9.0/hoodie_deltastreamer",editUrl:"https://github.com/apache/hudi/tree/asf-site/website/versioned_docs/version-0.9.0/hoodie_deltastreamer.md",version:"0.9.0",frontMatter:{title:"Streaming Ingestion",keywords:["hudi","deltastreamer","hoodiedeltastreamer"]}},u=[{value:"DeltaStreamer",id:"deltastreamer",children:[]},{value:"MultiTableDeltaStreamer",id:"multitabledeltastreamer",children:[]},{value:"Concurrency Control",id:"concurrency-control",children:[]}],d={toc:u};function p(e){var t=e.components,a=(0,n.Z)(e,i);return(0,o.kt)("wrapper",(0,r.Z)({},d,a,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h2",{id:"deltastreamer"},"DeltaStreamer"),(0,o.kt)("p",null,"The ",(0,o.kt)("inlineCode",{parentName:"p"},"HoodieDeltaStreamer")," utility (part of hudi-utilities-bundle) provides the way to ingest from different sources such as DFS or Kafka, with the following capabilities."),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"Exactly once ingestion of new events from Kafka, ",(0,o.kt)("a",{parentName:"li",href:"https://sqoop.apache.org/docs/1.4.2/SqoopUserGuide#_incremental_imports"},"incremental imports")," from Sqoop or output of ",(0,o.kt)("inlineCode",{parentName:"li"},"HiveIncrementalPuller")," or files under a DFS folder"),(0,o.kt)("li",{parentName:"ul"},"Support json, avro or a custom record types for the incoming data"),(0,o.kt)("li",{parentName:"ul"},"Manage checkpoints, rollback & recovery"),(0,o.kt)("li",{parentName:"ul"},"Leverage Avro schemas from DFS or Confluent ",(0,o.kt)("a",{parentName:"li",href:"https://github.com/confluentinc/schema-registry"},"schema registry"),"."),(0,o.kt)("li",{parentName:"ul"},"Support for plugging in transformations")),(0,o.kt)("p",null,"Command line options describe capabilities in more detail"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-java"},"[hoodie]$ spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` --help\nUsage: <main class> [options]\nOptions:\n    --checkpoint\n      Resume Delta Streamer from this checkpoint.\n    --commit-on-errors\n      Commit even when some records failed to be written\n      Default: false\n    --compact-scheduling-minshare\n      Minshare for compaction as defined in\n      https://spark.apache.org/docs/latest/job-scheduling\n      Default: 0\n    --compact-scheduling-weight\n      Scheduling weight for compaction as defined in\n      https://spark.apache.org/docs/latest/job-scheduling\n      Default: 1\n    --continuous\n      Delta Streamer runs in continuous mode running source-fetch -> Transform\n      -> Hudi Write in loop\n      Default: false\n    --delta-sync-scheduling-minshare\n      Minshare for delta sync as defined in\n      https://spark.apache.org/docs/latest/job-scheduling\n      Default: 0\n    --delta-sync-scheduling-weight\n      Scheduling weight for delta sync as defined in\n      https://spark.apache.org/docs/latest/job-scheduling\n      Default: 1\n    --disable-compaction\n      Compaction is enabled for MoR table by default. This flag disables it\n      Default: false\n    --enable-hive-sync\n      Enable syncing to hive\n      Default: false\n    --filter-dupes\n      Should duplicate records from source be dropped/filtered out before\n      insert/bulk-insert\n      Default: false\n    --help, -h\n\n    --hoodie-conf\n      Any configuration that can be set in the properties file (using the CLI\n      parameter \"--propsFilePath\") can also be passed command line using this\n      parameter\n      Default: []\n    --max-pending-compactions\n      Maximum number of outstanding inflight/requested compactions. Delta Sync\n      will not happen unlessoutstanding compactions is less than this number\n      Default: 5\n    --min-sync-interval-seconds\n      the min sync interval of each sync in continuous mode\n      Default: 0\n    --op\n      Takes one of these values : UPSERT (default), INSERT (use when input is\n      purely new data/inserts to gain speed)\n      Default: UPSERT\n      Possible Values: [UPSERT, INSERT, BULK_INSERT]\n    --payload-class\n      subclass of HoodieRecordPayload, that works off a GenericRecord.\n      Implement your own, if you want to do something other than overwriting\n      existing value\n      Default: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload\n    --props\n      path to properties file on localfs or dfs, with configurations for\n      hoodie client, schema provider, key generator and data source. For\n      hoodie client props, sane defaults are used, but recommend use to\n      provide basic things like metrics endpoints, hive configs etc. For\n      sources, referto individual classes, for supported properties.\n      Default: file:///Users/vinoth/bin/hoodie/src/test/resources/delta-streamer-config/dfs-source.properties\n    --schemaprovider-class\n      subclass of org.apache.hudi.utilities.schema.SchemaProvider to attach\n      schemas to input & target table data, built in options:\n      org.apache.hudi.utilities.schema.FilebasedSchemaProvider.Source (See\n      org.apache.hudi.utilities.sources.Source) implementation can implement\n      their own SchemaProvider. For Sources that return Dataset<Row>, the\n      schema is obtained implicitly. However, this CLI option allows\n      overriding the schemaprovider returned by Source.\n    --source-class\n      Subclass of org.apache.hudi.utilities.sources to read data. Built-in\n      options: org.apache.hudi.utilities.sources.{JsonDFSSource (default), \n      AvroDFSSource, AvroKafkaSource, CsvDFSSource, HiveIncrPullSource, \n      JdbcSource, JsonKafkaSource, ORCDFSSource, ParquetDFSSource, \n      S3EventsHoodieIncrSource, S3EventsSource, SqlSource}\n      Default: org.apache.hudi.utilities.sources.JsonDFSSource\n    --source-limit\n      Maximum amount of data to read from source. Default: No limit For e.g:\n      DFS-Source => max bytes to read, Kafka-Source => max events to read\n      Default: 9223372036854775807\n    --source-ordering-field\n      Field within source record to decide how to break ties between records\n      with same key in input data. Default: 'ts' holding unix timestamp of\n      record\n      Default: ts\n    --spark-master\n      spark master to use.\n      Default: local[2]\n  * --table-type\n      Type of table. COPY_ON_WRITE (or) MERGE_ON_READ\n  * --target-base-path\n      base path for the target hoodie table. (Will be created if did not exist\n      first time around. If exists, expected to be a hoodie table)\n  * --target-table\n      name of the target table in Hive\n    --transformer-class\n      subclass of org.apache.hudi.utilities.transform.Transformer. Allows\n      transforming raw source Dataset to a target Dataset (conforming to\n      target schema) before writing. Default : Not set. E:g -\n      org.apache.hudi.utilities.transform.SqlQueryBasedTransformer (which\n      allows a SQL query templated to be passed as a transformation function)\n")),(0,o.kt)("p",null,"The tool takes a hierarchically composed property file and has pluggable interfaces for extracting data, key generation and providing schema. Sample configs for ingesting from kafka and dfs are\nprovided under ",(0,o.kt)("inlineCode",{parentName:"p"},"hudi-utilities/src/test/resources/delta-streamer-config"),"."),(0,o.kt)("p",null,"For e.g: once you have Confluent Kafka, Schema registry up & running, produce some test data using (",(0,o.kt)("a",{parentName:"p",href:"https://docs.confluent.io/current/ksql/docs/tutorials/generate-custom-test-data"},"impressions.avro")," provided by schema-registry repo)"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-java"},"[confluent-5.0.0]$ bin/ksql-datagen schema=../impressions.avro format=avro topic=impressions key=impressionid\n")),(0,o.kt)("p",null,"and then ingest it as follows."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-java"},"[hoodie]$ spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` \\\n  --props file://${PWD}/hudi-utilities/src/test/resources/delta-streamer-config/kafka-source.properties \\\n  --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \\\n  --source-class org.apache.hudi.utilities.sources.AvroKafkaSource \\\n  --source-ordering-field impresssiontime \\\n  --target-base-path file:\\/\\/\\/tmp/hudi-deltastreamer-op \\ \n  --target-table uber.impressions \\\n  --op BULK_INSERT\n")),(0,o.kt)("p",null,"In some cases, you may want to migrate your existing table into Hudi beforehand. Please refer to ",(0,o.kt)("a",{parentName:"p",href:"/docs/migration_guide"},"migration guide"),"."),(0,o.kt)("h2",{id:"multitabledeltastreamer"},"MultiTableDeltaStreamer"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"HoodieMultiTableDeltaStreamer"),", a wrapper on top of ",(0,o.kt)("inlineCode",{parentName:"p"},"HoodieDeltaStreamer"),", enables one to ingest multiple tables at a single go into hudi datasets. Currently it only supports sequential processing of tables to be ingested and COPY_ON_WRITE storage type. The command line options for ",(0,o.kt)("inlineCode",{parentName:"p"},"HoodieMultiTableDeltaStreamer")," are pretty much similar to ",(0,o.kt)("inlineCode",{parentName:"p"},"HoodieDeltaStreamer")," with the only exception that you are required to provide table wise configs in separate files in a dedicated config folder. The following command line options are introduced"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-java"},"  * --config-folder\n    the path to the folder which contains all the table wise config files\n    --base-path-prefix\n    this is added to enable users to create all the hudi datasets for related tables under one path in FS. The datasets are then created under the path - <base_path_prefix>/<database>/<table_to_be_ingested>. However you can override the paths for every table by setting the property hoodie.deltastreamer.ingestion.targetBasePath\n")),(0,o.kt)("p",null,"The following properties are needed to be set properly to ingest data using ",(0,o.kt)("inlineCode",{parentName:"p"},"HoodieMultiTableDeltaStreamer"),"."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-java"},"hoodie.deltastreamer.ingestion.tablesToBeIngested\n  comma separated names of tables to be ingested in the format <database>.<table>, for example db1.table1,db1.table2\nhoodie.deltastreamer.ingestion.targetBasePath\n  if you wish to ingest a particular table in a separate path, you can mention that path here\nhoodie.deltastreamer.ingestion.<database>.<table>.configFile\n  path to the config file in dedicated config folder which contains table overridden properties for the particular table to be ingested.\n")),(0,o.kt)("p",null,"Sample config files for table wise overridden properties can be found under ",(0,o.kt)("inlineCode",{parentName:"p"},"hudi-utilities/src/test/resources/delta-streamer-config"),". The command to run ",(0,o.kt)("inlineCode",{parentName:"p"},"HoodieMultiTableDeltaStreamer")," is also similar to how you run ",(0,o.kt)("inlineCode",{parentName:"p"},"HoodieDeltaStreamer"),"."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-java"},"[hoodie]$ spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieMultiTableDeltaStreamer `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` \\\n  --props file://${PWD}/hudi-utilities/src/test/resources/delta-streamer-config/kafka-source.properties \\\n  --config-folder file://tmp/hudi-ingestion-config \\\n  --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \\\n  --source-class org.apache.hudi.utilities.sources.AvroKafkaSource \\\n  --source-ordering-field impresssiontime \\\n  --base-path-prefix file:\\/\\/\\/tmp/hudi-deltastreamer-op \\ \n  --target-table uber.impressions \\\n  --op BULK_INSERT\n")),(0,o.kt)("p",null,"For detailed information on how to configure and use ",(0,o.kt)("inlineCode",{parentName:"p"},"HoodieMultiTableDeltaStreamer"),", please refer ",(0,o.kt)("a",{parentName:"p",href:"/blog/2020/08/22/ingest-multiple-tables-using-hudi"},"blog section"),"."),(0,o.kt)("h2",{id:"concurrency-control"},"Concurrency Control"),(0,o.kt)("p",null,"The ",(0,o.kt)("inlineCode",{parentName:"p"},"HoodieDeltaStreamer")," utility (part of hudi-utilities-bundle) provides ways to ingest from different sources such as DFS or Kafka, with the following capabilities."),(0,o.kt)("p",null,"Using optimistic_concurrency_control via delta streamer requires adding the above configs to the properties file that can be passed to the\njob. For example below, adding the configs to kafka-source.properties file and passing them to deltastreamer will enable optimistic concurrency.\nA deltastreamer job can then be triggered as follows:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-java"},"[hoodie]$ spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` \\\n  --props file://${PWD}/hudi-utilities/src/test/resources/delta-streamer-config/kafka-source.properties \\\n  --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \\\n  --source-class org.apache.hudi.utilities.sources.AvroKafkaSource \\\n  --source-ordering-field impresssiontime \\\n  --target-base-path file:\\/\\/\\/tmp/hudi-deltastreamer-op \\ \n  --target-table uber.impressions \\\n  --op BULK_INSERT\n")),(0,o.kt)("p",null,"Read more in depth about concurrency control in the ",(0,o.kt)("a",{parentName:"p",href:"/docs/concurrency_control"},"concurrency control concepts")," section"))}p.isMDXComponent=!0}}]);