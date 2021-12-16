"use strict";(self.webpackChunkhudi=self.webpackChunkhudi||[]).push([[8019],{3905:function(e,t,a){a.d(t,{Zo:function(){return d},kt:function(){return c}});var n=a(67294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function i(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function l(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?i(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):i(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function o(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},i=Object.keys(e);for(n=0;n<i.length;n++)a=i[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)a=i[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var s=n.createContext({}),p=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):l(l({},t),e)),a},d=function(e){var t=p(e.components);return n.createElement(s.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,i=e.originalType,s=e.parentName,d=o(e,["components","mdxType","originalType","parentName"]),m=p(a),c=r,h=m["".concat(s,".").concat(c)]||m[c]||u[c]||i;return a?n.createElement(h,l(l({ref:t},d),{},{components:a})):n.createElement(h,l({ref:t},d))}));function c(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=a.length,l=new Array(i);l[0]=m;var o={};for(var s in t)hasOwnProperty.call(t,s)&&(o[s]=t[s]);o.originalType=e,o.mdxType="string"==typeof e?e:r,l[1]=o;for(var p=2;p<i;p++)l[p]=a[p];return n.createElement.apply(null,l)}return n.createElement.apply(null,a)}m.displayName="MDXCreateElement"},46061:function(e,t,a){a.r(t),a.d(t,{frontMatter:function(){return o},contentTitle:function(){return s},metadata:function(){return p},toc:function(){return d},default:function(){return m}});var n=a(87462),r=a(63366),i=(a(67294),a(3905)),l=["components"],o={version:"0.7.0",title:"Querying Hudi Tables",keywords:["hudi","hive","spark","sql","presto"],summary:"In this page, we go over how to enable SQL queries on Hudi built tables.",toc:!0,last_modified_at:new Date("2019-12-30T19:59:57.000Z")},s=void 0,p={unversionedId:"querying_data",id:"version-0.7.0/querying_data",isDocsHomePage:!1,title:"Querying Hudi Tables",description:"Conceptually, Hudi stores data physically once on DFS, while providing 3 different ways of querying, as explained before.",source:"@site/versioned_docs/version-0.7.0/querying_data.md",sourceDirName:".",slug:"/querying_data",permalink:"/docs/0.7.0/querying_data",editUrl:"https://github.com/apache/hudi/tree/asf-site/website/versioned_docs/version-0.7.0/querying_data.md",version:"0.7.0",frontMatter:{version:"0.7.0",title:"Querying Hudi Tables",keywords:["hudi","hive","spark","sql","presto"],summary:"In this page, we go over how to enable SQL queries on Hudi built tables.",toc:!0,last_modified_at:"2019-12-30T19:59:57.000Z"},sidebar:"version-0.7.0/docs",previous:{title:"Writing Hudi Tables",permalink:"/docs/0.7.0/writing_data"},next:{title:"Configurations",permalink:"/docs/0.7.0/configurations"}},d=[{value:"Support Matrix",id:"support-matrix",children:[{value:"Copy-On-Write tables",id:"copy-on-write-tables",children:[]},{value:"Merge-On-Read tables",id:"merge-on-read-tables",children:[]}]},{value:"Hive",id:"hive",children:[{value:"Incremental query",id:"incremental-query",children:[]}]},{value:"Spark SQL",id:"spark-sql",children:[]},{value:"Spark Datasource",id:"spark-datasource",children:[{value:"Snapshot query",id:"spark-snap-query",children:[]},{value:"Incremental query",id:"spark-incr-query",children:[]}]},{value:"PrestoDB",id:"prestodb",children:[]},{value:"Impala (3.4 or later)",id:"impala-34-or-later",children:[{value:"Snapshot Query",id:"snapshot-query",children:[]}]}],u={toc:d};function m(e){var t=e.components,a=(0,r.Z)(e,l);return(0,i.kt)("wrapper",(0,n.Z)({},u,a,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("p",null,"Conceptually, Hudi stores data physically once on DFS, while providing 3 different ways of querying, as explained ",(0,i.kt)("a",{parentName:"p",href:"/docs/concepts#query-types"},"before"),".\nOnce the table is synced to the Hive metastore, it provides external Hive tables backed by Hudi's custom inputformats. Once the proper hudi\nbundle has been installed, the table can be queried by popular query engines like Hive, Spark SQL, Spark Datasource API and PrestoDB."),(0,i.kt)("p",null,"Specifically, following Hive tables are registered based off ",(0,i.kt)("a",{parentName:"p",href:"/docs/configurations#TABLE_NAME_OPT_KEY"},"table name"),"\nand ",(0,i.kt)("a",{parentName:"p",href:"/docs/configurations#TABLE_TYPE_OPT_KEY"},"table type")," configs passed during write.   "),(0,i.kt)("p",null,"If ",(0,i.kt)("inlineCode",{parentName:"p"},"table name = hudi_trips")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"table type = COPY_ON_WRITE"),", then we get: "),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"hudi_trips")," supports snapshot query and incremental query on the table backed by ",(0,i.kt)("inlineCode",{parentName:"li"},"HoodieParquetInputFormat"),", exposing purely columnar data.")),(0,i.kt)("p",null,"If ",(0,i.kt)("inlineCode",{parentName:"p"},"table name = hudi_trips")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"table type = MERGE_ON_READ"),", then we get:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"hudi_trips_rt")," supports snapshot query and incremental query (providing near-real time data) on the table  backed by ",(0,i.kt)("inlineCode",{parentName:"li"},"HoodieParquetRealtimeInputFormat"),", exposing merged view of base and log data."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"hudi_trips_ro")," supports read optimized query on the table backed by ",(0,i.kt)("inlineCode",{parentName:"li"},"HoodieParquetInputFormat"),", exposing purely columnar data stored in base files.")),(0,i.kt)("p",null,"As discussed in the concepts section, the one key capability needed for ",(0,i.kt)("a",{parentName:"p",href:"https://www.oreilly.com/ideas/ubers-case-for-incremental-processing-on-hadoop"},"incrementally processing"),",\nis obtaining a change stream/log from a table. Hudi tables can be queried incrementally, which means you can get ALL and ONLY the updated & new rows\nsince a specified instant time. This, together with upserts, is particularly useful for building data pipelines where 1 or more source Hudi tables are incrementally queried (streams/facts),\njoined with other tables (tables/dimensions), to ",(0,i.kt)("a",{parentName:"p",href:"/docs/writing_data"},"write out deltas")," to a target Hudi table. Incremental queries are realized by querying one of the tables above,\nwith special configurations that indicates to query planning that only incremental data needs to be fetched out of the table. "),(0,i.kt)("h2",{id:"support-matrix"},"Support Matrix"),(0,i.kt)("p",null,"Following tables show whether a given query is supported on specific query engine."),(0,i.kt)("h3",{id:"copy-on-write-tables"},"Copy-On-Write tables"),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:null},"Query Engine"),(0,i.kt)("th",{parentName:"tr",align:null},"Snapshot Queries"),(0,i.kt)("th",{parentName:"tr",align:null},"Incremental Queries"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},(0,i.kt)("strong",{parentName:"td"},"Hive")),(0,i.kt)("td",{parentName:"tr",align:null},"Y"),(0,i.kt)("td",{parentName:"tr",align:null},"Y")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},(0,i.kt)("strong",{parentName:"td"},"Spark SQL")),(0,i.kt)("td",{parentName:"tr",align:null},"Y"),(0,i.kt)("td",{parentName:"tr",align:null},"Y")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},(0,i.kt)("strong",{parentName:"td"},"Spark Datasource")),(0,i.kt)("td",{parentName:"tr",align:null},"Y"),(0,i.kt)("td",{parentName:"tr",align:null},"Y")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},(0,i.kt)("strong",{parentName:"td"},"PrestoDB")),(0,i.kt)("td",{parentName:"tr",align:null},"Y"),(0,i.kt)("td",{parentName:"tr",align:null},"N")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},(0,i.kt)("strong",{parentName:"td"},"Impala")),(0,i.kt)("td",{parentName:"tr",align:null},"Y"),(0,i.kt)("td",{parentName:"tr",align:null},"N")))),(0,i.kt)("p",null,"Note that ",(0,i.kt)("inlineCode",{parentName:"p"},"Read Optimized")," queries are not applicable for COPY_ON_WRITE tables."),(0,i.kt)("h3",{id:"merge-on-read-tables"},"Merge-On-Read tables"),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:null},"Query Engine"),(0,i.kt)("th",{parentName:"tr",align:null},"Snapshot Queries"),(0,i.kt)("th",{parentName:"tr",align:null},"Incremental Queries"),(0,i.kt)("th",{parentName:"tr",align:null},"Read Optimized Queries"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},(0,i.kt)("strong",{parentName:"td"},"Hive")),(0,i.kt)("td",{parentName:"tr",align:null},"Y"),(0,i.kt)("td",{parentName:"tr",align:null},"Y"),(0,i.kt)("td",{parentName:"tr",align:null},"Y")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},(0,i.kt)("strong",{parentName:"td"},"Spark SQL")),(0,i.kt)("td",{parentName:"tr",align:null},"Y"),(0,i.kt)("td",{parentName:"tr",align:null},"Y"),(0,i.kt)("td",{parentName:"tr",align:null},"Y")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},(0,i.kt)("strong",{parentName:"td"},"Spark Datasource")),(0,i.kt)("td",{parentName:"tr",align:null},"Y"),(0,i.kt)("td",{parentName:"tr",align:null},"Y"),(0,i.kt)("td",{parentName:"tr",align:null},"Y")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},(0,i.kt)("strong",{parentName:"td"},"PrestoDB")),(0,i.kt)("td",{parentName:"tr",align:null},"Y"),(0,i.kt)("td",{parentName:"tr",align:null},"N"),(0,i.kt)("td",{parentName:"tr",align:null},"Y")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},(0,i.kt)("strong",{parentName:"td"},"Impala")),(0,i.kt)("td",{parentName:"tr",align:null},"N"),(0,i.kt)("td",{parentName:"tr",align:null},"N"),(0,i.kt)("td",{parentName:"tr",align:null},"Y")))),(0,i.kt)("p",null,"In sections, below we will discuss specific setup to access different query types from different query engines. "),(0,i.kt)("h2",{id:"hive"},"Hive"),(0,i.kt)("p",null,"In order for Hive to recognize Hudi tables and query correctly, "),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"the HiveServer2 needs to be provided with the ",(0,i.kt)("inlineCode",{parentName:"li"},"hudi-hadoop-mr-bundle-x.y.z-SNAPSHOT.jar")," in its ",(0,i.kt)("a",{parentName:"li",href:"https://www.cloudera.com/documentation/enterprise/5-6-x/topics/cm_mc_hive_udf#concept_nc3_mms_lr"},"aux jars path"),". This will ensure the input format\nclasses with its dependencies are available for query planning & execution. "),(0,i.kt)("li",{parentName:"ul"},"For MERGE_ON_READ tables, additionally the bundle needs to be put on the hadoop/hive installation across the cluster, so that queries can pick up the custom RecordReader as well.")),(0,i.kt)("p",null,"In addition to setup above, for beeline cli access, the ",(0,i.kt)("inlineCode",{parentName:"p"},"hive.input.format")," variable needs to be set to the fully qualified path name of the\ninputformat ",(0,i.kt)("inlineCode",{parentName:"p"},"org.apache.hudi.hadoop.HoodieParquetInputFormat"),". For Tez, additionally the ",(0,i.kt)("inlineCode",{parentName:"p"},"hive.tez.input.format")," needs to be set\nto ",(0,i.kt)("inlineCode",{parentName:"p"},"org.apache.hadoop.hive.ql.io.HiveInputFormat"),". Then proceed to query the table like any other Hive table."),(0,i.kt)("h3",{id:"incremental-query"},"Incremental query"),(0,i.kt)("p",null,(0,i.kt)("inlineCode",{parentName:"p"},"HiveIncrementalPuller")," allows incrementally extracting changes from large fact/dimension tables via HiveQL, combining the benefits of Hive (reliably process complex SQL queries) and\nincremental primitives (speed up querying tables incrementally instead of scanning fully). The tool uses Hive JDBC to run the hive query and saves its results in a temp table.\nthat can later be upserted. Upsert utility (",(0,i.kt)("inlineCode",{parentName:"p"},"HoodieDeltaStreamer"),") has all the state it needs from the directory structure to know what should be the commit time on the target table.\ne.g: ",(0,i.kt)("inlineCode",{parentName:"p"},"/app/incremental-hql/intermediate/{source_table_name}_temp/{last_commit_included}"),".The Delta Hive table registered will be of the form ",(0,i.kt)("inlineCode",{parentName:"p"},"{tmpdb}.{source_table}_{last_commit_included}"),"."),(0,i.kt)("p",null,"The following are the configuration options for HiveIncrementalPuller"),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:null},(0,i.kt)("strong",{parentName:"th"},"Config")),(0,i.kt)("th",{parentName:"tr",align:null},(0,i.kt)("strong",{parentName:"th"},"Description")),(0,i.kt)("th",{parentName:"tr",align:null},(0,i.kt)("strong",{parentName:"th"},"Default")))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"hiveUrl"),(0,i.kt)("td",{parentName:"tr",align:null},"Hive Server 2 URL to connect to"),(0,i.kt)("td",{parentName:"tr",align:null})),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"hiveUser"),(0,i.kt)("td",{parentName:"tr",align:null},"Hive Server 2 Username"),(0,i.kt)("td",{parentName:"tr",align:null})),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"hivePass"),(0,i.kt)("td",{parentName:"tr",align:null},"Hive Server 2 Password"),(0,i.kt)("td",{parentName:"tr",align:null})),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"queue"),(0,i.kt)("td",{parentName:"tr",align:null},"YARN Queue name"),(0,i.kt)("td",{parentName:"tr",align:null})),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"tmp"),(0,i.kt)("td",{parentName:"tr",align:null},"Directory where the temporary delta data is stored in DFS. The directory structure will follow conventions. Please see the below section."),(0,i.kt)("td",{parentName:"tr",align:null})),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"extractSQLFile"),(0,i.kt)("td",{parentName:"tr",align:null},"The SQL to execute on the source table to extract the data. The data extracted will be all the rows that changed since a particular point in time."),(0,i.kt)("td",{parentName:"tr",align:null})),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"sourceTable"),(0,i.kt)("td",{parentName:"tr",align:null},"Source Table Name. Needed to set hive environment properties."),(0,i.kt)("td",{parentName:"tr",align:null})),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"sourceDb"),(0,i.kt)("td",{parentName:"tr",align:null},"Source DB name. Needed to set hive environment properties."),(0,i.kt)("td",{parentName:"tr",align:null})),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"targetTable"),(0,i.kt)("td",{parentName:"tr",align:null},"Target Table Name. Needed for the intermediate storage directory structure."),(0,i.kt)("td",{parentName:"tr",align:null})),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"targetDb"),(0,i.kt)("td",{parentName:"tr",align:null},"Target table's DB name."),(0,i.kt)("td",{parentName:"tr",align:null})),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"tmpdb"),(0,i.kt)("td",{parentName:"tr",align:null},"The database to which the intermediate temp delta table will be created"),(0,i.kt)("td",{parentName:"tr",align:null},"hoodie_temp")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"fromCommitTime"),(0,i.kt)("td",{parentName:"tr",align:null},"This is the most important parameter. This is the point in time from which the changed records are queried from."),(0,i.kt)("td",{parentName:"tr",align:null})),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"maxCommits"),(0,i.kt)("td",{parentName:"tr",align:null},"Number of commits to include in the query. Setting this to -1 will include all the commits from fromCommitTime. Setting this to a value > 0, will include records that ONLY changed in the specified number of commits after fromCommitTime. This may be needed if you need to catch up say 2 commits at a time."),(0,i.kt)("td",{parentName:"tr",align:null},"3")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"help"),(0,i.kt)("td",{parentName:"tr",align:null},"Utility Help"),(0,i.kt)("td",{parentName:"tr",align:null})))),(0,i.kt)("p",null,"Setting fromCommitTime=0 and maxCommits=-1 will fetch the entire source table and can be used to initiate backfills. If the target table is a Hudi table,\nthen the utility can determine if the target table has no commits or is behind more than 24 hour (this is configurable),\nit will automatically use the backfill configuration, since applying the last 24 hours incrementally could take more time than doing a backfill. The current limitation of the tool\nis the lack of support for self-joining the same table in mixed mode (snapshot and incremental modes)."),(0,i.kt)("p",null,(0,i.kt)("strong",{parentName:"p"},"NOTE on Hive incremental queries that are executed using Fetch task:"),"\nSince Fetch tasks invoke InputFormat.listStatus() per partition, Hoodie metadata can be listed in\nevery such listStatus() call. In order to avoid this, it might be useful to disable fetch tasks\nusing the hive session property for incremental queries: ",(0,i.kt)("inlineCode",{parentName:"p"},"set hive.fetch.task.conversion=none;")," This\nwould ensure Map Reduce execution is chosen for a Hive query, which combines partitions (comma\nseparated) and calls InputFormat.listStatus() only once with all those partitions."),(0,i.kt)("h2",{id:"spark-sql"},"Spark SQL"),(0,i.kt)("p",null,"Once the Hudi tables have been registered to the Hive metastore, it can be queried using the Spark-Hive integration. It supports all query types across both Hudi table types,\nrelying on the custom Hudi input formats again like Hive. Typically notebook users and spark-shell users leverage spark sql for querying Hudi tables. Please add hudi-spark-bundle as described above via --jars or --packages."),(0,i.kt)("p",null,"By default, Spark SQL will try to use its own parquet reader instead of Hive SerDe when reading from Hive metastore parquet tables. However, for MERGE_ON_READ tables which has\nboth parquet and avro data, this default setting needs to be turned off using set ",(0,i.kt)("inlineCode",{parentName:"p"},"spark.sql.hive.convertMetastoreParquet=false"),".\nThis will force Spark to fallback to using the Hive Serde to read the data (planning/executions is still Spark). "),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},"$ spark-shell --driver-class-path /etc/hive/conf  --packages org.apache.hudi:hudi-spark-bundle_2.11:0.5.3,org.apache.spark:spark-avro_2.11:2.4.4 --conf spark.sql.hive.convertMetastoreParquet=false --num-executors 10 --driver-memory 7g --executor-memory 2g  --master yarn-client\n\nscala> sqlContext.sql(\"select count(*) from hudi_trips_mor_rt where datestr = '2016-10-02'\").show()\nscala> sqlContext.sql(\"select count(*) from hudi_trips_mor_rt where datestr = '2016-10-02'\").show()\n")),(0,i.kt)("p",null,"For COPY_ON_WRITE tables, either Hive SerDe can be used by turning off ",(0,i.kt)("inlineCode",{parentName:"p"},"spark.sql.hive.convertMetastoreParquet=false")," as described above or Spark's built in support can be leveraged.\nIf using spark's built in support, additionally a path filter needs to be pushed into sparkContext as follows. This method retains Spark built-in optimizations for reading parquet files like vectorized reading on Hudi Hive tables."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-scala"},'spark.sparkContext.hadoopConfiguration.setClass("mapreduce.input.pathFilter.class", classOf[org.apache.hudi.hadoop.HoodieROTablePathFilter], classOf[org.apache.hadoop.fs.PathFilter]);\n')),(0,i.kt)("h2",{id:"spark-datasource"},"Spark Datasource"),(0,i.kt)("p",null,"The Spark Datasource API is a popular way of authoring Spark ETL pipelines. Hudi COPY_ON_WRITE and MERGE_ON_READ tables can be queried via Spark datasource similar to how standard\ndatasources work (e.g: ",(0,i.kt)("inlineCode",{parentName:"p"},"spark.read.parquet"),"). MERGE_ON_READ table supports snapshot querying and COPY_ON_WRITE table supports both snapshot and incremental querying via Spark datasource. Typically spark jobs require adding ",(0,i.kt)("inlineCode",{parentName:"p"},"--jars <path to jar>/hudi-spark-bundle_2.11-<hudi version>.jar")," to classpath of drivers\nand executors. Alternatively, hudi-spark-bundle can also fetched via the ",(0,i.kt)("inlineCode",{parentName:"p"},"--packages")," options (e.g: ",(0,i.kt)("inlineCode",{parentName:"p"},"--packages org.apache.hudi:hudi-spark-bundle_2.11:0.5.3"),")."),(0,i.kt)("h3",{id:"spark-snap-query"},"Snapshot query"),(0,i.kt)("p",null,"This method can be used to retrieve the data table at the present point in time.\nNote: The file path must be suffixed with a number of wildcard asterisk (",(0,i.kt)("inlineCode",{parentName:"p"},"/*"),') one greater than the number of partition levels. Eg: with table file path "tablePath" partitioned by columns "a", "b", and "c", the load path must be ',(0,i.kt)("inlineCode",{parentName:"p"},'tablePath + "/*/*/*/*"')),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-scala"},'val hudiIncQueryDF = spark\n     .read()\n     .format("org.apache.hudi")\n     .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())\n     .load(tablePath + "/*") //The number of wildcard asterisks here must be one greater than the number of partition\n')),(0,i.kt)("h3",{id:"spark-incr-query"},"Incremental query"),(0,i.kt)("p",null,"Of special interest to spark pipelines, is Hudi's ability to support incremental queries, like below. A sample incremental query, that will obtain all records written since ",(0,i.kt)("inlineCode",{parentName:"p"},"beginInstantTime"),", looks like below.\nThanks to Hudi's support for record level change streams, these incremental pipelines often offer 10x efficiency over batch counterparts, by only processing the changed records.\nThe following snippet shows how to obtain all records changed after ",(0,i.kt)("inlineCode",{parentName:"p"},"beginInstantTime")," and run some SQL on them."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},' Dataset<Row> hudiIncQueryDF = spark.read()\n     .format("org.apache.hudi")\n     .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())\n     .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY(), <beginInstantTime>)\n     .option(DataSourceReadOptions.INCR_PATH_GLOB_OPT_KEY(), "/year=2020/month=*/day=*") // Optional, use glob pattern if querying certain partitions\n     .load(tablePath); // For incremental query, pass in the root/base path of table\n     \nhudiIncQueryDF.createOrReplaceTempView("hudi_trips_incremental")\nspark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_trips_incremental where fare > 20.0").show()\n')),(0,i.kt)("p",null,"For examples, refer to ",(0,i.kt)("a",{parentName:"p",href:"/docs/quick-start-guide#setup-spark-shell"},"Setup spark-shell in quickstart"),".\nPlease refer to ",(0,i.kt)("a",{parentName:"p",href:"/docs/configurations#spark-datasource"},"configurations")," section, to view all datasource options."),(0,i.kt)("p",null,"Additionally, ",(0,i.kt)("inlineCode",{parentName:"p"},"HoodieReadClient")," offers the following functionality using Hudi's implicit indexing."),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:null},(0,i.kt)("strong",{parentName:"th"},"API")),(0,i.kt)("th",{parentName:"tr",align:null},(0,i.kt)("strong",{parentName:"th"},"Description")))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"read(keys)"),(0,i.kt)("td",{parentName:"tr",align:null},"Read out the data corresponding to the keys as a DataFrame, using Hudi's own index for faster lookup")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"filterExists()"),(0,i.kt)("td",{parentName:"tr",align:null},"Filter out already existing records from the provided ",(0,i.kt)("inlineCode",{parentName:"td"},"RDD[HoodieRecord]"),". Useful for de-duplication")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"checkExists(keys)"),(0,i.kt)("td",{parentName:"tr",align:null},"Check if the provided keys exist in a Hudi table")))),(0,i.kt)("h2",{id:"prestodb"},"PrestoDB"),(0,i.kt)("p",null,"PrestoDB is a popular query engine, providing interactive query performance. PrestoDB currently supports snapshot querying on COPY_ON_WRITE tables.\nBoth snapshot and read optimized queries are supported on MERGE_ON_READ Hudi tables. Since PrestoDB-Hudi integration has evolved over time, the installation\ninstructions for PrestoDB would vary based on versions. Please check the below table for query types supported and installation instructions\nfor different versions of PrestoDB."),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:null},(0,i.kt)("strong",{parentName:"th"},"PrestoDB Version")),(0,i.kt)("th",{parentName:"tr",align:null},(0,i.kt)("strong",{parentName:"th"},"Installation description")),(0,i.kt)("th",{parentName:"tr",align:null},(0,i.kt)("strong",{parentName:"th"},"Query types supported")))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"< 0.233"),(0,i.kt)("td",{parentName:"tr",align:null},"Requires the ",(0,i.kt)("inlineCode",{parentName:"td"},"hudi-presto-bundle")," jar to be placed into ",(0,i.kt)("inlineCode",{parentName:"td"},"<presto_install>/plugin/hive-hadoop2/"),", across the installation."),(0,i.kt)("td",{parentName:"tr",align:null},"Snapshot querying on COW tables. Read optimized querying on MOR tables.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},">= 0.233"),(0,i.kt)("td",{parentName:"tr",align:null},"No action needed. Hudi (0.5.1-incubating) is a compile time dependency."),(0,i.kt)("td",{parentName:"tr",align:null},"Snapshot querying on COW tables. Read optimized querying on MOR tables.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},">= 0.240"),(0,i.kt)("td",{parentName:"tr",align:null},"No action needed. Hudi 0.5.3 version is a compile time dependency."),(0,i.kt)("td",{parentName:"tr",align:null},"Snapshot querying on both COW and MOR tables")))),(0,i.kt)("h2",{id:"impala-34-or-later"},"Impala (3.4 or later)"),(0,i.kt)("h3",{id:"snapshot-query"},"Snapshot Query"),(0,i.kt)("p",null,"Impala is able to query Hudi Copy-on-write table as an ",(0,i.kt)("a",{parentName:"p",href:"https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/impala_tables#external_tables"},"EXTERNAL TABLE")," on HDFS.  "),(0,i.kt)("p",null,"To create a Hudi read optimized table on Impala:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"CREATE EXTERNAL TABLE database.table_name\nLIKE PARQUET '/path/to/load/xxx.parquet'\nSTORED AS HUDIPARQUET\nLOCATION '/path/to/load';\n")),(0,i.kt)("p",null,"Impala is able to take advantage of the physical partition structure to improve the query performance.\nTo create a partitioned table, the folder should follow the naming convention like ",(0,i.kt)("inlineCode",{parentName:"p"},"year=2020/month=1"),".\nImpala use ",(0,i.kt)("inlineCode",{parentName:"p"},"=")," to separate partition name and partition value.",(0,i.kt)("br",{parentName:"p"}),"\n","To create a partitioned Hudi read optimized table on Impala:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"CREATE EXTERNAL TABLE database.table_name\nLIKE PARQUET '/path/to/load/xxx.parquet'\nPARTITION BY (year int, month int, day int)\nSTORED AS HUDIPARQUET\nLOCATION '/path/to/load';\nALTER TABLE database.table_name RECOVER PARTITIONS;\n")),(0,i.kt)("p",null,"After Hudi made a new commit, refresh the Impala table to get the latest results."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"REFRESH database.table_name\n")))}m.isMDXComponent=!0}}]);