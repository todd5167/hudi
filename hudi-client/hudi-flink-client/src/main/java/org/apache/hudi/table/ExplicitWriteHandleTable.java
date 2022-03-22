/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.util.List;

/***
 *  对 hoodie table 的操作。 upsert、delete、insert overwrite， insert overwrite table
 *  通过 HoodieWriteHandle 向 HoodieTable 插入、delete数据。
 *
 * HoodieTable that need to pass in the
 * {@link org.apache.hudi.io.HoodieWriteHandle} explicitly.
 */
public interface ExplicitWriteHandleTable<T extends HoodieRecordPayload> {
  /**
   *  在提供的 instantTime 将一批新记录更新到 Hoodie 表中。
   * Upsert a batch of new records into Hoodie table at the supplied instantTime.
   *
   *    指定 HoodieWriteHandle，以便对底层文件进行细粒度控制。
   * <p>Specifies the write handle explicitly in order to have fine grained control with
   * the underneath file.
   *
   * @param context     HoodieEngineContext
   * @param writeHandle The write handle
   * @param instantTime Instant Time for the action
   * @param records     hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  HoodieWriteMetadata<List<WriteStatus>> upsert(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      List<HoodieRecord<T>> records);

  /**
   * Insert a batch of new records into Hoodie table at the supplied instantTime.
   *
   * <p>Specifies the write handle explicitly in order to have fine grained control with
   * the underneath file.
   *
   * @param context     HoodieEngineContext
   * @param writeHandle The write handle
   * @param instantTime Instant Time for the action
   * @param records     hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  HoodieWriteMetadata<List<WriteStatus>> insert(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      List<HoodieRecord<T>> records);

  /**
   * Deletes a list of {@link HoodieKey}s from the Hoodie table, at the supplied instantTime {@link HoodieKey}s will be
   * de-duped and non existent keys will be removed before deleting.
   *
   * <p>Specifies the write handle explicitly in order to have fine grained control with
   * the underneath file.
   *
   * @param context     HoodieEngineContext
   * @param writeHandle The write handle
   * @param instantTime Instant Time for the action
   * @param keys   {@link List} of {@link HoodieKey}s to be deleted
   * @return HoodieWriteMetadata
   */
  HoodieWriteMetadata<List<WriteStatus>> delete(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      List<HoodieKey> keys);

  /**
   * Upserts the given prepared records into the Hoodie table, at the supplied instantTime.
   *
   * <p>This implementation requires that the input records are already tagged 被标记, and de-duped if needed.
   *
   * <p>Specifies the write handle explicitly in order to have fine grained control with
   * the underneath file.
   *
   * @param context    HoodieEngineContext
   * @param instantTime Instant Time for the action
   * @param preppedRecords  hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  HoodieWriteMetadata<List<WriteStatus>> upsertPrepped(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      List<HoodieRecord<T>> preppedRecords);

  /**
   * Inserts the given prepared records into the Hoodie table, at the supplied instantTime.
   *
   * <p>This implementation requires that the input records are already tagged, and de-duped if needed.
   *
   * <p>Specifies the write handle explicitly in order to have fine grained control with
   * the underneath file.
   *
   * @param context    HoodieEngineContext
   * @param instantTime Instant Time for the action
   * @param preppedRecords  hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  HoodieWriteMetadata<List<WriteStatus>> insertPrepped(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      List<HoodieRecord<T>> preppedRecords);

  /**
   *   插入动态分区
   *  对于输入记录中包含的分区路径，替换所有现有记录并将指定的新记录插入到 Hoodie 表中。
   *
   * Replaces all the existing records and inserts the specified new records into Hoodie table at the supplied instantTime,
   * for the partition paths contained in input records.
   *
   * @param context HoodieEngineContext
   * @param writeHandle The write handle
   * @param instantTime Instant time for the replace action
   * @param records input records
   * @return HoodieWriteMetadata
   */
  HoodieWriteMetadata<List<WriteStatus>> insertOverwrite(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      List<HoodieRecord<T>> records);

  /**
   *  插入静态分区
   *  对于输入记录中包含的分区路径，删除 Hoodie 表的所有现有记录，并在提供的 InstantTime 将指定的新记录插入到 Hoodie 表中。
   *
   * Deletes all the existing records of the Hoodie table and inserts the specified new records into Hoodie table at the supplied instantTime,
   * for the partition paths contained in input records.
   *
   * @param context HoodieEngineContext
   * @param writeHandle The write handle
   * @param instantTime Instant time for the replace action
   * @param records input records
   * @return HoodieWriteMetadata
   */
  HoodieWriteMetadata<List<WriteStatus>> insertOverwriteTable(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      List<HoodieRecord<T>> records);
}
