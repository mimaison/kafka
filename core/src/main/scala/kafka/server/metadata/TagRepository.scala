/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server.metadata

import org.apache.kafka.common.TagResource
import org.apache.kafka.common.message.MetadataRequestData

import java.util.Properties

trait TagRepository {
  /**
   * Return a copy of the tags for the given resource. Future changes will not be reflected.
   *
   * @param tagResource the resource for which the tags will be returned
   * @return a copy of the topic tags for the given resource
   */
  def topicTags(tagResource: TagResource): Properties

  def queryTopics(predicates: MetadataRequestData.TopicTagCollection): collection.Set[String]

}
