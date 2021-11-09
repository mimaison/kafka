/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import kafka.server.metadata.TagRepository
import kafka.utils.Logging
import org.apache.kafka.common.TagResource
import org.apache.kafka.common.message.DescribeTagsRequestData.DescribeTagsResource
import org.apache.kafka.common.message.DescribeTagsResponseData
import org.apache.kafka.common.message.DescribeTagsResponseData.DescribeTagsResourceResult
import org.apache.kafka.common.protocol.Errors

import scala.jdk.CollectionConverters._

class TagHelper(metadataCache: MetadataCache, tagRepository: Option[TagRepository]) extends Logging {

  def describeTags(resources: List[DescribeTagsResource]): List[DescribeTagsResponseData.DescribeTagsResult] = {
    if (tagRepository.isDefined) {
      val results = resources.map { r =>
        val resource = new TagResource(TagResource.Type.forId(r.resourceType()), r.resourceName())
        val tagsProp = tagRepository.get.topicTags(resource)
        val tags = tagsProp.asScala.map(t => new DescribeTagsResourceResult()
            .setName(t._1)
            .setValue(t._2)
        ).toList.asJava
        new DescribeTagsResponseData.DescribeTagsResult()
          .setTags(tags)
          .setResourceName(r.resourceName())
          .setResourceType(r.resourceType())
          .setErrorCode(Errors.NONE.code)
      }
      //TODO
      println(results)
      results
    } else {
      println("No tag repository!!")
      List()
    }
  }
}
