/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.karthikvasanthan.processors.MakeRedshiftManifest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Tags({"redis","manifest","redshift","custom"})
@CapabilityDescription("Reads all members of a redis set and creates a manifest file to queue for loading in redshift")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MakeRedshiftManifest extends AbstractProcessor {

    public static final PropertyDescriptor REDIS_HOST = new PropertyDescriptor
            .Builder().name("Redis Host")
            .description("Hostname of the redis server")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor REDIS_KEY = new PropertyDescriptor
            .Builder().name("Redis Key")
            .description("The redis key to which the member is to be added")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created manifest file")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to create manifest file")
            .build();

    public static final Relationship REL_EMPTY = new Relationship.Builder()
            .name("empty")
            .description("The file list key is empty")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(REDIS_HOST);
        descriptors.add(REDIS_KEY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_EMPTY);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        String hostName = context.getProperty(REDIS_HOST).toString();
        this.jedis = getJedis(hostName);
    }

    private volatile Jedis jedis;

    private String NUM_FILES = "num_s3_files";

    private Jedis getJedis(final String hostName) {

        return new Jedis(hostName);

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        String redisKey = context.getProperty(REDIS_KEY).evaluateAttributeExpressions(flowFile).getValue();
        String manifestPrefix =  "{\n" +
                " \"entries\": ";
        String manifestSuffix = "\n" +
                "}";

        try {
            Set<String> files = jedis.smembers(redisKey);
            Integer numFiles = files.size();
            if (numFiles > 0) {
                ArrayList<S3DataFile> s3DataFiles = new ArrayList<>();
                for (String file:files) {
                    s3DataFiles.add(new S3DataFile(file,true));
                    jedis.srem(redisKey,file);
                }

                final ObjectMapper mapper = new ObjectMapper();
                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                String manifestFile = manifestPrefix + mapper.writeValueAsString(s3DataFiles) + manifestSuffix;
                //FlowFile manifestFlowFile = session.create(flowFile);
                flowFile = session.write(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(OutputStream outputStream) throws IOException {
                        outputStream.write(manifestFile.getBytes(StandardCharsets.UTF_8));
                    }
                });
                session.putAttribute(flowFile,NUM_FILES,numFiles.toString());
                session.transfer(flowFile,REL_SUCCESS);
            }
            else {
                session.transfer(flowFile,REL_EMPTY);
            }
        }
        catch (ProcessException | JsonProcessingException e) {
            session.transfer(flowFile,REL_FAILURE);
        }

    }
}
