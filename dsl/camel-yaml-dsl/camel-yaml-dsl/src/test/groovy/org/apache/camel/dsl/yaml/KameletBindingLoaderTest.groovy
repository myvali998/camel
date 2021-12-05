/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.dsl.yaml

import org.apache.camel.dsl.yaml.support.YamlTestSupport
import org.apache.camel.model.ToDefinition

class KameletBindingLoaderTest extends YamlTestSupport {
    @Override
    def doSetup() {
        context.start()
    }

    def "kamelet binding from kamelet to kamelet"() {
        when:
            loadBindings('''
                apiVersion: camel.apache.org/v1alpha1
                kind: KameletBinding
                metadata:
                  name: timer-event-source                  
                spec:
                  source:
                    ref:
                      kind: Kamelet
                      apiVersion: camel.apache.org/v1
                      name: timer-source
                    properties:
                      message: "Hello world!"
                  sink:
                    ref:
                      kind: Kamelet
                      apiVersion: camel.apache.org/v1
                      name: log-sink
            ''')
        then:
            context.routeDefinitions.size() == 3

            with (context.routeDefinitions[0]) {
                routeId == 'timer-event-source'
                input.endpointUri == 'kamelet:timer-source?message=Hello+world%21'
                outputs.size() == 1
                with (outputs[0], ToDefinition) {
                    endpointUri == 'kamelet:log-sink'
                }
            }
    }

    def "kamelet binding from uri to kamelet"() {
        when:
        loadBindings('''
                apiVersion: camel.apache.org/v1alpha1
                kind: KameletBinding
                metadata:
                  name: timer-event-source                  
                spec:
                  source:
                    uri: timer:foo
                  sink:
                    ref:
                      kind: Kamelet
                      apiVersion: camel.apache.org/v1
                      name: log-sink
            ''')
        then:
        context.routeDefinitions.size() == 2

        with (context.routeDefinitions[0]) {
            routeId == 'timer-event-source'
            input.endpointUri == 'timer:foo'
            outputs.size() == 1
            with (outputs[0], ToDefinition) {
                endpointUri == 'kamelet:log-sink'
            }
        }
    }

    def "kamelet binding from uri to uri"() {
        when:
        loadBindings('''
                apiVersion: camel.apache.org/v1alpha1
                kind: KameletBinding
                metadata:
                  name: timer-event-source                  
                spec:
                  source:
                    uri: timer:foo
                  sink:
                    uri: log:bar
            ''')
        then:
        context.routeDefinitions.size() == 1

        with (context.routeDefinitions[0]) {
            routeId == 'timer-event-source'
            input.endpointUri == 'timer:foo'
            outputs.size() == 1
            with (outputs[0], ToDefinition) {
                endpointUri == 'log:bar'
            }
        }
    }

}
