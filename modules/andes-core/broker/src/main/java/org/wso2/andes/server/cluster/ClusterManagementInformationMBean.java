/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.andes.server.cluster;

import org.wso2.andes.kernel.*;
import org.wso2.andes.management.common.mbeans.ClusterManagementInformation;
import org.wso2.andes.management.common.mbeans.annotations.MBeanConstructor;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;
import org.wso2.andes.server.management.AMQManagedObject;

import javax.management.JMException;

import java.util.ArrayList;
import java.util.List;


/**
 * <code>ClusterManagementInformationMBean</code> The the JMS MBean that expose cluster management information
 */
public class ClusterManagementInformationMBean extends AMQManagedObject implements ClusterManagementInformation {

    private ClusterManager clusterManager;

    @MBeanConstructor("Creates an MBean exposing an Cluster Manager")
    public ClusterManagementInformationMBean(ClusterManager clusterManager) throws JMException {
        super(ClusterManagementInformation.class , ClusterManagementInformation.TYPE);
        this.clusterManager = clusterManager;
    }

    public String getObjectInstanceName() {
        return ClusterManagementInformation.TYPE;
    }

    public String[] getGlobalQueuesAssigned(String nodeId) {
        return clusterManager.getGlobalQueuesAssigned(nodeId);
    }
    
    public boolean updateWorkerForQueue(String queueToBeMoved,String newNodeToAssign) {
        return clusterManager.updateWorkerForQueue(queueToBeMoved,newNodeToAssign);
    }

    public boolean isClusteringEnabled() {
        return clusterManager.isClusteringEnabled();
    }

    public String getMyNodeID() {
        return clusterManager.getMyNodeID();
    }

    public int getMessageCount(@MBeanOperationParameter(name = "queueName", description = "Name of the queue which message count is required") String queueName) {
        int count = 0;
        try{
            count = clusterManager.numberOfMessagesInGlobalQueue(queueName);
        } catch (AndesException e) {
            throw new RuntimeException(e);
        }
        return count;
    }

    public String getIPAddressForNode(@MBeanOperationParameter(name = "nodeID", description = "Node ID") String nodeId) {
        try {
            return clusterManager.getNodeAddress(nodeId);
        } catch (AndesException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getDestinationQueuesOfCluster() {
        List<String> queueList = new ArrayList<String>();
        for (AndesQueue queue : AndesContext.getInstance().getSubscriptionStore().getDurableQueues()) {
            queueList.add(queue.queueName);
        }
        return queueList;
    }

    public int getNodeQueueMessageCount(String nodeId, String destinationQueue) {
        try {
        return clusterManager.getNodeQueueMessageCount(nodeId, destinationQueue);
        } catch (AndesException e) {
            throw new RuntimeException(e);
        }
    }

    public int getNodeQueueSubscriberCount(String nodeId, String destinationQueue) {
    	throw new UnsupportedOperationException();
    }

    public List<String> getTopics() {
        List<String> topics = null;
        try {
            topics = AndesContext.getInstance().getSubscriptionStore().getTopics();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return topics;
    }

    public List<String> getSubscribers(String topic){
    	throw new UnsupportedOperationException("Check what this should return (subscription IDs?)");
    }

    public int getSubscriberCount(@MBeanOperationParameter(name = "Topic", description = "Topic name") String topic) {
    	throw new UnsupportedOperationException("Check what this should return (subscription IDs?)");

    }
}
