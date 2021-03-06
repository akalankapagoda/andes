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

package org.wso2.andes.kernel;

import java.util.List;
import java.util.Map;

public interface AndesContextStore {


    /**
     * initialize the storage with the connection
     * @param connection  connection to the storage (JDBC/Cassandra/H2)
     * @throws AndesException
     */
    public void init(DurableStoreConnection connection) throws AndesException;

    /**
     * get all durable encoded subscriptions as strings
     * @return list of <id,subscriptions>
     */
    public Map<String, List<String>> getAllStoredDurableSubscriptions() throws AndesException;

    /**
     * store subscription to the durable store
     * @param destinationIdentifier  identifier of the destination (queue/topic) of the queue this subscription is bound to
     * @param subscriptionID  id of the subscription
     * @param subscriptionEncodeAsStr string encoded subscription
     */
    public void storeDurableSubscription(String destinationIdentifier, String subscriptionID, String subscriptionEncodeAsStr) throws AndesException;

    /**
     * Remove stored subscription from durable store
     * @param destinationIdentifier  identifier of the destination (queue/topic) of the queue this subscription is bound to
     * @param subscriptionID  id of the subscription
     */
    public void removeDurableSubscription(String destinationIdentifier, String subscriptionID) throws AndesException;

    /**
     * store details of node
     * @param nodeID id of the node
     * @param data detail to store
     */
    public void storeNodeDetails(String nodeID, String data) throws AndesException;

    /**
     * get all node information stored
     * @return  map of node information
     */
    public Map<String,String> getAllStoredNodeData() throws AndesException;

    /**
     * remove stored node information
     * @param nodeID  id of the node
     */
    public void removeNodeData(String nodeID) throws AndesException;

    /**
     * add message counting entry for queue
     * @param destinationQueueName  name of queue
     */
    public void addMessageCounterForQueue(String destinationQueueName) throws AndesException;

    /**
     * get message count of queue
     * @param destinationQueueName  name of queue
     * @return  message count
     */
    public long getMessageCountForQueue(String destinationQueueName) throws AndesException;


    /**
     * remove Message counting entry
     * @param destinationQueueName  name of queue
     */
    public void removeMessageCounterForQueue(String destinationQueueName) throws AndesException;

    /**
     * store exchange information (amqp)
     * @param exchangeName name of string
     * @param exchangeInfo info for exchange
     */
    public void storeExchangeInformation(String exchangeName, String exchangeInfo) throws AndesException;

    /**
     * get all exchanges stored
     * @return  list of exchanges
     */
    public List<AndesExchange> getAllExchangesStored() throws AndesException;


    /**
     * delete all exchange information
     * @param exchangeName name of exchange
     */
    public void deleteExchangeInformation(String exchangeName) throws AndesException;

    /**
     * close the context store
     */
    public void close();

}
