/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.wso2.andes.kernel.slot;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.thrift.exception.ThriftClientException;
import org.wso2.andes.thrift.slot.gen.SlotInfo;
import org.wso2.andes.thrift.slot.gen.SlotManagementService;

import java.util.Set;

/**
 * Thrift client to retrieve slots.
 */
public class SlotThriftClient {

    private boolean reconnectingStarted = false;

    private TTransport transport;

    private SlotManagementService.Client client = null;

    private static final Log log = LogFactory.getLog(SlotThriftClient.class);

    public Slot getSlot(String queueName,
                                            String nodeId) throws ConnectionException {
        SlotInfo slotInfo;
        try {
            client = getServiceClient();
            slotInfo = client.getSlotInfo(queueName, nodeId);
            return convertSlotInforToSlot(slotInfo);
        } catch (TException e) {
            try {
                //retry once
                reConnectToServer();
                slotInfo = client.getSlotInfo(queueName, nodeId);
                return convertSlotInforToSlot(slotInfo);
            } catch (TException e1) {
                handleCoordinatorChanges();
                throw new ConnectionException("Coordinator has changed", e);
            }

        } catch (ThriftClientException e) {
            handleCoordinatorChanges();
            throw new ConnectionException("Error occurred in thrift client " + e.getMessage(), e);
        }
    }

    private SlotManagementService.Client getServiceClient() throws TTransportException, ThriftClientException {
        if (client == null) {
            HazelcastAgent hazelcastAgent = HazelcastAgent.getInstance();
            String thriftCoordinatorServerIP = hazelcastAgent.getThriftServerDetailsMap().get(
                    SlotCoordinationConstants.THRIFT_COORDINATOR_SERVER_IP);
            String thriftCoordinatorServerPortString = hazelcastAgent.getThriftServerDetailsMap().
                    get(SlotCoordinationConstants.THRIFT_COORDINATOR_SERVER_PORT);

            Integer soTimeout = AndesConfigurationManager.readValue(AndesConfiguration.COORDINATION_THRIFT_SO_TIMEOUT);

            if((null == thriftCoordinatorServerIP) || (null == thriftCoordinatorServerPortString)){
                throw new ThriftClientException(
                        "Thrift coordinator details are not updated in the map yet");
            }else{

                int thriftCoordinatorServerPort = Integer.parseInt(thriftCoordinatorServerPortString);
                transport = new TSocket(thriftCoordinatorServerIP,
                        thriftCoordinatorServerPort, soTimeout);
                try {
                    transport.open();
                    TProtocol protocol = new TBinaryProtocol(transport);
                    return new SlotManagementService.Client(protocol);
                } catch (TTransportException e) {
                    log.error("Could not initialize the Thrift client. " + e.getMessage(), e);
                    throw new TTransportException(
                            "Could not initialize the Thrift client. " + e.getMessage(), e);
                }
            }

        }
        return client;
    }

    private Slot convertSlotInforToSlot(SlotInfo slotInfo) {
        Slot slot = new Slot();
        slot.setStartMessageId(slotInfo.getStartMessageId());
        slot.setEndMessageId(slotInfo.getEndMessageId());
        slot.setStorageQueueName(slotInfo.getQueueName());
        return slot;
    }

    private void reConnectToServer() throws TTransportException {
        int thriftCoordinatorServerPort = 0;
        String thriftCoordinatorServerIP = null;
        Long reconnectTimeout = (Long) AndesConfigurationManager.readValue
                (AndesConfiguration.COORDINATOR_THRIFT_RECONNECT_TIMEOUT) * 1000;
        try {
            //Reconnect timeout set because Hazelcast coordinator may still not elected in failover scenario
            Thread.sleep(reconnectTimeout);
            HazelcastAgent hazelcastAgent = HazelcastAgent.getInstance();
            thriftCoordinatorServerIP = hazelcastAgent.getThriftServerDetailsMap().get(
                    SlotCoordinationConstants.THRIFT_COORDINATOR_SERVER_IP);
            thriftCoordinatorServerPort = Integer.parseInt(
                    hazelcastAgent.getThriftServerDetailsMap().
                            get(SlotCoordinationConstants.THRIFT_COORDINATOR_SERVER_PORT));
            transport = new TSocket(thriftCoordinatorServerIP, thriftCoordinatorServerPort);
            log.info("Reconnecting to Slot Coordinator " + thriftCoordinatorServerIP + ":"
                    + thriftCoordinatorServerPort);

            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            client = new SlotManagementService.Client(protocol);
        } catch (TTransportException e) {
            log.error("Could not connect to the Thrift Server " + thriftCoordinatorServerIP + ":" +
                    thriftCoordinatorServerPort + e.getMessage(), e);
            throw new TTransportException(
                    "Could not connect to the Thrift Server " + thriftCoordinatorServerIP + ":" +
                            thriftCoordinatorServerPort, e);
        } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
        }
    }

    private void handleCoordinatorChanges() {
        resetServiceClient();
        if (!isReconnectingStarted()) {
            setReconnectingFlag(true);
        }
        startServerReconnectingThread();
    }

    private void resetServiceClient() {
        client = null;
        transport.close();
    }

    /**
     * A flag to specify whether the reconnecting to thrift server is happening or not
     *
     * @return whether the reconnecting to thrift server is happening or not
     */
    public boolean isReconnectingStarted() {
        return reconnectingStarted;
    }

    /**
     * Set reconnecting flag
     *
     * @param reconnectingFlag  flag to see whether the reconnecting to the thrift server is
     *                          started
     */
    public void setReconnectingFlag(boolean reconnectingFlag) {
        reconnectingStarted = reconnectingFlag;
    }

    /**
     * This thread is responsible of reconnecting to the thrift server of the coordinator until it
     * gets succeeded
     */
    private void startServerReconnectingThread() {
        new Thread() {
            public void run() {
                /**
                 * this thread will try to connect to thrift server while reconnectingStarted
                 * flag is true
                 * After successfully connecting to the server this flag will be set to true.
                 * While loop is therefore intentional.
                 */
                SlotDeliveryWorkerManager slotDeliveryWorkerManager = SlotDeliveryWorkerManager
                        .getInstance();
                while (reconnectingStarted) {

                    try {
                        reConnectToServer();
                        /**
                         * If re connect to server is successful, following code segment will be
                         * executed
                         */
                        reconnectingStarted = false;
                        Set<AndesSubscription> localSubscribers =
                                AndesContext.getInstance().getSubscriptionEngine().getActiveLocalSubscribersForNode();

                        slotDeliveryWorkerManager.startAllSlotDeliveryWorkers(localSubscribers);
                    } catch (TTransportException e) {
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        }
                    } catch (AndesException e) {
                        log.error("Error starting SlotDeliveryWorkers after reconnecting to the thrift server", e);
                    }

                }
            }
        }.start();
    }
}
