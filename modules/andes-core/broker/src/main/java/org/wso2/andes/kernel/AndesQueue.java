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

import java.io.Serializable;

public class AndesQueue implements Serializable {
     public String queueName;
     public String queueOwner;
     public boolean isExclusive;
     public boolean isDurable;
     public int subscriptionCount;

    public AndesQueue(String queueName, String queueOwner, boolean isExclusive, boolean isDurable) {
        this.queueName = queueName;
        this.queueOwner = queueOwner;
        this.isExclusive = isExclusive;
        this.isDurable = isDurable;
        this.subscriptionCount =1;
    }
}
