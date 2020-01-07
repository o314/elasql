/*******************************************************************************
 * Copyright 2016, 2018 elasql.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.elasql.procedure.calvin;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.calvin.CalvinPostOffice;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.server.Elasql;
import org.elasql.sql.CachedRecord;
import org.elasql.sql.CachedRecordBuilder;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.NotificationPartMetaMgr;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.tx.recovery.DdRecoveryMgr;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public abstract class AllExecuteProcedure<H extends StoredProcedureParamHelper>
		extends CalvinStoredProcedure<H> {
	private static Logger logger = Logger.getLogger(AllExecuteProcedure.class
			.getName());

	private static final String KEY_FINISH = "finish";
	
	private static final int MASTER_NODE = 0;

	public AllExecuteProcedure(long txNum, H paramHelper) {
		super(txNum, paramHelper);
	}
	
	public void prepare(Object... pars) {
		// prepare parameters
		paramHelper.prepareParameters(pars);

		// create a transaction
		boolean isReadOnly = paramHelper.isReadOnly();
		tx = Elasql.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, isReadOnly, txNum);
		tx.addLifecycleListener(new DdRecoveryMgr(tx.getTransactionNumber()));

		// prepare keys
		prepareKeys();
		
		// for the cache layer
		// NOTE: always creates a CacheMgr that can accept remote records
		CalvinPostOffice postOffice = (CalvinPostOffice) Elasql.remoteRecReceiver();
		cacheMgr = postOffice.createCacheMgr(tx, true);
	}
	
	public boolean isParticipated() {
		// Only return true in order to force all nodes to participate.
		return true;
	}
	
	public boolean willResponseToClients() {
		// The master node is the only one that will response to the clients.
		return localNodeId == MASTER_NODE;
	}

	protected void executeTransactionLogic() {
		executeSql(null);

		// Notification for finish
		if (localNodeId == MASTER_NODE) {
			if (logger.isLoggable(Level.INFO))
				logger.info("Waiting for other servers...");

			// Master: Wait for notification from other nodes
			waitForNotification();
			
			if (logger.isLoggable(Level.INFO))
				logger.info("Other servers completion comfirmed.");
		} else {
			// Salve: Send notification to the master
			sendNotification();
		}
	}

	private void waitForNotification() {
		// Wait for notification from other nodes
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++)
			if (nodeId != MASTER_NODE) {
				if (logger.isLoggable(Level.FINE))
					logger.fine("Waiting for the notification from node no." + nodeId);
				
				RecordKey notKey = NotificationPartMetaMgr.createRecordKey(nodeId, MASTER_NODE);
				CachedRecord rec = cacheMgr.readFromRemote(notKey);
				Constant con = rec.getVal(KEY_FINISH);
				int value = (int) con.asJavaVal();
				if (value != 1)
					throw new RuntimeException(
							"Notification value error, node no." + nodeId
									+ " sent " + value);

				if (logger.isLoggable(Level.FINE))
					logger.fine("Receive notification from node no." + nodeId);
			}
	}

	private void sendNotification() {
		RecordKey notKey = NotificationPartMetaMgr.createRecordKey(Elasql.serverId(), MASTER_NODE);
		CachedRecordBuilder builder = new CachedRecordBuilder(notKey, true, true);
		builder.addFldVal(KEY_FINISH, new IntegerConstant(1));
		CachedRecord notRec = builder.build();
		notRec.setSrcTxNum(txNum);

		TupleSet ts = new TupleSet(-1);
		// Use node id as source tx number
		ts.addTuple(notKey, txNum, txNum, notRec);
		Elasql.connectionMgr().pushTupleSet(0, ts);

		if (logger.isLoggable(Level.FINE))
			logger.fine("The notification is sent to the master by tx." + txNum);
	}
}
