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
package org.elasql.cache.tpart;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasql.schedule.tpart.sink.SunkPlan;
import org.elasql.server.Elasql;
import org.elasql.sql.CachedRecord;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.tx.Transaction;

public class TPartTxLocalCache {

	private Transaction tx;
	private long txNum;
	private TPartCacheMgr cacheMgr;
	private Map<RecordKey, CachedRecord> recordCache = new HashMap<RecordKey, CachedRecord>();
	private long localStorageId;

	public TPartTxLocalCache(Transaction tx) {
		this.tx = tx;
		this.txNum = tx.getTransactionNumber();
		this.cacheMgr = (TPartCacheMgr) Elasql.remoteRecReceiver();
		this.localStorageId = TPartCacheMgr.toSinkId(Elasql.serverId());
	}

	/**
	 * Reads a CachedRecord with the specified key from a previous sink. A sink
	 * may be a T-Graph or the local storage.
	 * 
	 * @param key
	 *            the key of the record
	 * @return the specified record
	 */
	public CachedRecord readFromSink(RecordKey key) {

		CachedRecord rec = null;
		rec = cacheMgr.readFromSink(key, tx);
		rec.setSrcTxNum(txNum);
		recordCache.put(key, rec);

		return rec;
	}

	/**
	 * Reads a CachedRecord from the cache. If the specified record does not
	 * exist, reads from the specified transaction through {@code TPartCacheMgr}
	 * .
	 * 
	 * @param key
	 *            the key of the record
	 * @param src
	 *            the id of the transaction who will pass the record to the
	 *            caller
	 * @return the specified record
	 */
	public CachedRecord read(RecordKey key, long src) {
		CachedRecord rec = recordCache.get(key);
		if (rec != null)
			return rec;

		rec = cacheMgr.takeFromTx(key, src, txNum);
		recordCache.put(key, rec);
		return rec;
	}

	public void update(RecordKey key, CachedRecord rec) {
		rec.setSrcTxNum(txNum);
		recordCache.put(key, rec);
	}

	public void insert(RecordKey key, Map<String, Constant> fldVals) {
		CachedRecord rec = CachedRecord.newRecordForInsertion(key, fldVals);
		rec.setSrcTxNum(txNum);
		recordCache.put(key, rec);
	}

	public void delete(RecordKey key) {
		CachedRecord dummyRec = CachedRecord.newRecordForDeletion(key);
		dummyRec.setSrcTxNum(txNum);
		recordCache.put(key, dummyRec);
	}

	public void flush(SunkPlan plan, List<CachedEntryKey> cachedEntrySet) {
		// Pass to the transactions
		for (Map.Entry<RecordKey, CachedRecord> entry : recordCache.entrySet()) {
			Long[] dests = plan.getWritingDestOfRecord(entry.getKey());
			if (dests != null) {
				for (long dest : dests) {
					// The destination might be the local storage (txNum < 0)
					if (dest >= 0) {
						CachedRecord clonedRec = new CachedRecord(entry.getValue());
						cacheMgr.passToTheNextTx(entry.getKey(), clonedRec, txNum, dest, false);
					}
				}
			}
		}

		if (plan.isLocalTask()) {
			// Flush to the local storage (write back)
			for (RecordKey key : plan.getLocalWriteBackInfo()) {

				CachedRecord rec = recordCache.get(key);

				// If there is no such record in the local cache,
				// it might be pushed from the same transaction on the other
				// machine.

				cacheMgr.writeBack(key, rec, tx);

			}
		} else {

			// Flush to the local storage (write back)
			for (RecordKey key : plan.getLocalWriteBackInfo()) {

				CachedRecord rec = cacheMgr.takeFromTx(key, txNum, localStorageId);

				cacheMgr.writeBack(key, rec, tx);

			}

		}

	}
}
