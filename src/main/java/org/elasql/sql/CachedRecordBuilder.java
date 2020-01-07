package org.elasql.sql;

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.sql.Constant;

public class CachedRecordBuilder {

	private RecordKey primaryKey;
	private boolean isNewInserted, isTemp;
	private transient Map<String, Constant> nonKeyFldVals = 
			new HashMap<String, Constant>();
	
	public CachedRecordBuilder(RecordKey primaryKey,
			boolean isNewInserted, boolean isTemp) {
		this.primaryKey = primaryKey;
		this.isNewInserted = isNewInserted;
		this.isTemp = isTemp;
	}
	
	public void addFldVal(String field, Constant val) {
		Constant keyVal = primaryKey.getVal(field);
		if (keyVal == null)
			nonKeyFldVals.put(field, val);
		else if (!keyVal.equals(val))
			throw new UnsupportedOperationException(
					"cannot modify key field: " + field);
	}
	
	public CachedRecord build() {
		return new CachedRecord(
			primaryKey,
			new HashMap<String, Constant>(nonKeyFldVals),
			isNewInserted,
			isTemp
		);
	}
}
