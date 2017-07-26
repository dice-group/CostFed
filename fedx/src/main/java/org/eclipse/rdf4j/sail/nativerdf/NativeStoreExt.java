/*
 * Copyright (C) 2008-2013, fluid Operations AG
 *
 * FedX is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.eclipse.rdf4j.sail.nativerdf;

import java.io.File;
import java.io.IOException;

import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;


/**
 * Native Store extension which introduces a hook with a specialized connection (cf 
 * {@link NativeStoreConnectionExt}), which allows for efficient evaluation of
 * prepared queries without prior optimization.<p>
 * 
 * Whenever a native store is to be used as a repository within FedX, use this extension.
 * 
 * @author Andreas Schwarte
 * @see NativeStoreConnectionExt
 *
 */
public class NativeStoreExt extends NativeStore {

	public NativeStoreExt() {
		super();
	}

	public NativeStoreExt(File dataDir, String tripleIndexes) {
		super(dataDir, tripleIndexes);
	}

	public NativeStoreExt(File dataDir) {
		super(dataDir);
	}
	
	
	@Override
	protected NotifyingSailConnection getConnectionInternal() throws SailException {
		try {
			return new NativeStoreConnectionExt(this);
		} catch (IOException e) {
			throw new SailException(e);
		}
	}

}
