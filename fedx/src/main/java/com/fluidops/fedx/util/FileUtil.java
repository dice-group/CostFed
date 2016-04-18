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

package com.fluidops.fedx.util;

import java.io.File;

import com.fluidops.fedx.Config;


/**
 * Utility function for files
 * 
 * @author Andreas Schwarte
 *
 */
public class FileUtil {

	/**
	 * location utility.<p>
	 * 
	 *  if the specified path is absolute, it is returned as is, 
	 *  otherwise a location relative to {@link Config#getBaseDir()} is returned<p>
	 *  
	 *  examples:<p>
	 *  
	 *  <code>
	 *  /home/data/myPath -> absolute linux path
	 *  c:\\data -> absolute windows path
	 *  \\\\myserver\data -> absolute windows network path (see {@link File#isAbsolute()})
	 *  data/myPath -> relative path (relative location to baseDir is returned)
	 *  </code>
	 *  
	 * @param path
	 * @return
	 * 			the file corresponding to the abstract path
	 */
	public static File getFileLocation(String path) {
		
		// check if path is an absolute path that already exists
		File f = new File(path);
		
		if (f.isAbsolute())
			return f;
		
		f = new File( Config.getConfig().getBaseDir() + path);
		return f;
	}
	
}
