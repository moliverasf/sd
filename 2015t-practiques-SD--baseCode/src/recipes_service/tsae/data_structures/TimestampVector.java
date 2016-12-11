/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.data_structures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TimestampVector implements Serializable{

	private static final long serialVersionUID = -765026247959198886L;
	/**
	 * This class stores a summary of the timestamps seen by a node.
	 * For each node, stores the timestamp of the last received operation.
	 */
	
	private ConcurrentHashMap<String, Timestamp> timestampVector= new ConcurrentHashMap<String, Timestamp>();
	
	/**
	 * Copy-constructor
	 * @param timestamVector: The vector from the object to be copied
	 */
	public TimestampVector(ConcurrentHashMap<String, Timestamp> timestampVector) {
        this.timestampVector = new ConcurrentHashMap<>(timestampVector);
    }
	
	public TimestampVector (List<String> participants){
		// create and empty TimestampVector
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			String id = it.next();
			// when sequence number of timestamp < 0 it means that the timestamp is the null timestamp
			timestampVector.put(id, new Timestamp(id, Timestamp.NULL_TIMESTAMP_SEQ_NUMBER));
		}
	}

	/**
	 * Updates the timestamp vector with a new timestamp. 
	 * @param timestamp
	 */
	public synchronized void updateTimestamp(Timestamp timestamp){
		//MOLIVERAF: Replace old timestamp of the relevant node for the one received
		String node = timestamp.getHostid();
		Timestamp oldTimestamp = timestampVector.get(node);		
		timestampVector.replace(node, oldTimestamp, timestamp);
	}
	
	/**
	 * merge in another vector, taking the elementwise maximum
	 * @param tsVector (a timestamp vector)
	 */
	public synchronized void updateMax(TimestampVector tsVector){
		//MOLIVERASF: For each node, replace the current values for the 
		//ones in tsVector that are newer
		for (ConcurrentHashMap.Entry<String, Timestamp> node : timestampVector.entrySet()) {
			Timestamp currentTS = node.getValue();
			Timestamp paramTS = tsVector.getLast(node.getKey());
		    if(currentTS.compare(paramTS) < 0){
		    	timestampVector.replace(node.getKey(), paramTS);
		    }
		}
	}
	
	/**
	 * 
	 * @param node
	 * @return the last timestamp issued by node that has been
	 * received.
	 */
	public Timestamp getLast(String node){
		//MOLIVERASF: Return the timestamp of the given node
		return timestampVector.get(node);
	}
	
	/**
	 * merges local timestamp vector with tsVector timestamp vector taking
	 * the smallest timestamp for each node.
	 * After merging, local node will have the smallest timestamp for each node.
	 *  @param tsVector (timestamp vector)
	 */
	public synchronized void mergeMin(TimestampVector tsVector){
		//MOLIVERASF: For each node, replace the current values for the 
		//ones in tsVector that are newer
		for (ConcurrentHashMap.Entry<String, Timestamp> node : timestampVector.entrySet()) {
			Timestamp currentTS = node.getValue();
			Timestamp paramTS = tsVector.getLast(node.getKey());
		    if(currentTS.compare(paramTS) > 0){
		    	timestampVector.replace(node.getKey(), paramTS);
		    }
		}
	}
	
	/**
	 * clone
	 * MOLIVERASF: Added synchronized and called the new constructor-copy.
	 */
	public synchronized TimestampVector clone(){
		return new TimestampVector(this.timestampVector);
	}
	
	/**
	 * equals
	 */
	public boolean equals(Object obj){
		//MOLIVERASF: Check that tsVector has the same nodes and values than the attribute in this		
		/**if(obj != null && obj instanceof Log){
			 return this.toString().equals(((TimestampVector)obj).toString());
		}else{
			return false;
		}**/
		
		
		boolean equals = false;
		if(obj != null && this != obj){
			if(obj instanceof TimestampVector){
				TimestampVector tsVector = (TimestampVector) obj;
				//if obj is a TimestampVector, check that attributes timestampVector to compare are not null
				if(this.timestampVector != null && tsVector.timestampVector != null){
					if(this.timestampVector == tsVector.timestampVector){
						equals = true; //they point to the same ref.
					}else{
						equals = this.timestampVector.equals(tsVector.timestampVector);
					}
				}
			}
		}
		return equals;
	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all="";
		if(timestampVector==null){
			return all;
		}
		for(Enumeration<String> en=timestampVector.keys(); en.hasMoreElements();){
			String name=en.nextElement();
			if(timestampVector.get(name)!=null)
				all+=timestampVector.get(name)+"\n";
		}
		return all;
	}
}
