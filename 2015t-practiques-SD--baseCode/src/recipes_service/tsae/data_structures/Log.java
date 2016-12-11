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
import java.util.ListIterator;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import recipes_service.data.Operation;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class Log implements Serializable{

	private static final long serialVersionUID = -4864990265268259700L;
	/**
	 * This class implements a log, that stores the operations
	 * received  by a client.
	 * They are stored in a ConcurrentHashMap (a hash table),
	 * that stores a list of operations for each member of 
	 * the group.
	 */
	private ConcurrentHashMap<String, List<Operation>> log= new ConcurrentHashMap<String, List<Operation>>();  

	public Log(List<String> participants){
		// create an empty log
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			log.put(it.next(), new Vector<Operation>());
		}
	}

	/**
	 * inserts an operation into the log. Operations are 
	 * inserted in order. If the last operation for 
	 * the user is not the previous operation than the one 
	 * being inserted, the insertion will fail.
	 * 
	 * @param op
	 * @return true if op is inserted, false otherwise.
	 */
	public synchronized boolean add(Operation op){
		//MOLIVERASF: Get the host, and add the operation if it's newer than the last
		boolean isAdded = false;
		String host = op.getTimestamp().getHostid();
		List<Operation> hostOps = log.get(host);
		
		if(hostOps.size() == 0){
			isAdded = true;
		}else{
			Operation lastOp = hostOps.get(hostOps.size()-1);
			//Before adding the operation, check if the last operation of
			//the host is older than the one being inserted
			if(op.getTimestamp().compare(lastOp.getTimestamp()) > 0){
				isAdded = true;
			}
		}
		
		if(isAdded){
			hostOps.add(op);
		}	
		return isAdded;
	}

	/**
	 * Checks the received summary (sum) and determines the operations
	 * contained in the log that have not been seen by
	 * the proprietary of the summary.
	 * Returns them in an ordered list.
	 * @param sum
	 * @return list of operations
	 */
	public synchronized List<Operation> listNewer(TimestampVector sum){
		List<Operation> newerOps = new ArrayList<Operation>();		
		
		for (String node : this.log.keySet()){
			//Get the list of operations for the given node in the local log
			List<Operation> operations = this.log.get(node);
			
			//Get the last last timestamp of the received summary for the same node
			Timestamp timestampToCompare = sum.getLast(node);
			
			//Add each operation with time stamp newer than the last in the received summary to the final list 
			for (Operation op : operations) {
				if(op.getTimestamp().compare(timestampToCompare) > 0){
					newerOps.add(op);
				}
			}
		}
		
		return newerOps;	
	}

	/**
	 * Removes from the log the operations that have
	 * been acknowledged by all the members
	 * of the group, according to the provided
	 * ackSummary. 
	 * @param ack: ackSummary.
	 */
	public void purgeLog(TimestampMatrix ack){
	}

	/**
	 * equals
	 */
	@Override
	public boolean equals(Object obj) {
		//MOLIVERASF: Validate that obj is a Log and then compare its contents with this
		if(obj != null && obj instanceof Log){
			 return this.toString().equals(((Log)obj).toString());
		}else{
			return false;
		}
		//TODO: Not for phase 1: Instead of compare results of toString() loop
		//over the maps values or try this.log.equals(((Log)obj).log)
	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String name="";
		for(Enumeration<List<Operation>> en=log.elements(); en.hasMoreElements(); ){
			List<Operation> sublog=en.nextElement();
			for(ListIterator<Operation> en2=sublog.listIterator(); en2.hasNext();){
				name+=en2.next().toString()+"\n";
			}
		}

		return name;
	}
}
