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

package recipes_service.tsae.sessions;


import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import communication.ObjectInputStream_DS;
import communication.ObjectOutputStream_DS;
import recipes_service.ServerData;
import recipes_service.communication.Message;
import recipes_service.communication.MessageAErequest;
import recipes_service.communication.MessageEndTSAE;
import recipes_service.communication.MessageOperation;
import recipes_service.communication.MsgType;
import recipes_service.data.AddOperation;
import recipes_service.data.Operation;
import recipes_service.data.OperationType;
import recipes_service.tsae.data_structures.Log;
import recipes_service.tsae.data_structures.Timestamp;
import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TSAESessionPartnerSide extends Thread{
	private Socket socket = null;
	private ServerData serverData = null;
	
	public TSAESessionPartnerSide(Socket socket, ServerData serverData) {
		super("TSAEPartnerSideThread");
		this.socket = socket;
		this.serverData = serverData;
	}

	public synchronized void run() {
				
		try {
			ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());
			ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());
			
			// receive originator's summary and ack
			Message msg;
			msg = (Message)in.readObject();			
			if(msg.type() == MsgType.AE_REQUEST){

				MessageAErequest msgRequest = (MessageAErequest)msg;
				//System.out.println("Partner: Clone originator summary");
				TimestampVector originatorSummary = msgRequest.getSummary().clone();
				//System.out.println("Partner: Clone originator ACK");
				TimestampMatrix originatorAck = msgRequest.getAck().clone();				
				
				/**
				 * Get local values copy:
				 * Use synchronized statement to not block the full object all the time and only the
				 * serverData attribute during the moment that we are getting the local values
				 * and updating the ACK
				 */
				//System.out.println("Partner: Get local attributes");
				TimestampVector localSummary;
				TimestampMatrix localAck;
				Log localLog = serverData.getLog(); //TODO Do we need to clone the log???
				synchronized (serverData){
					localSummary = serverData.getSummary().clone();				
					TimestampMatrix localAckRef = serverData.getAck();
					localAckRef.update(serverData.getId(), localSummary);
					localAck = localAckRef.clone();
				}

				//For each host, send to the originator all the local operations newer
				//than the last one registered in his summary
				//System.out.println("Partner: Send newer operations to originator");
				List<Operation> newOps = localLog.listNewer(originatorSummary);
				for(Operation op : newOps){
					msg = new MessageOperation(op);
					out.writeObject(msg);
				}
				
				// send to originator: local's summary and ack
				msg = new MessageAErequest(localSummary, localAck);
				out.writeObject(msg);
				
	            // receive operations
				//System.out.println("Partner: Receive operations from the originator");
				msg = (Message)in.readObject();
				List<Operation> addOperations = new ArrayList<Operation>();
				List<Operation> removeOperations = new ArrayList<Operation>();
				while (msg.type() == MsgType.OPERATION){
					MessageOperation msgOp = (MessageOperation)msg;
					Operation op = msgOp.getOperation();
					
					//Create a list of add and remove operations
					if(op.getType() == OperationType.ADD){
						addOperations.add(op);
					}else if(op.getType() == OperationType.REMOVE){
						removeOperations.add(op);
					}else{
						//If this code was supposed to evolve, here an error should be triggered
					}
					msg = (Message)in.readObject(); //next message	
				}
				
				
				// receive message to inform about the ending of the TSAE session
				//System.out.println("Partner: Receive end of TSAE");
				if(msg.type() == MsgType.END_TSAE){
					// send and "end of TSAE session" message
					msg = new MessageEndTSAE();
					out.writeObject(msg);
					
					//At this point, the TSAE session is completed and the communication 
					//with the originator is over, it's the moment to update the local values
					//protecting from concurrent access
					synchronized(serverData){
						//Add partner operations and their recipes to the local server
						for(Operation op : addOperations){
							serverData.getLog().add(op);
							serverData.getRecipes().add(((AddOperation)op).getRecipe());
						}
						
						//TODO implement remove operations method to process the list of this type of operation.
						for(Operation op : removeOperations){
						}
						
						//Update Summary and ACK
						serverData.getSummary().updateMax(originatorSummary);
						serverData.getAck().updateMax(originatorAck);
						//TODO purge log in later phase						
					}					
					
				}				
			}			
			socket.close();
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
            System.exit(1);
		}catch (IOException e) {
			//e.printStackTrace();
	    }finally{
	    	//In case of other errors, it will always try to close the socket
	    	/**try{
	    		socket.close();
	    	}catch(IOException e){
	    		e.printStackTrace();
	    	}**/
	    }
	}
}
