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

	public void run() {
				
		try {
			ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());
			ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());

			// receive originator's summary and ack
			Message msg;
			TimestampVector localSummary = serverData.getSummary();
			TimestampMatrix localAck = serverData.getAck();
			Log localLog = serverData.getLog();
			
			msg = (Message)in.readObject();			
			if(msg.type() == MsgType.AE_REQUEST){
				// send operations
				//TODO I think this part is done right, just worried about error
				//     handling and that I need AE_REQUEST from the PartnerSide which
				//     seems strange. But lets do the other part of the code
				//TODO check instanceof or with the type is enough?
				MessageAErequest msgRequest = (MessageAErequest)msg;
				TimestampVector partnerSummary = msgRequest.getSummary().clone();
				List<Operation> newOps = localLog.listNewer(partnerSummary);
				for(Operation op : newOps){
					msg = new MessageOperation(op);
					out.writeObject(msg);
				}
				
				// send to originator: local's summary and ack
				//TODO: Ensure like in the originator if increas Timestamp is necessary
				//		here or somewhere else...
				msg = new MessageAErequest(localSummary, localAck);
				out.writeObject(msg);
				
	            // receive operations
				msg = (Message)in.readObject(); //TODO how do I know that I'm ready to read?
				while (msg.type() == MsgType.OPERATION){
					//TODO validate this add Operation
					MessageOperation msgOp = (MessageOperation)msg;
					Operation op = msgOp.getOperation();
					if(op instanceof AddOperation){ //TODO I think this must be synchronized
						localLog.add(op);
						localSummary.updateTimestamp(op.getTimestamp());
						//TODO do we have to addRecipe like in the code below???
						serverData.getRecipes().add(((AddOperation)op).getRecipe());	
					}
					
					msg = (Message)in.readObject(); //next message
				}
				
				// receive message to inform about the ending of the TSAE session
				if(msg.type() == MsgType.END_TSAE){
					// send and "end of TSAE session" message
					msg = new MessageEndTSAE();
					out.writeObject(msg);
				}
				
				
			}			
			socket.close();	//TODO - This here or after sending END_TSAE?	
		} catch (ClassNotFoundException e) {
			// TODO What to do with this exception and why it can happen
			//e.printStackTrace();
            //System.exit(1);
		}catch (IOException e) {
			//TODO What to do with this exception?
	    }
	}
}
