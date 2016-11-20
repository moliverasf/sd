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
import java.util.TimerTask;

import communication.ObjectInputStream_DS;
import communication.ObjectOutputStream_DS;
import recipes_service.ServerData;
import recipes_service.activity_simulation.SimulationData;
import recipes_service.communication.Host;
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
public class TSAESessionOriginatorSide extends TimerTask{
	private ServerData serverData;
	public TSAESessionOriginatorSide(ServerData serverData){
		super();
		this.serverData=serverData;		
	}
	
	/**
	 * Implementation of the TimeStamped Anti-Entropy protocol
	 */
	public void run(){
		sessionWithN(serverData.getNumberSessions());
	}

	/**
	 * This method performs num TSAE sessions
	 * with num random servers
	 * @param num
	 */
	public void sessionWithN(int num){
		if(!SimulationData.getInstance().isConnected())
			return;
		List<Host> partnersTSAEsession= serverData.getRandomPartners(num);
		Host n;
		for(int i=0; i<partnersTSAEsession.size(); i++){
			n=partnersTSAEsession.get(i);
			sessionTSAE(n);
		}
	}		//TODO: Change the signature of the method to use Object and check the clas
	
	/**
	 * This method perform a TSAE session
	 * with the partner server n
	 * @param n
	 */
	private void sessionTSAE(Host n){
		if (n == null) return;

		try {
			Socket socket = new Socket(n.getAddress(), n.getPort());
			ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());
			ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());

			// send localSummary and localAck
			TimestampVector localSummary = serverData.getSummary();
			TimestampMatrix localAck = serverData.getAck();
			Log localLog = serverData.getLog();
			
			//TODO - Validate if we do have to increment time stamps, if yes
			//	     is the code below the proper way to do it?
			//       Should we take care of increase number sessions too?
			Timestamp ts = new Timestamp(serverData.getId(), serverData.getNumberSessions()+1);
			localSummary.updateTimestamp(ts);
			//End of TODO
			
			Message msg = new MessageAErequest(localSummary, localAck);
			out.writeObject(msg);

            // receive operations from partner
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

            // receive partner's summary and ack
			if (msg.type() == MsgType.AE_REQUEST){
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
				
				// send and "end of TSAE session" message
				msg = new MessageEndTSAE();
				out.writeObject(msg);
				
				// receive message to inform about the ending of the TSAE session
				msg = (Message)in.readObject();
				if(msg.type() == MsgType.END_TSAE){
					//TODO - Not sure at all that the close socket goes here,
					//	     specially being nested in the AE_REQUEST message
					socket.close();
				}
			}


		} catch (ClassNotFoundException e) { 
			// TODO Why in.readObject triggers this exception? What to do with it?
			//e.printStackTrace();
            //System.exit(1);
		}catch (IOException e) {
			//TODO: send msg request can throw IO Exception, but what do we do? Print? Retry?
	    }
	
	}
}
