/*
 *
 * Hands-On code of the book Introduction to Reliable Distributed Programming
 * by Christian Cachin, Rachid Guerraoui and Luis Rodrigues
 * Copyright (C) 2005-2011 Luis Rodrigues
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
 *
 * Contact
 * 	Address:
 *		Rua Alves Redol 9, Office 605
 *		1000-029 Lisboa
 *		PORTUGAL
 * 	Email:
 * 		ler@ist.utl.pt
 * 	Web:
 *		http://homepages.gsd.inesc-id.pt/~ler/
 * 
 */

package org.vanilladb.comm.protocols.zabAccept;

import java.util.logging.Level;
import java.util.logging.Logger;

import net.sf.appia.core.AppiaEventException;
import net.sf.appia.core.Channel;
import net.sf.appia.core.Direction;
import net.sf.appia.core.Event;
import net.sf.appia.core.Layer;
import net.sf.appia.core.Session;

import org.vanilladb.comm.messages.TotalOrderMessage;
import org.vanilladb.comm.protocols.consensusUtils.PaxosObjectProposal;
import org.vanilladb.comm.protocols.consensusUtils.PaxosProposal;
import org.vanilladb.comm.protocols.events.Crash;
import org.vanilladb.comm.protocols.events.Nack;
import org.vanilladb.comm.protocols.events.PaxosPropose;
import org.vanilladb.comm.protocols.events.PaxosReturn;
import org.vanilladb.comm.protocols.events.ProcessInitEvent;
import org.vanilladb.comm.protocols.events.Write;
import org.vanilladb.comm.protocols.events.WriteAck;
import org.vanilladb.comm.protocols.events.ZabCacheTom;
import org.vanilladb.comm.protocols.utils.ProcessSet;

/**
 * implements basic Paxos consensus
 */
public class ZabAcceptSession extends Session {

	public ZabAcceptSession(Layer layer) {
		super(layer);
	}

	private ProcessSet correct = null;

	private PaxosProposal tempValue = null;

	private PaxosProposal val = null;

	private long tstamp = 0;

	private int wAcks = 0;

	private int curr_epoch = 0;

	private long curr_sn = 0;

	private int epoch = 0;

	private long sn = 0;

	private boolean proposed = false;

	private long paxosStartTime;

	/**
	 * 
	 * 
	 * @see net.sf.appia.core.Session#handle(net.sf.appia.core.Event)
	 */
	public void handle(Event event) {
		if (event instanceof WriteAck)
			handleWriteAck((WriteAck) event);
		else if (event instanceof Write)
			handleWrite((Write) event);
		else if (event instanceof PaxosPropose)
			handlePaxosPropose((PaxosPropose) event);
		else if (event instanceof Nack)
			handleNack((Nack) event);
		else if (event instanceof Crash)
			handleCrash((Crash) event);
		else if (event instanceof ProcessInitEvent)
			handleProcessInit((ProcessInitEvent) event);
	}

	/**
     * 
     */
	private void init() {
		tempValue = new PaxosProposal();
		val = new PaxosProposal();
		tempValue.abort = val.abort = true;
		wAcks = 0;

		tstamp = correct.getSelfRank();
	}

	/**
	 * 
	 * @param event
	 */
	private void handleProcessInit(ProcessInitEvent event) {
		correct = event.getProcessSet();
		init();
		try {
			event.go();
		} catch (AppiaEventException ex) {
			ex.printStackTrace();
		}
	}

	/*********** begin Prepare phase ***********/

	/**
	 * 
	 * @param ap
	 */
	private void handlePaxosPropose(PaxosPropose pp) {
		if (Logger.getLogger(ZabAcceptSession.class.getName()).isLoggable(
				Level.FINE)) {
			Logger.getLogger(ZabAcceptSession.class.getName()).fine(
					"Paxos Propose for epoch = " + curr_epoch + ", sn = "
							+ curr_sn);
			paxosStartTime = System.currentTimeMillis();
		}

		tstamp = tstamp + correct.getSize();
		tempValue = pp.value;

		this.curr_epoch = pp.epoch;
		this.curr_sn = pp.sn;
		proposed = true;
		try {
			Write ev = new Write(pp.getChannel(), Direction.DOWN, this);
			ev.getMessage().pushObject(tempValue);
			ev.getMessage().pushLong(curr_sn);
			ev.getMessage().pushInt(curr_epoch);
			ev.getMessage().pushInt(correct.getSelfRank());
			ev.go();
		} catch (AppiaEventException ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * Newer proposal acknowledged, propose abort
	 * 
	 * @param nack
	 */
	private void handleNack(Nack nack) {
		if (Logger.getLogger(ZabAcceptSession.class.getName()).isLoggable(
				Level.FINE)) {
			Logger.getLogger(ZabAcceptSession.class.getName()).fine(
					"handle nack");
		}

		proposed = false;
		try {
			PaxosReturn ev = new PaxosReturn(nack.getChannel(), Direction.UP,
					this);
			tempValue.abort = true;
			ev.decision = tempValue;
			ev.go();
		} catch (AppiaEventException ex) {
			ex.printStackTrace();
		}
	}

	/*********** begin Accept phase ***********/
	/**
	 * The learns receive the paxos write event and decide whether to Accept it or Nack it.
	 * @param write
	 */
	private void handleWrite(Write write) {
		int sender = write.getMessage().popInt();
		if (!correct.getProcess(sender).isCorrect()) {
			return;
		}

		int epoch = write.getMessage().popInt();
		long sn = write.getMessage().popLong();
		// TODO Assignment 6
		// Check if the write event is correct
		// If so, there are two messages to send
		// 	1. send the total order message to ZabTOB layer for caching so that it can use it when the ZabCommit event is received later
		//  2. send the WriteAck event back to Leader
		// If not, send the Nack event back to Leader
		// Hint, you may use epoch and sn to identify the correctness of the write event
	}

	/**
	 * 
	 * @param wa
	 */
	private void handleWriteAck(WriteAck wa) {

		if (proposed && wa.getMessage().popLong() == this.curr_sn
				&& wa.getMessage().popInt() == this.curr_epoch) {
			
			wAcks += 1;
			
			// TODO Assignment 6
			// Check if the number of Ack received is over majority to decide whether the paxos request is finished.
			// If so, create a PaxosReturn event and send it UP to ZabTOBLayer.
			// *Remember to set the PaxosPropose object's abort to false and pack this object as the PaxosReturn's decision			
		

		} else {
			if (Logger.getLogger(ZabAcceptSession.class.getName()).isLoggable(
					Level.FINE)) {
				Logger.getLogger(ZabAcceptSession.class.getName())
						.fine("invalid WriteAck, wAcks = " + wAcks
								+ ", Paxos Time = "
								+ (System.currentTimeMillis() - paxosStartTime));
			}
		}
	}

	/*********** end Accept phase ***********/

	/**
	 * Called when some process crashed.
	 * 
	 * @param crash
	 */
	private void handleCrash(Crash crash) {
		int crashedProcess = crash.getCrashedProcess();
		Logger.getLogger(ZabAcceptSession.class.getName()).fine(
				"Process " + crashedProcess + " failed.");
		// changes the state of the process to "failed"
		correct.getProcess(crashedProcess).setCorrect(false);
		try {
			crash.go();
		} catch (AppiaEventException e) {
			e.printStackTrace();
		}
	}
}
