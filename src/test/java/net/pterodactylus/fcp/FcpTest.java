/*
 * jSite2 - FcpTest.java -
 * Copyright © 2008 David Roden
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 */

package net.pterodactylus.fcp;

import java.io.IOException;

import org.junit.Test;
import org.junit.After;
import org.junit.Before;

import static junit.framework.Assert.assertNotNull;

/**
 * Tests various commands and the FCP connection.
 * 
 * @author David ‘Bombe’ Roden &lt;bombe@freenetproject.org&gt;
 */
public class FcpTest {

    public static final String NODE_HOST = "localhost";
    public static final Integer NODE_PORT = 9481;


    /** The FCP connection. */
	private FcpConnection fcpConnection;

	/**
	 * {@inheritDoc}
	 */
	@Before
	public void setUp() throws Exception {
		fcpConnection = new FcpConnection(NODE_HOST, NODE_PORT);
		fcpConnection.connect();
		fcpConnection.sendMessage(new ClientHello("FcpTest"));
	}

	/**
	 * {@inheritDoc}
	 */
	@After
	public void tearDown() throws Exception {
		fcpConnection.close();
	}

	/**
	 * Tests the FCP connection be simply {@link #setUp() setting it up} and
	 * {@link #tearDown() tearing it down} again.
	 */
    @Test
	public void testFcpConnection() {
		/* do nothing. */
	}

	/**
	 * Generates an SSK key pair.
	 * 
	 * @throws IOException
	 *             if an I/O error occurs
	 * @throws InterruptedException
	 *             if {@link Object#wait()} wakes up spuriously
	 */
    @Test
	public void testGenerateSSK() throws IOException, InterruptedException {
		final SSKKeypair[] keypair = new SSKKeypair[1];
		FcpAdapter fcpAdapter = new FcpAdapter() {

			/**
			 * {@inheritDoc}
			 */
			@Override
			public void receivedSSKKeypair(FcpConnection fcpConnection, SSKKeypair sskKeypair) {
				keypair[0] = sskKeypair;
				synchronized (this) {
					notify();
				}
			}
		};
		fcpConnection.addFcpListener(fcpAdapter);
		synchronized (fcpAdapter) {
			fcpConnection.sendMessage(new GenerateSSK());
			fcpAdapter.wait();
		}
		assertNotNull("ssk keypair", keypair[0]);
	}

}
