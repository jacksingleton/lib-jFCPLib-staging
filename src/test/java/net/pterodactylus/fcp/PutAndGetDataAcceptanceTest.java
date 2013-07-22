package net.pterodactylus.fcp;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * PutAndGetDataAcceptanceTest
 *
 * May take a few minutes to run
 */
public class PutAndGetDataAcceptanceTest {

    public static final String NODE_HOST = "localhost";
    public static final Integer NODE_PORT = 9481;

    @Test
    public void test_put_and_get_chk_string_data() throws Throwable {
        // Given
        final String content = "some-content";

        // When
        final FcpConnection connection = createConnection();
        final URIGenerated uriGenerated = putCHKData(connection, content);
        final String retrievedContent = getDataAsString(connection, uriGenerated.getURI());

        // Then
        assertEquals("Retrieved content should be the same as inserted content", content, retrievedContent);
    }

    @Test
    public void test_put_and_get_ssk_string_data() throws Throwable {
        // Given
        final String content = "some-content";

        // When
        final FcpConnection connection = createConnection();
        final SSKKeypair keypair = generateSSKKeypair(connection);
        final URIGenerated uriGenerated = putSSKData(connection, keypair.getInsertURI() + "test", content);
        final String retrievedContent = getDataAsString(connection, uriGenerated.getURI());

        // Then
        assertEquals("Retrieved content should be the same as inserted content", content, retrievedContent);
    }

    private FcpConnection createConnection() throws Throwable {
        final FcpConnection connection = new FcpConnection(NODE_HOST, NODE_PORT);
        connection.connect();

        final ClientHello helloMessage = new ClientHello("PutAndGetDataAcceptanceTest");

        final AtomicReference<NodeHello> response = new AtomicReference<NodeHello>(null);
        final FcpAdapter fcpListener = new FcpAdapter() {
            @Override
            public void receivedNodeHello(FcpConnection fcpConnection, NodeHello nodeHello) {
                response.set(nodeHello);
                synchronized (this) { this.notify(); }
            }
        };
        connection.addFcpListener(fcpListener);

        synchronized (fcpListener) {
            connection.sendMessage(helloMessage);
            fcpListener.wait();
        }

        assertNotNull(response.get());
        assertEquals("The node should use FCP version 2.0", "2.0", response.get().getFCPVersion());

        return connection;
    }

    private URIGenerated putCHKData(FcpConnection connection, String data) throws Throwable {
        final String conversationIdentifier = UUID.randomUUID().toString();
        final ClientPut putMessage = new ClientPut("CHK@", conversationIdentifier);
        putMessage.setDataLength(data.getBytes("UTF-8").length);
        putMessage.setMetadataContentType("text/plain");
        putMessage.setPayloadInputStream(new ByteArrayInputStream(data.getBytes("UTF-8")));

        final AtomicReference<URIGenerated> response = new AtomicReference<URIGenerated>(null);
        final FcpAdapter fcpListener = new FcpAdapter() {
            @Override
            public void receivedURIGenerated(FcpConnection fcpConnection, URIGenerated uriGenerated) {
                if (conversationIdentifier.equals(uriGenerated.getIdentifier())) {
                    response.set(uriGenerated);
                }
                synchronized (this) { this.notify(); }
            }

            @Override
            public void receivedPutFailed(FcpConnection fcpConnection, PutFailed putFailed) {
                fail("Received PutFailed message: " + putFailed.getCodeDescription());
            }
        };
        connection.addFcpListener(fcpListener);

        synchronized (fcpListener) {
            connection.sendMessage(putMessage);
            fcpListener.wait();
        }

        assertNotNull(response.get());
        assertTrue("We should receive a CHK uri", response.get().getURI().startsWith("CHK@"));

        return response.get();
    }

    private URIGenerated putSSKData(FcpConnection connection, String insertURI, String data) throws Throwable {
        final String conversationId = UUID.randomUUID().toString();
        final ClientPut putMessage = new ClientPut(insertURI, conversationId);
        putMessage.setVerbosity(Verbosity.ALL);
        putMessage.setDataLength(data.getBytes("UTF-8").length);
        putMessage.setMetadataContentType("text/plain");
        putMessage.setPayloadInputStream(new ByteArrayInputStream(data.getBytes("UTF-8")));

        final AtomicReference<URIGenerated> response = new AtomicReference<URIGenerated>(null);
        final FcpAdapter fcpListener = new FcpAdapter() {
            @Override
            public void receivedURIGenerated(FcpConnection fcpConnection, URIGenerated uriGenerated) {
                if (conversationId.equals(uriGenerated.getIdentifier())) response.set(uriGenerated);
                synchronized (this) { this.notify(); }
            }
        };
        connection.addFcpListener(fcpListener);

        synchronized (fcpListener) {
            connection.sendMessage(putMessage);
            fcpListener.wait();
        }

        final URIGenerated uriGenerated = response.get();

        assertNotNull("We should receive a UriGenerated response", uriGenerated);

        return uriGenerated;
    }

    private String getDataAsString(FcpConnection connection, String uri) throws Throwable {
        final String conversationIdentifier = UUID.randomUUID().toString();
        final ClientGet getMessage = new ClientGet(uri, conversationIdentifier);
        getMessage.setMaxRetries(-1);

        final AtomicReference<String> response = new AtomicReference<String>(null);
        final FcpAdapter fcpListener = new FcpAdapter() {
            @Override
            public void receivedAllData(FcpConnection fcpConnection, AllData allData) {
                if (conversationIdentifier.equals(allData.getIdentifier())) {
                    response.set(new Scanner(allData.getPayloadInputStream(), "UTF-8").useDelimiter("\\A").next());
                }
                synchronized (this) { this.notify(); }
            }

            @Override
            public void receivedGetFailed(FcpConnection fcpConnection, GetFailed getFailed) {
                fail("Received GetFailed message: " + getFailed.getCodeDescription());
            }
        };
        connection.addFcpListener(fcpListener);

        synchronized (fcpListener) {
            connection.sendMessage(getMessage);
            fcpListener.wait();
        }

        final String data = response.get();

        assertNotNull("We should receive some data", data);

        return data;
    }

    private SSKKeypair generateSSKKeypair(FcpConnection connection) throws Throwable {
        final GenerateSSK generateSSKMessage = new GenerateSSK();

        final AtomicReference<SSKKeypair> response = new AtomicReference<SSKKeypair>(null);
        final FcpAdapter fcpListener = new FcpAdapter() {
            @Override
            public void receivedSSKKeypair(FcpConnection fcpConnection, SSKKeypair sskKeypair) {
                response.set(sskKeypair);
                synchronized (this) { this.notify(); }
            }
        };
        connection.addFcpListener(fcpListener);

        synchronized (fcpListener) {
            connection.sendMessage(generateSSKMessage);
            fcpListener.wait();
        }

        final SSKKeypair keypair = response.get();

        assertNotNull("We should receive a SSKKeypair from the server", keypair);

        return keypair;
    }

}