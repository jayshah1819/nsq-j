package com.sproutsocial.nsq;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class AtomicBatchPublishingTest {

    @Mock
    private Client mockClient;

    @Mock
    private PubConnection mockConnection;

    @Mock
    private BalanceStrategy mockBalanceStrategy;

    @Mock
    private NsqdInstance mockNsqdInstance;

    private Publisher atomicPublisher;
    private Publisher nonAtomicPublisher;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        // Setup mock behavior
        when(mockBalanceStrategy.getNsqdInstance()).thenReturn(mockNsqdInstance);
        when(mockNsqdInstance.getCon()).thenReturn(mockConnection);

        // Create publishers with different atomic settings
        atomicPublisher = new Publisher(mockClient, new Publisher.BalanceStrategyProvider() {
            @Override
            public BalanceStrategy get(Client c, Publisher p) {
                return mockBalanceStrategy;
            }
        }, true);
        nonAtomicPublisher = new Publisher(mockClient, new Publisher.BalanceStrategyProvider() {
            @Override
            public BalanceStrategy get(Client c, Publisher p) {
                return mockBalanceStrategy;
            }
        }, false);
    }

    @After
    public void tearDown() {
        if (atomicPublisher != null) {
            atomicPublisher.stop();
        }
        if (nonAtomicPublisher != null) {
            nonAtomicPublisher.stop();
        }
    }

    @Test
    public void testAtomicBatchPublishing_SuccessfulMPUB() throws Exception {
        // Arrange
        String topic = "test-topic";
        List<byte[]> messages = Arrays.asList(
                "message1".getBytes(),
                "message2".getBytes(),
                "message3".getBytes());

        // Mock successful MPUB
        doNothing().when(mockConnection).publish(topic, messages);

        // Act
        atomicPublisher.publish(topic, messages);

        // Assert
        verify(mockConnection, times(1)).publish(topic, messages);
        verify(mockConnection, never()).publish(eq(topic), any(byte[].class));
        verify(mockNsqdInstance, never()).markFailure();
    }

    @Test(expected = NSQException.class)
    public void testAtomicBatchPublishing_MPUBFailure_ThrowsException() throws Exception {
        // Arrange
        String topic = "test-topic";
        List<byte[]> messages = Arrays.asList(
                "message1".getBytes(),
                "message2".getBytes());

        // Mock MPUB failure
        doThrow(new RuntimeException("MPUB failed")).when(mockConnection).publish(topic, messages);

        // Act & Assert - should throw NSQException
        atomicPublisher.publish(topic, messages);
    }

    @Test
    public void testNonAtomicBatchPublishing_MPUBFailure_FallsBackToSequential() throws Exception {
        // Arrange
        String topic = "test-topic";
        List<byte[]> messages = Arrays.asList(
                "message1".getBytes(),
                "message2".getBytes(),
                "message3".getBytes());

        // Mock MPUB failure but individual publishes succeed
        doThrow(new RuntimeException("MPUB failed")).when(mockConnection).publish(topic, messages);
        doNothing().when(mockConnection).publish(eq(topic), any(byte[].class));

        // Act
        nonAtomicPublisher.publish(topic, messages);

        // Assert
        verify(mockConnection, times(1)).publish(topic, messages); // MPUB attempted once
        verify(mockConnection, times(3)).publish(eq(topic), any(byte[].class)); // 3 individual publishes
        verify(mockNsqdInstance, times(1)).markFailure(); // Failure marked once for MPUB
    }

    @Test
    public void testNonAtomicBatchPublishing_SuccessfulMPUB_NoFallback() throws Exception {
        // Arrange
        String topic = "test-topic";
        List<byte[]> messages = Arrays.asList(
                "message1".getBytes(),
                "message2".getBytes());

        // Mock successful MPUB
        doNothing().when(mockConnection).publish(topic, messages);

        // Act
        nonAtomicPublisher.publish(topic, messages);

        // Assert
        verify(mockConnection, times(1)).publish(topic, messages);
        verify(mockConnection, never()).publish(eq(topic), any(byte[].class));
        verify(mockNsqdInstance, never()).markFailure();
    }

    @Test
    public void testAtomicBatchPublishing_VerifyFailureMarked() throws Exception {
        // Arrange
        String topic = "test-topic";
        List<byte[]> messages = Arrays.asList("message1".getBytes());

        doThrow(new RuntimeException("Connection failed")).when(mockConnection).publish(topic, messages);

        // Act & Assert
        try {
            atomicPublisher.publish(topic, messages);
            fail("Expected NSQException to be thrown");
        } catch (NSQException e) {
            verify(mockNsqdInstance, times(1)).markFailure();
            assertTrue("Exception message should mention atomic batch publishing",
                    e.getMessage().contains("Atomic batch publishing failed"));
        }
    }

    @Test
    public void testNonAtomicBatchPublishing_PartialSequentialFailure() throws Exception {
        // Arrange
        String topic = "test-topic";
        AtomicInteger publishCallCount = new AtomicInteger(0);
        List<byte[]> messages = Arrays.asList(
                "message1".getBytes(),
                "message2".getBytes(),
                "message3".getBytes());

        // Mock MPUB failure
        doThrow(new RuntimeException("MPUB failed")).when(mockConnection).publish(topic, messages);

        // Mock individual publish behavior: first succeeds, second fails, third
        // succeeds
        doAnswer(invocation -> {
            int callNumber = publishCallCount.incrementAndGet();
            if (callNumber == 2) { // Second call fails
                throw new RuntimeException("Individual publish failed");
            }
            return null; // Success for calls 1 and 3
        }).when(mockConnection).publish(eq(topic), any(byte[].class));

        // Act - should not throw exception even with partial failures
        nonAtomicPublisher.publish(topic, messages);

        // Assert
        verify(mockConnection, times(1)).publish(topic, messages); // MPUB attempted
        verify(mockConnection, times(3)).publish(eq(topic), any(byte[].class)); // 3 individual attempts
        verify(mockNsqdInstance, times(1)).markFailure(); // Only MPUB failure marked
    }

    @Test
    public void testEmptyMessageList_ThrowsException() {
        // Arrange
        String topic = "test-topic";
        List<byte[]> emptyMessages = Arrays.asList();

        // Act & Assert
        try {
            atomicPublisher.publish(topic, emptyMessages);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            nonAtomicPublisher.publish(topic, emptyMessages);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    @Test
    public void testNullParameters_ThrowsException() {
        // Test null topic
        try {
            atomicPublisher.publish(null, Arrays.asList("message".getBytes()));
            fail("Expected NullPointerException for null topic");
        } catch (NullPointerException e) {
            // Expected
        }

        // Test null message list
        try {
            atomicPublisher.publish("topic", (java.util.List<byte[]>) null);
            fail("Expected NullPointerException for null message list");
        } catch (NullPointerException e) {
            // Expected
        }
    }

    // Add debugging logs to identify issues in the Publisher class
    @Test
    public void testDebuggingPublisher() throws IOException {
        // Arrange
        String topic = "debug-topic";
        List<byte[]> messages = Arrays.asList("debug-message1".getBytes(), "debug-message2".getBytes());

        try {
            // Act
            atomicPublisher.publish(topic, messages);
        } catch (Exception e) {
            // Log the exception for debugging
            System.err.println("Debugging Exception: " + e.getMessage());
            e.printStackTrace();
        }

        // Assert
        verify(mockConnection, times(1)).publish(topic, messages);
    }
}