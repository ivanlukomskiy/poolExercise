import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ivanl <ilukomskiy@sbdagroup.com> on 06.07.2018.
 */
public class PoolTest {
    private static final int POOL_LIMIT = 4;
    private static final int POOL_TIMEOUT = 2000;

    private Pool.ConnectionPool dataSource;

    @Before
    public void initConnectionsPool() throws Exception {
        Pool.DataSource dataSourceOrigin = mock(Pool.DataSource.class);
        when(dataSourceOrigin.getConnection()).thenAnswer(invocation -> mock(Pool.Connection.class));
        dataSource = new Pool.ConnectionPool(POOL_LIMIT, POOL_TIMEOUT, dataSourceOrigin);
    }

    @Test
    public void testGetConnection() throws Exception {
        Pool.Connection connection = dataSource.getConnection();
        assertNotNull(connection);
    }

    @Test
    public void testDoSomethingWithConnection() throws Exception {
        Pool.Connection connection = dataSource.getConnection();
        connection.doSomething();
    }

    @Test(expected = IOException.class)
    public void testCantDealWithClosedConnection() throws Exception {
        Pool.Connection connection = dataSource.getConnection();
        connection.close();
        connection.doSomething();
    }

    @Test(expected = TimeoutException.class)
    public void testExceedConnectionsLimit() throws Exception {
        for (int i = 0; i < POOL_LIMIT + 1; i++) {
            dataSource.getConnection();
        }
    }

    @Test
    public void testWaitForFreeConnection() throws Exception {
        Pool.Connection connection = dataSource.getConnection();
        // Close connection after a second
        new Thread(() -> {
            try {
                Thread.sleep(POOL_TIMEOUT / 2);
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        for (int i = 0; i < POOL_LIMIT; i++) {
            dataSource.getConnection();
        }
    }

    @Test
    public void testConnectionsReused() throws Exception {
        Pool.Connection connection = dataSource.getConnection();
        connection.doSomething();
        connection.close();
        dataSource.getConnection();
        assertEquals(dataSource.getConnectionsCreated(), 1);
    }

    @Test
    public void testConcurrentAccess() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(POOL_LIMIT * 2 + 1);

        Runnable dbTask = new Runnable() {
            Random random = new Random();
            @Override
            public void run() {
                try {
                    // Await for other threads to initialize
                    barrier.await(1, SECONDS);

                    for (int i = 0; i < 300; i++) {
                        Pool.Connection connection = dataSource.getConnection();
                        connection.doSomething();
                        Thread.sleep(random.nextInt(10));
                        System.out.println(Thread.currentThread().getName());
                        connection.doSomething();
                        connection.close();
                    }
                    barrier.await(10, SECONDS);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        for (int i = 0; i < POOL_LIMIT * 2; i++) {
            new Thread(dbTask).start();
        }
        barrier.await(1, SECONDS); // Wait for the threads to get ready
        barrier.await(10, SECONDS); // Wait for the threads to finish
    }
}
