import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Created by ivanl <ilukomskiy@sbdagroup.com> on 06.07.2018.
 */
public class Pool {

    interface DataSource {
        Connection getConnection() throws Exception;
    }

    interface Connection {
        void close() throws Exception;

        void doSomething() throws Exception;
    }

    static class ConnectionPool implements DataSource {

        private final int limit;
        private final DataSource originalDataSource;
        private final int timeout;

        private AtomicInteger connectionsCreated = new AtomicInteger(0);
        private final BlockingQueue<Connection> freeConnections = new LinkedBlockingQueue<>();

        ConnectionPool(int limit, int timeout, DataSource dataSource) {
            this.limit = limit;
            this.timeout = timeout;
            originalDataSource = dataSource;
        }

        public Connection getConnection() throws Exception {
            boolean canCreateNewConnection;

            // Try to return free connection if present
            Connection connection = freeConnections.poll();
            if (connection != null) {
                return new PooledConnection(connection, this);
            }
            canCreateNewConnection = connectionsCreated.getAndUpdate(i -> i < limit ? ++i : limit) < limit;

            if (canCreateNewConnection) {
                return new PooledConnection(originalDataSource.getConnection(), this);
            } else {
                // Wait for the next available free connection
                Connection freeConnection = freeConnections.poll(timeout, MILLISECONDS);
                if (freeConnection == null) {
                    throw new TimeoutException();
                }
                return new PooledConnection(freeConnection, this);
            }
        }

        void releaseConnection(Connection connectionOrigin) {
            freeConnections.add(connectionOrigin);
        }

        int getConnectionsCreated() {
            return connectionsCreated.get();
        }
    }

    static class PooledConnection implements Connection {

        private final Connection origin;
        private final ConnectionPool pool;
        private boolean closed = false;

        PooledConnection(Connection origin, ConnectionPool pool) {
            this.origin = origin;
            this.pool = pool;
        }

        public void close() throws Exception {
            if (!closed) {
                closed = true;
                pool.releaseConnection(origin);
            }
        }

        public void doSomething() throws Exception {
            if (closed) {
                throw new IOException("Connection is closed");
            }
            origin.doSomething();
        }
    }
}
