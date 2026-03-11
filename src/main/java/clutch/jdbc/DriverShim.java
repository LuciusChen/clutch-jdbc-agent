package clutch.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Wraps an externally-loaded JDBC Driver so DriverManager will accept it.
 *
 * DriverManager.getConnection() checks that the Driver was loaded by the
 * caller's classloader or an ancestor. Drivers in drivers/ are loaded by
 * a URLClassLoader that is NOT an ancestor of the system classloader from
 * DriverManager's perspective, so registration fails without this shim.
 */
public class DriverShim implements Driver {

    private final Driver delegate;

    public DriverShim(Driver delegate) {
        this.delegate = delegate;
    }

    @Override public Connection connect(String url, Properties info) throws SQLException {
        return delegate.connect(url, info);
    }

    @Override public boolean acceptsURL(String url) throws SQLException {
        return delegate.acceptsURL(url);
    }

    @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return delegate.getPropertyInfo(url, info);
    }

    @Override public int getMajorVersion() { return delegate.getMajorVersion(); }
    @Override public int getMinorVersion() { return delegate.getMinorVersion(); }
    @Override public boolean jdbcCompliant() { return delegate.jdbcCompliant(); }
    @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return delegate.getParentLogger();
    }
}
