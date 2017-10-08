////////////////////////////////////////////////////////////////////////////////
//
// JdbcHelper.java
// ==================
// a simple, light-weight JDBC utility class to interact with database
// It supports both static Statement and PreparedStatement
//
// AUTHOR:  F. Fulya Demirkan (demirkaf@sheridancollege.ca)
// CREATED: 19/Sep/2017
// UPDATED: 06/Oct/2017
//
////////////////////////////////////////////////////////////////////////////////
package fd.sql;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.util.ArrayList;

public class JdbcHelper
{

    // instance variables
    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;
    private String errorMessage;
    private String activeSql;
    private PreparedStatement activeStatement;

    ////////////////////////////////////////////////////////////////////////////
    // Default JDBCHelper constructor.
    ////////////////////////////////////////////////////////////////////////////
    public JdbcHelper()
    {
        connection = null;
        statement = null;
        resultSet = null;
        errorMessage = "no error";
    }

    ////////////////////////////////////////////////////////////////////////////
    // Makes a connection to db, and returns true if succesfull, otherwise 
    // returns false
    ////////////////////////////////////////////////////////////////////////////
    public boolean connect(String url, String user, String pass)
    {

        boolean connected = false;

        //reset error message
        errorMessage = "no error";

        // check url if it's null or empty
        if (url == null || url.isEmpty())
        {
            return connected;
        }

        // check username if it's null or empty
        if (user == null || user.isEmpty())
        {
            return connected;
        }

        // check pass if it's null or empty
        if (pass == null || pass.isEmpty())
        {
            return connected;
        }

        // try to connect
        try
        {
            // first load proper driver
            initJdbcDriver(url);

            // make connection
            connection = DriverManager.getConnection(url, user, pass);

            // try to create statement object automatically
            statement = connection.createStatement();

            connected = true;

        } catch (SQLException e)
        {
            System.err.println("[SQL ERROR] " + e.getSQLState() + ": "
                    + e.getMessage());

        } catch (Exception e)
        {
            System.err.println("[ERROR]: " + e.getMessage());
        }

        return connected;
    }

    ////////////////////////////////////////////////////////////////////////////
    // clear JDBC resources
    ////////////////////////////////////////////////////////////////////////////
    public void disconnect()
    {
        try
        {
            resultSet.close();
        } catch (Exception e)
        {
        }
        try
        {
            statement.close();
        } catch (Exception e)
        {
        }
        try
        {
            connection.close();
        } catch (Exception e)
        {
        }
    }

    //////////////////////////////////////////////////////////////////////////
    // Execute static SQL query statement. It returns ResultSet object if 
    // successful, otherwise returns null.
    //////////////////////////////////////////////////////////////////////////
    public ResultSet query(String sql, ArrayList<Object> params)
    {
        // init return value
        resultSet = null;

        try
        {
            // create new prepared statement only if sql was changed
            if (!sql.equals(activeSql))
            {
                activeStatement = connection.prepareStatement(sql);
                activeSql = sql;
            }

            // set all parameter values of prepared statement
            if (params != null)
            {
                setParametersForPreparedStatement(params);

            } else
            {
                System.err.println("[WARNING] SQL string is null or empty in "
                        + "query()");
                return resultSet;
            }

            // check connection before execute SQL
            if (connection == null || connection.isClosed())
            {
                System.err.println("[WARNING] Connection is NOT established. "
                        + "Make connection to DB before calling query().");
                return resultSet;
            }

            // execute the prepared statement
            resultSet = activeStatement.executeQuery();

        } catch (SQLException e)
        {
            System.err.println("[SQL ERROR] " + e.getSQLState() + ": "
                    + e.getMessage());

        } catch (Exception e)
        {
            System.err.println("[ERROR]: " + e.getMessage());
        }
        return resultSet;
    }

    //////////////////////////////////////////////////////////////////////////
    // Execute static SQL update statement. It returns 0 or # of rows are 
    // changed if successful, otherwise returns -1.
    //////////////////////////////////////////////////////////////////////////
    public int update(String sql, ArrayList<Object> params)
    {
        // init return value
        int result = -1;

        try
        {
            // create new prepared statement only if sql was changed
            if (!sql.equals(activeSql))
            {
                activeStatement = connection.prepareStatement(sql);
                activeSql = sql;
            }

            // set all parameter values of prepared statement
            if (params != null)
            {
                setParametersForPreparedStatement(params);

            } else
            {
                System.err.println("[WARNING] SQL string is null or empty in "
                        + "query()");
                return result;
            }

            // check connection before execute SQL
            if (connection == null || connection.isClosed())
            {
                System.err.println("[WARNING] Connection is NOT established. "
                        + "Make connection to DB before calling query().");
                return result;
            }

            // execute the prepared statement
            result = activeStatement.executeUpdate();

        } catch (SQLException e)
        {
            System.err.println("[SQL ERROR] " + e.getSQLState() + ": "
                    + e.getMessage());

        } catch (Exception e)
        {
            System.err.println("[ERROR]: " + e.getMessage());
        }

        return result;
    }

    //////////////////////////////////////////////////////////////////////////
    // Set the params of the prepared statement.
    // It will cast each param to original data type before calling setXXX() 
    //////////////////////////////////////////////////////////////////////////
    private void setParametersForPreparedStatement(ArrayList<Object> params)
    {
        errorMessage = "";
        Object param = null;
        try
        {
            for (int i = 0; i < params.size(); ++i)
            {
                param = params.get(i);
                if (param instanceof Integer)
                {
                    activeStatement.setInt(i + 1, (int) param);
                } else if (param instanceof Double)
                {
                    activeStatement.setDouble(i + 1, (double) param);
                } else if (param instanceof String)
                {
                    activeStatement.setString(i + 1, "%" + (String) param + "%");
                } else if (param instanceof Long)
                {
                    activeStatement.setLong(i + 1, (long) param);
                } else if (param instanceof Date)
                {
                    activeStatement.setDate(i + 1, (Date) param);
                } else if (param instanceof Time)
                {
                    activeStatement.setTime(i + 1, (Time) param);
                } else if (param instanceof Blob)
                {
                    activeStatement.setBlob(i + 1, (Blob) param);
                }
            }
        } catch (SQLException e)
        {
            errorMessage = e.getSQLState() + ": " + e.getMessage();
            System.err.println(errorMessage);
        } catch (Exception e)
        {
            errorMessage = e.getMessage();
            System.err.println(errorMessage);
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // Register the JDBC driver based on URL 
    ////////////////////////////////////////////////////////////////////////////
    private void initJdbcDriver(String url)
    {
        // test various databases
        try
        {
            if (url.contains("jdbc:mysql"))
            {
                Class.forName("com.mysql.jdbc.Driver");
            } else if (url.contains("jdbc:oracle"))
            {
                Class.forName("oracle.jdbc.OracleDriver");
            } else if (url.contains("jdbc:derby"))
            {
                Class.forName("org.apache.derby.jdbc.ClientDriver");
            } else if (url.contains("jdbc:db2"))
            {
                Class.forName("com.ibm.db2.jcc.DB2Driver");
            } else if (url.contains("jdbc:postgresql"))
            {
                Class.forName("org.postgresql.Driver");
            } else if (url.contains("jdbc:sqlite"))
            {
                Class.forName("org.sqlite.JDBC");
            } else if (url.contains("jdbc:sqlserver"))
            {
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            } else if (url.contains("jdbc:sybase"))
            {
                Class.forName("sybase.jdbc.sqlanywhere.IDriver");
            }
        } catch (ClassNotFoundException e)
        {
            errorMessage = "[ERROR] Failed to initialize JDBC driver class.";
            System.err.println(errorMessage);
        }
    }
}
