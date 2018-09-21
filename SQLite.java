//AD-JUSTER HOMEWORK BRENT LEE

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class SQLite {

    public static void createNewDatabase(String url) {

        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new database has been created.");
            }
        }   catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        return;
    }

    public static void createNewTable(String url, String tableInfo){
        try(Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement()) {
            stmt.execute(tableInfo);
        }   catch(SQLException e) {
                System.out.println(e.getMessage());
            }
    }

    public static void sqliteQuery(String url, String query)
    {
        try (Connection conn = DriverManager.getConnection(url);
        PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.executeUpdate();
        }   catch (SQLException e) {
                System.out.println(e.getMessage());
            }
    }
}