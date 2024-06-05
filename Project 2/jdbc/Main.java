package org.example;

import java.sql.*;

public class Main {
    public static void main(String[] args) {

        String url = "jdbc:sqlserver://localhost:13001;databaseName=BIClass;encrypt=true;trustServerCertificate=true;";
        String username = "sa";
        String password = "PH@123456789";


        int groupMemberUserAuthorizationKey = 1;


        String sqlProcedure = "{call Project2.LoadStarSchemaData(?)}";

        String sqlQuery = "SELECT * FROM [Process].[WorkflowSteps]";

        try (

                Connection conn = DriverManager.getConnection(url, username, password);

                CallableStatement stmtProc = conn.prepareCall(sqlProcedure);

                Statement stmtQuery = conn.createStatement();
        ) {

            stmtProc.setInt(1, groupMemberUserAuthorizationKey);


            stmtProc.execute();
            System.out.println("Stored procedure executed successfully.");


            ResultSet rs = stmtQuery.executeQuery(sqlQuery);
            while (rs.next()) {

                System.out.println("WorkFlowStepKey: " + rs.getInt("WorkFlowStepKey"));
                System.out.println("WorkFlowStepDescription: " + rs.getString("WorkFlowStepDescription"));
                System.out.println("WorkFlowStepTableRowCount: " + rs.getInt("WorkFlowStepTableRowCount"));
                Timestamp startTimestamp = rs.getTimestamp("StartingDateTime");
                Timestamp endTimestamp = rs.getTimestamp("EndingDateTime");
                System.out.println("StartingDateTime: " + (startTimestamp != null ? startTimestamp.toLocalDateTime() : "null"));
                System.out.println("EndingDateTime: " + (endTimestamp != null ? endTimestamp.toLocalDateTime() : "null"));


                String classTime = rs.getString("ClassTime");
                System.out.println("ClassTime: " + (classTime != null ? classTime.trim() : "null"));

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
