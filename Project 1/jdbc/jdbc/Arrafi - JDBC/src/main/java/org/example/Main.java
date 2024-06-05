//Arrafi Talukder
//JCBD File

package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.ResultSetMetaData;


public class Main {

    public static void main(String[] args) {
        System.out.println("Connect to SQL Server and demo executing queries on different databases.");

        // Base connection URL without specifying the database
        String connectionUrlBase = "jdbc:sqlserver://localhost:13001;" + "databaseName=master;" +
                "encrypt=true;" +
                "trustServerCertificate=true;" +
                "user=sa;" +
                "password=AT@24032195;";


        // Queries to execute on different databases
        String[][] queries = {
                //Top 3 Queries
                {"TSQLv4", "SELECT E.empid, E.firstname, E.lastname, O.orderid, O.orderdate " + // Added space before FROM
                        "FROM HR.Employees AS E " + // Added space before JOIN
                        "JOIN dbo.Nums AS N " + // Added comment for clarity
                        "ON N.n BETWEEN 1 AND 5 " + // Added space before LEFT JOIN
                        "LEFT JOIN dbo.Orders AS O " + // Added comment for clarity
                        "ON E.empid = O.empid " + // Added space before ORDER BY
                        "ORDER BY E.empid, N.n;"},




                {"TSQLV4", "SELECT C.custid, C.companyname, COUNT(DISTINCT O.orderid) AS num_orders, SUM(OD.qty) AS total_quantity_ordered, AVG(OD.unitprice) AS avg_unit_price " + // Added space before FROM
                        "FROM Sales.Customers AS C " + // Added space before INNER JOIN and corrected table names
                        "INNER JOIN Sales.Orders AS O ON O.custid = C.custid " + // Added space before INNER JOIN and corrected table names
                        "INNER JOIN Sales.OrderDetails AS OD ON OD.orderid = O.orderid " + // Added space before WHERE
                        "WHERE C.country = N'UK' " + // Added space before GROUP BY
                        "GROUP BY C.custid, C.companyname " + // Added space before ORDER BY
                        "ORDER BY total_quantity_ordered DESC;"},


                {"Northwinds2022TSQLV7", "DROP FUNCTION IF EXISTS dbo.GetEmployeeSales; " + // Preparing to redefine function if it exists
                        "GO " + // SQL batch separator

                        "CREATE FUNCTION dbo.GetEmployeeSales(@EmployeeId AS INT) " + // Function creation starts
                        "RETURNS TABLE " + // Specifies the function returns a table
                        "AS " +
                        "RETURN ( " +
                        "SELECT O.EmployeeId, O.OrderId, SUM(OD.UnitPrice * OD.Quantity * (1 - OD.DiscountPercentage)) AS TotalSales " + // Select statement within function
                        "FROM Sales.[Order] O " + // Inner join with OrderDetails table
                        "INNER JOIN Sales.OrderDetail OD ON O.OrderId = OD.OrderId " + // Condition for joining
                        "WHERE O.EmployeeId = @EmployeeId " + // Where clause to filter by EmployeeId
                        "GROUP BY O.EmployeeId, O.OrderId " + // Grouping results by EmployeeId and OrderId
                        "); " +
                        "GO " + // SQL batch separator
                        ";WITH EmployeeSales AS ( " + // CTE definition starts
                        "SELECT E.EmployeeId, E.EmployeeLastName, E.EmployeeFirstName, ISNULL(ES.TotalSales, 0) AS TotalSales " + // Select statement within CTE
                        "FROM HumanResources.Employee E " + // Outer apply with the function GetEmployeeSales
                        "OUTER APPLY dbo.GetEmployeeSales(E.EmployeeId) ES " + // End of CTE definition
                        ") " +
                        "SELECT ES.EmployeeId, ES.EmployeeLastName, ES.EmployeeFirstName, SUM(ES.TotalSales) AS TotalSalesValue, AVG(ES.TotalSales) AS AverageOrderValue " + // Select statement using CTE
                        "FROM EmployeeSales ES " + // Grouping results by EmployeeId, LastName, and FirstName
                        "GROUP BY ES.EmployeeId, ES.EmployeeLastName, ES.EmployeeFirstName " + // Order the final result by TotalSalesValue in descending order
                        "ORDER BY TotalSalesValue DESC;"},

                //Top 3 Worst-Fixed Queries

                {"Northwinds2022TSQLV7", "SELECT C.CustomerId, O.OrderId, OD.ProductId, P.ProductName, OD.Quantity " + // Start of the SELECT statement
                        "FROM Sales.Customer AS C " + // FROM clause with Customer table
                        "LEFT JOIN Sales.[Order] AS O ON C.CustomerId = O.CustomerId " + // LEFT JOIN with Order table
                        "INNER JOIN Sales.OrderDetail AS OD ON O.OrderId = OD.OrderId " + // INNER JOIN with OrderDetail table
                        "INNER JOIN Production.Product AS P ON OD.ProductId = P.ProductId;"}, // INNER JOIN with Product table




                {"TSQLV4", "SELECT C.custid, C.companyname, COUNT(DISTINCT O.orderid) AS num_unique_orders, COUNT(OD.productid) AS num_ordered_products, SUM(OD.qty * OD.unitprice) AS total_order_amount " + // Beginning of SELECT statement with aggregation functions
                        "FROM Sales.Customers AS C " + // FROM clause starting with Customers table
                        "INNER JOIN Sales.Orders AS O ON O.custid = C.custid " + // INNER JOIN with Orders table
                        "INNER JOIN Sales.OrderDetails AS OD ON OD.orderid = O.orderid " + // INNER JOIN with OrderDetails table
                        "WHERE C.country = N'USA' " + // WHERE clause to filter by country 'USA'
                        "GROUP BY C.custid, C.companyname;"},// GROUP BY clause for aggregation





                /*{"Northwinds2022TSQLV7", "DROP FUNCTION IF EXISTS dbo.GetProductsFromSupplier; " + // Checks and drops the function if it already exists
                        "GO " + // Separates SQL batches

                        "CREATE FUNCTION dbo.GetProductsFromSupplier(@SupplierId INT) " + // Starts function creation
                        "RETURNS TABLE " + // Indicates the function returns a table
                        "AS " +
                        "RETURN ( " +
                        "SELECT P.ProductId, P.SupplierId, P.ProductName, P.UnitPrice " + // Select statement inside the function
                        "FROM Production.Product P " + // From Production.Product table
                        "WHERE P.SupplierId = @SupplierId " + // Where clause filters by SupplierId
                        "); " +
                        "GO " + // Separates SQL batches

                        "DECLARE @SupplierId INT = 2; " + // Declares variable for SupplierId

                        "SELECT GPS.SupplierId, COUNT(GPS.ProductId) AS TotalProducts, AVG(GPS.UnitPrice) AS AveragePrice " + // Select statement uses the function
                        "FROM dbo.GetProductsFromSupplier(@SupplierId) AS GPS " + // From clause calling the function
                        "GROUP BY GPS.SupplierId;"}, // Group by SupplierId for aggregation */
        };


        for (String[] queryInfo : queries) {
            String dbName = queryInfo[0];
            String sqlQuery = queryInfo[1];

            // Construct connection URL for the specific database
            String connectionUrl = connectionUrlBase + "databaseName=" + dbName + ";";
            try (Connection connection = DriverManager.getConnection(connectionUrl);
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sqlQuery)) {

                System.out.println("Connected to database: " + dbName);
                System.out.println("Executing query: " +'\n'+ sqlQuery);

                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();

                // Print column names
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(metaData.getColumnLabel(i));
                    if (i < columnCount) System.out.print(" | ");
                }
                System.out.println();

                // Display the result set
                while (resultSet.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(resultSet.getString(i));
                        if (i < columnCount) System.out.print(" | ");
                    }
                    System.out.println();
                }
                System.out.println("Query completed on database: " + dbName);
            } catch (Exception e) {
                System.out.println("Error occurred in database: " + dbName);
                e.printStackTrace();
            }
        }
    }
}
