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
        String connectionUrlBase = "jdbc:sqlserver://localhost:13001;" +
                "databaseName=master;" +
                "encrypt=true;" +
                "trustServerCertificate=true;" +
                "user=sa;" +
                "password= PH@123456789;";

        // Queries to execute on different databases
        String[][] queries = {
//------------------------------------------------------------------------------------
 //                Add more databases and queries as needed

                //---------------------------------Top 3-------------------------------
                {"AdventureWorks2017", "USE AdventureWorks2017; " +
                        "SELECT TOP (10) P.BusinessEntityID, PS.PasswordHash\n" +
                        "FROM Person.Person AS P\n" +
                        "JOIN Person.Password AS PS ON P.BusinessEntityID = PS.BusinessEntityID\n" +
                        "INNER JOIN Person.PersonPhone AS PP ON PP.BusinessEntityID = PS.BusinessEntityID\n" +
                        "FOR JSON PATH, ROOT('person');"},

                {"TSQLv4", "USE TSQLV4; \n" +
                        "SELECT C.custid, COUNT(DISTINCT O.orderid) AS num_orders, SUM(OD.qty) AS total_quantity_ordered \n" +
                        "FROM Sales.Customers AS C \n" +
                        "INNER JOIN Sales.Orders AS O ON O.custid = C.custid \n" +
                        "INNER JOIN Sales.OrderDetails AS OD ON OD.orderid = O.orderid \n" +
                        "WHERE C.country = N'Canada' \n" +
                        "GROUP BY C.custid \n" +
                        "ORDER BY total_quantity_ordered DESC \n" +
                        "FOR JSON PATH, ROOT('canada');"},

                {"TSQLV4", "USE TSQLV4; " +
                        "SELECT C.custid, COUNT(O.orderid) AS numorders, COALESCE(SUM(OD.qty), 0) AS totalqty\n" +
                        "FROM Sales.Customers AS C\n" +
                        "LEFT JOIN Sales.Orders AS O ON O.custid = C.custid\n" +
                        "LEFT JOIN Sales.OrderDetails AS OD ON OD.orderid = O.orderid\n" +
                        "WHERE C.country = N'USA'\n" +
                        "GROUP BY C.custid\n" +
                        "FOR JSON PATH, ROOT('customer');"},


    //--------------------------------Worst 3 ----------------------------------------
                {"TSQLV4", "USE TSQLV4; \n" +
                        "SELECT TOP (5) E.empid, E.firstname, E.lastname\n" +
                        "FROM HR.Employees AS E\n" +
                        "JOIN dbo.Nums AS N ON N.n BETWEEN 1 AND 5\n" +
                        "JOIN dbo.Orders AS O ON E.empid = O.empid\n" +
                        "ORDER BY E.empid, N.n\n" +
                        "FOR JSON PATH, ROOT('employees');"},


                {"AdventureWorksDW2017", "USE AdventureWorksDW2017; " +
                "SELECT TOP (30) DC.CustomerKey, DC.FirstName, DC.MiddleName, DC.LastName, DC.BirthDate\n" +
                "FROM dbo.DimCustomer AS DC\n" +
                "INNER JOIN dbo.DimEmployee AS DE ON DE.LastName = DC.LastName\n" +
                "INNER JOIN dbo.DimDepartmentGroup AS G ON G.DepartmentGroupName = DE.DepartmentName\n" +
                "FOR JSON PATH, ROOT('dimcustomer');"},


                {"WideWorldImporters", "USE WideWorldImporters; " +
                "SELECT TOP (10) O.CustomerID, COUNT(DISTINCT O.OrderID) AS num_orders\n" +
                "FROM Sales.Orders AS O\n" +
                "LEFT OUTER JOIN Sales.SpecialDeals AS SP ON SP.CustomerID = O.CustomerID\n" +
                "INNER JOIN Sales.OrderLines AS OD ON O.orderid = OD.orderid\n" +
                "GROUP BY O.CustomerID\n" +
                "ORDER BY O.CustomerID ASC\n" +
                "FOR JSON PATH, ROOT('orders');"},

 //-----------------------------------------------------------------------------------------------------------------------


                {"WideWorldImportersDW", "USE WideWorldImportersDW; " +
                "SELECT COUNT(DISTINCT CombinedOrders.[Salesperson Key]) AS SalespersonKey_Count\n" +
                "FROM (SELECT O.[Order Key], O.[Order Date Key], O.[Salesperson Key]\n" +
                "FROM Fact.[Order] O\n" +
                "LEFT OUTER JOIN Fact.Purchase AS P ON P.[Date Key] = O.[Order Date Key]\n" +
                "LEFT OUTER JOIN Fact.[Transaction] AS T ON T.[Lineage Key] = P.[Lineage Key]) AS CombinedOrders\n" +
                "FOR JSON PATH, ROOT('worldwidedw');"},


//
                {"Northwinds2022TSQLV7", "USE Northwinds2022TSQLV7; " +
                "SELECT C.CustomerId, COUNT(O.orderid) AS numorders, SUM(OD.Quantity) AS totalqty\n" +
                "FROM Sales.Customer AS C\n" +
                "INNER JOIN Sales.[Order] AS O ON O.CustomerId = C.CustomerId\n" +
                "INNER JOIN Sales.OrderDetail AS OD ON OD.orderid = O.orderid\n" +
                "GROUP BY C.CustomerId\n" +
                "FOR JSON PATH, ROOT('customer');"},


                {"TSQLv4", "USE TSQLV4; " +
                "SELECT C.custid, COUNT(DISTINCT O.orderid) AS num_orders, SUM(OD.qty) AS total_quantity_ordered\n" +
                "FROM Sales.Customers AS C\n" +
                "INNER JOIN Sales.Orders AS O ON O.custid = C.custid\n" +
                "INNER JOIN Sales.OrderDetails AS OD ON OD.orderid = O.orderid\n" +
                "WHERE C.country = N'JPN'\n" +
                "GROUP BY C.custid\n" +
                "FOR JSON PATH, ROOT('customer');"},

                {"Northwinds2022TSQLV7", "USE Northwinds2022TSQLV7; " +
                "SELECT TOP (50) C.CustomerId, O.OrderId, OD.ProductId, OD.Quantity\n" +
                "FROM Sales.Customer AS C\n" +
                "LEFT OUTER JOIN (Sales.[Order] AS O\n" +
                "INNER JOIN Sales.OrderDetail AS OD ON O.OrderId = OD.OrderId)\n" +
                "ON C.CustomerId = O.CustomerId\n" +
                "FOR JSON PATH, ROOT('customer');"},

                {"TSQLv4", "USE TSQLV4; " +
                "SELECT C.custid, COUNT(DISTINCT O.orderid) AS num_orders, SUM(OD.qty) AS total_quantity_ordered\n" +
                "FROM Sales.Customers AS C\n" +
                "INNER JOIN Sales.Orders AS O ON O.custid = C.custid\n" +
                "INNER JOIN Sales.OrderDetails AS OD ON OD.orderid = O.orderid\n" +
                "WHERE C.country = N'USA'\n" +
                "GROUP BY C.custid\n" +
                "ORDER BY num_orders ASC\n" +
                "FOR JSON PATH, ROOT('orderdetails');"},


                {"PrestigeCars", "USE PrestigeCars; \n" +
                        "SELECT c.CustomerName, mk.MakeName, md.ModelName \n" +
                        "FROM [Data].[Customer] c \n" +
                        "INNER JOIN [Data].[Model] md ON c.CustomerID = md.ModelID \n" +
                        "INNER JOIN [Data].[Make] mk ON md.MakeID = mk.MakeID \n" +
                        "ORDER BY c.CustomerName, mk.MakeName, md.ModelName \n" +
                        "FOR JSON PATH, ROOT('prestigecars');"},




                {"TSQLv4", "USE TSQLV4; \n" +
                "SELECT C.custid, COUNT(DISTINCT O.orderid) AS numorders, SUM(OD.qty) AS totalqty \n" +
                "FROM Sales.Customers AS C \n" +
                "INNER JOIN Sales.Orders AS O ON O.custid = C.custid \n" +
                "INNER JOIN Sales.OrderDetails AS OD ON OD.orderid = O.orderid \n" +
                "WHERE C.country = N'Mexico' \n" +
                "GROUP BY C.custid \n" +
                "ORDER BY C.custid \n" +
                "FOR JSON PATH, ROOT('mexico');"},






//                {"TSQLv4", "USE TSQLV4; " +
//                        "DROP FUNCTION IF EXISTS dbo.GetCustOrders; " +
//                        "GO " +
//                        "CREATE FUNCTION dbo.GetCustOrders " +
//                        "(@cid AS INT) RETURNS TABLE " +
//                        "AS " +
//                        "RETURN " +
//                        "SELECT orderid, custid, empid, orderdate, requireddate, shipregion, shippostalcode, shipcountry " +
//                        "FROM Sales.Orders " +
//                        "WHERE custid = @cid; " +
//                        "GO " +
//                        "SELECT C.custid, COUNT(DISTINCT ODA.productid) AS numorders, SUM(OD.qty) AS totalqty " +
//                        "FROM dbo.GetCustOrders(1) AS C " +
//                        "INNER JOIN Sales.Orders AS O ON O.custid = C.custid " +
//                        "INNER JOIN Sales.OrderDetails AS OD ON OD.orderid = O.orderid " +
//                        "LEFT OUTER JOIN Audit.TransactionTimingSequence AS ODA ON O.orderid = ODA.productid " +
//                        "GROUP BY C.custid " +
//                        "FOR JSON PATH, ROOT('one');"},
//
//                {"TSQLv4", "USE TSQLV4; " +
//                        "DROP FUNCTION IF EXISTS dbo.GetCustOrders; " +
//                        "GO " +
//                        "CREATE FUNCTION dbo.GetCustOrders " +
//                        "(@cid AS INT) RETURNS TABLE " +
//                        "AS " +
//                        "RETURN " +
//                        "SELECT orderid, custid, empid, orderdate, requireddate, shipregion, shippostalcode, shipcountry " +
//                        "FROM Sales.Orders " +
//                        "WHERE custid = @cid; " +
//                        "GO " +
//                        "SELECT C.custid, COUNT(DISTINCT ODA.productid) AS numorders, SUM(OD.qty) AS totalqty " +
//                        "FROM dbo.GetCustOrders(2) AS C " +
//                        "INNER JOIN Sales.Orders AS O ON O.custid = C.custid " +
//                        "INNER JOIN Sales.OrderDetails AS OD ON OD.orderid = O.orderid " +
//                        "LEFT OUTER JOIN Audit.TransactionTimingSequence AS ODA ON O.orderid = ODA.productid " +
//                        "GROUP BY C.custid " +
//                        "FOR JSON PATH, ROOT('two');"},
//
//                {"TSQLv4", "USE TSQLV4; " +
//                        "DROP FUNCTION IF EXISTS dbo.GetCustOrders; " +
//                        "GO " +
//                        "CREATE FUNCTION dbo.GetCustOrders " +
//                        "(@cid AS INT) RETURNS TABLE " +
//                        "AS " +
//                        "RETURN " +
//                        "SELECT orderid, custid, empid, orderdate, requireddate, shipregion, shippostalcode, shipcountry " +
//                        "FROM Sales.Orders " +
//                        "WHERE custid = @cid; " +
//                        "GO " +
//                        "SELECT C.custid, COUNT(DISTINCT ODA.productid) AS numorders, SUM(OD.qty) AS totalqty " +
//                        "FROM dbo.GetCustOrders(3) AS C " +
//                        "INNER JOIN Sales.Orders AS O ON O.custid = C.custid " +
//                        "INNER JOIN Sales.OrderDetails AS OD ON OD.orderid = O.orderid " +
//                        "LEFT OUTER JOIN Audit.TransactionTimingSequence AS ODA ON O.orderid = ODA.productid " +
//                        "GROUP BY C.custid " +
//                        "FOR JSON PATH, ROOT('three');"},
//
//                {"TSQLv4", "USE TSQLV4; " +
//                        "DROP FUNCTION IF EXISTS dbo.GetCustOrders; " +
//                        "GO " +
//                        "CREATE FUNCTION dbo.GetCustOrders " +
//                        "(@cid AS INT) RETURNS TABLE " +
//                        "AS " +
//                        "RETURN " +
//                        "SELECT orderid, custid, empid, orderdate, requireddate, shipregion, shippostalcode, shipcountry " +
//                        "FROM Sales.Orders " +
//                        "WHERE custid = @cid; " +
//                        "GO " +
//                        "SELECT C.custid, COUNT(DISTINCT ODA.productid) AS numorders, SUM(OD.qty) AS totalqty " +
//                        "FROM dbo.GetCustOrders(4) AS C " +
//                        "INNER JOIN Sales.Orders AS O ON O.custid = C.custid " +
//                        "INNER JOIN Sales.OrderDetails AS OD ON OD.orderid = O.orderid " +
//                        "LEFT OUTER JOIN Audit.TransactionTimingSequence AS ODA ON O.orderid = ODA.productid " +
//                        "GROUP BY C.custid " +
//                        "FOR JSON PATH, ROOT('four');"},
//
//                {"TSQLv4", "USE TSQLV4; " +
//                        "DROP FUNCTION IF EXISTS dbo.GetCustOrders; " +
//                        "GO " +
//                        "CREATE FUNCTION dbo.GetCustOrders " +
//                        "(@cid AS INT) RETURNS TABLE " +
//                        "AS " +
//                        "RETURN " +
//                        "SELECT orderid, custid, empid, orderdate, requireddate, shipregion, shippostalcode, shipcountry " +
//                        "FROM Sales.Orders " +
//                        "WHERE custid = @cid; " +
//                        "GO " +
//                        "SELECT C.custid, COUNT(DISTINCT ODA.productid) AS numorders, SUM(OD.qty) AS totalqty " +
//                        "FROM dbo.GetCustOrders(5) AS C " +
//                        "INNER JOIN Sales.Orders AS O ON O.custid = C.custid " +
//                        "INNER JOIN Sales.OrderDetails AS OD ON OD.orderid = O.orderid " +
//                        "LEFT OUTER JOIN Audit.TransactionTimingSequence AS ODA ON O.orderid = ODA.productid " +
//                        "GROUP BY C.custid " +
//                        "FOR JSON PATH, ROOT('five');"},
//
//                {"TSQLv4", "USE TSQLV4; " +
//                        "DROP FUNCTION IF EXISTS dbo.GetCustOrders; " +
//                        "GO " +
//                        "CREATE FUNCTION dbo.GetCustOrders " +
//                        "(@cid AS INT) RETURNS TABLE " +
//                        "AS " +
//                        "RETURN " +
//                        "SELECT orderid, custid, empid, orderdate, requireddate, shipregion, shippostalcode, shipcountry " +
//                        "FROM Sales.Orders " +
//                        "WHERE custid = @cid; " +
//                        "GO " +
//                        "SELECT C.custid, COUNT(DISTINCT ODA.productid) AS numorders, SUM(OD.qty) AS totalqty " +
//                        "FROM dbo.GetCustOrders(6) AS C " +
//                        "INNER JOIN Sales.Orders AS O ON O.custid = C.custid " +
//                        "INNER JOIN Sales.OrderDetails AS OD ON OD.orderid = O.orderid " +
//                        "LEFT OUTER JOIN Audit.TransactionTimingSequence AS ODA ON O.orderid = ODA.productid " +
//                        "GROUP BY C.custid " +
//                        "FOR JSON PATH, ROOT('six');"}


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
//                System.out.println("Executing query: " +'\n'+ sqlQuery);

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