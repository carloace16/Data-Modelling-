package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.ResultSetMetaData;


public class Main {

    public static void main(String[] args) {
        System.out.println("Connect to SQL Server and demo executing queries on different databases.");

        String connectionUrlBase = "jdbc:sqlserver://localhost:13001;" +
                                    "encrypt=true;" +
                                    "trustServerCertificate=true;" +
                                    "user=sa;" +
                                    "password=PH@123456789;";

        String[][] queries = {
                //Ileana Aguilar SUBMISSION
                // Top Query (1)
                    {"Northwinds2022TSQLV7", "WITH\n" +
                            "  CustomerOrders\n" +
                            "  AS\n" +
                            "  (\n" +
                            "    SELECT\n" +
                            "      c.CustomerID,\n" +
                            "      c.CustomerCompanyName,\n" +
                            "      COUNT(o.OrderID) AS NumberOfOrders\n" +
                            "    FROM\n" +
                            "      Sales.Customer c\n" +
                            "      JOIN Sales.[Order] o ON c.CustomerID = o.CustomerID\n" +
                            "    GROUP BY\n" +
                            "    c.CustomerID, c.CustomerCompanyName\n" +
                            "  )\n" +
                            "SELECT\n" +
                            "  CustomerID,\n" +
                            "  CustomerCompanyName,\n" +
                            "  NumberOfOrders\n" +
                            "FROM\n" +
                            "  CustomerOrders\n" +
                            "WHERE\n" +
                            "  CustomerID IN (\n" +
                            "    SELECT\n" +
                            "  CustomerID\n" +
                            "FROM\n" +
                            "  CustomerOrders\n" +
                            "WHERE\n" +
                            "      NumberOfOrders > 5\n" +
                            "  )\n" +
                            "FOR JSON PATH, ROOT('CustomerOrdersDetails');"},
                // Top Query (2)
                            {
                                "PrestigeCars", 
                                "WITH\n" +
                                "    ModelCosts\n" +
                                "    AS\n" +
                                "    (\n" +
                                "        SELECT\n" +
                                "            ModelID,\n" +
                                "            AVG(Cost) AS AvgCost,\n" +
                                "            AVG(RepairsCost) AS AvgRepairsCost\n" +
                                "        FROM Data.Stock\n" +
                                "        GROUP BY ModelID\n" +
                                "    )\n" +
                                "\n" +
                                "SELECT\n" +
                                "    m.ModelName,\n" +
                                "    mc.AvgCost,\n" +
                                "    mc.AvgRepairsCost,\n" +
                                "    (SELECT COUNT(*)\n" +
                                "    FROM Data.Stock\n" +
                                "    WHERE ModelID = m.ModelID) AS TotalStockCount\n" +
                                "FROM Data.Model m\n" +
                                "    INNER JOIN ModelCosts mc ON m.ModelID = mc.ModelID\n" +
                                "ORDER BY TotalStockCount DESC\n" +
                                "FOR JSON PATH, ROOT('SalesCustomerCategory');"
                            },
                // Top Query (3)
                            {
                                "Northwinds2022TSQLV7", 
                                "CREATE OR ALTER FUNCTION dbo.CalculateAverageSale\n" +
                                "(\n" +
                                "    @TotalSalesAmount MONEY,\n" +
                                "    @NumberOfSales INT\n" +
                                ")\n" +
                                "RETURNS MONEY\n" +
                                "AS\n" +
                                "BEGIN\n" +
                                "    RETURN CASE WHEN @NumberOfSales > 0 THEN @TotalSalesAmount / @NumberOfSales ELSE 0 END;\n" +
                                "END;\n" +
                                "WITH\n" +
                                "    EmployeeCustomerSales\n" +
                                "    AS\n" +
                                "    (\n" +
                                "        SELECT\n" +
                                "            o.EmployeeId,\n" +
                                "            o.CustomerId,\n" +
                                "            SUM(o.Freight) AS TotalSalesValue,\n" +
                                "            COUNT(o.OrderId) AS NumberOfSales\n" +
                                "        FROM\n" +
                                "            Sales.[Order] o\n" +
                                "        WHERE\n" +
                                "        o.OrderDate BETWEEN '2014-01-01' AND '2014-12-31'\n" +
                                "        GROUP BY\n" +
                                "        o.EmployeeId,\n" +
                                "        o.CustomerId\n" +
                                "    )\n" +
                                "\n" +
                                "SELECT\n" +
                                "    e.EmployeeFirstName,\n" +
                                "    e.EmployeeLastName,\n" +
                                "    c.CustomerCompanyName,\n" +
                                "    ECS.TotalSalesValue,\n" +
                                "    dbo.CalculateAverageSale(ECS.TotalSalesValue, ECS.NumberOfSales) AS AverageSaleValue\n" +
                                "FROM\n" +
                                "    EmployeeCustomerSales ECS\n" +
                                "    INNER JOIN\n" +
                                "    HumanResources.Employee e ON ECS.EmployeeId = e.EmployeeId\n" +
                                "    INNER JOIN\n" +
                                "    Sales.Customer c ON ECS.CustomerId = c.CustomerId\n" +
                                "GROUP BY\n" +
                                "    e.EmployeeFirstName,\n" +
                                "    e.EmployeeLastName,\n" +
                                "    c.CustomerCompanyName,\n" +
                                "    ECS.TotalSalesValue,\n" +
                                "    ECS.NumberOfSales\n" +
                                "ORDER BY\n" +
                                "    e.EmployeeLastName,\n" +
                                "    c.CustomerCompanyName\n" +
                                "FOR JSON PATH, ROOT('AverageSales');"
                            },
                //WORST QUERY (1)
                            {
                                "AdventureWorks2017", 
                                "WITH\n" +
                                "    EmailCount\n" +
                                "    AS\n" +
                                "    (\n" +
                                "        SELECT\n" +
                                "            BusinessEntityID,\n" +
                                "            COUNT(EmailAddressID) AS EmailTotal\n" +
                                "        FROM Person.EmailAddress\n" +
                                "        GROUP BY BusinessEntityID\n" +
                                "    )\n" +
                                "\n" +
                                "SELECT\n" +
                                "    BEC.BusinessEntityID,\n" +
                                "    EC.EmailTotal,\n" +
                                "    (SELECT COUNT(PhoneNumber)\n" +
                                "    FROM Person.PersonPhone PP\n" +
                                "    WHERE PP.BusinessEntityID = BEC.BusinessEntityID) AS PhoneTotal\n" +
                                "FROM\n" +
                                "    Person.BusinessEntityContact BEC\n" +
                                "    LEFT JOIN EmailCount EC ON BEC.BusinessEntityID = EC.BusinessEntityID\n" +
                                "GROUP BY BEC.BusinessEntityID, EC.EmailTotal\n" +
                                "ORDER BY BEC.BusinessEntityID\n" +
                                "FOR JSON PATH, ROOT('BusinessEmailCount');"
                            },
                //WORST QUERY (2)
                            {
                                "Northwinds2022TSQLV7", 
                                "WITH\n" +
                                "    OrderSales\n" +
                                "    AS\n" +
                                "    (\n" +
                                "        SELECT\n" +
                                "            o.CustomerId,\n" +
                                "            o.OrderId,\n" +
                                "            SUM(CONVERT(int, oda.NewVal) * p.UnitPrice) AS TotalSales\n" +
                                "        FROM Sales.OrderDetailAudit oda\n" +
                                "            INNER JOIN Sales.[Order] o ON oda.OrderId = o.OrderId\n" +
                                "            INNER JOIN Production.Product p ON oda.ProductId = p.ProductId\n" +
                                "        GROUP BY o.CustomerId, o.OrderId\n" +
                                "    )\n" +
                                "SELECT\n" +
                                "    c.CustomerId,\n" +
                                "    c.CustomerCompanyName,\n" +
                                "    SUM(os.TotalSales) AS TotalSalesAmount\n" +
                                "FROM OrderSales os\n" +
                                "    INNER JOIN Sales.Customer c ON os.CustomerId = c.CustomerId\n" +
                                "GROUP BY c.CustomerId, c.CustomerCompanyName\n" +
                                "ORDER BY TotalSalesAmount DESC\n" +
                                "FOR JSON PATH, ROOT('TotalNRepairCosts');"
                            },
                //WORST QUERY (3)
                            {
                                "AdventureWorksDW2017", 
                                "WITH\n" +
                                "    AverageSalesByCurrency\n" +
                                "    AS\n" +
                                "    (\n" +
                                "        SELECT\n" +
                                "            f.CurrencyKey,\n" +
                                "            AVG(f.SalesAmount) AS AvgSalesAmount\n" +
                                "        FROM\n" +
                                "            dbo.FactInternetSales f\n" +
                                "            INNER JOIN\n" +
                                "            dbo.DimCurrency c ON f.CurrencyKey = c.CurrencyKey\n" +
                                "        WHERE\n" +
                                "        f.OrderDateKey IN (SELECT DateKey\n" +
                                "        FROM dbo.DimDate\n" +
                                "        WHERE CalendarYear = 2014)\n" +
                                "        GROUP BY\n" +
                                "        f.CurrencyKey\n" +
                                "    )\n" +
                                "SELECT\n" +
                                "    c.CurrencyName,\n" +
                                "    c.CurrencyAlternateKey,\n" +
                                "    AVG(a.AvgSalesAmount) AS OverallAverageSalesAmount\n" +
                                "FROM\n" +
                                "    AverageSalesByCurrency a\n" +
                                "    INNER JOIN\n" +
                                "    dbo.DimCurrency c ON a.CurrencyKey = c.CurrencyKey\n" +
                                "GROUP BY\n" +
                                "    c.CurrencyName, c.CurrencyAlternateKey\n" +
                                "ORDER BY\n" +
                                "    OverallAverageSalesAmount DESC\n" +
                                "FOR JSON PATH, ROOT('AverageSalesAmount');"
                            },

                //FIXED QUERY (1)
                            {
                                    "AdventureWorks2017",
                                    "WITH\n" +
                                            "    EmailCount\n" +
                                            "    AS\n" +
                                            "    (\n" +
                                            "        SELECT\n" +
                                            "            BusinessEntityID,\n" +
                                            "            COUNT(EmailAddressID) AS EmailTotal\n" +
                                            "        FROM Person.EmailAddress\n" +
                                            "        GROUP BY BusinessEntityID\n" +
                                            "    )\n" +
                                            "\n" +
                                            "SELECT\n" +
                                            "    BEC.BusinessEntityID,\n" +
                                            "    EC.EmailTotal,\n" +
                                            "    (SELECT COUNT(PhoneNumber)\n" +
                                            "    FROM Person.PersonPhone PersonPhoneNumber\n" +
                                            "    WHERE PersonPhoneNumber.BusinessEntityID = BEC.BusinessEntityID) AS PhoneTotal\n" +
                                            "FROM\n" +
                                            "    Person.BusinessEntityContact BEC\n" +
                                            "    LEFT JOIN EmailCount EC ON BEC.BusinessEntityID = EC.BusinessEntityID\n" +
                                            "GROUP BY BEC.BusinessEntityID, EC.EmailTotal\n" +
                                            "ORDER BY BEC.BusinessEntityID\n" +
                                            "FOR JSON PATH, ROOT('BusinessEmailCount');"
                            },
                //FIXED QUERY (2)
                            {
                                    "Northwinds2022TSQLV7",
                                    "WITH\n" +
                                            "    OrderSales\n" +
                                            "    AS\n" +
                                            "    (\n" +
                                            "        SELECT\n" +
                                            "            OrderD.CustomerId,\n" +
                                            "            OrderD.OrderId,\n" +
                                            "            SUM(CONVERT(int, OrderDA.NewVal) * p.UnitPrice) AS TotalSales\n" +
                                            "        FROM Sales.OrderDetailAudit OrderDA\n" +
                                            "            INNER JOIN Sales.[Order] OrderD ON OrderDA.OrderId = OrderD.OrderId\n" +
                                            "            INNER JOIN Production.Product p ON OrderDA.ProductId = p.ProductId\n" +
                                            "        GROUP BY OrderD.CustomerId, OrderD.OrderId\n" +
                                            "    )\n" +
                                            "SELECT\n" +
                                            "    c.CustomerId,\n" +
                                            "    c.CustomerCompanyName,\n" +
                                            "    SUM(os.TotalSales) AS TotalSalesAmount\n" +
                                            "FROM OrderSales os\n" +
                                            "    INNER JOIN Sales.Customer c ON os.CustomerId = c.CustomerId\n" +
                                            "GROUP BY c.CustomerId, c.CustomerCompanyName\n" +
                                            "ORDER BY TotalSalesAmount DESC\n" +
                                            "FOR JSON PATH, ROOT('TotalNRepairCosts');"
                            },
                //FIXED QUERY (3)
                            {
                                    "AdventureWorksDW2017",
                                    "WITH\n" +
                                            "    AverageSalesByCurrency\n" +
                                            "    AS\n" +
                                            "    (\n" +
                                            "        SELECT\n" +
                                            "            f.CurrencyKey,\n" +
                                            "            AVG(f.SalesAmount) AS AvgSalesAmount\n" +
                                            "        FROM\n" +
                                            "            dbo.FactInternetSales f\n" +
                                            "            INNER JOIN\n" +
                                            "            dbo.DimCurrency c ON f.CurrencyKey = c.CurrencyKey\n" +
                                            "        WHERE\n" +
                                            "        f.OrderDateKey IN (SELECT DateKey\n" +
                                            "        FROM dbo.DimDate\n" +
                                            "        WHERE CalendarYear = 2014)\n" +
                                            "        GROUP BY\n" +
                                            "        f.CurrencyKey\n" +
                                            "    )\n" +
                                            "SELECT\n" +
                                            "    c.CurrencyName,\n" +
                                            "    c.CurrencyAlternateKey,\n" +
                                            "    AVG(a.AvgSalesAmount) AS OverallAverageSalesAmount\n" +
                                            "FROM\n" +
                                            "    AverageSalesByCurrency a\n" +
                                            "    INNER JOIN\n" +
                                            "    dbo.DimCurrency c ON a.CurrencyKey = c.CurrencyKey\n" +
                                            "GROUP BY\n" +
                                            "    c.CurrencyName, c.CurrencyAlternateKey\n" +
                                            "ORDER BY\n" +
                                            "    OverallAverageSalesAmount DESC\n" +
                                            "FOR JSON PATH, ROOT('AverageSalesAmount');"
                            },
                // TARUNA'S SUBMISSION
                // Top Query (1)
                {"WideWorldImporters", "WITH SupplierPerformance AS (\n" +
                        "    SELECT\n" +
                        "        s.SupplierID,\n" +
                        "        s.SupplierName,\n" +
                        "        AVG(DATEDIFF(day, po.OrderDate, po.ExpectedDeliveryDate)) AS AverageLeadTimeDays,\n" +
                        "        SUM(pol.ExpectedUnitPricePerOuter * pol.OrderedOuters) AS TotalPurchaseAmount\n" +
                        "    FROM\n" +
                        "        Purchasing.Suppliers s\n" +
                        "    INNER JOIN Purchasing.PurchaseOrders po ON s.SupplierID = po.SupplierID\n" +
                        "    INNER JOIN Purchasing.PurchaseOrderLines pol ON po.PurchaseOrderID = pol.PurchaseOrderID\n" +
                        "    WHERE\n" +
                        "        po.OrderDate BETWEEN '2013-01-01' AND '2013-12-31'\n" +
                        "    GROUP BY\n" +
                        "        s.SupplierID, s.SupplierName\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    SupplierName,\n" +
                        "    AverageLeadTimeDays,\n" +
                        "    TotalPurchaseAmount\n" +
                        "FROM\n" +
                        "    SupplierPerformance\n" +
                        "ORDER BY\n" +
                        "    TotalPurchaseAmount DESC\n" +
                        "FOR JSON PATH, ROOT('SupplierPerformance');"
                },

                // TOP QUERY (2)
                {"PrestigeCars", "CREATE OR ALTER FUNCTION dbo.CalculateTotalCost\n" +
                        "(@Cost MONEY,@RepairsCost MONEY,@PartsCost MONEY,@TransportInCost MONEY)\n" +
                        "   RETURNS MONEY AS BEGIN  " +
                        "   RETURN @Cost + @RepairsCost + @PartsCost + @TransportInCost;\n" +
                        "END;\n" +
                        "GO\n" +
                        "WITH CarSalesAnalysis AS (\n " +
                        "SELECT MK.MakeName,MD.ModelName,\n" +
                        "dbo.CalculateTotalCost(ST.Cost, ST.RepairsCost, ST.PartsCost, ST.TransportInCost)\n" +
                        "    AS TotalCost, SD.SalePriceFROM Data.Stock ST n" +
                        "INNER JOIN Data.Model MD ON ST.ModelID = MD.ModelID\n" +
                        "INNER JOIN Data.Make MK ON MD.MakeID = MK.MakeID\n " +
                        "INNER JOIN Data.SalesDetails SD ON ST.StockCode = SD.StockID\n)," +
                        "SalesSummary AS (SELECT MakeName,ModelName,SUM(TotalCost) AS TotalCosts,\n" +
                        "   SUM(SalePrice) AS TotalSales, AVG(SalePrice) AS AverageSalePrice,\n" +
                        "   COUNT(*) AS NumberOfSales FROM CarSalesAnalysis\n" +
                        "   GROUP BY MakeName, ModelName)\n" +
                        "   SELECT MakeName, ModelName, TotalCosts, TotalSales, AverageSalePrice, NumberOfSales\n" +
                        "FROM SalesSummary\nFOR JSON PATH, ROOT('SalesSummary');"
                },

                // TOP QUERY (3)
                {"PrestigeCars", "WITH CarSalesSummary AS (\n" +
                        "SELECT\n" +
                        "   MK.MakeName,\n" +
                        "   MD.ModelName,\n " +
                        "   SUM(SD.SalePrice) AS TotalSales,\n " +
                        "   AVG(SD.SalePrice) AS AverageSalePrice,\n" +
                        "   COUNT(SD.SalesID) AS NumberOfSales\n" +
                        "FROM Data.Sales SA\n" +
                        "INNER JOIN Data.SalesDetails SD ON SA.SalesID = SD.SalesID\n" +
                        "INNER JOIN Data.Stock ST ON SD.StockID = ST.StockCode\n" +
                        "INNER JOIN Data.Model MD ON ST.ModelID = MD.ModelID\n" +
                        "INNER JOIN Data.Make MK ON MD.MakeID = MK.MakeID\n" +
                        "GROUP BY MK.MakeName, MD.ModelName\n" +
                        ")\n" +
                        "SELECT MakeName, ModelName, TotalSales, AverageSalePrice, NumberOfSales\n" +
                        "FROM CarSalesSummary\nFOR JSON PATH, ROOT('CarSalesSummary');"
                },

                //WORST QUERY (1)
                {"PrestigeCars","\n" +
                        "GO\n" +
                        "WITH SalesSummary AS (\n" +
                        "    SELECT\n" +
                        "       CountryName,\n" +
                        "       SUM(SalePrice - LineItemDiscount) AS TotalSalesValue,\n" +
                        "       AVG(SalePrice - LineItemDiscount) AS AverageSalePrice,\n " +
                        "       COUNT(DISTINCT InvoiceNumber) AS NumberOfTransactions\n" +
                        "FROM [Data].[SalesByCountry]\n" +
                        "GROUP BY CountryName)\n" +
                        "SELECT\n" +
                        "    CountryName,\n" +
                        "    TotalSalesValue,\n" +
                        "    AverageSalePrice,\n" +
                        "    NumberOfTransactions\n" +
                        "FROM SalesSummary\n" +
                        "FOR JSON PATH, ROOT('SalesSummary');"
                },

                //WORST QUERY (2)
                {"WideWorldImportersDW","WITH MonthlySupplierPurchases AS (\n" +
                        "    SELECT\n" +
                        "        d.[Calendar Month Label] AS Month,\n" +
                        "        d.[Calendar Year] AS Year,\n" +
                        "        s.Supplier,\n" +
                        "        SUM(p.[Ordered Quantity]) AS TotalQuantity,\n" +
                        "        AVG(p.[Ordered Quantity]) AS AverageQuantity\n" +
                        "    FROM\n" +
                        "        Fact.Purchase AS p\n" +
                        "        JOIN Dimension.Date AS d ON p.[Date Key] = d.Date\n" +
                        "        JOIN Dimension.Supplier AS s ON p.[Supplier Key] = s.[Supplier Key]\n" +
                        "    GROUP BY\n" +
                        "        d.[Calendar Month Label],\n" +
                        "        d.[Calendar Year],\n" +
                        "        s.Supplier\n" +
                        ")\n" +
                        "SELECT Month, Year, Supplier, TotalQuantity, AverageQuantity\n" +
                        "FROM MonthlySupplierPurchases\n" +
                        "FOR JSON PATH, ROOT('MonthlySupplierPurchases');"
                },

                //WORST QUERY (3)
                {"AdventureWorksDW2017","WITH ProductPriceSummary AS (\n" +
                        "    SELECT\n" +
                        "        pc.EnglishProductCategoryName AS CategoryName,\n" +
                        "        psc.EnglishProductSubcategoryName AS SubcategoryName,\n" +
                        "        AVG(p.ListPrice) AS AverageListPrice\n" +
                        "    FROM dbo.DimProduct AS p\n" +
                        "    INNER JOIN dbo.DimProductSubcategory AS psc ON p.ProductSubcategoryKey = psc.ProductSubcategoryKey\n" +
                        "    INNER JOIN dbo.DimProductCategory AS pc ON psc.ProductCategoryKey = pc.ProductCategoryKey\n" +
                        "    WHERE p.ListPrice > 0 -- Excluding products with no list price\n" +
                        "    GROUP BY pc.EnglishProductCategoryName, psc.EnglishProductSubcategoryName\n" +
                        ")\n" +
                        "SELECT CategoryName, SubcategoryName, AverageListPrice\n" +
                        "FROM ProductPriceSummary\n" +
                        "FOR JSON PATH, ROOT('ProductPriceSummary');"
                },

                // MEDIUM QUERY
                {"AdventureWorks2017", "WITH CustomerSalesSummary AS (\n" +
                        "    SELECT\n" +
                        "        c.CustomerID,\n" +
                        "        COUNT(soh.SalesOrderID) AS TotalOrders,\n" +
                        "        SUM(sod.LineTotal) AS TotalSales,\n" +
                        "        AVG(sod.LineTotal) AS AverageOrderValue\n" +
                        "    FROM Sales.Customer c\n" +
                        "    INNER JOIN Sales.SalesOrderHeader soh ON c.CustomerID = soh.CustomerID\n" +
                        "    INNER JOIN Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID\n" +
                        "    GROUP BY c.CustomerID\n" +
                        ")\n" +
                        "SELECT CustomerID, TotalOrders, TotalSales, AverageOrderValue\n" +
                        "FROM CustomerSalesSummary\n" +
                        "FOR JSON PATH, ROOT('CustomerSalesSummary');"
                },

                //MEDIUM QUERY
                {"Northwinds2022TSQLV7","WITH CustomerOrderSummary AS (\n" +
                        "    SELECT\n" +
                        "        c.CustomerCompanyName,\n" +
                        "        COUNT(DISTINCT o.OrderId) AS TotalOrders,\n" +
                        "        SUM(od.UnitPrice * od.Quantity) AS TotalSalesValue,\n" +
                        "        SUM(od.Quantity) AS TotalProductsOrdered\n" +
                        "    FROM Sales.[Order] o\n" +
                        "    INNER JOIN Sales.Customer c ON o.CustomerId = c.CustomerId\n" +
                        "    INNER JOIN Sales.OrderDetail od ON o.OrderId = od.OrderId\n" +
                        "    GROUP BY c.CustomerCompanyName\n" +
                        ")\n" +
                        "SELECT CustomerCompanyName, TotalOrders, TotalSalesValue, TotalProductsOrdered\n" +
                        "FROM CustomerOrderSummary\n" +
                        "FOR JSON PATH, ROOT('CustomerOrderSummary');"
                },

                // MEDIUM QUERY
                {"Northwinds2022TSQLV7", "WITH SupplierProductSummary AS (\n" +
                        "    SELECT\n" +
                        "        s.SupplierCompanyName,\n" +
                        "        COUNT(p.ProductId) AS TotalProducts,\n" +
                        "        AVG(p.UnitPrice) AS AverageUnitPrice\n" +
                        "    FROM Production.Product p\n" +
                        "    INNER JOIN Production.Supplier s ON p.SupplierId = s.SupplierId\n" +
                        "    GROUP BY s.SupplierCompanyName\n" +
                        ")\n" +
                        "SELECT SupplierCompanyName, TotalProducts, AverageUnitPrice\n" +
                        "FROM SupplierProductSummary\n" +
                        "FOR JSON PATH, ROOT('SupplierProductSummary');"

                },
                //Carlo SUBMISSION
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
                //Bin SUBMISSION
                ////////////////////////////////////////////////////// 1   TOP 3
                {"USE Northwinds2022TSQLV7\n" +
                        "SELECT c.CustomerId\n" +
                        "FROM Sales.[Order] c\n" +
                        "LEFT JOIN Sales.Customer o ON c.CustomerId = o.CustomerId\n" +
                        "GROUP BY c.CustomerId\n" +
                        "INTERSECT\n" +
                        "(SELECT c.CustomerId\n" +
                        "FROM Sales.[Order] c\n" +
                        "LEFT JOIN Sales.Customer o ON c.CustomerId = o.CustomerId\n" +
                        "GROUP BY c.CustomerId)\n"},
/////////////////////////////////////////////////////// 2
                {"USE Northwinds2022TSQLV7\n" +
                        "SELECT c.CustomerId\n" +
                        "FROM Sales.[Order] c\n" +
                        "LEFT JOIN Sales.Customer o ON c.CustomerId = o.CustomerId\n" +
                        "EXCEPT\n" +
                        "(SELECT o.CustomerId\n" +
                        "FROM Sales.Customer o\n" +
                        "GROUP BY o.CustomerId)\n"},
/////////////////////////////////////////////////////// 3
                {"USE Northwinds2022TSQLV7\n" +
                        "SELECT c.CustomerId\n" +
                        "FROM Sales.[Order] c\n" +
                        "LEFT JOIN\n" +
                        "Sales.Customer o ON c.CustomerId = o.CustomerId\n" +
                        "EXCEPT\n" +
                        "(SELECT\n" +
                        "CustomerId\n" +
                        "FROM\n" +
                        "Sales.Customer\n" +
                        "WHERE\n" +
                        "CustomerCountry = 'USA'\n" +
                        "GROUP BY\n" +
                        "CustomerId)\n"},
///////////////////////////////////////////////// 4 WORST 3
                {"USE Northwinds2022TSQLV7\n" +
                        "SELECT \n" +
                        "    c.OrderId,\n" +
                        "    SUM(o.UnitPrice * o.Quantity) AS TotalValue\n" +
                        "FROM \n" +
                        "    Sales.[Order] c\n" +
                        "LEFT JOIN \n" +
                        "    Sales.OrderDetail o ON c.OrderId = o.OrderId\n" +
                        "GROUP BY \n" +
                        "    c.OrderId\n" +
                        "HAVING SUM(o.UnitPrice * o.Quantity) >= 200\n" +
                        "INTERSECT\n" +
                        "    (SELECT \n" +
                        "        c.OrderId,\n" +
                        "        SUM(o.UnitPrice * o.Quantity) AS TotalValue\n" +
                        "    FROM \n" +
                        "        Sales.[Order] c\n" +
                        "    LEFT JOIN \n" +
                        "        Sales.OrderDetail o ON c.OrderId = o.OrderId\n" +
                        "    GROUP BY \n" +
                        "        c.OrderId\n" +
                        "    HAVING SUM(o.UnitPrice * o.Quantity) <= 300)"},
//////////////////////////////////////////////////// 5
                {"USE Northwinds2022TSQLV7\n" +
                        "SELECT \n" +
                        "    c.OrderId,\n" +
                        "    o.ProductId\n" +
                        "FROM \n" +
                        "    Sales.[Order] c\n" +
                        "LEFT JOIN \n" +
                        "    Sales.OrderDetail o ON c.OrderId = o.OrderId\n" +
                        "WHERE \n" +
                        "    o.ProductId >= 20\n" +
                        "GROUP BY \n" +
                        "    c.OrderId, o.ProductId\n" +
                        "HAVING \n" +
                        "    o.ProductId <= 30\n" +
                        "\n" +
                        "INTERSECT\n" +
                        "(SELECT \n" +
                        "    c.OrderId,\n" +
                        "    o.ProductId\n" +
                        "FROM \n" +
                        "    Sales.[Order] c\n" +
                        "LEFT JOIN \n" +
                        "    Sales.OrderDetail o ON c.OrderId = o.OrderId\n" +
                        "WHERE \n" +
                        "    o.ProductId <= 30\n" +
                        "GROUP BY \n" +
                        "    c.OrderId, o.ProductId\n" +
                        "HAVING \n" +
                        "    o.ProductId >= 20)"},
/////////////////////////////////////////////// 6
                {"USE Northwinds2022TSQLV7\n" +
                        "SELECT \n" +
                        "    c.EmployeeId,\n" +
                        "    o.EmployeeLastName\n" +
                        "FROM \n" +
                        "    Sales.[Order] c\n" +
                        "LEFT JOIN \n" +
                        "    HumanResources.Employee o ON c.EmployeeId = o.EmployeeId\n" +
                        "WHERE o.EmployeeLastName = 'Lew'\n" +
                        "GROUP BY \n" +
                        "    c.EmployeeId, o.EmployeeLastName\n" +
                        "\n" +
                        "\n" +
                        "INTERSECT\n" +
                        "(SELECT \n" +
                        "    c.EmployeeId,\n" +
                        "    o.EmployeeLastName\n" +
                        "FROM \n" +
                        "    Sales.[Order] c\n" +
                        "LEFT JOIN \n" +
                        "    HumanResources.Employee o ON c.EmployeeId = o.EmployeeId\n" +
                        "GROUP BY \n" +
                        "    c.EmployeeId, o.EmployeeLastName)"},
                //Arrafi Talukder SUBMISSION
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
                        "GROUP BY C.custid, C.companyname; "},// GROUP BY clause for aggregation



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
