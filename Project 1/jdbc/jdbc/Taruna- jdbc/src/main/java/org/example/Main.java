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
        String connectionUrlBase = "jdbc:sqlserver://localhost:13001;"+"databaseName=master;" +
                "encrypt=true;" +
                "trustServerCertificate=true;" +
                "user=sa;" +
                "password=PH@123456789;";

        // Queries to execute on different databases
        String[][] queries = {
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

                // MEDIUM QUERY
                {"WideWorldImportersDW", "WITH SalesByCustomer AS (\n" +
                        "    SELECT\n" +
                        "        c.[Customer Key],\n" +
                        "        c.Customer,\n" +
                        "        ci.City,\n" +
                        "        SUM(s.[Total Including Tax]) AS TotalSales,\n" +
                        "        AVG(s.[Total Including Tax]) AS AverageSaleAmount\n" +
                        "    FROM\n" +
                        "        Dimension.Customer AS c\n" +
                        "        JOIN Fact.Sale AS s ON c.[Customer Key] = s.[Customer Key]\n" +
                        "        JOIN Dimension.City AS ci ON s.[City Key] = ci.[City Key]\n" +
                        "    GROUP BY\n" +
                        "        c.[Customer Key],\n" +
                        "        c.Customer,\n" +
                        "        ci.City\n" +
                        ")\n" +
                        "SELECT [Customer Key], Customer, City, TotalSales, AverageSaleAmount\n" +
                        "FROM SalesByCustomer\n" +
                        "FOR JSON PATH, ROOT('SalesByCustomer');"
                },

                //MEDIUM QUERY
                {"WideWorldimporters", "WITH CustomerSales AS (\n" +
                        "    SELECT\n" +
                        "        c.CustomerID,\n" +
                        "        c.CustomerCategoryID,\n" +
                        "        i.InvoiceDate,\n" +
                        "        il.InvoiceLineID,\n" +
                        "        il.ExtendedPrice\n" +
                        "    FROM\n" +
                        "        Sales.Customers c\n" +
                        "    JOIN Sales.Invoices i ON c.CustomerID = i.CustomerID\n" +
                        "    JOIN Sales.InvoiceLines il ON i.InvoiceID = il.InvoiceID\n" +
                        "    WHERE\n" +
                        "        i.InvoiceDate BETWEEN '2014-01-01' AND '2014-12-31'\n" +
                        "),\n" +
                        "AggregatedSales AS (\n" +
                        "    SELECT\n" +
                        "        CustomerCategoryID,\n" +
                        "        SUM(ExtendedPrice) AS TotalSales,\n" +
                        "        AVG(ExtendedPrice) AS AverageSalePerInvoice\n" +
                        "    FROM\n" +
                        "        CustomerSales\n" +
                        "    GROUP BY\n" +
                        "        CustomerCategoryID\n" +
                        ")\n" +
                        "SELECT\n " +
                        "   cc.CustomerCategoryName,\n" +
                        "    asales.TotalSales,\n" +
                        "    asales.AverageSalePerInvoice\n" +
                        "FROM\n" +
                        "    AggregatedSales asales\n" +
                        "JOIN Sales.CustomerCategories cc ON asales.CustomerCategoryID = cc.CustomerCategoryID\n" +
                        "ORDER BY\n" +
                        "    TotalSales DESC\n" +
                        "FOR JSON PATH, ROOT('CustomerSales');"
                },

                //MEDIUM QUERY
                {"WideWorldimporters", "WITH StockSummary AS (\n" +
                        "    SELECT\n" +
                        "        si.StockItemID,\n" +
                        "        si.StockItemName,\n" +
                        "        sish.QuantityOnHand,\n" +
                        "        sig.StockGroupName\n" +
                        "    FROM\n" +
                        "        Warehouse.StockItemHoldings sish\n" +
                        "    JOIN Warehouse.StockItems si ON sish.StockItemID = si.StockItemID\n" +
                        "    JOIN Warehouse.StockItemStockGroups sisg ON si.StockItemID = sisg.StockItemID\n" +
                        "    JOIN Warehouse.StockGroups sig ON sisg.StockGroupID = sig.StockGroupID\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    StockItemName,\n" +
                        "    QuantityOnHand,\n" +
                        "    StockGroupName\n" +
                        "FROM\n" +
                        "    StockSummary\n" +
                        "ORDER BY\n" +
                        "    StockGroupName, StockItemName\n" +
                        "FOR JSON PATH, ROOT('StockSummary');"
                },

                //COMPLEX QUERY
                {"WideWorldimportersDW", "WITH RegionalSalesData AS (\n" +
                        "    SELECT\n" +
                        "        ci.Region,\n" +
                        "        si.[Stock Item],\n" +
                        "        SUM(fs.[Total Including Tax]) AS TotalSales\n" +
                        "    FROM\n" +
                        "        Fact.Sale fs\n" +
                        "    INNER JOIN Dimension.[Stock Item] si ON fs.[Stock Item Key] = si.[Stock Item Key]\n" +
                        "    INNER JOIN Dimension.City ci ON fs.[City Key] = ci.[City Key]\n" +
                        "    INNER JOIN Dimension.Date d ON fs.[Invoice Date Key] = d.Date\n" +
                        "    WHERE\n" +
                        "        d.[Calendar Year] = 2013\n" +
                        "    GROUP BY\n" +
                        "        ci.Region,\n" +
                        "        si.[Stock Item]\n" +
                        "),\n" +
                        "RankedRegionalSales AS (\n" +
                        "    SELECT\n" +
                        "        Region,\n" +
                        "        [Stock Item],\n" +
                        "        TotalSales,\n" +
                        "        RANK() OVER (PARTITION BY Region ORDER BY TotalSales DESC) AS SalesRank\n" +
                        "    FROM\n" +
                        "        RegionalSalesData\n" +
                        ")\n" +
                        "    SELECT\n" +
                        "       Region,\n" +
                        "       [Stock Item],\n" +
                        "       TotalSales\n" +
                        "     FROM\n" +
                        "       RankedRegionalSales\n" +
                        "    WHERE\n" +
                        "        SalesRank <= 5\n" +
                        "    ORDER BY\n" +
                        "        Region, \n" +
                        "    SalesRank\n" +
                        "FOR JSON PATH, ROOT('RegionalSalesData');"
                },

                //COMPLEX QUERY
                {"WideWorldImporters", "CREATE OR ALTER FUNCTION dbo.GetTotalInvoiceTax(@InvoiceID INT)\n" +
                        "RETURNS DECIMAL(18,2)\n" +
                        "AS\n" +
                        "BEGIN\n" +
                        "    DECLARE @TotalTax DECIMAL(18,2);\n" +
                        "    SELECT @TotalTax = SUM(TaxAmount) FROM Sales.InvoiceLines WHERE InvoiceID = @InvoiceID;\n" +
                        "    RETURN @TotalTax;\n" +
                        "END;\n" +
                        "GO\n" +
                        ";WITH InvoiceSummary AS (\n" +
                        "    SELECT\n" +
                        "        i.InvoiceID,\n" +
                        "        i.CustomerID,\n" +
                        "        i.InvoiceDate,\n" +
                        "        dbo.GetTotalInvoiceTax(i.InvoiceID) AS TotalTax,\n" +
                        "        SUM(il.ExtendedPrice) AS TotalExtendedPrice,\n" +
                        "        COUNT(il.InvoiceLineID) AS NumberOfItems\n" +
                        "    FROM Sales.Invoices i\n" +
                        "    INNER JOIN Sales.InvoiceLines il ON i.InvoiceID = il.InvoiceID\n" +
                        "    GROUP BY i.InvoiceID, i.CustomerID, i.InvoiceDate\n" +
                        "),\n" +
                        "StockTransactions AS (\n" +
                        "    SELECT\n" +
                        "        sit.StockItemID,\n" +
                        "        sit.Quantity,\n" +
                        "        sit.TransactionOccurredWhen,\n" +
                        "        sit.InvoiceID\n" +
                        "    FROM Warehouse.StockItemTransactions sit\n" +
                        "    WHERE sit.InvoiceID IS NOT NULL\n" +
                        ")\n" +
                        "    SELECT\n" +
                        "        ISum.InvoiceID,\n" +
                        "        ISum.CustomerID,\n" +
                        "        ISum.InvoiceDate,\n" +
                        "        ISum.TotalTax,\n" +
                        "        ISum.TotalExtendedPrice,\n" +
                        "        ISum.NumberOfItems,\n" +
                        "        ST.Quantity AS TransactionQuantity,\n" +
                        "        ST.TransactionOccurredWhen\n" +
                        "    FROM InvoiceSummary ISum\n" +
                        "    INNER JOIN StockTransactions ST ON ISum.InvoiceID = ST.InvoiceID\n" +
                        "    ORDER BY ISum.InvoiceDate DESC, ISum.InvoiceID\n" +
                        "FOR JSON PATH, ROOT('dbo.GetTotalInvoiceTax');"
                },

                //COMPLEX QUERY
                {"WideWorldImporters", "CREATE OR ALTER FUNCTION dbo.AvgCostPerItem(@PurchaseOrderID INT)\n" +
                        "RETURNS DECIMAL(18,2)\n" +
                        "AS\n" +
                        "BEGIN\n" +
                        "    RETURN (\n" +
                        "        SELECT AVG(ExpectedUnitPricePerOuter)\n" +
                        "        FROM Purchasing.PurchaseOrderLines\n" +
                        "        WHERE PurchaseOrderID = @PurchaseOrderID\n" +
                        "    );\n" +
                        "END;\n" +
                        "GO\n" +
                        "\n" +
                        ";WITH PurchaseOrderDetails AS (\n" +
                        "    SELECT\n" +
                        "        pol.PurchaseOrderID,\n" +
                        "        pol.StockItemID,\n" +
                        "        SUM(pol.OrderedOuters) AS TotalOrderedOuters,\n" +
                        "        SUM(pol.ReceivedOuters) AS TotalReceivedOuters\n" +
                        "    FROM Purchasing.PurchaseOrderLines pol\n" +
                        "    GROUP BY pol.PurchaseOrderID, pol.StockItemID\n" +
                        "), StockSummary AS (\n" +
                        "    SELECT\n" +
                        "        sih.StockItemID,\n" +
                        "        sih.QuantityOnHand,\n" +
                        "        AVG(sih.LastCostPrice) AS AvgLastCostPrice\n" +
                        "    FROM Warehouse.StockItemHoldings sih\n" +
                        "    GROUP BY sih.StockItemID, sih.QuantityOnHand\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    po.PurchaseOrderID,\n" +
                        "    po.SupplierID,\n" +
                        "    po.OrderDate,\n" +
                        "    POD.TotalOrderedOuters,\n" +
                        "    POD.TotalReceivedOuters,\n" +
                        "    dbo.AvgCostPerItem(po.PurchaseOrderID) AS AvgCostPerItem,\n" +
                        "    SS.QuantityOnHand,\n" +
                        "    SS.AvgLastCostPrice\n" +
                        "FROM Purchasing.PurchaseOrders po\n" +
                        "INNER JOIN PurchaseOrderDetails POD ON po.PurchaseOrderID = POD.PurchaseOrderID\n" +
                        "LEFT JOIN StockSummary SS ON POD.StockItemID = SS.StockItemID\n" +
                        "ORDER BY po.OrderDate DESC\n" +
                        "FOR JSON PATH, ROOT('dbo.AvgCostPerItem');"
                },

                // COMPLEX QUERY
                {"WideWorldImportersDW", "Create or alter a custom function to calculate adjusted sales amount\n" +
                        "CREATE OR ALTER FUNCTION dbo.CalculateAdjustedSalesAmount(@totalSalesIncludingTax DECIMAL(18,2))\n" +
                        "RETURNS DECIMAL(18,2)\n" +
                        "AS\n" +
                        "BEGIN\n" +
                        "    RETURN @totalSalesIncludingTax * 1.1;\nEND;\nGO\n\nWITH EmployeeSalesPerformance AS (\n" +
                        "    SELECT\n" +
                        "        e.[Employee Key],\n" +
                        "        e.Employee,\n" +
                        "        d.[Calendar Year],\n" +
                        "        SUM(dbo.CalculateAdjustedSalesAmount(s.[Total Including Tax])) AS AdjustedSalesAmount\n" +
                        "    FROM\n" +
                        "        Fact.Sale s\n" +
                        "    INNER JOIN Dimension.Employee e ON s.[Salesperson Key] = e.[Employee Key]\n" +
                        "    INNER JOIN Dimension.Date d ON s.[Invoice Date Key] = d.[Date]\n" +
                        "    WHERE\n" +
                        "        d.[Calendar Year] = 2013\n" +
                        "    GROUP BY\n" +
                        "        e.[Employee Key],\n" +
                        "        e.Employee,\n" +
                        "        d.[Calendar Year]\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    Employee,\n" +
                        "    CalendarYear,\n" +
                        "    AdjustedSalesAmount\n" +
                        "FROM\n" +
                        "    EmployeeSalesPerformance\n" +
                        "ORDER BY\n" +
                        "    AdjustedSalesAmount DESC\n" +
                        "FOR JSON PATH, ROOT('EmployeeSalesPerformance');"
                },

                //COMPLEX QUERY
                {"AdventureWorksDW2017", "CREATE OR ALTER FUNCTION dbo.CalculateNetSales(\n" +
                        "    @SalesAmount MONEY,\n" +
                        "    @DiscountAmount FLOAT\n" +
                        ")\n" +
                        "RETURNS MONEY\n" +
                        "AS\n" +
                        "BEGIN\n" +
                        "    RETURN @SalesAmount - (@SalesAmount * @DiscountAmount);\n" +
                        "END;\n" +
                        "GO\n" +
                        "WITH SalesSummary AS (\n" +
                        "    SELECT\n" +
                        "        pc.EnglishProductCategoryName AS CategoryName,\n" +
                        "        d.CalendarYear,\n" +
                        "        SUM(dbo.CalculateNetSales(fs.SalesAmount, fs.DiscountAmount)) AS TotalNetSales,\n" +
                        "        AVG(fs.DiscountAmount) AS AverageDiscount\n" +
                        "    FROM\n" +
                        "        dbo.FactInternetSales fs\n" +
                        "    INNER JOIN dbo.DimProduct p ON fs.ProductKey = p.ProductKey\n" +
                        "    INNER JOIN dbo.DimProductCategory pc ON p.ProductSubcategoryKey = pc.ProductCategoryKey\n" +
                        "    INNER JOIN dbo.DimDate d ON fs.OrderDateKey = d.DateKey\n" +
                        "    WHERE\n" +
                        "        d.CalendarYear = 2013\n" +
                        "    GROUP BY\n" +
                        "        pc.EnglishProductCategoryName,\n" +
                        "        d.CalendarYear\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    CategoryName,\n" +
                        "    CalendarYear,\n" +
                        "    TotalNetSales,\n" +
                        "    AverageDiscount\n" +
                        "FROM\n" +
                        "    SalesSummary\n" +
                        "ORDER BY\n" +
                        "    TotalNetSales DESC\n" +
                        "FOR JSON PATH, ROOT('dbo.CalculateNetSales');"
                },

                //COMPLEX QUERY
                {
                        "Northwinds2022TSQLV7", "CREATE OR ALTER FUNCTION dbo.CategorizeSalesPerformance(@TotalSales MONEY)\n" +
                        "RETURNS NVARCHAR(50)\n" +
                        "AS\n" +
                        "BEGIN\n" +
                        "    RETURN CASE \n" +
                        "        WHEN @TotalSales > 10000 THEN 'High'\n" +
                        "        WHEN @TotalSales BETWEEN 5000 AND 10000 THEN 'Medium'\n" +
                        "        ELSE 'Low'\n" +
                        "    END;\n" +
                        "END;\n" +
                        "GO\n" +
                        "WITH ProductSales AS (\n" +
                        "    SELECT\n" +
                        "        pc.CategoryName,\n" +
                        "        p.ProductName,\n" +
                        "        SUM(od.Quantity * od.UnitPrice) AS TotalSales\n" +
                        "    FROM\n" +
                        "        Production.Product p\n" +
                        "    JOIN Production.Category pc ON p.CategoryID = pc.CategoryID\n" +
                        "    JOIN Sales.OrderDetail od ON p.ProductID = od.ProductID\n" +
                        "    GROUP BY\n" +
                        "        pc.CategoryName,\n" +
                        "        p.ProductName\n" +
                        "),\n" +
                        "SalesPerformance AS (\n" +
                        "    SELECT\n" +
                        "        CategoryName,\n" +
                        "        ProductName,\n" +
                        "        TotalSales,\n" +
                        "        dbo.CategorizeSalesPerformance(TotalSales) AS Performance\n" +
                        "    FROM\n" +
                        "        ProductSales\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    CategoryName,\n" +
                        "    ProductName,\n" +
                        "    TotalSales,\n" +
                        "    Performance\n" +
                        "FROM\n" +
                        "    SalesPerformance\n" +
                        "ORDER BY\n" +
                        "    CategoryName, TotalSales DESC\n" +
                        "FOR JSON PATH, ROOT('dbo.CategorizeSalesPerformance');"
                },

                //COMPLEX QUERY
                {"AdventureWorks2017", "CREATE OR ALTER FUNCTION dbo.EvaluateRestockingPriority(\n" +
                        "    @CurrentStockLevel INT,\n" +
                        "    @RestockThreshold INT\n" +
                        ")\n" +
                        "RETURNS NVARCHAR(10)\n" +
                        "AS\n" +
                        "BEGIN\n" +
                        "    RETURN CASE \n" +
                        "        WHEN @CurrentStockLevel < @RestockThreshold THEN 'High'\n" +
                        "        WHEN @CurrentStockLevel BETWEEN @RestockThreshold AND (@RestockThreshold * 2) THEN 'Medium'\n" +
                        "        ELSE 'Low'\n" +
                        "    END;\n" +
                        "END;\n" +
                        "GO\n" +
                        "WITH StockAnalysis AS (\n" +
                        "    SELECT\n" +
                        "        ps.Name AS SubcategoryName,\n" +
                        "        p.Name AS ProductName,\n" +
                        "        l.Name AS LocationName,\n" +
                        "        pi.Quantity AS StockLevel,\n" +
                        "        dbo.EvaluateRestockingPriority(pi.Quantity, 100) AS RestockingPriority \n" +
                        "    FROM\n" +
                        "        Production.ProductInventory pi\n" +
                        "    INNER JOIN Production.Product p ON pi.ProductID = p.ProductID\n" +
                        "    INNER JOIN Production.ProductSubcategory ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID\n" +
                        "    INNER JOIN Production.Location l ON pi.LocationID = l.LocationID\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    SubcategoryName,\n" +
                        "    ProductName,\n" +
                        "    LocationName,\n" +
                        "    StockLevel,\n" +
                        "    RestockingPriority\n" +
                        "FROM\n" +
                        "    StockAnalysis\n" +
                        "ORDER BY\n" +
                        "    SubcategoryName, RestockingPriority, StockLevel\n" +
                        "FOR JSON PATH, ROOT('dbo.EvaluateRestockingPriority');"
                }
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