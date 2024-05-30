SELECT Convert(nvarchar(10), '{source_db}') AS SourceDB,
       'Invoice' AS TransactionType,
       [No_] AS InvoiceNo,
       [Document Date] AS InvoiceDate,
       [Posting Date] AS PostingDate,
       [Sell-to Customer No_] AS CustomerNo,
       [Shortcut Dimension 1 Code] AS DepartmentCode,
       [Salesperson Code] AS SalespersonCode,
       [Order No_] AS OrederNo,
       [Order Date] AS OrderDate,
       [Gen_ Bus_ Posting Group] AS SaleChannel,
       [External Document No_] AS ReferenceDocumentNo ,
       [Cust_ Ledger Entry No_] AS CustLedgerEntryNo,
       [Payment Method Code] AS PaymentMethodCode,
       [Dimension Set ID] AS DimensionSetID,
       [Shipment Date] AS ShipmentDate,
       [Car ID] AS CarID,
       [Location Code] AS LocationCode
FROM [{table_prefix}$Sales Invoice Header] with(nolock)
WHERE 1=1
  AND ([Shortcut Dimension 1 Code] Not In ('FEC08-1001',
                                           'FEC10-5001',
                                           'FEC08-1001',
                                           'FEC10-5001'))
                                      
 UNION ALL
 SELECT Convert(nvarchar(10), '{source_db}') AS SourceDB,
       'CR MEMO' AS TransactionType,
       [No_] AS InvoiceNo,
       [Document Date] AS InvoiceDate,
       [Posting Date] AS PostingDate,
       [Sell-to Customer No_] AS CustomerNo,
       [Shortcut Dimension 1 Code] AS DepartmentCode,
       [Salesperson Code] AS SalespersonCode,
       NULL  AS OrederNo,
       NULL AS OrderDate,
       [Gen_ Bus_ Posting Group] AS SaleChannel,
       [External Document No_] AS ReferenceDocumentNo,
       [Cust_ Ledger Entry No_] AS CustLedgerEntryNo,
       [Payment Method Code] AS PaymentMethodCode,
       [Dimension Set ID] AS DimensionSetID,
       [Shipment Date] AS ShipmentDate,
       NULL AS CarID,
       NULL AS LocationCode
FROM [{table_prefix}$Sales Cr_Memo Header] with(nolock)
WHERE 1=1
  AND ([Shortcut Dimension 1 Code] Not In ('FEC08-1001',
                                           'FEC10-5001',
                                           'FEC08-1001',
                                           'FEC10-5001'))