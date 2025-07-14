import pandas as pd
import sqlite3
import logging
from ingestion_db import ingest_db
logging.basicConfig(
    filename="logs/summary_table.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)
def create_vendor_summary(con):
    vendor_sales_summary=pd.read_sql_query(""" with FreightSummary as
                                       (select sum(Freight)as FreightCost,
                                       VendorNumber from vendor_invoice group by VendorNumber)
                                       ,PurchaseSummary as (select p.VendorName,
                                       p.VendorNumber,p.Brand,sum(p.Quantity)as TotalPurchaseQuantity,p.Description,
                                       pp.Price as ActualPrice,
                                       pp.Volume,p.PurchasePrice,
                                       sum(p.Dollars) as TotalPurchaseDollars 
                                       from purchases p join purchase_prices pp on p.Brand=pp.Brand 
                                       where p.PurchasePrice>0 
                                       group by p.VendorNumber ,
                                       p.VendorName,p.Brand,p.Description,
                                       p.PurchasePrice,pp.Price,pp.Volume order by sum(Dollars) desc),
                                       salesSummary as
                                       (select VendorNo,Brand,
                                       sum(SalesQuantity) as TotalSalesQuantity,
                                       sum(SalesDollars) as TotalSalesDollars,
                                       sum(ExciseTax)as TotalExciseDuty,
                                       SalesPrice from sales group by VendorNo,Brand)
                                       select ps.VendorName,ps.VendorNumber,ps.Brand,
                                       ps.Purchaseprice,ps.Volume,ps.ActualPrice,ps.TotalPurchaseDollars,ps.Description,
                                       ps.TotalPurchaseQuantity,ss.TotalSalesDollars,ss.TotalSalesQuantity,
                                       ss.TotalExciseDuty,fs.FreightCost
                                       from PurchaseSummary ps left join SalesSummary ss on
                                       ps.VendorNumber=ss.VendorNo and
                                       ps.Brand=ss.Brand
                                       left join FreightSummary fs on ps.VendorNumber=fs.VendorNumber
                                       order by TotalPurchaseDollars desc""",con)

    return vendor_sales_summary
def clean_data(df):

    df['Volume']=df['Volume'].astype('float64')
    df.fillna(0,inplace=True)
    df['VendorName']=df['VendorName'].str.strip()
    df['GrossProfit']=df['TotalSalesDollars']-df['TotalPurchaseDollars']
    df['ProfitMargin']=(df['GrossProfit']/df['TotalSalesDollars'])*100  
    df['SalesTurnover']=df['TotalSalesQuantity']/df['TotalPurchaseQuantity']
    df['SalesToPurchaseRation']=df['TotalSalesDollars']/df['TotalPurchaseDollars']
    return df


if __name__=="__name__":
    con=sqlite3.connect('inventory.db')
    logging.info("creating vendor summary table")
    summary_df=create_vendor_summary(con)
    logging.info(summary_df.head())

    logging.info("Summary data")
    clean_df=clean_data(summary_df)
    logging.info(clean_df.head())
    

    logging.info("ingesting data")
    ingest_db(clean_df,"vendor_summary_table",con)
    logging.info("completed")