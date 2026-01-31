\# Dashboard Guide



\## Purpose



The e-commerce analytics dashboard provides insights into sales performance, customer behavior, and product trends based on the `warehouse` schema.



\## Data Sources



\- `warehouse.fact\_sales` for transaction-level metrics.

\- `warehouse.dim\_customers` for customer attributes such as country and signup date.

\- `warehouse.dim\_products` for product attributes such as category and price.

\- Aggregate tables (`agg\_`) for precomputed KPIs where applicable.



\## Dashboard Pages



1\. \*\*Sales Overview\*\*

&nbsp;  - Metrics: total revenue, total orders, average order value.

&nbsp;  - Visuals: time series by day/month, bar charts by country and channel.



2\. \*\*Customer Insights\*\*

&nbsp;  - Metrics: new vs returning customers, customers by country.

&nbsp;  - Visuals: bar charts and maps using `dim\_customers` joined to `fact\_sales`.



3\. \*\*Product Performance\*\*

&nbsp;  - Metrics: top products by revenue, quantity, and margin.

&nbsp;  - Visuals: bar charts by product and category using `dim\_products` and `fact\_sales`.



4\. \*\*Operational Metrics\*\*

&nbsp;  - Metrics: data freshness (latest `transaction\_date`), row counts.

&nbsp;  - Visuals: summary tiles and tables using monitoring queries.



\## Usage



\- Filter by date range, country, and product category to focus analysis.

\- Drill down from high-level KPIs to specific customers or products.

\- Export dashboard views as images or PDFs for reports.



