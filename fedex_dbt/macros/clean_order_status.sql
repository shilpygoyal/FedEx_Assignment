/*
Macro: clean_order_status

Description:
Standardizes raw order status values into a consistent set of
analytics-friendly categories. The source dataset contains multiple
granular shipping and delivery states which are not suitable for
reporting. This macro maps those detailed statuses into simplified
business categories.

These standardized categories improve consistency across analytical
models and enable easier reporting in BI tools.

Parameters:
status_col (column) – Column containing the raw order status values.

Returns:
A cleaned order status value such as:
- pending
- shipped
- delivered
- returned
- cancelled
- exception
- unknown

Example:
{{ clean_order_status('order_status') }}

Example Usage in a Model:
SELECT
    order_id,
    {{ clean_order_status('order_status') }} AS order_status
FROM {{ ref('stg_sales') }}
*/

{% macro clean_order_status(status_col) %}
  CASE
    WHEN {{ status_col }} LIKE 'pending%' THEN 'pending'

    WHEN {{ status_col }} = 'shipped - delivered to buyer'
        THEN 'delivered'

    WHEN {{ status_col }} IN (
        'shipped - rejected by buyer',
        'shipped - returned to seller',
        'shipped - returning to seller'
    ) THEN 'returned'

    WHEN {{ status_col }} IN (
        'shipped - lost in transit',
        'shipped - damaged'
    ) THEN 'exception'

    WHEN {{ status_col }} = 'cancelled'
        THEN 'cancelled'

    WHEN {{ status_col }} LIKE 'shipped%'
        THEN 'shipped'

    ELSE 'unknown'
END
{% endmacro %}