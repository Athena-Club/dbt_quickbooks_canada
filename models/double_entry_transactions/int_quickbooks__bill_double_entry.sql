/*
Table that creates a debit record to the specified expense account and credit record to accounts payable for each bill transaction.
*/

--To disable this model, set the using_bill variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_bill', True)) }}

with bills as (
    select *
    from {{ ref('stg_quickbooks__bill') }}
),

bill_lines as (
    select *
    from {{ ref('stg_quickbooks__bill_line') }}
),

items as (
    select 
        item.*, 
        parent.expense_account_id as parent_expense_account_id,
        parent.income_account_id as parent_income_account_id
    from {{ref('stg_quickbooks__item')}} item

    left join {{ref('stg_quickbooks__item')}} parent
        on item.parent_item_id = parent.item_id
),

bill_join as (
    select
        bills.bill_id as transaction_id, 
        bill_lines.index,
        bills.transaction_date,
        -- make an exception here for a Quickbook app error fix. Unfortunately we need to use the CAD amount instead of USD
        -- https://www.notion.so/9-Quickbooks-6bcfa312f4d74dc58f5b86cf1d8657fb 
        case when bills.bill_id = 9659 then bill_lines.amount else round(bill_lines.amount*bills.exchange_rate,2) end as amount,
        coalesce(bill_lines.account_expense_account_id, items.expense_account_id, items.parent_expense_account_id, items.expense_account_id, items.parent_income_account_id, items.income_account_id) as payed_to_account_id,
        bills.payable_account_id,
        coalesce(bill_lines.account_expense_customer_id, bill_lines.item_expense_customer_id) as customer_id,
        bills.vendor_id
    from bills

    inner join bill_lines 
        on bills.bill_id = bill_lines.bill_id

    left join items
        on bill_lines.item_expense_item_id = items.item_id
),

final as (
    select 
        transaction_id,
        index,
        transaction_date,
        customer_id,
        vendor_id,
        amount,
        payed_to_account_id as account_id,
        'debit' as transaction_type,
        'bill' as transaction_source
    from bill_join

    union all

    select
        transaction_id,
        index,
        transaction_date,
        customer_id,
        vendor_id,
        amount,
        payable_account_id as account_id,
        'credit' as transaction_type,
        'bill' as transaction_source
    from bill_join
)

select *
from final