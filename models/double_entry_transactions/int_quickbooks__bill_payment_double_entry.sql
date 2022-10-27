/*
Table that creates a debit record to accounts payable and a credit record to the specified cash account.
*/

--To disable this model, set the using_bill_payment variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_bill', True)) }}

with bill_payments as (
    select *
    from {{ ref('stg_quickbooks__bill_payment') }}
),

bill_payment_lines as (
    select *
    from {{ ref('stg_quickbooks__bill_payment_line') }}
),

bills as (
    select *
    from {{ ref('stg_quickbooks__bill') }}
),

bill_linked_payments as (
    select *
    from {{ ref('stg_quickbooks__bill_linked_txn') }}
),

bill_pay_currency as (
    select
        bill_linked_payments.bill_payment_id,
        sum(bills.total_amount*exchange_rate) as total_amount
    from bill_linked_payments 
    left join bills
        on bill_linked_payments.bill_id = bills.bill_id
    where bill_linked_payments.bill_payment_id is not null
        and bills.currency_id != 'USD'
    group by 1
),

accounts as (
    select *
    from {{ ref('stg_quickbooks__account') }}
),

ap_accounts as (
    select
        account_id,
        currency_id
    from accounts
    
    where account_type = 'Accounts Payable'
        and is_active
        and not is_sub_account
),

exchange_gl_accounts as (
    select
        account_id
    from accounts
    where name = 'Exchange Gain or Loss'
        and is_active
        and not is_sub_account        
),

bill_payment_join as (
    select
        bill_payments.bill_payment_id as transaction_id,
        row_number() over(partition by bill_payments.bill_payment_id order by bill_payments.transaction_date) - 1 as index,
        bill_payments.transaction_date,
        round(coalesce(bill_pay_currency.total_amount,bill_payments.total_amount*bill_payments.exchange_rate),2) as payment_amount,
        round(bill_payments.total_amount*bill_payments.exchange_rate,2) as bank_amount,
        coalesce(bill_payments.credit_card_account_id,bill_payments.check_bank_account_id) as payment_account_id,
        ap_accounts.account_id,
        bill_payments.vendor_id
    from bill_payments
    left join bill_pay_currency
       on bill_payments.bill_payment_id = bill_pay_currency.bill_payment_id
    left join ap_accounts
        on bill_payments.currency_id = ap_accounts.currency_id
),

final as (
    select
        transaction_id,
        index,
        transaction_date,
        cast(null as {{ dbt_utils.type_string() }}) as customer_id,
        vendor_id,
        bank_amount as amount,
        payment_account_id as account_id,
        'credit' as transaction_type,
        'bill payment' as transaction_source
    from bill_payment_join

    union all

    select
        transaction_id,
        index,
        transaction_date,
        cast(null as {{ dbt_utils.type_string() }}) as customer_id,
        vendor_id,
        payment_amount as amount,
        account_id,
        'debit' as transaction_type,
        'bill payment' as transaction_source
    from bill_payment_join

    union all

    select
        transaction_id,
        index,
        transaction_date,
        cast(null as {{ dbt_utils.type_string() }}) as customer_id,
        vendor_id,
        coalesce(bank_amount-payment_amount,0) as amount,
        exchange_gl_accounts.account_id,
        'debit' as transaction_type,
        'bill payment' as transaction_source
    from bill_payment_join
    cross join exchange_gl_accounts
    where payment_amount != bank_amount
)

select *
from final