/*
Table that creates a debit record to Credit Card Payments and a credit record to the specified income account.
*/

--To disable this model, set the using_credit_card_payment variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_credit_card_payment', True)) }}

with credit_card_payments as (
    select *
    from {{ref('stg_quickbooks__credit_card_payment_txn')}}
),

credit_card_payment_join as (
    select
        credit_card_payments.credit_card_payment_txn_id as transaction_id,
        row_number() over(partition by credit_card_payments.credit_card_payment_txn_id order by credit_card_payments.transaction_date) - 1 as index,
        credit_card_payments.transaction_date,
        amount,
        credit_card_payments.credit_card_account_id as payment_account_id,
        credit_card_payments.bank_account_id as account_id,
        null as vendor_id
    from credit_card_payments
),

final as (
    select
        transaction_id,
        index,
        transaction_date,
        cast(null as {{ dbt_utils.type_string() }}) as customer_id,
        vendor_id,
        amount * -1 as amount,
        payment_account_id as account_id,
        'credit' as transaction_type,
        'credit_card_payment' as transaction_source
    from credit_card_payment_join

    union all

    select
        transaction_id,
        index,
        transaction_date,
        cast(null as {{ dbt_utils.type_string() }}) as customer_id,
        vendor_id,
        amount * -1 as amount,
        account_id,
        'debit' as transaction_type,
        'credit_card_payment' as transaction_source
    from credit_card_payment_join
)

select *
from final