with general_ledger_balances as (
    select *
    from {{ref('int_quickbooks__general_ledger_balances')}}
),

retained_earnings as (
    select *
    from {{ref('int_quickbooks__retained_earnings')}}
),

additional_summaries as (
    select *
    from {{ref('int_quickbooks__additional_summaries')}}
),

final as (
    select *
    from general_ledger_balances

    union all 

    select *
    from retained_earnings
    
    union all 

    select *
    from additional_summaries    
)

select *
from final