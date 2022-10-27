with general_ledger_by_period as (
    select *
    from {{ref('quickbooks__general_ledger_by_period')}}
    where financial_statement_helper = 'balance_sheet'

), 

final as (
    select
        period_first_day as calendar_date,
        account_class,
        is_sub_account,
        parent_account_number,
        parent_account_name,
        account_type,
        account_sub_type,
        account_number,
        account_id,
        account_name,
        period_ending_balance as amount
    from general_ledger_by_period
    where account_number not like '31%'
    and parent_account_name not like 'Loan from%'
    
    -- AC wants to see "Equities" and "Loan from shareholders" in summarized format, 
    -- so we give the account_number 3100 to equities and 2600 to loand to each and summarize them
    -- i.e. , we still want the numbers in there, but not at the detail level of the account name

    union all
    
    select
        period_first_day as calendar_date,
        'Equity' as account_class,
        false as is_sub_account,
        '3100' as parent_account_number,
        'Equity' as parent_account_name,
        'Equity' as account_type,
        'CommonStock' as account_sub_type,
        '3100' as account_number,
        'Equity' as account_id,
        'Summarized Equity' as account_name,
        sum(period_ending_balance) as amount
    from general_ledger_by_period
    where account_number like '31%'   
    group by 1

    union all
    
    select
        period_first_day as calendar_date,
        'Liability' as account_class,
        false as is_sub_account,
        '2600' as parent_account_number,
        'Loan from Shareholder' as parent_account_name,
        'Long Term Liability' as account_type,
        'NotesPayable' as account_sub_type,
        '2600' as account_number,
        'Loan' as account_id,
        'Loan from Shareholder' as account_name,
        sum(period_ending_balance) as amount
    from general_ledger_by_period
    where parent_account_name like 'Loan from%'  
    group by 1

)

select *
from final