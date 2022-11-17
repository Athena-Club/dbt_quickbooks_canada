with general_ledger_balances as (
    select *
    from {{ref('int_quickbooks__general_ledger_balances')}}
),

gross_profit as (
    select
        cast('99991' as {{ dbt_utils.type_string() }}) as account_id,
        cast('5399' as {{ dbt_utils.type_string() }}) as account_number,
        cast('GROSS PROFIT' as {{ dbt_utils.type_string() }}) as account_name,
        false as is_sub_account,
        cast(null as {{ dbt_utils.type_string() }}) as parent_account_number,
        cast(null as {{ dbt_utils.type_string() }}) as parent_account_name,
        cast('GROSS PROFIT' as {{ dbt_utils.type_string() }}) as account_type,
        cast('GROSS PROFIT' as {{ dbt_utils.type_string() }})as account_sub_type,
        cast('Amount sum 4' as {{ dbt_utils.type_string() }}) as account_class,
        cast('income_statement' as {{ dbt_utils.type_string() }}) as financial_statement_helper,
        cast({{ dbt_utils.date_trunc("year", "period_first_day") }} as date) as date_year,
        cast(period_first_day as date) as period_first_day,
        {{ dbt_utils.last_day("period_first_day", "month") }} as period_last_day,
        sum(case when account_type = 'Cost of Goods Sold' then period_net_change*-1 else period_net_change end) as period_net_change
    from general_ledger_balances
    where account_type in ('Cost of Goods Sold','Income')
    group by period_first_day
),

net_operating_income as (
    select
        cast('99992' as {{ dbt_utils.type_string() }}) as account_id,
        cast('6999' as {{ dbt_utils.type_string() }}) as account_number,
        cast('NET OPERATING INCOME' as {{ dbt_utils.type_string() }}) as account_name,
        false as is_sub_account,
        cast(null as {{ dbt_utils.type_string() }}) as parent_account_number,
        cast(null as {{ dbt_utils.type_string() }}) as parent_account_name,
        cast('NET OPERATING INCOME' as {{ dbt_utils.type_string() }}) as account_type,
        cast('NET OPERATING INCOME' as {{ dbt_utils.type_string() }})as account_sub_type,
        cast('Amount sum 3' as {{ dbt_utils.type_string() }}) as account_class,
        cast('income_statement' as {{ dbt_utils.type_string() }}) as financial_statement_helper,
        cast({{ dbt_utils.date_trunc("year", "period_first_day") }} as date) as date_year,
        cast(period_first_day as date) as period_first_day,
        {{ dbt_utils.last_day("period_first_day", "month") }} as period_last_day,
        sum(case 
                when account_type = 'Expense' then period_net_change*-1
                when account_type = 'Cost of Goods Sold' then period_net_change*-1  
                else period_net_change
            end) as period_net_change
    from general_ledger_balances
    where account_type in ('Cost of Goods Sold','Income','Expense')
    group by period_first_day
),

net_other_income as (
    select
        cast('99993' as {{ dbt_utils.type_string() }}) as account_id,
        cast('9998' as {{ dbt_utils.type_string() }}) as account_number,
        cast('NET OTHER INCOME' as {{ dbt_utils.type_string() }}) as account_name,
        false as is_sub_account,
        cast(null as {{ dbt_utils.type_string() }}) as parent_account_number,
        cast(null as {{ dbt_utils.type_string() }}) as parent_account_name,
        cast('NET OTHER INCOME' as {{ dbt_utils.type_string() }}) as account_type,
        cast('NET OTHER INCOME' as {{ dbt_utils.type_string() }})as account_sub_type,
        cast('Amount sum 2' as {{ dbt_utils.type_string() }}) as account_class,
        cast('income_statement' as {{ dbt_utils.type_string() }}) as financial_statement_helper,
        cast({{ dbt_utils.date_trunc("year", "period_first_day") }} as date) as date_year,
        cast(period_first_day as date) as period_first_day,
        {{ dbt_utils.last_day("period_first_day", "month") }} as period_last_day,
        sum(case 
                when account_type = 'Other Expense' then period_net_change*-1
                else period_net_change
            end) as period_net_change
    from general_ledger_balances
    where account_type in ('Other Income','Other Expense')
    group by period_first_day
),

net_income as (
    select
        cast('99994' as {{ dbt_utils.type_string() }}) as account_id,
        cast('9999' as {{ dbt_utils.type_string() }}) as account_number,
        cast('NET INCOME' as {{ dbt_utils.type_string() }}) as account_name,
        false as is_sub_account,
        cast(null as {{ dbt_utils.type_string() }}) as parent_account_number,
        cast(null as {{ dbt_utils.type_string() }}) as parent_account_name,
        cast('NET INCOME' as {{ dbt_utils.type_string() }}) as account_type,
        cast('NET INCOME' as {{ dbt_utils.type_string() }})as account_sub_type,
        cast('Amount sum 1' as {{ dbt_utils.type_string() }}) as account_class,
        cast('income_statement' as {{ dbt_utils.type_string() }}) as financial_statement_helper,
        cast({{ dbt_utils.date_trunc("year", "period_first_day") }} as date) as date_year,
        cast(period_first_day as date) as period_first_day,
        {{ dbt_utils.last_day("period_first_day", "month") }} as period_last_day,
        sum(case 
                when account_type = 'Expense' then period_net_change*-1
                when account_type = 'Cost of Goods Sold' then period_net_change*-1  
                when account_type = 'Other Expense' then period_net_change*-1
                else period_net_change
            end) as period_net_change
    from general_ledger_balances
    where account_type in ('Cost of Goods Sold','Income','Expense','Other Income','Other Expense')
    group by period_first_day
),

additional_summaries as (
    select * from gross_profit
    union
    select * from net_operating_income
    union
    select * from net_other_income
    union
    select * from net_income    
),

additional_summaries_ending as (
    select
        *,
        sum(coalesce(period_net_change,0)) over (order by account_name, period_first_day, period_first_day rows unbounded preceding) as period_ending_balance
    from additional_summaries
),

final as (
    select
        account_id,
        account_number,
        account_name,
        is_sub_account,
        parent_account_number,
        parent_account_name,
        account_type,
        account_sub_type,
        account_class,
        financial_statement_helper,
        date_year,
        period_first_day,
        period_last_day,
        period_net_change,
        lag(coalesce(period_ending_balance,0)) over (order by account_name, period_first_day) as period_beginning_balance,
        period_ending_balance
    from additional_summaries_ending
)

select *
from final
