The methodology used in this app is the following:
- Active holders are those that made 5 or more transactions on Solana since December 25th (when the token was launched).
- Involvement on the Solana NFT space considers not only sales but also mint wallets.
- Some wallets were not taken into account when checking at amounts transferred by holder wallets. Those belong to the following:
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  
  These wallets were obtained from a query from someone in the Flipside Crypto community: https://app.flipsidecrypto.com/velocity/queries/6bbb795c-9612-4bf2-9320-09179c6fa75c
  
  Queries by Section in the dashboard:
 
 #-------------------   HOLDERS   ------------------- #
 
# Query BA1
sql_statement_ba1 = """
with transfers as (
  select date_trunc('day',block_timestamp) as date,
  tx_from as wallet,
  tx_id as id,
  -1*sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  UNION
  select date_trunc('day',block_timestamp) as date,
  tx_to as wallet,
  tx_id as id,
  sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  ),
  
main as (
  select date,
  wallet,
  sum(BONK_amount) as BONK_balance_per_day,
  sum(BONK_balance_per_day) over (partition by wallet order by date asc rows between unbounded preceding and current row) as cumulative_BONK_held_per_wallet
  from transfers
  group by 1,2
  qualify cumulative_BONK_held_per_wallet > 0
  ),

active_wallets as (
  select distinct signers[0] as wallet,
  count(distinct tx_id) as number_of_transactions
  from solana.core.fact_transactions a
  inner join main b on a.signers[0] = b.wallet
  where block_timestamp >= '2022-12-25'
  group by 1
  ),

main2 as (
select date,
  'All Holders' as type,
  count(distinct wallet) as unique_holders,
  sum(unique_holders) over (order by date asc) as cumulative_holders,
  avg(cumulative_BONK_held_per_wallet) as average_bonk_held,
  sum(cumulative_BONK_held_per_wallet) as total_bonk_held
  from main
  group by 1
UNION
select date,
  'Active Holders' as type,
  count(distinct a.wallet) as unique_holders,
  sum(unique_holders) over (order by date asc) as cumulative_holders,
  avg(cumulative_BONK_held_per_wallet) as average_bonk_held,
  sum(cumulative_BONK_held_per_wallet) as total_bonk_held
  from main a
  inner join active_wallets b using(wallet)
  where number_of_transactions >= 5
  group by 1
  order by 1 desc
  )

select *
  from main2
  where type = 'All Holders'
"""

#--------------------------------------------------------------------------------------

sql_statement_ba2 = """
with transfers as (
  select date_trunc('day',block_timestamp) as date,
  tx_from as wallet,
  tx_id as id,
  -1*sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  UNION
  select date_trunc('day',block_timestamp) as date,
  tx_to as wallet,
  tx_id as id,
  sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  ),
  
main as (
  select date,
  wallet,
  sum(BONK_amount) as BONK_balance_per_day,
  sum(BONK_balance_per_day) over (partition by wallet order by date asc rows between unbounded preceding and current row) as cumulative_BONK_held_per_wallet
  from transfers
  group by 1,2
  qualify cumulative_BONK_held_per_wallet > 0
  ),

main2 as (
  select wallet,
  avg(cumulative_BONK_held_per_wallet) as average_bonk_held
  from main
  group by 1
  )

select count(distinct wallet) as number_of_wallets,
  case when average_bonk_held > 0 and average_bonk_held <= 10000000 then 'Less than 10M $BONK'
  when average_bonk_held > 10000000 and average_bonk_held <= 100000000 then 'Between 10M and 100M $BONK'
  when average_bonk_held > 100000000 and average_bonk_held <= 1000000000 then 'Between 100M and 1B $BONK'
  when average_bonk_held > 1000000000 and average_bonk_held <= 10000000000 then 'Between 1B and 10B $BONK'
  else 'Over 10B $BONK'
  end as amount_distribution
  from main2
  group by 2
"""
)

#--------------------------------------------------------------------------------------

# Query BA3
sql_statement_ba3 = """
with transfers as (
  select date_trunc('day',block_timestamp) as date,
  tx_from as wallet,
  tx_id as id,
  -1*sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  UNION
  select date_trunc('day',block_timestamp) as date,
  tx_to as wallet,
  tx_id as id,
  sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  ),
  
main as (
  select date,
  wallet,
  sum(BONK_amount) as BONK_balance_per_day,
  sum(BONK_balance_per_day) over (partition by wallet order by date asc rows between unbounded preceding and current row) as cumulative_BONK_held_per_wallet
  from transfers
  group by 1,2
  qualify cumulative_BONK_held_per_wallet > 0
  )

select wallet,
  avg(cumulative_BONK_held_per_wallet) as average_bonk_held,
  row_number() over (order by average_bonk_held desc) as rank
  from main
  group by 1
  qualify rank <= 10
"""

#--------------------------------------------------------------------------------------

sql_statement_ba5 = """
with transfers as (
  select date_trunc('day',block_timestamp) as date,
  tx_from as wallet,
  tx_id as id,
  -1*sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  UNION
  select date_trunc('day',block_timestamp) as date,
  tx_to as wallet,
  tx_id as id,
  sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  ),
  
main as (
  select date,
  wallet,
  sum(BONK_amount) as BONK_balance_per_day,
  sum(BONK_balance_per_day) over (partition by wallet order by date asc rows between unbounded preceding and current row) as cumulative_BONK_held_per_wallet
  from transfers
  group by 1,2
  qualify cumulative_BONK_held_per_wallet > 0
  ),

active_wallets as (
  select distinct signers[0] as wallet,
  count(distinct tx_id) as number_of_transactions
  from solana.core.fact_transactions a
  inner join main b on a.signers[0] = b.wallet
  where block_timestamp >= '2022-12-25'
  group by 1
  ),

first_and_last_tx as (
  select wallet,
  min(date) as first_tx_date,
  max(date) as last_tx_date
  from main
  group by 1
  ),

hold_time_per_wallet as (
  select wallet,
  datediff('day', first_tx_date, last_tx_date) as time_holding
  from first_and_last_tx
  )

select 'All Types' as holder_type,
  avg(time_holding) as average_days_holding
  from hold_time_per_wallet
UNION
select 'More than One Day Holders' as holder_type,
  avg(time_holding) as average_days_holding
  from hold_time_per_wallet
  where time_holding > 0
"""

#--------------------------------------------------------------------------------------

sql_statement_ba6 = """
with transfers as (
  select date_trunc('day',block_timestamp) as date,
  tx_from as wallet,
  tx_id as id,
  -1*sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  UNION
  select date_trunc('day',block_timestamp) as date,
  tx_to as wallet,
  tx_id as id,
  sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  ),
  
main as (
  select date,
  wallet,
  sum(BONK_amount) as BONK_balance_per_day,
  sum(BONK_balance_per_day) over (partition by wallet order by date asc rows between unbounded preceding and current row) as cumulative_BONK_held_per_wallet
  from transfers
  group by 1,2
  qualify cumulative_BONK_held_per_wallet > 0
  ),

active_wallets as (
  select distinct signers[0] as wallet,
  count(distinct tx_id) as number_of_transactions
  from solana.core.fact_transactions a
  inner join main b on a.signers[0] = b.wallet
  where block_timestamp >= '2022-12-25'
  group by 1
  ),

first_and_last_tx as (
  select wallet,
  min(date) as first_tx_date,
  max(date) as last_tx_date
  from main
  group by 1
  ),

hold_time_per_wallet as (
  select wallet,
  datediff('day', first_tx_date, last_tx_date) as time_holding
  from first_and_last_tx
  )

select count(distinct wallet) as number_of_wallets,
  case when time_holding = 0 then 'Held $BONK for less than 1 day'
  when time_holding > 0 and time_holding <= 2 then 'Held $BONK between 0 and 2 days'
  when time_holding > 2 and time_holding <= 4 then 'Held $BONK between 2 and 4 days'
  when time_holding > 4 and time_holding <= 6 then 'Held $BONK between 6 and 6 days'
  else 'Held $BONK for over 6 days'
  end as wallet_distribution
  from hold_time_per_wallet
  group by 2
"""

#--------------------------------------------------------------------------------------

sql_statement_ba7 = """
with transfers as (
  select date_trunc('day',block_timestamp) as date,
  tx_from as wallet,
  tx_id as id,
  -1*sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  UNION
  select date_trunc('day',block_timestamp) as date,
  tx_to as wallet,
  tx_id as id,
  sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  ),
  
main as (
  select date,
  wallet,
  sum(BONK_amount) as BONK_balance_per_day,
  sum(BONK_balance_per_day) over (partition by wallet order by date asc rows between unbounded preceding and current row) as cumulative_BONK_held_per_wallet
  from transfers
  group by 1,2
  qualify cumulative_BONK_held_per_wallet > 0
  ),

nft_involved as (  
  select distinct wallet
  from main
  inner join solana.core.fact_nft_mints on purchaser = wallet
  UNION
  select distinct wallet
  from main
  inner join solana.core.fact_nft_sales on purchaser = wallet
  )

select 'Involved with Solana NFTs' as type,
  count(distinct wallet) as wallets
  from nft_involved
UNION ALL
select 'Not Involved with Solana NFTs' as type,
  count(distinct b.wallet) - count(distinct a.wallet) as wallets
  from nft_involved a
  full outer join main b using(wallet)
"""

#-------------------   ACTIVE HOLDERS   ------------------- #

# Query BB1
sql_statement_bb1 = """
with transfers as (
  select date_trunc('day',block_timestamp) as date,
  tx_from as wallet,
  tx_id as id,
  -1*sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  UNION
  select date_trunc('day',block_timestamp) as date,
  tx_to as wallet,
  tx_id as id,
  sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  ),
  
main as (
  select date,
  wallet,
  sum(BONK_amount) as BONK_balance_per_day,
  sum(BONK_balance_per_day) over (partition by wallet order by date asc rows between unbounded preceding and current row) as cumulative_BONK_held_per_wallet
  from transfers
  group by 1,2
  qualify cumulative_BONK_held_per_wallet > 0
  ),

active_wallets as (
  select distinct signers[0] as wallet,
  count(distinct tx_id) as number_of_transactions
  from solana.core.fact_transactions a
  inner join main b on a.signers[0] = b.wallet
  where block_timestamp >= '2022-12-25'
  group by 1
  ),

main2 as (
select date,
  'All Holders' as type,
  count(distinct wallet) as unique_holders,
  sum(unique_holders) over (order by date asc) as cumulative_holders,
  avg(cumulative_BONK_held_per_wallet) as average_bonk_held,
  sum(cumulative_BONK_held_per_wallet) as total_bonk_held
  from main
  group by 1
UNION
select date,
  'Active Holders' as type,
  count(distinct a.wallet) as unique_holders,
  sum(unique_holders) over (order by date asc) as cumulative_holders,
  avg(cumulative_BONK_held_per_wallet) as average_bonk_held,
  sum(cumulative_BONK_held_per_wallet) as total_bonk_held
  from main a
  inner join active_wallets b using(wallet)
  where number_of_transactions >= 5
  group by 1
  order by 1 desc
  )

select *
  from main2
  where type = 'Active Holders'
"""

#--------------------------------------------------------------------------------------

# Query BB2
sql_statement_bb2 = """
with transfers as (
  select date_trunc('day',block_timestamp) as date,
  tx_from as wallet,
  tx_id as id,
  -1*sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  UNION
  select date_trunc('day',block_timestamp) as date,
  tx_to as wallet,
  tx_id as id,
  sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  ),
  
main as (
  select date,
  wallet,
  sum(BONK_amount) as BONK_balance_per_day,
  sum(BONK_balance_per_day) over (partition by wallet order by date asc rows between unbounded preceding and current row) as cumulative_BONK_held_per_wallet
  from transfers
  group by 1,2
  qualify cumulative_BONK_held_per_wallet > 0
  ),

active_wallets as (
  select distinct signers[0] as wallet,
  count(distinct tx_id) as number_of_transactions
  from solana.core.fact_transactions a
  inner join main b on a.signers[0] = b.wallet
  where block_timestamp >= '2022-12-25'
  group by 1
  ),

main2 as (
  select wallet,
  avg(cumulative_BONK_held_per_wallet) as average_bonk_held
  from main
  inner join active_wallets using(wallet)
  where number_of_transactions >= 5
  group by 1
  )

select count(distinct wallet) as number_of_wallets,
  case when average_bonk_held > 0 and average_bonk_held <= 10000000 then 'Less than 10M $BONK'
  when average_bonk_held > 10000000 and average_bonk_held <= 100000000 then 'Between 10M and 100M $BONK'
  when average_bonk_held > 100000000 and average_bonk_held <= 1000000000 then 'Between 100M and 1B $BONK'
  when average_bonk_held > 1000000000 and average_bonk_held <= 10000000000 then 'Between 1B and 10B $BONK'
  else 'Over 10B $BONK'
  end as amount_distribution
  from main2
  group by 2
"""

#--------------------------------------------------------------------------------------

# Query BB3
sql_statement_bb3 = """
with transfers as (
  select date_trunc('day',block_timestamp) as date,
  tx_from as wallet,
  tx_id as id,
  -1*sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  UNION
  select date_trunc('day',block_timestamp) as date,
  tx_to as wallet,
  tx_id as id,
  sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  ),
  
main as (
  select date,
  wallet,
  sum(BONK_amount) as BONK_balance_per_day,
  sum(BONK_balance_per_day) over (partition by wallet order by date asc rows between unbounded preceding and current row) as cumulative_BONK_held_per_wallet
  from transfers
  group by 1,2
  qualify cumulative_BONK_held_per_wallet > 0
  ),

active_wallets as (
  select distinct signers[0] as wallet,
  count(distinct tx_id) as number_of_transactions
  from solana.core.fact_transactions a
  inner join main b on a.signers[0] = b.wallet
  where block_timestamp >= '2022-12-25'
  group by 1
  )

select wallet,
  avg(cumulative_BONK_held_per_wallet) as average_bonk_held,
  row_number() over (order by average_bonk_held desc) as rank
  from main
  inner join active_wallets using(wallet)
  where number_of_transactions >= 5
  group by 1
  qualify rank <= 10
"""

#--------------------------------------------------------------------------------------

# Query BB5
sql_statement_bb5 = """
with transfers as (
  select date_trunc('day',block_timestamp) as date,
  tx_from as wallet,
  tx_id as id,
  -1*sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  UNION
  select date_trunc('day',block_timestamp) as date,
  tx_to as wallet,
  tx_id as id,
  sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  ),
  
main as (
  select date,
  wallet,
  sum(BONK_amount) as BONK_balance_per_day,
  sum(BONK_balance_per_day) over (partition by wallet order by date asc rows between unbounded preceding and current row) as cumulative_BONK_held_per_wallet
  from transfers
  group by 1,2
  qualify cumulative_BONK_held_per_wallet > 0
  ),

active_wallets as (
  select distinct signers[0] as wallet,
  count(distinct tx_id) as number_of_transactions
  from solana.core.fact_transactions a
  inner join main b on a.signers[0] = b.wallet
  where block_timestamp >= '2022-12-25'
  group by 1
  ),

first_and_last_tx as (
  select wallet,
  min(date) as first_tx_date,
  max(date) as last_tx_date
  from main
  inner join active_wallets using(wallet)
  where number_of_transactions >= 5
  group by 1
  ),

hold_time_per_wallet as (
  select wallet,
  datediff('day', first_tx_date, last_tx_date) as time_holding
  from first_and_last_tx
  )

select 'All Types' as holder_type,
  avg(time_holding) as average_days_holding
  from hold_time_per_wallet
UNION
select 'More than One Day Holders' as holder_type,
  avg(time_holding) as average_days_holding
  from hold_time_per_wallet
  where time_holding > 0
"""

# --------------------------------------------------------------------------------------

# Query BB6
sql_statement_bb6 = """
with transfers as (
  select date_trunc('day',block_timestamp) as date,
  tx_from as wallet,
  tx_id as id,
  -1*sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  UNION
  select date_trunc('day',block_timestamp) as date,
  tx_to as wallet,
  tx_id as id,
  sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  ),
  
main as (
  select date,
  wallet,
  sum(BONK_amount) as BONK_balance_per_day,
  sum(BONK_balance_per_day) over (partition by wallet order by date asc rows between unbounded preceding and current row) as cumulative_BONK_held_per_wallet
  from transfers
  group by 1,2
  qualify cumulative_BONK_held_per_wallet > 0
  ),

active_wallets as (
  select distinct signers[0] as wallet,
  count(distinct tx_id) as number_of_transactions
  from solana.core.fact_transactions a
  inner join main b on a.signers[0] = b.wallet
  where block_timestamp >= '2022-12-25'
  group by 1
  ),

first_and_last_tx as (
  select wallet,
  min(date) as first_tx_date,
  max(date) as last_tx_date
  from main
  inner join active_wallets using(wallet)
  where number_of_transactions >= 5
  group by 1
  ),

hold_time_per_wallet as (
  select wallet,
  datediff('day', first_tx_date, last_tx_date) as time_holding
  from first_and_last_tx
  )

select count(distinct wallet) as number_of_wallets,
  case when time_holding = 0 then 'Held $BONK for less than 1 day'
  when time_holding > 0 and time_holding <= 2 then 'Held $BONK between 0 and 2 days'
  when time_holding > 2 and time_holding <= 4 then 'Held $BONK between 2 and 4 days'
  when time_holding > 4 and time_holding <= 6 then 'Held $BONK between 6 and 6 days'
  else 'Held $BONK for over 6 days'
  end as wallet_distribution
  from hold_time_per_wallet
  group by 2
"""

#--------------------------------------------------------------------------------------

# Query BB7
sql_statement_bb7 = """
with transfers as (
  select date_trunc('day',block_timestamp) as date,
  tx_from as wallet,
  tx_id as id,
  -1*sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  UNION
  select date_trunc('day',block_timestamp) as date,
  tx_to as wallet,
  tx_id as id,
  sum(amount) as BONK_amount
  from solana.core.fact_transfers
  where block_timestamp >= '2022-12-25'
  and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and wallet not in (
  '2yBBKgCwGdVpo192D8WZeAtqyhyP8DkCMnmTLeVYfKtA', -- 'bonk marketing wallet'
  'DBR2ZUvjZTcgy6R9US64t96pBEZMyr9DPW6G2scrctQK', -- 'bonk dao wallet'
  '4CUMsJG7neKqZuuLeoBoMuqufaNBc2wdwQiXnoH4aJcD', -- 'bonk team wallet'
  '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw', -- 'bonk airdrop wallet 1'
  '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p', -- 'bonk airdrop wallet 2'
  '2PFvRYt5h88ePdQXBrH3dyFmQqJHTNZYLztE847dHWYz', -- 'dex bonk-usdc pool'
  '5P6n5omLbLbP4kaPGL8etqQAHEx2UCkaUyvjLDnwV4EY', -- 'orca bonk-usdc pool'
  'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez' -- 'orca bonk-sol pool'
  )
  group by 1,2,3
  ),
  
main as (
  select date,
  wallet,
  sum(BONK_amount) as BONK_balance_per_day,
  sum(BONK_balance_per_day) over (partition by wallet order by date asc rows between unbounded preceding and current row) as cumulative_BONK_held_per_wallet
  from transfers
  group by 1,2
  qualify cumulative_BONK_held_per_wallet > 0
  ),

active_wallets as (
  select distinct signers[0] as wallet,
  count(distinct tx_id) as number_of_transactions
  from solana.core.fact_transactions a
  inner join main b on a.signers[0] = b.wallet
  where block_timestamp >= '2022-12-25'
  group by 1
  ),

nft_involved as (  
  select distinct wallet
  from active_wallets
  inner join solana.core.fact_nft_mints on purchaser = wallet
  where number_of_transactions >= 5
  UNION
  select distinct wallet
  from active_wallets
  inner join solana.core.fact_nft_sales on purchaser = wallet
  where number_of_transactions >= 5
  )

select 'Involved with Solana NFTs' as type,
  count(distinct wallet) as wallets
  from nft_involved
UNION ALL
select 'Not Involved with Solana NFTs' as type,
  count(distinct b.wallet) - count(distinct a.wallet) as wallets
  from nft_involved a
  full outer join main b using(wallet)
"""


#-------------------   AIRDROP   ------------------- #

# Query A1 - Airdrop
sql_statement_a1 = """
select date_trunc('day', block_timestamp) as date,
  count(distinct tx_to) as airdrop_recipients,
  sum(airdrop_recipients) over (order by date) as cumulative_airdrop_recipients,
  sum(amount) as BONK_aidropped,
  sum(BONK_aidropped) over (order by date) as cumulative_BONK_aidropped
  from solana.core.fact_transfers
  where mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' -- $BONK token
  and tx_from in ('9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw','6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p') -- Airdrop Wallets
  and block_timestamp >= '2022-12-25'
  group by 1
  order by 1 desc
"""

#--------------------------------------------------------------------------------------

# Query A2
sql_statement_a2 = """
with airdrop_data as (
  select date_trunc('day', block_timestamp) as date,
  tx_to as wallet,
  sum(amount) as BONK_aidropped,
  sum(BONK_aidropped) over (partition by wallet order by date asc rows between unbounded preceding and current row) as cumulative_BONK_airdropped_per_wallet
  from solana.core.fact_transfers
  where mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' -- $BONK token
  and tx_from in ('9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw','6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p') -- Airdrop Wallets
  and block_timestamp >= '2022-12-25'
  group by 1,2
  order by 1 desc
  ),

main2 as (
  select wallet,
  avg(cumulative_BONK_airdropped_per_wallet) as average_bonk_airdropped
  from airdrop_data
  group by 1
  )

select count(distinct wallet) as number_of_wallets,
  case when average_bonk_airdropped > 0 and average_bonk_airdropped <= 100000000 then 'Less than 100M $BONK'
  when average_bonk_airdropped > 100000000 and average_bonk_airdropped <= 500000000 then 'Between 100M and 500M $BONK'
  when average_bonk_airdropped > 500000000 and average_bonk_airdropped <= 1000000000 then 'Between 500M and 1B $BONK'
  when average_bonk_airdropped > 1000000000 and average_bonk_airdropped <= 10000000000 then 'Between 1B and 10B $BONK'
  else 'Over 10B $BONK'
  end as amount_distribution
  from main2
  group by 2
"""

#--------------------------------------------------------------------------------------

# Query A3
sql_statement_a3 = """
with airdrop_data as (
  select date_trunc('day', block_timestamp) as date,
  tx_to as wallet,
  sum(amount) as BONK_aidropped,
  sum(BONK_aidropped) over (partition by wallet order by date asc rows between unbounded preceding and current row) as cumulative_BONK_airdropped_per_wallet
  from solana.core.fact_transfers
  where mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' -- $BONK token
  and tx_from in ('9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw','6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p') -- Airdrop Wallets
  and block_timestamp >= '2022-12-25'
  group by 1,2
  order by 1 desc
  )

select wallet,
  sum(cumulative_BONK_airdropped_per_wallet) as total_bonk_airdropped,
  row_number() over (order by total_bonk_airdropped desc) as rank
  from airdrop_data
  group by 1
  qualify rank <= 10
"""

#--------------------------------------------------------------------------------------

# Query A7
sql_statement_a7 = """
with airdrop_data as (
  select date_trunc('day', block_timestamp) as date,
  tx_to as wallet,
  sum(amount) as BONK_aidropped,
  sum(BONK_aidropped) over (partition by wallet order by date asc rows between unbounded preceding and current row) as cumulative_BONK_airdropped_per_wallet
  from solana.core.fact_transfers
  where mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' -- $BONK token
  and tx_from in ('9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw','6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p') -- Airdrop Wallets
  and block_timestamp >= '2022-12-25'
  group by 1,2
  order by 1 desc
  ),

nft_involved as (  
  select distinct wallet
  from airdrop_data
  inner join solana.core.fact_nft_mints on purchaser = wallet
  UNION
  select distinct wallet
  from airdrop_data
  inner join solana.core.fact_nft_sales on purchaser = wallet
  )

select 'Involved with Solana NFTs' as type,
  count(distinct wallet) as wallets
  from nft_involved
UNION ALL
select 'Not Involved with Solana NFTs' as type,
  count(distinct b.wallet) - count(distinct a.wallet) as wallets
  from nft_involved a
  full outer join airdrop_data b using(wallet)
"""

#-------------------   SWAPS   ------------------- #

# Query C1 - Swaps
sql_statement_c1 = """
select date_trunc('day', block_timestamp) as date,
  initcap(split(swap_program, ' ')[0]::string) AS dex, -- formatting of the exchange
  count(distinct swapper) as unique_buyers,
  sum(unique_buyers) over (partition by dex order by date) as cumulative_unique_buyers,
  sum(swap_to_amount) as total_swap_to_amount,
  sum(total_swap_to_amount) over (partition by dex order by date) as cumulative_total_swap_to_amount
  from solana.core.fact_swaps
  where block_timestamp >= '2022-12-25' 
  and swap_to_mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and succeeded
  group by 1,2
  order by 1 desc
"""

#--------------------------------------------------------------------------------------

# Query C2
sql_statement_c2 = """
select date_trunc('day', block_timestamp) as date,
  initcap(split(swap_program, ' ')[0]::string) AS dex, -- formatting of the exchange
  count(distinct swapper) as unique_sellers,
  sum(unique_sellers) over (partition by dex order by date) as cumulative_unique_sellers,
  sum(swap_from_amount) as total_swap_from_amount,
  sum(total_swap_from_amount) over (partition by dex order by date) as cumulative_total_swap_from_amount
  from solana.core.fact_swaps
  where block_timestamp >= '2022-12-25' 
  and swap_from_mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and succeeded
  group by 1,2
"""

#--------------------------------------------------------------------------------------

# Query C3
sql_statement_c3 = """
select dayname(block_timestamp::date) as day_of_the_Week,
  count(distinct swapper) as unique_buyers,
  sum(swap_to_amount) as total_swap_to_amount
  from solana.core.fact_swaps
  where block_timestamp >= '2022-12-25' 
  and swap_to_mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and succeeded
  group by 1
"""

#--------------------------------------------------------------------------------------

# Query C4
sql_statement_c4 = """
select dayname(block_timestamp::date) as day_of_the_Week,
  count(distinct swapper) as unique_sellers,
  sum(swap_from_amount) as total_swap_from_amount
  from solana.core.fact_swaps
  where block_timestamp >= '2022-12-25' 
  and swap_from_mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and succeeded
  group by 1
"""

#--------------------------------------------------------------------------------------

# Query C5
sql_statement_c5 = """
select swap_from_mint,
  case when swap_from_mint = 'FLTHudk5B5zag7JmGXqrYrFfey6otevLQA6jm1UHHLEE' then 'FLTH'
  else token_name end as token_name,
  sum(swap_to_amount) as total_swap_to_amount
  from solana.core.fact_swaps
  inner join solana.core.dim_tokens on token_address = swap_from_mint
  where block_timestamp >= '2022-12-25' 
  and swap_to_mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and succeeded
  group by 1,2
  qualify row_number() over (order by total_swap_to_amount desc) <= 10
"""

#--------------------------------------------------------------------------------------

# Query C6
sql_statement_c6 = """
select swap_to_mint,
  case when swap_to_mint = 'FLTHudk5B5zag7JmGXqrYrFfey6otevLQA6jm1UHHLEE' then 'FLTH'
  else token_name end as token_name,
  sum(swap_from_amount) as total_swap_from_amount
  from solana.core.fact_swaps
  inner join solana.core.dim_tokens on token_address = swap_to_mint
  where block_timestamp >= '2022-12-25' 
  and swap_from_mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
  and succeeded
  group by 1,2
  qualify row_number() over (order by total_swap_from_amount desc) <= 10
"""

#--------------------------------------------------------------------------------------
