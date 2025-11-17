import pandas as pd
from collections import Counter

# Load CSV
df = pd.read_csv("token.csv")

acp_contract_address = "0xa6c9ba866992cfd7fd6460ba912bfa405ada9df0" 
meta_vault_address = "0xea98fd8d0e4515fe5989ebe2b666264d25d1021c"
agent_address = "0xa908b2a981ae1106e5b047ef44604ccaa1e1c7f9"
zero_address = "0x0000000000000000000000000000000000000000"

target_token_value = "4,987.916819"
target_tx_hash = "0xec99ca02eeae9246457d11f408f11b22b34ce21776f8c59bcbcc0114236e5955"

over_minted_values = [480.0, 1860.0, 6490.0]
un_minted_values = [1961.0, 4090.0, 4987.916819]

# Extract filtered dataframes
inflow_df = df.loc[(df['TokenSymbol'] == 'USDC') & (df['From'] == acp_contract_address) & (df['To'] == agent_address)].copy()
outflow_df = df.loc[(df['TokenSymbol'] == 'USDC') & (df['From'] == agent_address) & (df['To'] == meta_vault_address)].copy()



# Convert TokenValue to numeric (remove commas first)
inflow_df['TokenValue'] = pd.to_numeric(inflow_df['TokenValue'].astype(str).str.replace(',', ''), errors='coerce')
outflow_df['TokenValue'] = pd.to_numeric(outflow_df['TokenValue'].astype(str).str.replace(',', ''), errors='coerce')

over_minted_outflow_df = outflow_df[outflow_df['TokenValue'].isin(over_minted_values)].copy().sort_values(by='UnixTimestamp', ascending=True)
print_df = over_minted_outflow_df[['UnixTimestamp','Transaction Hash', 'TokenValue']]
print_df.to_csv('over_minted_outflow.csv', index=False)

un_minted_inflow_df = inflow_df[inflow_df['TokenValue'].isin(un_minted_values)].copy().sort_values(by='UnixTimestamp', ascending=True)
print_df = un_minted_inflow_df[['UnixTimestamp','Transaction Hash', 'TokenValue']]
print_df.to_csv('un_minted_outflow.csv', index=False)

# Calculate sums
usdc_inflow = inflow_df['TokenValue'].sum()
usdc_outflow = outflow_df['TokenValue'].sum()


# Check for exact matching token values
inflow_values = sorted(inflow_df['TokenValue'].dropna().tolist())
outflow_values = sorted(outflow_df['TokenValue'].dropna().tolist())

print(f"Inflow count: {len(inflow_values)}")
print(f"Outflow count: {len(outflow_values)}")
print(f"Values match exactly: {inflow_values == outflow_values}")
print(f"Sum match: {usdc_inflow == usdc_outflow}")
print(f"Imbalance: {usdc_inflow - usdc_outflow}")

if inflow_values != outflow_values:
    print("\nDifferences found:")
        
    # Show value counts
    inflow_counts = Counter(inflow_values)
    outflow_counts = Counter(outflow_values)
    
    print("\nValue frequency comparison:")
    all_values = set(inflow_values) | set(outflow_values)
    for val in sorted(all_values):
        in_count = inflow_counts.get(val, 0)
        out_count = outflow_counts.get(val, 0)
        if in_count != out_count:
            print(f"  Value {val}: inflow={in_count}, outflow={out_count}")
print()
