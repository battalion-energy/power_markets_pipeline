import pandas as pd

# Load the data
df = pd.read_csv('bess_complete_analysis/bess_annual_revenues_complete.csv')

# Show summary statistics
print('\n📊 BESS Revenue Analysis Results')
print('='*80)
print(f'Total BESS-years analyzed: {len(df)}')
print(f'Unique BESS resources: {df["BESS_Asset_Name"].nunique()}')
print(f'Years covered: {sorted(df["Year"].unique())}')
print(f'\nTotal revenue across all years: ${df["Total_Revenue"].sum():,.2f}')

# Show breakdown by revenue type
print('\n💰 Revenue Breakdown (All Years):')
revenue_cols = ['RT_Revenue', 'DA_Revenue', 'Spin_Revenue', 'NonSpin_Revenue', 
                'RegUp_Revenue', 'RegDown_Revenue', 'ECRS_Revenue']
for col in revenue_cols:
    total = df[col].sum()
    pct = (total / df['Total_Revenue'].sum() * 100) if df['Total_Revenue'].sum() > 0 else 0
    print(f'  {col:<20} ${total:>15,.2f} ({pct:>5.1f}%)')

# Show 2024 specific data
df_2024 = df[df['Year'] == 2024]
print(f'\n📅 2024 Analysis:')
print(f'  Active BESS: {len(df_2024)}')
print(f'  Total Revenue: ${df_2024["Total_Revenue"].sum():,.2f}')
print(f'  Average per BESS: ${df_2024["Total_Revenue"].mean():,.2f}')

# Show top 5 BESS in 2024
print('\n🏆 Top 5 BESS in 2024:')
top_2024 = df_2024.nlargest(5, 'Total_Revenue')
for idx, row in top_2024.iterrows():
    print(f'  {row["BESS_Asset_Name"]:<20} ${row["Total_Revenue"]:>12,.2f}')