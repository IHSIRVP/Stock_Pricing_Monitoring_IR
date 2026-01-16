import pandas as pd

df = pd.read_csv('/root/Stock_Pricing_Monitoring_IR/Combined_Volume_Panel.csv')
df = df.replace('INE298G01027', 'INE298G01035')
print(df)

df.to_csv('/root/Stock_Pricing_Monitoring_IR/Combined_Volume_Panel.csv')
df = pd.read_csv('/root/Stock_Pricing_Monitoring_IR/Combined_Volume_Panel.csv')
print(df)