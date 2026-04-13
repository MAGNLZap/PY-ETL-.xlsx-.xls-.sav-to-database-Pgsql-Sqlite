import pandas as pd
df_in = pd.DataFrame({
    'a': ['010102', '110', '003'],
    'b': [pd.Timestamp('2021-01-01'), pd.Timestamp('2021-01-02'), pd.Timestamp('2021-01-03')],
    'c': [1.1, 2.2, 3.3],
    'd': [1, 2, 3]
})
df_in.to_excel('test.xlsx', index=False)

df_out = pd.read_excel('test.xlsx', dtype=object)
print("dtypes before convert_dtypes:")
print(df_out.dtypes)

df_out2 = df_out.convert_dtypes()
print("\ndtypes after convert_dtypes:")
print(df_out2.dtypes)
print(df_out2)
