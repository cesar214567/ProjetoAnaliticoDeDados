from turtle import color
import streamlit as st
import pandas as pd
import altair as alt
from src.spark.spark_session import SparkDF

sdf = SparkDF()

# Volume data  
volume_rollup_df = sdf.get_query_volume_import_roll_up().toPandas()
volume_chart_rollup_df = volume_rollup_df.drop(columns = ['Mês', 'Ano'], inplace = False)
volume_chart_rollup_df['Data'] = volume_rollup_df['Mês'] + ' - ' + volume_rollup_df['Ano'].astype(str)
rollup_1_chart = alt.Chart(volume_chart_rollup_df).mark_line().encode(
  x = alt.X('Data:N', sort = None),
  y = alt.Y('Volume (m3):Q'),
  color = alt.Color('Produto / Combustível:N')
).properties(title = "Gráfico: total de volume de importação de combustível por mês") \
  .interactive()

prices_rollup_df = sdf.get_query_average_prices_roll_up().toPandas()
prices_chart_rollup_df = prices_rollup_df.drop(columns = ['Mês', 'Ano'], inplace = False)
prices_chart_rollup_df['Data'] = prices_rollup_df['Mês'] + ' - ' + prices_rollup_df['Ano'].astype(str)
rollup_2_chart = alt.Chart(prices_chart_rollup_df).mark_line().encode(
  x = alt.X('Data:N', sort = None),
  y = alt.Y('Preços (R$):Q'),
  color = alt.Color('Produto / Combustível:N')
).properties(title = "Gráfico: média dos preços médios dos combustíveis por mês") \
  .interactive()

semester_slice_df = sdf.get_query_imports_semester_slice().toPandas()
semester_chart_slice_df = semester_slice_df.drop(columns = ['Mês', 'Ano'], inplace = False)
semester_chart_slice_df['Data'] = semester_slice_df['Mês'] + ' - ' + semester_slice_df['Ano'].astype(str)
slice_chart = alt.Chart(semester_chart_slice_df).mark_line().encode(
  x = alt.X('Data:N', sort = None),
  y = alt.Y('Valor FOB (R$):Q'),
  color = alt.Color('Produto / Combustível:N')
).properties(title = "Gráfico: custo total, por mês, das importações do primeiro semestre de 2020") \
  .interactive()


imports_pivot_df = sdf.get_query_country_imports_pivot().toPandas()
imports_chart_pivot_df = imports_pivot_df.drop(columns = ['Mês', 'Ano'], inplace = False)
imports_chart_pivot_df['Data'] = imports_pivot_df['Mês'] + ' - ' + imports_pivot_df['Ano'].astype(str)
pivot_chart = alt.Chart(imports_chart_pivot_df).mark_line().encode(
  x = alt.X('Data:N', sort = None),
  y = alt.Y('Valor FOB (R$):Q'),
  color = alt.Color('País:N')
).properties(title = "Gráfico: total de custo das importações por país, por mês") \
  .interactive()

prices_imports_dr_across_df = sdf.get_query_prices_imports_drill_across().toPandas()
prices_imports_dr_chart_across_df = prices_imports_dr_across_df.drop(columns = ['Mês', 'Ano'], inplace = False)
prices_imports_dr_chart_across_df['Data'] = prices_imports_dr_across_df['Mês'] + ' - ' + prices_imports_dr_across_df['Ano'].astype(str)
drill_across_imports_chart = alt.Chart(prices_imports_dr_chart_across_df).mark_line().encode(
  x = alt.X('Data:N', sort = None),
  y = alt.Y('Valor FOB (R$):Q'),
  color = alt.Color('Produto / Combustível:N')
).properties(title = "Gráfico: total de custo das importações por país, por mês") \
  .interactive()

drill_across_prices_chart = alt.Chart(prices_imports_dr_chart_across_df).mark_line().encode(
  x = alt.X('Data:N', sort = None),
  y = alt.Y('Preço Médio (R$):Q'),
  color = alt.Color('Produto / Combustível:N')
).properties(title = "Gráfico: total de custo das importações por país, por mês") \
  .interactive()

'''
# Importações e preços dos combustíveis
---
'''

'''
### **ROLL UP**

Qual é o total de volume de importação de combustível por mês, por combustível?

'''

volume_rollup_df
st.altair_chart(rollup_1_chart, use_container_width=True)

'''
### **ROLL UP**

Qual é a média dos preços médios dos combustíveis por mês, por combustível?
'''

prices_rollup_df
st.altair_chart(rollup_2_chart, use_container_width=True)


'''
### **DICE**

Qual é o custo total, por mês, das importações entre Janeiro e Junho de 2020?
'''

semester_slice_df
st.altair_chart(slice_chart, use_container_width = True)

'''
### **PIVOT**

Qual é o total de custo das importações por país, por mês?
'''

imports_pivot_df
st.altair_chart(pivot_chart, use_container_width = True)

'''
### **DRILL ACROSS**

Como a variação das importações influenciaram no preço médio dos combustíveis, por mês?
'''

prices_imports_dr_across_df
st.altair_chart(drill_across_imports_chart, use_container_width = True)
st.altair_chart(drill_across_prices_chart, use_container_width = True)
