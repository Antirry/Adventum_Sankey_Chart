import pandas as pd
import holoviews as hv
from parts.index import render_def
import clickhouse_connect

print('Ожидайте...', sep='\n\n')

def params() -> list[dict]:
    from parts._extract_config import Extract_from_Config
    params = Extract_from_Config('MyConfig.ini', 'Default-Clickhouse').config
    params1 = Extract_from_Config('MyConfig.ini', 'Default-Query').config
    return [params, params1]

params = params()

client = clickhouse_connect.get_client(**params[0])



df = client.query_df(render_def(**params[1]))

print(df)

print('Введите название графика ->', sep='\n')
name = input()
print('Ожидайте...')

hv.extension('bokeh')
sankey = hv.Sankey(df)


sankey.opts(
            label_position='outer',
            edge_color='target',
            edge_line_width=0,
            node_alpha=1.0,
            node_width=40,
            node_sort=True,
            width=1620,
            height=880,
            bgcolor='snow',
            title=f'Санкей диаграмма по таблице Адвентум Ивенты HOLOVIEWS ({name})')

hv.save(sankey, f'{name}.html')
