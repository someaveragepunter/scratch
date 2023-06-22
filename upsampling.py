df = df.drop(columns=['longitude', 'latitude'])
df = df.resample('h').mean().interpolate(method='cubic')
df[['ssr', 'ssrd', 'tp']] = df[['ssr', 'ssrd', 'tp']].diff()

# TODO: for ssr and ssrd fillna(0.0) in Europe, but not for tp.
# How to do that for Japan locations for example???
df = df.fillna(0.0)
df[['ssr', 'ssrd', 'tp']] = df[['ssr', 'ssrd', 'tp']].clip(0.0)

df['longitude'] = lon
df['latitude'] = lat

import pandas as pd
import polars as pl
from polars import col as c, DataFrame
from contexttimer import Timer
pk = ['longitude', 'latitude']
metrics = ['ssr', 'ssrd', 'tp']
sortkey = pk + ['flow_date']
df = pl.read_parquet(
    'c:/temp/test.parquet'
)
with Timer() as t:
    df = df[sortkey + metrics].sort(sortkey).set_sorted(sortkey)
print('sort', t)
with Timer() as t:
    dfup = df.upsample('flow_date', every='1h', by=pk).interpolate()
print('upsample', t)

dp = df.to_pandas()
dpup = dfup.to_pandas()

dd = dfup.with_columns(c(x).diff() for x in metrics)

#################### pandas ############################
import pandas as pd
from contexttimer import Timer
metrics = ['ssr', 'ssrd', 'tp']
sortkey = ['longitude', 'latitude', 'flow_date']


with Timer() as t:
    df = pd.read_parquet(
        'c:/temp/test.parquet'
    )
print('read', t)
del df['run_date']
del df['scenario']

with Timer() as t:
    df = df.set_index(sortkey)
    df_interp = df.unstack(level=[0, 1]
    ).sort_index(
    ).resample('h').first(
    ).interpolate(
        # method='cubic'
    )
print('sort, pivot and resample', t)

diffs = ['ssr', 'ssrd', 'tp']
for diff in diffs:
    with Timer() as t:
        df_interp[diff] = df_interp[diff].diff()
    print('diff ', diff, t)

with Timer() as t:
    df_records = df_interp.stack(level=[-2, -1])
print('stack', t)



