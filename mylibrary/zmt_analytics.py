# 09 Feb 2019 | Zomato Data Analytics

"""Zomato Client
Library that:
 1. From database, retrieves parameters that restrict data fetched from Zomato.com
 2. Fetches data from Zomato.com via Zomato's public APIs
 3. Populates the data into the Zomato datamart

 API Documentation: https://developers.zomato.com/api#headline1
"""

import logging
from mylibrary.db_oracle import OracleClient
from mylibrary.zmt_db_oracle import ZomatoDBSelectOracle
import matplotlib.pyplot as plt, mpld3
import pandas as pd

# Define Oracle Variables
DB = OracleClient()
db_conn = DB.db_login()
db_cur = db_conn.cursor()

ZmtSelect = ZomatoDBSelectOracle()

log = logging.getLogger(__name__)


class ZomatoAnalytics(object):

    def plot_locality_stats(self):
        """Plot Locality Stats"""
        log.debug("plot_locality_stats() | <START>")

        LOC = ['HSR', 'Indiranagar', 'Sarjapur Road']

        # Retrieve Stats from Database
        loc_analytics = ZmtSelect.select_locality_stats()

        # Create DataFrame
        df = pd.DataFrame(loc_analytics, columns=['LOCALITY', 'PERIOD', 'RSTRNT_CNT_OTH', 'RSTRNT_CNT_TOP',
                                                  'RSTRNT_PCT_TOP', 'AVG_COST_FOR_TWO', 'AVG_RTNG_ALL', 'TOP_RTNG_ALL'])

        df = df.astype({"PERIOD": int, "RSTRNT_CNT_OTH": int, "RSTRNT_CNT_TOP": int, "RSTRNT_PCT_TOP": int})

        # Create DataFrame by Locality
        df1 = df[df['LOCALITY'] == 'HSR']
        df2 = df[df['LOCALITY'] == 'Indiranagar']
        df3 = df[df['LOCALITY'] == 'Sarjapur Road']

        df1_all = df1.groupby(['PERIOD'])['RSTRNT_CNT_TOP', 'RSTRNT_CNT_OTH'].mean()
        df2_all = df2.groupby(['PERIOD'])['RSTRNT_CNT_TOP', 'RSTRNT_CNT_OTH'].mean()
        df3_all = df3.groupby(['PERIOD'])['RSTRNT_CNT_TOP', 'RSTRNT_CNT_OTH'].mean()

        # Create Matplotlib Figures & Axes
        fig = plt.figure(figsize=(10, 6.5))
        ax1 = fig.add_axes([0.10, 0.6, 0.2, 0.3])
        ax2 = fig.add_axes([0.35, 0.6, 0.2, 0.3])
        ax3 = fig.add_axes([0.60, 0.6, 0.2, 0.3])
        ax4 = fig.add_axes([0.2, 0.1, 0.2, 0.3])
        ax5 = fig.add_axes([0.5, 0.1, 0.2, 0.3])

        # Plot Restaurant Distribution by Locality
        df1_all.plot(kind='bar', stacked=True, ax=ax1, legend=None, ylim=(0, 120), grid=True, title='HSR')
        df2_all.plot(kind='bar', stacked=True, ax=ax2, legend=None, ylim=(0, 120), grid=True, title='Indiranagar')
        df3_all.plot(kind='bar', stacked=True, ax=ax3, legend=None, ylim=(0, 120), grid=True, title='Sarjapur Road')
        ax3.legend(('Rating >= 4', 'Rating < 4'), loc='center left', bbox_to_anchor=(1, 0.5))
        ax1.set_ylabel('# of Restaurants')
        ax1.set_xlabel('Month of Year')
        ax2.set_xlabel('Month of Year')
        ax3.set_xlabel('Month of Year')

        # Plot Top Rated Restaurants % across all localities
        df1.plot(kind='line', x='PERIOD', y='RSTRNT_PCT_TOP', ax=ax4, legend=None, grid=True, title='Top Rated %')
        df2.plot(kind='line', x='PERIOD', y='RSTRNT_PCT_TOP', ax=ax4, legend=None, grid=True)
        df3.plot(kind='line', x='PERIOD', y='RSTRNT_PCT_TOP', ax=ax4, legend=None, grid=True)

        # Plot Average Restaurant Rating across all localities
        df1.plot(kind='line', x='PERIOD', y='AVG_RTNG_ALL', ax=ax5, legend=None, grid=True, title='Average Rating',
                 label='HSR')
        df2.plot(kind='line', x='PERIOD', y='AVG_RTNG_ALL', ax=ax5, legend=None, grid=True, label='Indiranagar')
        df3.plot(kind='line', x='PERIOD', y='AVG_RTNG_ALL', ax=ax5, legend=None, grid=True, label='Sarjapur Road')
        ax5.legend(loc='center left', bbox_to_anchor=(1, 0.5))

        df_pct = df.groupby(['LOCALITY', 'PERIOD'])['RSTRNT_PCT_TOP'].mean()
        #df_top = df.groupby(['LOCALITY', 'PERIOD'])['RSTRNT_CNT_TOP'].mean()
        #df_all = df.groupby(['LOCALITY', 'PERIOD'])['RSTRNT_CNT_TOP', 'RSTRNT_CNT_OTH'].mean()

        #df_top.unstack().plot.barh()
        #df_all.stack(0).plot.barh()

        #df_pct.plot(kind='line', ax=ax4, legend=None, grid=True, title='Top Rating %')
        #df_all.plot(kind='barh', stacked=True, ax=ax5)
        #color='#FFC222'
        #color='#F78F1E'

        #plt.show()
        plt.savefig('plot.png')
        #fig_html = mpld3.fig_to_html(fig, no_extras=False, template_type='simple')
        #print(fig_html)

        #return fig_html
        log.debug("plot_locality_stats() | <END>")