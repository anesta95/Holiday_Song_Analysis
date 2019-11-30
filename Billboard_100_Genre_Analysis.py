import requests
from bs4 import BeautifulSoup
from datetime import date, timedelta
from time import time
from time import sleep
from random import randint
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import matplotlib.dates as mdates


# creating spotify charts scraper
base_url = 'https://spotifycharts.com/regional/us/daily/'
start = date(2017, 1, 1)
end = date.today()
iter = timedelta(days=1)

start_time = time()
serve = 0

mydate = start
spotifyCharts = pd.DataFrame(columns = ['Rank', 'Artist', 'Title', 'Stream', 'Date'])
while mydate <= end:
    try:
        # combining base_url with the formatted mydate variable to get each iteration of dates for the whole dataset
        r = requests.get(base_url + mydate.strftime('%Y-%m-%d'))
        mydate += iter
        #pause the loop
        sleep(randint(1,3))
        # monitor requests
        serve += 1
        elapsed_time = time() - start_time
        # using bs4 to create variable to clean up site content
        soup = BeautifulSoup(r.text, 'html.parser')
        # establishing where data we are interested starts and ends
        chart = soup.find('table', {'class': 'chart-table'})
        tbody = chart.find('tbody')
        # empty array to be used for holding all variables we are scraping
        all_rows = []
        # Print message for progress
        print('Starting date ' + mydate.strftime('%Y-%m-%d'))
        # actual scraping
        for tr in tbody.find_all('tr'):
            # scrape rank for each chart position
            rank_text = tr.find('td', {'class': 'chart-table-position'}).text.encode('utf-8')
            # scrape artist name for each position and remove "by " so we only get artist name
            artist_text = tr.find('td', {'class': 'chart-table-track'}).find('span').text
            artist_text = artist_text.replace('by ','').strip()
            # scrape title of track for each position
            title_text = tr.find('td', {'class': 'chart-table-track'}).find('strong').text.encode('utf-8')
            # scrape number of streams for each position
            streams_text = tr.find('td', {'class': 'chart-table-streams'}).text.encode('utf-8')
            # do this to get program to start on first date 1/1/2017 instead of 1/2/2017
            date = (mydate - iter)
            # appending all variables we scraped to all_rows empty array and adding date to see exactly at which dates
            # program is failing to update skip variable, also for analysis for later when doing time series and regression
            all_rows.append( [str(rank_text), str(artist_text), str(title_text), str(streams_text), date.strftime('%Y-%m-%d')] )
            
        # create dataframe array to store all data
        chart = pd.DataFrame(all_rows)
        spotifyCharts = pd.concat([spotifyCharts, chart], ignore_index = True)
        
    except:
        pass

# writing csv file to output results
with open('Spotify200Chart.csv', 'a', encoding='utf-8') as f:
    spotifyCharts.to_csv(f, header=False, index=False)

spotifyCharts200 = pd.read_csv('spotify200Chart.csv',  
                                skip_blank_lines=True, 
                                header=None,
                                dtype= {'Rank': 'str', 'Artist': 'str', 'Song': 'str', 'Streams':'str', 'Date': 'str'})


spotifyCharts200 = spotifyCharts200.iloc[:,1:5]
spotifyCharts200.columns = ['Artist', 'Song', 'Streams', 'Date']

spotifyCharts200.iloc[:,1] = spotifyCharts200.iloc[:,1].str.replace(r"'$", '')
spotifyCharts200.iloc[:,1] = spotifyCharts200.iloc[:,1].str.replace("b'", '')

spotifyCharts200.iloc[:,2] = spotifyCharts200.iloc[:,2].str.replace(r"'$", '')
spotifyCharts200.iloc[:,2] = spotifyCharts200.iloc[:,2].str.replace("b'", '')

spotifyCharts200.iloc[:,1] = spotifyCharts200.iloc[:,1].str.replace(r'"$', '')
spotifyCharts200.iloc[:,1] = spotifyCharts200.iloc[:,1].str.replace('b"', '')

spotifyCharts200.iloc[:,2] = spotifyCharts200.iloc[:,2].str.replace(r'"$', '')
spotifyCharts200.iloc[:,2] = spotifyCharts200.iloc[:,2].str.replace('b"', '')


spotifyCharts200.iloc[:,2] = spotifyCharts200.iloc[:,2].str.replace(",", "")

pd.to_numeric(spotifyCharts200['Streams'])
pd.to_datetime(spotifyCharts200['Date'])
spotifyCharts200.set_index("Date")

holidaySongs = spotifyCharts200[(spotifyCharts200.Song == "All I Want for Christmas Is You") | (spotifyCharts200.Song == "Rockin' Around The Christmas Tree") | (spotifyCharts200.Song == "Rockin' Around The Christmas Tree - Single Version") | (spotifyCharts200.Song == "Rockin' Around The Christmas Tree - Recorded at Spotify Studios NYC")].reset_index(drop = True)

with open('holidaySongsSpotifyCharts200.csv', 'a', encoding='utf-8') as f:
    holidaySongs.to_csv(f, header=True, index=False)

billboardHot100 = pd.read_csv("billboardHot100.csv", header=0, dtype={"url": "str", "WeekID": "str", "Week Position": "int", "Song": "str", "Performer": "str", "SongID": "str", "Instance": "Int64", "Previous Week Position": "Int64", "Peak Position": "Int64", "Weeks on Chart": "Int64"}, parse_dates=["WeekID"])
billboardHot100 = billboardHot100.iloc[:,1:10]
billboardHot100 = billboardHot100.sort_values("WeekID").reset_index(drop = True)

holidaySongsBillboard = billboardHot100[(billboardHot100.Song == "All I Want For Christmas Is You") | (billboardHot100.Song == "All I Want For Christmas Is You (SuperFestive!)") | (billboardHot100.Song == "Rockin' Around The Christmas Tree")].sort_values("WeekID").reset_index(drop = True)
holidaySongsBillboard = holidaySongsBillboard[holidaySongsBillboard.Performer != 'Michael Buble'].reset_index(drop = True)

holidaySongsBillboard.iloc[13,2:4] = ["All I Want For Christmas Is You", "Mariah Carey"]

holidaySongsBillboard['Holiday Season'] = holidaySongsBillboard.apply(lambda row: row.WeekID.year if row.WeekID.month > 9 else (row.WeekID.year - 1), axis = 1)
holidaySongsBillboardNew = holidaySongsBillboard[holidaySongsBillboard['Holiday Season'] > 2000].reset_index(drop = True)
holidaySongsBillboardNew['Date'] = holidaySongsBillboardNew.apply(lambda row: row.WeekID.strftime("%m%d"), axis = 1)

holidaySongsBillboardNew['Date'] = pd.to_datetime(holidaySongsBillboardNew['Date'], format='%m%d')
holidaySongsBillboardNew['Date'] = holidaySongsBillboardNew.apply(lambda row: row.Date.replace(row.Date.year + 1) if row.Date.month == 1 else row.Date, axis = 1)

mariahCareyBillboard = holidaySongsBillboardNew[holidaySongsBillboardNew['Performer'] == 'Mariah Carey'].reset_index(drop = True)
mariahCareyBillboard = mariahCareyBillboard.iloc[1:29,:]
brendaLeeBillboard = holidaySongsBillboardNew[holidaySongsBillboardNew['Performer'] == 'Brenda Lee'].reset_index(drop = True)


fig, ax = plt.subplots(figsize=(18, 12))
fig.suptitle('All I Want for Christmas Is You - Billboard Hot 100 Performace', fontsize=20, fontweight='bold')
dates = mdates.date2num(mariahCareyBillboard['Date'])
hfmt = DateFormatter('%m/%d')
ax.xaxis.set_major_formatter(hfmt)
ax.set_ylim(100, 0)
# Remove top and right borders and set title
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.set_xlabel("Date", fontsize=16, fontweight='bold')
ax.set_ylabel("Chart Position", fontsize=16, fontweight='bold')
ax.set_xticklabels(["11/29", "12/1", "12/08", "12/15", "12/22", "12/29", "01/01", "01/08", "01/15"], fontsize=16, fontweight='bold')
ax.set_yticklabels(["0", "20", "40", "60", "80", "100"], fontsize=16, fontweight='bold')
sns.set(rc={"lines.linewidth": 3})
sns.lineplot(x="Date", 
        y="Week Position", 
        hue="Holiday Season",
        palette=sns.color_palette("hls", n_colors=7), 
        data=mariahCareyBillboard)


plt.show()
plt.close()

brendaLeeBillboard['Holiday Season'].nunique()
mdates.date2num(pd.to_datetime("1901-01-04"))

fig, ax = plt.subplots(figsize=(18, 12))
fig.suptitle("Rockin' Around the Christmas Tree - Billboard Hot 100 Performace", fontsize=20, fontweight='bold')
dates = mdates.date2num(brendaLeeBillboard['Date'])
hfmt = DateFormatter('%m/%d')
ax.xaxis.set_major_formatter(hfmt)
ax.set_ylim(100, 0)
# Remove top and right borders and set title
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.set_xlabel("Date", fontsize=16, fontweight='bold')
ax.set_ylabel("Chart Position", fontsize=16, fontweight='bold')
ax.set_xticklabels(["11/29", "12/1", "12/08", "12/15", "12/22", "12/29", "01/01", "01/08", "01/15"], fontsize=16, fontweight='bold')
ax.set_yticklabels(["0", "20", "40", "60", "80", "100"], fontsize=16, fontweight='bold')
ax.text(693964.0, 50, "---", fontsize = 22, color = "#DB5E56", weight = "bold")
sns.set(rc={"lines.linewidth": 3})
sns.lineplot(x="Date", 
        y="Week Position", 
        hue="Holiday Season",
        palette=sns.color_palette("hls", n_colors=5), 
        data=brendaLeeBillboard)


plt.show()
plt.close()


# holidaySongs['Date'].min()
# holidaySongs['Date'].max()
# holidaySongs['Date'].unique()

# holidaySongs.info()
# holidaySongs['Streams'] = pd.to_numeric(holidaySongs['Streams'])
# holidaySongs['Date'] = pd.to_datetime(holidaySongs['Date'])


# holidaySongs = holidaySongs.set_index(pd.DatetimeIndex(holidaySongs['Date']))
# groupedHolidaySeries = holidaySongs.Streams.resample('W').sum()

# groupedHolidaydf = pd.DataFrame(groupedHolidaySeries)
# groupedHolidaydf = groupedHolidaydf[groupedHolidaydf.Streams != 0]

# groupedHolidaydf['Year'] = ['2017', '2017', '2017', '2017', '2017', '2017', '2017', '2017', '2017', '2018', '2018', '2018', '2018', '2018', '2018', '2018', '2018', '2018', '2019', '2019', '2019', '2019']
# groupedHolidaydf['Week'] = ['Nov. Week 1', 'Nov. Week 2', 'Nov. Week 3', 'Nov. Week 4', 'Dec. Week 1', 'Dec. Week 2', 'Dec. Week 3', 'Dec. Week 4', 'Dec. Week 5', 'Nov. Week 1', 'Nov. Week 2', 'Nov. Week 3', 'Nov. Week 4', 'Dec. Week 1', 'Dec. Week 2', 'Dec. Week 3', 'Dec. Week 4', 'Dec. Week 5', 'Nov. Week 1', 'Nov. Week 2', 'Nov. Week 3', 'Nov. Week 4']

# groupedHolidayNPArrays = groupedHolidaydf
# groupedHolidayNPArrays = groupedHolidayNPArrays.reset_index(drop = True)
# groupedHolidayNPArrays = np.asarray([[546307, 1017798, 2240688, 5811927, 8760032, 10278271, 10301468, 14886331, 5393799], [982784, 2421855, 5277108, 7748466, 10934091, 11796559, 11512752, 14000928, 9446967], [846422, 2327214, 4360211, 2614410, np.nan, np.nan, np.nan, np.nan, np.nan]])
# groupedHolidaydf2 = pd.DataFrame(data = {"2017": [546307, 1017798, 2240688, 5811927, 8760032, 10278271, 10301468, 14886331, 5393799], 
#                                          "2018": [982784, 2421855, 5277108, 7748466, 10934091, 11796559, 11512752, 14000928, 9446967], 
#                                          "2019": [846422, 2327214, 4360211, 2614410, np.nan, np.nan, np.nan, np.nan, np.nan]})


mariahCareyHeatmap = holidaySongs[holidaySongs['Artist'] == 'Mariah Carey'].reset_index(drop = True)
brendaLeeHeatmap = holidaySongs[holidaySongs['Artist'] == 'Brenda Lee'].reset_index(drop = True)

mariahCareyHeatmap['Streams'] = pd.to_numeric(mariahCareyHeatmap['Streams'])
mariahCareyHeatmap['Date'] = pd.to_datetime(mariahCareyHeatmap['Date'])
brendaLeeHeatmap['Streams'] = pd.to_numeric(brendaLeeHeatmap['Streams'])
brendaLeeHeatmap['Date'] = pd.to_datetime(brendaLeeHeatmap['Date'])


mariahCareyHeatmap = mariahCareyHeatmap.set_index(pd.DatetimeIndex(mariahCareyHeatmap['Date']))
mariahCareyHeatmap = mariahCareyHeatmap.Streams.resample('W').sum()
mariahCareyHeatmap = mariahCareyHeatmap[mariahCareyHeatmap != 0]

mariahCareyHeatmap2017 = mariahCareyHeatmap[0:9].to_numpy()
mariahCareyHeatmap2018 = mariahCareyHeatmap[9:18].to_numpy()
mariahCareyHeatmap2019 = mariahCareyHeatmap[18:24].to_numpy()

mariahCareyHeatmap2019 = np.concatenate((mariahCareyHeatmap2019, [np.nan, np.nan, np.nan, np.nan, np.nan]))

mariahCareyHeatmap = np.column_stack((mariahCareyHeatmap2017, mariahCareyHeatmap2018, mariahCareyHeatmap2019))

brendaLeeHeatmap = brendaLeeHeatmap.set_index(pd.DatetimeIndex(brendaLeeHeatmap['Date']))
brendaLeeHeatmap = brendaLeeHeatmap.Streams.resample('W').sum()
brendaLeeHeatmap = brendaLeeHeatmap[brendaLeeHeatmap != 0]

brendaLeeHeatmap2017 = brendaLeeHeatmap[0:9].to_numpy()
brendaLeeHeatmap2018 = brendaLeeHeatmap[9:18].to_numpy()
brendaLeeHeatmap2019 = brendaLeeHeatmap[18:24].to_numpy()

brendaLeeHeatmap2019 = np.concatenate((mariahCareyHeatmap2019, [np.nan, np.nan, np.nan, np.nan, np.nan]))

brendaLeeHeatmap = np.column_stack((mariahCareyHeatmap2017, mariahCareyHeatmap2018, mariahCareyHeatmap2019))


fig = plt.subplots(figsize=(18, 12))
sns.set(font_scale=2)
sns.heatmap(mariahCareyHeatmap, linewidths=0.5, cmap="Reds", xticklabels=['2017', '2018', '2019'], yticklabels=['Nov. Week 1', 'Nov. Week 2', 'Nov. Week 3', 'Nov. Week 4', 'Dec. Week 1', 'Dec. Week 2', 'Dec. Week 3', 'Dec. Week 4', 'Dec. Week 5'])
plt.title("All I Want for Christmas Is You Weekly Spotify Streams", fontsize=20, fontweight='bold')
plt.show()
plt.close()

fig = plt.subplots(figsize=(18, 12))
sns.set(font_scale=2)
sns.heatmap(brendaLeeHeatmap, linewidths=0.5, cmap="Greens", xticklabels=['2017', '2018', '2019'], yticklabels=['Nov. Week 1', 'Nov. Week 2', 'Nov. Week 3', 'Nov. Week 4', 'Dec. Week 1', 'Dec. Week 2', 'Dec. Week 3', 'Dec. Week 4', 'Dec. Week 5'])
plt.title("Rockin' Around the Christmas Tree Weekly Spotify Streams", fontsize=20, fontweight='bold')
plt.show()
plt.close()


mariahCareySpotifyMonthly = pd.read_csv("mariahCareySpotifyMonthlyListeners.csv", parse_dates=["DateTime"])
mariahCareySpotifyMonthly = mariahCareySpotifyMonthly.iloc[:,0:2].dropna(how='any', axis=0)
mariahCareySpotifyMonthly = mariahCareySpotifyMonthly[(mariahCareySpotifyMonthly['DateTime'] >= '2018-04-01') & (mariahCareySpotifyMonthly['DateTime'] <= '2019-03-31')].reset_index(drop = True)
mariahCareySpotifyMonthly = mariahCareySpotifyMonthly.groupby(mariahCareySpotifyMonthly.DateTime.dt.month).mean().reset_index()
mariahCareySpotifyMonthly['Artist'] = np.repeat(['Mariah Carey'], ['12'], axis=0)

toniBraxtonSpotifyMonthly = pd.read_csv("toniBraxtonSpotifyMonthlyListeners.csv", parse_dates=["DateTime"])
toniBraxtonSpotifyMonthly = toniBraxtonSpotifyMonthly.iloc[:,0:2].dropna(how='any', axis=0)
toniBraxtonSpotifyMonthly = toniBraxtonSpotifyMonthly[(toniBraxtonSpotifyMonthly['DateTime'] >= '2018-04-01') & (toniBraxtonSpotifyMonthly['DateTime'] <= '2019-03-31')].reset_index(drop = True)
toniBraxtonSpotifyMonthly = toniBraxtonSpotifyMonthly.groupby(toniBraxtonSpotifyMonthly.DateTime.dt.month).mean().reset_index()
toniBraxtonSpotifyMonthly['Artist'] = np.repeat(['Toni Braxton'], ['12'], axis=0)

mariahToni = pd.concat([mariahCareySpotifyMonthly, toniBraxtonSpotifyMonthly], ignore_index=True).sort_values('DateTime').reset_index(drop = True)
mariahToni['DateTime'][0:6] = [13, 13, 14, 14, 15, 15]

mariahToni = mariahToni.sort_values('DateTime').reset_index(drop = True)


fig, ax = plt.subplots(figsize=(18, 12))
fig.suptitle("Mariah Carey vs. Toni Braxton Monthly Average Spotify Listeners", fontsize=22, fontweight='bold')
sns.set(style="whitegrid", font_scale=1.05)
mariahToniGraph = sns.barplot(x='DateTime', y='Monthly Listeners', hue='Artist', data=mariahToni, palette=["#B3000C", "#0000FF"])
ax.set_xticklabels(["Apr. 2018", "May 2018", "Jun. 2018", "Jul. 2018", "Aug 2018", "Sept. 2018", "Oct. 2018", "Nov. 2018", "Dec. 2018", "Jan. 2018", "Feb. 2019", "Mar. 2019"], fontsize=14, fontweight = 'bold')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.set_facecolor('#EAEAF2')
ax.set_yticklabels(['0', '5M', '10M', '15M', '20M', '25M', '30M', '35M'], fontsize=14, fontweight = 'bold')
plt.legend(fontsize = 16)
plt.xlabel('')
plt.ylabel('')
plt.show()
plt.close()

brendaLeeSpotifyMonthly = pd.read_csv("brendaLeeSpotifyMonthlyListeners.csv", parse_dates=["DateTime"])
brendaLeeSpotifyMonthly = brendaLeeSpotifyMonthly.iloc[:,0:2].dropna(how='any', axis=0)
brendaLeeSpotifyMonthly = brendaLeeSpotifyMonthly[(brendaLeeSpotifyMonthly['DateTime'] >= '2018-04-01') & (brendaLeeSpotifyMonthly['DateTime'] <= '2019-03-31')].reset_index(drop = True)
brendaLeeSpotifyMonthly = brendaLeeSpotifyMonthly.groupby(brendaLeeSpotifyMonthly.DateTime.dt.month).mean().reset_index()
brendaLeeSpotifyMonthly['Artist'] = np.repeat(['Brenda Lee'], ['12'], axis=0)

wandaJacksonSpotifyMonthly = pd.read_csv("wandaJacksonSpotifyMonthlyListeners.csv", parse_dates=["DateTime"])
wandaJacksonSpotifyMonthly = wandaJacksonSpotifyMonthly.iloc[:,0:2].dropna(how='any', axis=0)
wandaJacksonSpotifyMonthly = wandaJacksonSpotifyMonthly[(wandaJacksonSpotifyMonthly['DateTime'] >= '2018-04-01') & (wandaJacksonSpotifyMonthly['DateTime'] <= '2019-03-31')].reset_index(drop = True)
wandaJacksonSpotifyMonthly = wandaJacksonSpotifyMonthly.groupby(wandaJacksonSpotifyMonthly.DateTime.dt.month).mean().reset_index()
wandaJacksonSpotifyMonthly['Artist'] = np.repeat(['Wanda Jackson'], ['12'], axis=0)

brendaWanda = pd.concat([brendaLeeSpotifyMonthly, wandaJacksonSpotifyMonthly], ignore_index=True).sort_values('DateTime').reset_index(drop = True)
brendaWanda['DateTime'][0:6] = [13, 13, 14, 14, 15, 15]

brendaWanda = brendaWanda.sort_values('DateTime').reset_index(drop = True)


fig, ax = plt.subplots(figsize=(18, 12))
fig.suptitle("Brenda Lee vs. Wanda Jackson Monthly Average Spotify Listeners", fontsize=20, fontweight='bold')
sns.set(style="whitegrid", font_scale=1.05)
sns.barplot(x='DateTime', y='Monthly Listeners', hue='Artist', data=brendaWanda, palette=["#00B32C", "#FFFF00"])
ax.set_xticklabels(["Apr. 2018", "May 2018", "Jun. 2018", "Jul. 2018", "Aug 2018", "Sept. 2018", "Oct. 2018", "Nov. 2018", "Dec. 2018", "Jan. 2018", "Feb. 2019", "Mar. 2019"], fontsize=14, fontweight = 'bold')
ax.set_yticklabels(['0', '2.5M', '5M', '7.5M', '10M', '12.5M', '15M', '17.5M'], fontsize = 14, fontweight = 'bold')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.set_facecolor('#EAEAF2')
plt.legend(fontsize = 16)
plt.xlabel('')
plt.ylabel('')
plt.show()
plt.close()
